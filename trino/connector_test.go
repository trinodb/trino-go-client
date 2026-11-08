package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingTransport struct {
	requests atomic.Int32
}

func (c *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.requests.Add(1)
	return http.DefaultTransport.RoundTrip(req)
}

func TestConnectorUsesHTTPClient(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	transport := &countingTransport{}

	connector, err := NewConnector(&Config{ServerURI: fc.url(), HTTPClient: &http.Client{Transport: transport}})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.EqualValues(t, len(fc.capturedRequests()), transport.requests.Load())
}

func TestNewConnectorValidatesConfig(t *testing.T) {
	t.Parallel()
	_, err := NewConnector(&Config{ServerURI: "http://user:secret@localhost:8080"})
	assert.Error(t, err, "a password needs https, as in a DSN")

	negative := -time.Second
	_, err = NewConnector(&Config{ServerURI: "http://localhost", HeartbeatInterval: &negative})
	assert.ErrorContains(t, err, "heartbeat_interval must be positive")

	zero := time.Duration(0)
	_, err = NewConnector(&Config{ServerURI: "http://localhost", RequestRetryTimeout: &zero})
	assert.ErrorContains(t, err, "request_retry_timeout must be positive")

	noAttempts := 0
	_, err = NewConnector(&Config{ServerURI: "http://localhost", RequestRetryMaxAttempts: &noAttempts})
	assert.ErrorContains(t, err, "request_retry_max_attempts must be a positive integer")

	_, err = NewConnector(&Config{ServerURI: "https://localhost", SSLClientCert: "PEM"})
	assert.ErrorContains(t, err, "a client certificate and its key must be specified together")

	_, err = NewConnector(&Config{ServerURI: "https://localhost", SSLVerification: "bogus"})
	assert.ErrorContains(t, err, "invalid SSL verification mode")
}

func TestNewConnectorLeavesConfigUnchanged(t *testing.T) {
	t.Parallel()
	conf := &Config{ServerURI: "http://localhost", HTTPClient: &http.Client{}}
	_, err := NewConnector(conf)
	require.NoError(t, err)
	assert.Empty(t, conf.Source)
}

func TestFormatDSNRejectsHTTPClient(t *testing.T) {
	t.Parallel()
	_, err := (&Config{ServerURI: "http://localhost", HTTPClient: &http.Client{}}).FormatDSN()
	assert.ErrorContains(t, err, "use NewConnector")
}

func TestHTTPClientConflicts(t *testing.T) {
	t.Parallel()
	for name, conf := range map[string]*Config{
		"custom client": {ServerURI: "https://localhost", CustomClientName: "any"},
		"SSL cert":      {ServerURI: "https://localhost", SSLCert: "PEM"},
		"SSL cert path": {ServerURI: "https://localhost", SSLCertPath: "/cert.pem"},
		"client cert":   {ServerURI: "https://localhost", SSLClientCert: "PEM", SSLClientKey: "PEM"},
		"verification":  {ServerURI: "https://localhost", SSLVerification: SSLVerificationNone},
	} {
		conf.HTTPClient = &http.Client{}
		_, err := NewConnector(conf)
		assert.ErrorContains(t, err, "HTTPClient cannot be combined", name)
	}
}

// An HTTPClient must not follow a redirect, which would send the extra
// credentials to another host, even when its own CheckRedirect would.
func TestConnectorHTTPClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	t.Cleanup(source.Close)

	transport := &countingTransport{}
	follow := func(*http.Request, []*http.Request) error { return nil }
	client := &http.Client{Transport: transport, CheckRedirect: follow}
	connector, err := NewConnector(&Config{
		ServerURI:        source.URL,
		ExtraCredentials: map[string]string{"token": "secret"},
		HTTPClient:       client,
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")
	assert.ErrorContains(t, err, "not followed")
	assert.Zero(t, redirected.Load())
	assert.Positive(t, transport.requests.Load(), "the request goes through HTTPClient")
	assert.Equal(t, reflect.ValueOf(follow).Pointer(), reflect.ValueOf(client.CheckRedirect).Pointer(), "the caller's client is not modified")
}

func TestConnectorConnectHonoursCancelledContext(t *testing.T) {
	t.Parallel()
	connector, err := NewConnector(&Config{ServerURI: "http://localhost"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = connector.Connect(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// clone must copy every map, slice and pointer field, so that a change to the
// caller's Config cannot reach a Connector. Fields tagged dsn:"-" are shared
// on purpose.
func TestConfigCloneCopiesReferenceFields(t *testing.T) {
	t.Parallel()
	conf := &Config{}
	v := reflect.ValueOf(conf).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Map:
			f.Set(reflect.MakeMap(f.Type()))
			f.SetMapIndex(reflect.Zero(f.Type().Key()), reflect.Zero(f.Type().Elem()))
		case reflect.Slice:
			f.Set(reflect.MakeSlice(f.Type(), 1, 1))
		case reflect.Pointer:
			f.Set(reflect.New(f.Type().Elem()))
		}
	}

	cloned := reflect.ValueOf(conf.clone()).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		switch v.Field(i).Kind() {
		case reflect.Map, reflect.Slice, reflect.Pointer:
			if field.Tag.Get("dsn") == "-" {
				continue
			}
			assert.NotEqual(t, v.Field(i).Pointer(), cloned.Field(i).Pointer(), "%s is shared with the clone", field.Name)
		}
	}
}

// Map values are passed as typed, without the DSN's separators.
func TestConnectorKeepsSeparatorsInMapValues(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	conf := &Config{
		ServerURI:         fc.url(),
		SessionProperties: map[string]string{"prop": "a;b"},
		ExtraCredentials:  map[string]string{"a:b": "c"},
	}
	connector, err := NewConnector(conf)
	require.NoError(t, err)
	conf.SessionProperties["prop"] = "changed"

	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	header := fc.capturedRequests()[0].header
	assert.Equal(t, []string{"prop=a%3Bb"}, header.Values(trinoSessionHeader))
	assert.Equal(t, []string{"a:b=c"}, header.Values(trinoExtraCredentialHeader))
}

func TestOpenRejectsInvalidDSN(t *testing.T) {
	t.Parallel()
	_, err := sql.Open("trino", "://")
	assert.Error(t, err)
}

// A registered custom client must not follow a coordinator redirect either:
// the request carries the connection's extra credentials and user.
func TestCustomClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var targetHits atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
	}))
	t.Cleanup(target.Close)
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v1/statement", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(source.Close)
	name := "redirect-test-" + t.Name()
	require.NoError(t, RegisterCustomClient(name, source.Client()))
	t.Cleanup(func() { DeregisterCustomClient(name) })

	db, err := sql.Open("trino", source.URL+"?custom_client="+url.QueryEscape(name)+"&extra_credentials=k%3AEXTRA")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Query("SELECT 1")
	assert.ErrorContains(t, err, "not followed")
	assert.Zero(t, targetHits.Load(), "the redirect target must not receive the statement")
}

func TestConnectorConnectionsShareTransport(t *testing.T) {
	t.Parallel()
	fc, sockets := newSocketCountingTLSCoordinator(t)
	connector, err := NewConnector(&Config{ServerURI: fc.url(), SSLCert: fc.certificatePEM()})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// Every query gets a new connection, which closes when it returns.
	db.SetMaxIdleConns(0)

	const queries = 10
	for range queries {
		querySelectOne(t, fc, db)
	}

	assert.Less(t, sockets.accepted.Load(), int32(queries),
		"connections of one Connector must reuse the sockets of connections closed before them")
}

func TestConnectorConnectionsShareTransportInstance(t *testing.T) {
	t.Parallel()
	connector, err := NewConnector(&Config{ServerURI: "https://localhost:8443", SSLVerification: SSLVerificationNone})
	require.NoError(t, err)

	first, err := connector.Connect(context.Background())
	require.NoError(t, err)
	second, err := connector.Connect(context.Background())
	require.NoError(t, err)

	transport := first.(*Conn).httpClient.Transport
	require.IsType(t, &http.Transport{}, transport)
	assert.Same(t, transport, second.(*Conn).httpClient.Transport)
	assert.Same(t, transport, second.(*Conn).segmentHTTPClient.Transport)
}

func TestClosingDBReleasesIdleSockets(t *testing.T) {
	t.Parallel()
	fc, sockets := newSocketCountingTLSCoordinator(t)
	connector, err := NewConnector(&Config{ServerURI: fc.url(), SSLCert: fc.certificatePEM()})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxIdleConns(0)
	for range 5 {
		querySelectOne(t, fc, db)
	}
	require.NoError(t, db.Close())

	assert.Eventually(t, func() bool { return sockets.open.Load() == 0 }, 5*time.Second, 10*time.Millisecond,
		"closing the database must not keep the sockets of its connections open")
}

func TestDriverOpenConnectionsReleaseTheirIdleSockets(t *testing.T) {
	t.Parallel()
	fc, sockets := newSocketCountingTLSCoordinator(t)
	db := sql.OpenDB(driverOpenConnector{dsn: fc.url() + "?SSLCert=" + url.QueryEscape(fc.certificatePEM())})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxIdleConns(0)
	for range 5 {
		querySelectOne(t, fc, db)
	}

	assert.Eventually(t, func() bool { return sockets.open.Load() == 0 }, 5*time.Second, 10*time.Millisecond,
		"connections closed by the pool must not keep sockets open")
}

func TestConnectorLeavesApplicationTransportsOpen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		open func(t *testing.T, fc *fakeCoordinator, client *http.Client) *sql.DB
	}{
		{
			name: "HTTPClient",
			open: func(t *testing.T, fc *fakeCoordinator, client *http.Client) *sql.DB {
				connector, err := NewConnector(&Config{ServerURI: fc.url(), HTTPClient: client})
				require.NoError(t, err)
				return sql.OpenDB(connector)
			},
		},
		{
			name: "custom client",
			open: func(t *testing.T, fc *fakeCoordinator, client *http.Client) *sql.DB {
				name := "idle-test-" + t.Name()
				require.NoError(t, RegisterCustomClient(name, client))
				t.Cleanup(func() { DeregisterCustomClient(name) })
				db, err := sql.Open("trino", fc.url()+"?custom_client="+url.QueryEscape(name))
				require.NoError(t, err)
				return db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			transport := &idleClosingTransport{RoundTripper: fc.server.Client().Transport}
			db := tc.open(t, fc, &http.Client{Transport: transport})
			db.SetMaxIdleConns(0)
			querySelectOne(t, fc, db)
			require.NoError(t, db.Close())

			assert.Zero(t, transport.closes.Load(), "the application owns its transport")
		})
	}
}

// newSocketCountingTLSCoordinator returns a TLS fake coordinator and the
// counts of the sockets it accepted and still has open.
func newSocketCountingTLSCoordinator(t testing.TB) (*fakeCoordinator, *socketCounts) {
	t.Helper()
	sockets := &socketCounts{}
	fc := startFakeCoordinator(t, func(h http.Handler) *httptest.Server {
		server := httptest.NewUnstartedServer(h)
		server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				sockets.accepted.Add(1)
				sockets.open.Add(1)
			case http.StateClosed, http.StateHijacked:
				sockets.open.Add(-1)
			}
		}
		server.StartTLS()
		return server
	})
	return fc, sockets
}

type socketCounts struct {
	accepted atomic.Int32
	open     atomic.Int32
}

func querySelectOne(t testing.TB, fc *fakeCoordinator, db *sql.DB) {
	t.Helper()
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	var v int
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&v))
}

// driverOpenConnector opens connections with Driver.Open, as database/sql
// does for a driver that does not implement driver.DriverContext.
type driverOpenConnector struct {
	dsn string
}

func (c driverOpenConnector) Connect(context.Context) (driver.Conn, error) {
	return (&Driver{}).Open(c.dsn)
}

func (c driverOpenConnector) Driver() driver.Driver {
	return &Driver{}
}

type idleClosingTransport struct {
	http.RoundTripper
	closes atomic.Int32
}

func (t *idleClosingTransport) CloseIdleConnections() {
	t.closes.Add(1)
}

package trino

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
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
// credentials to another host.
func TestConnectorHTTPClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	t.Cleanup(source.Close)

	connector, err := NewConnector(&Config{
		ServerURI:        source.URL,
		ExtraCredentials: map[string]string{"token": "secret"},
		HTTPClient:       &http.Client{},
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")
	assert.ErrorContains(t, err, "not followed")
	assert.Zero(t, redirected.Load())
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

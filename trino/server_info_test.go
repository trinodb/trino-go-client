package trino

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingFetchesServerInfoAndValidatesCredentials(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusOK, `{"nodeVersion":{"version":"483"},"environment":"test","coordinator":true,"starting":false,"uptime":"6.21m"}`)
	db := fc.open(t, "?catalog=memory")

	require.NoError(t, db.PingContext(context.Background()))

	requests := fc.capturedRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, http.MethodGet, requests[0].method)
	assert.Equal(t, "/v1/info", requests[0].path)
	assert.Equal(t, "memory", requests[0].header.Get(trinoCatalogHeader), "the info request should carry the connection headers")
	assert.Equal(t, http.MethodHead, requests[1].method)
	assert.Equal(t, "/v1/statement", requests[1].path)
	assert.Equal(t, "memory", requests[1].header.Get(trinoCatalogHeader), "the credentials check should carry the connection headers")
}

func TestPingSendsCredentialsToStatementResource(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfoAndCredentialsCheck(fc, http.StatusOK)
	db := fc.open(t, "?accessToken=token")

	require.NoError(t, db.PingContext(context.Background()))

	requests := fc.capturedRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, http.MethodHead, requests[1].method)
	assert.Equal(t, "/v1/statement", requests[1].path)
	assert.Equal(t, "Bearer token", requests[1].header.Get(authorizationHeader))
}

// /v1/info is public, so only the statement resource rejects bad
// credentials.
func TestPingFailsWhenCredentialsAreRejected(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfoAndCredentialsCheck(fc, http.StatusUnauthorized)
	db := fc.open(t, "?accessToken=expired")

	err := db.PingContext(context.Background())

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusUnauthorized, queryFailed.StatusCode)
}

// Servers before Trino 469 have no HEAD /v1/statement.
func TestPingSucceedsWhenServerCannotValidateCredentials(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfoAndCredentialsCheck(fc, http.StatusMethodNotAllowed)
	db := fc.open(t, "")

	require.NoError(t, db.PingContext(context.Background()))
}

func TestPingFailsWhenCredentialsCheckFails(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfoAndCredentialsCheck(fc, http.StatusForbidden)
	db := fc.open(t, "")

	err := db.PingContext(context.Background())

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusForbidden, queryFailed.StatusCode)
}

func TestPingFailsWhileServerStarting(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusOK, `{"nodeVersion":{"version":"483"},"environment":"test","coordinator":true,"starting":true,"uptime":"1.20s"}`)
	db := fc.open(t, "")

	err := db.PingContext(context.Background())

	require.ErrorContains(t, err, "server is still starting")
	requests := fc.capturedRequests()
	require.Len(t, requests, 1, "the credentials check should be skipped")
	assert.Equal(t, "/v1/info", requests[0].path)
}

func TestPingFailsOnServerError(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusInternalServerError, `internal error`)
	db := fc.open(t, "")

	err := db.PingContext(context.Background())

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusInternalServerError, queryFailed.StatusCode)
}

func TestServerInfo(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusOK, `{"nodeId":"node-1","state":"ACTIVE","nodeVersion":{"version":"483-e.1"},"environment":"production","coordinator":true,"coordinatorId":"64z8k","starting":true,"uptime":"3.00d"}`)
	db := fc.open(t, "")

	info, err := rawServerInfo(t, db)
	require.NoError(t, err)

	assert.Equal(t, ServerInfo{
		NodeVersion: "483-e.1",
		Environment: "production",
		Coordinator: true,
		Starting:    true,
		Uptime:      72 * time.Hour,
	}, info)
}

// Servers older than the uptime field omit it.
func TestServerInfoWithoutUptime(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusOK, `{"nodeVersion":{"version":"350"},"environment":"test","coordinator":false,"starting":false}`)
	db := fc.open(t, "")

	info, err := rawServerInfo(t, db)
	require.NoError(t, err)

	assert.Equal(t, ServerInfo{NodeVersion: "350", Environment: "test"}, info)
}

func TestServerInfoRejectsInvalidUptime(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	serveServerInfo(fc, http.StatusOK, `{"nodeVersion":{"version":"483"},"environment":"test","coordinator":true,"starting":false,"uptime":"3.00w"}`)
	db := fc.open(t, "")

	_, err := rawServerInfo(t, db)

	require.ErrorContains(t, err, `unknown time unit in duration "3.00w"`)
}

func TestParseAirliftDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value    string
		expected time.Duration
	}{
		{"1.00ns", time.Nanosecond},
		{"2.50us", 2500 * time.Nanosecond},
		{"12.50ms", 12500 * time.Microsecond},
		{"0.50s", 500 * time.Millisecond},
		{"6.21m", 6*time.Minute + 12600*time.Millisecond},
		{"2.00h", 2 * time.Hour},
		{"3.00d", 72 * time.Hour},
		{"5d", 120 * time.Hour},
		{" 1.5 h ", 90 * time.Minute},
	} {
		t.Run(tc.value, func(t *testing.T) {
			actual, err := parseAirliftDuration(tc.value)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestParseAirliftDurationRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "d", "1.d", "-1.00s", "1.00", "1.00 days"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseAirliftDuration(value)
			assert.Error(t, err)
		})
	}
}

func serveServerInfo(fc *fakeCoordinator, status int, body string) {
	fc.onRequest(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/info" {
			return false
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
		return true
	})
}

// serveServerInfoAndCredentialsCheck serves a started server's /v1/info and
// answers HEAD /v1/statement with headStatus.
func serveServerInfoAndCredentialsCheck(fc *fakeCoordinator, headStatus int) {
	fc.onRequest(func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/info":
			_, _ = w.Write([]byte(`{"nodeVersion":{"version":"483"},"environment":"test","coordinator":true,"starting":false}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/statement":
			w.WriteHeader(headStatus)
		default:
			return false
		}
		return true
	})
}

// rawServerInfo reads the server info the way a database/sql user does.
func rawServerInfo(t *testing.T, db *sql.DB) (ServerInfo, error) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	var info ServerInfo
	err = conn.Raw(func(driverConn any) error {
		var err error
		info, err = driverConn.(*Conn).ServerInfo(context.Background())
		return err
	})
	return info, err
}

package trino

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripRetryQueryError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status int
		// the first response closes the connection, so the retry cannot
		// reuse it and must send the whole request again
		closeConnection bool
		wantErr         string
	}{
		{name: "retry 502 Bad Gateway", status: http.StatusBadGateway, wantErr: "200 OK"},
		{name: "retry 503 Service Unavailable", status: http.StatusServiceUnavailable, wantErr: "200 OK"},
		{name: "retry 504 Gateway Timeout", status: http.StatusGatewayTimeout, wantErr: "200 OK"},
		{name: "retry 503 on a fresh connection", status: http.StatusServiceUnavailable, closeConnection: true, wantErr: "200 OK"},
		{name: "no retry 404 Not Found", status: http.StatusNotFound, wantErr: "404 Not Found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					if tc.closeConnection {
						w.Header().Set("Connection", "close")
					}
					w.WriteHeader(tc.status)
					return
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(&stmtResponse{
					Error: ErrTrino{
						ErrorName: "TEST",
					},
				})
			}))

			t.Cleanup(ts.Close)

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)

			t.Cleanup(func() {
				assert.NoError(t, db.Close())
			})

			_, err = db.Query("SELECT 1")
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestRoundTripRefusesRedirects(t *testing.T) {
	t.Parallel()
	var redirectedRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{})
	}))
	t.Cleanup(target.Close)
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+"/v1/statement", status)
			}))
			t.Cleanup(ts.Close)
			db, err := sql.Open("trino", ts.URL+"?extra_credentials=token%3Asecret")
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, db.Close()) })

			_, err = db.Query("SELECT 1")
			var queryFailed *ErrQueryFailed
			require.ErrorAs(t, err, &queryFailed)
			assert.Equal(t, status, queryFailed.StatusCode)
			assert.ErrorContains(t, err, "redirect to "+target.URL+"/v1/statement not followed")
			assert.Zero(t, redirectedRequests.Load(), "the redirect target should not receive the statement")
		})
	}
	assert.Nil(t, http.DefaultClient.CheckRedirect, "the shared default client must not be modified")
}

func TestRoundTripBogusData(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		// some invalid JSON
		w.Write([]byte(`{"stats": {"progressPercentage": ""}}`))
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestRoundTripCancellation(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)

	_, err = db.QueryContext(ctx, "SELECT 1")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTokenAuth(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(resultPage([][]any{{1}}))
	db := fc.open(t, "?accessToken=token")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "Bearer token", requests[0].header.Get("Authorization"), "Authorization header")
}

func TestRoleHeader(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		roles         map[string]string
		namedArgRoles map[string]string
		wantHeader    string
	}{
		{
			name:       "roles from config",
			roles:      map[string]string{"catalog1": "role1", "catalog2": "role2"},
			wantHeader: `catalog1=ROLE{role1},catalog2=ROLE{role2}`,
		},
		{
			name:          "override dsn roles with named argument",
			roles:         map[string]string{"catalog1": "role1"},
			namedArgRoles: map[string]string{"catalog3": "role3", "catalog4": "role4", "catalog5": "ALL"},
			wantHeader:    `catalog3=ROLE{role3},catalog4=ROLE{role4},catalog5=ALL`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(resultPage([][]any{{1}}))
			dsn, err := (&Config{ServerURI: fc.url(), Roles: tc.roles}).FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)

			var args []any
			if tc.namedArgRoles != nil {
				args = append(args, sql.Named("X-Trino-Role", tc.namedArgRoles))
			}
			rows, err := db.QueryContext(ctx, "SELECT 1", args...)
			require.NoError(t, err)
			require.NoError(t, rows.Close())

			requests := fc.capturedRequests()
			require.Len(t, requests, 1)
			assert.Equal(t, tc.wantHeader, requests[0].header.Get(trinoRoleHeader), "X-Trino-Role header")
		})
	}
}

func TestQueryFailure(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusInternalServerError, queryFailed.StatusCode)
}

func TestForwardAuthorizationHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(resultPage([][]any{{1}}))
	db := fc.open(t, "?forwardAuthorizationHeader=true")

	rows, err := db.Query("SELECT 1", sql.Named("accessToken", "token"))
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "Bearer token", requests[0].header.Get("Authorization"), "Authorization header")
}

func TestForwardAuthorizationHeaderDisabled(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT ?", sql.Named("accessToken", "token"))

	assert.ErrorIs(t, err, ErrForwardAuthorizationHeaderNotEnabled)
	assert.Empty(t, fc.capturedRequests(), "the access token must never reach the server")
}

func TestForwardAuthorizationHeaderNonStringToken(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?forwardAuthorizationHeader=true")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT ?", sql.Named("accessToken", 42))
	assert.EqualError(t, err, "trino: accessToken must be a string, got int64")
}

func TestQueryTimeoutDeadline(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		queryTimeout string
		hang         bool
		wantErr      string
	}{
		{name: "with timeout", queryTimeout: "10ms", hang: true, wantErr: "context deadline exceeded"},
		{name: "without timeout", queryTimeout: "10s", wantErr: "EOF"}, // the empty response
		{name: "bad timeout", queryTimeout: "abc", wantErr: "trino: invalid timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testDone := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.hang {
					// answer only once the driver has given up
					<-testDone
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(ts.Close)
			// registered after the server's cleanup, so it runs first and
			// the parked handler cannot block the server from closing
			t.Cleanup(func() { close(testDone) })
			db, err := sql.Open("trino", ts.URL+"?query_timeout="+tc.queryTimeout)
			if err == nil {
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				_, err = db.Query("SELECT 1")
			}
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestFormatRoles(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		roles map[string]string
		want  string
	}{
		{name: "named role", roles: map[string]string{"hive": "admin"}, want: "hive=ROLE{admin}"},
		{name: "all", roles: map[string]string{"hive": "ALL"}, want: "hive=ALL"},
		{name: "none", roles: map[string]string{"hive": "NONE"}, want: "hive=NONE"},
		{name: "sorted by catalog", roles: map[string]string{"tpch": "NONE", "hive": "admin", "memory": "ALL"}, want: "hive=ROLE{admin},memory=ALL,tpch=NONE"},
		{name: "empty", roles: map[string]string{}, want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatRolesFromMap(tc.roles))
		})
	}
}

func TestNamedRoleArgumentMustBeMap(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1", sql.Named(trinoRoleHeader, "admin"))

	require.EqualError(t, err, "X-Trino-Role must be a map[string]string, got string")
	assert.Empty(t, fc.capturedRequests(), "the query must be rejected before anything is sent")
}

func TestQueryFailedWrapsTrinoError(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(pageOf(&stmtResponse{
		ID: fakeQueryID,
		Error: ErrTrino{
			Message:   "line 1:8: mismatched input 'FORM'",
			ErrorCode: 1,
			ErrorName: "SYNTAX_ERROR",
			ErrorType: "USER_ERROR",
		},
	}))
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1 FORM dual")

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusOK, queryFailed.StatusCode)
	var trinoErr *ErrTrino
	require.ErrorAs(t, err, &trinoErr)
	assert.Equal(t, "SYNTAX_ERROR", trinoErr.ErrorName)
	assert.EqualError(t, err, `trino: query failed (200 OK): "USER_ERROR: line 1:8: mismatched input 'FORM'"`)
}

func TestQueryFailedTruncatesLongResponseBody(t *testing.T) {
	t.Parallel()
	const limit = 8 * 1024
	body := strings.Repeat("x", 2*limit)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, body[:limit]+"...", queryFailed.Reason.Error())
}

package trino

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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
		// failures is how many times status is served before the 200; it
		// defaults to 1 when status is set.
		failures int
		// the first response closes the connection, so the retry cannot
		// reuse it and must send the whole request again
		closeConnection bool
		wantErr         string
	}{
		{name: "retry 502 Bad Gateway", status: http.StatusBadGateway, wantErr: "200 OK"},
		{name: "retry 503 Service Unavailable", status: http.StatusServiceUnavailable, wantErr: "200 OK"},
		{name: "retry 504 Gateway Timeout", status: http.StatusGatewayTimeout, wantErr: "200 OK"},
		{name: "retry 503 on a fresh connection", status: http.StatusServiceUnavailable, closeConnection: true, wantErr: "200 OK"},
		{name: "retry 503 three times", status: http.StatusServiceUnavailable, failures: 3, wantErr: "200 OK"},
		{name: "no retry 404 Not Found", status: http.StatusNotFound, wantErr: "404 Not Found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failures := tc.failures
			if failures == 0 {
				failures = 1
			}
			var requests atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if int(requests.Add(1)) <= failures {
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
			if tc.failures > 1 {
				assert.Equal(t, int32(failures+1), requests.Load(), "must retry exactly failures times before the 200")
			}
		})
	}
}

// TestRoundTripRetryNextURIGet covers the GET side of the retry loop — the
// existing table above only exercises the statement POST — with several
// consecutive 503s on the nextUri GET before it succeeds.
func TestRoundTripRetryNextURIGet(t *testing.T) {
	t.Parallel()
	const failures = 3
	var getRequests atomic.Int32
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&stmtResponse{ID: "q", NextURI: ts.URL + "/next"})
			return
		}
		if int(getRequests.Add(1)) <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&queryResponse{})
	}))
	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())
	assert.Equal(t, int32(failures+1), getRequests.Load(), "the nextUri GET must be retried until it succeeds")
}

// A permanently failing request gives up once request_retry_timeout elapses.
func TestRoundTripRequestRetryTimeoutBudget(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?request_retry_timeout=200ms")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	start := time.Now()
	_, err = db.Query("SELECT 1")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, time.Second, "must give up close to the configured budget")
	n := requests.Load()
	assert.Greater(t, n, int32(1), "must have retried at least once")
	assert.ErrorContains(t, err, fmt.Sprintf("%d attempts", n))
	assert.ErrorContains(t, err, "request_retry_timeout")
}

// A permanently failing request gives up after request_retry_max_attempts,
// even with time left.
func TestRoundTripRequestRetryMaxAttempts(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?request_retry_max_attempts=3&request_retry_timeout=1h")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")
	require.Error(t, err)
	assert.EqualValues(t, 3, requests.Load())
	assert.ErrorContains(t, err, "3 attempts")
	assert.ErrorContains(t, err, "request_retry_max_attempts=3")
}

// TestRoundTripCancelDuringBackoffReturnsPromptly cancels the context while
// the retry loop is sleeping between attempts (rather than at a deadline, as
// TestRoundTripCancellation does), and checks that the cancellation is
// noticed immediately instead of waiting out the backoff.
func TestRoundTripCancelDuringBackoffReturnsPromptly(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err = db.QueryContext(ctx, "SELECT 1")
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 500*time.Millisecond, "cancellation during backoff must return promptly, not wait out the current sleep")
}

// roundTripperFunc adapts a function to http.RoundTripper, so a test can
// hand db.Query a real *http.Client — going through Client.Do exactly like
// production code, including its *url.Error wrapping — without opening a
// real socket.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// postConnectResetError is a connection reset after the request was sent. It
// is synthesized because a real reset races net/http's connection reuse and
// makes the test flaky.
func postConnectResetError() error {
	return &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}
}

func jsonResponse(t testing.TB, req *http.Request, v any) *http.Response {
	t.Helper()
	body, err := json.Marshal(v)
	require.NoError(t, err)
	return &http.Response{
		Request:    req,
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

// A reset after the request was sent is retried only for idempotent requests.
func TestRoundTripTwoTierNetworkErrorPredicate(t *testing.T) {
	t.Parallel()

	t.Run("POST is not retried after a post-connect reset", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, postConnectResetError()
		})}
		require.NoError(t, RegisterCustomClient("post-not-retried-after-reset", client))

		db, err := sql.Open("trino", "http://example.invalid?custom_client=post-not-retried-after-reset")
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, db.Close()) })

		_, err = db.Query("SELECT 1")
		require.Error(t, err)
		assert.EqualValues(t, 1, calls.Load(), "the statement POST must not be retried after a post-connect reset")
	})

	t.Run("nextUri GET is retried after a post-connect reset", func(t *testing.T) {
		t.Parallel()
		const failures = 2
		var getCalls atomic.Int32
		client := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost {
				return jsonResponse(t, req, &stmtResponse{ID: "q", NextURI: "http://example.invalid/next"}), nil
			}
			if getCalls.Add(1) <= failures {
				return nil, postConnectResetError()
			}
			return jsonResponse(t, req, &queryResponse{}), nil
		})}
		require.NoError(t, RegisterCustomClient("get-retried-after-reset", client))

		db, err := sql.Open("trino", "http://example.invalid?custom_client=get-retried-after-reset")
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, db.Close()) })

		rows, err := db.Query("SELECT 1")
		require.NoError(t, err)
		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
		assert.EqualValues(t, failures+1, getCalls.Load(), "the GET must be retried until it succeeds")
	})
}

func TestNetworkErrorPolicies(t *testing.T) {
	t.Parallel()
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	readErr := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	cases := []struct {
		name              string
		err               error
		wantDialPhaseOnly bool
		wantTransient     bool
	}{
		{name: "dial error", err: dialErr, wantDialPhaseOnly: true, wantTransient: true},
		{name: "connection reset", err: fmt.Errorf("wrapped: %w", syscall.ECONNRESET), wantTransient: true},
		{name: "EOF", err: io.EOF, wantTransient: true},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, wantTransient: true},
		{name: "timeout", err: fmt.Errorf("wrapped: %w", timeoutError{}), wantTransient: true},
		{name: "post-connect read error, not a timeout", err: readErr, wantTransient: false},
		{name: "context canceled", err: context.Canceled, wantTransient: false},
		// A per-attempt http.Client.Timeout surfaces as DeadlineExceeded and
		// is retried; roundTrip checks the caller's ctx before the policy.
		{name: "context deadline exceeded", err: context.DeadlineExceeded, wantTransient: true},
		{name: "unrelated error", err: errors.New("boom"), wantTransient: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantDialPhaseOnly, dialPhaseOnly(tc.err), "dialPhaseOnly")
			assert.Equal(t, tc.wantTransient, transientNetworkError(tc.err), "transientNetworkError")
		})
	}
}

// timeoutError is a minimal net.Error whose Timeout() is true, standing in
// for the error http.Client's own Timeout field produces.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

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
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			_, err = db.Query("SELECT 1")
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

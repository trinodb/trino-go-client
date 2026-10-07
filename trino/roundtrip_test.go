package trino

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		{name: "retry 429 Too Many Requests", status: http.StatusTooManyRequests, wantErr: "200 OK"},
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
				json.MarshalWrite(w, &stmtResponse{
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
			json.MarshalWrite(w, &stmtResponse{ID: "q", NextURI: ts.URL + "/next"})
			return
		}
		if int(getRequests.Add(1)) <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.MarshalWrite(w, &queryResponse{})
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

// throttle answers the first `times` requests that match with status and,
// when retryAfter is not empty, a Retry-After header, and counts the
// requests that match.
func throttle(fc *fakeCoordinator, match func(*http.Request) bool, times int32, status int, retryAfter string) *atomic.Int32 {
	var matched atomic.Int32
	fc.onRequest(func(w http.ResponseWriter, r *http.Request) bool {
		if !match(r) || matched.Add(1) > times {
			return false
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		return true
	})
	return &matched
}

func isMethod(method string) func(*http.Request) bool {
	return func(r *http.Request) bool { return r.Method == method }
}

func TestRoundTripRetryTooManyRequestsNextURIGet(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	gets := throttle(fc, isMethod(http.MethodGet), 2, http.StatusTooManyRequests, "")
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	assert.EqualValues(t, 3, gets.Load(), "the nextUri GET must be retried until it succeeds")
}

// The default backoff starts at 100ms, so a wait of a second or more can only
// come from the Retry-After header.
func TestRoundTripHonorsRetryAfter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		status     int
		retryAfter func() string
		wantWait   time.Duration
	}{
		{
			name:       "429 with delta-seconds",
			status:     http.StatusTooManyRequests,
			retryAfter: func() string { return "1" },
			wantWait:   time.Second,
		},
		{
			name:   "429 with HTTP-date",
			status: http.StatusTooManyRequests,
			// the date has a one second resolution, so the wait is between
			// one and two seconds
			retryAfter: func() string { return time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat) },
			wantWait:   time.Second,
		},
		{
			name:       "503 with delta-seconds",
			status:     http.StatusServiceUnavailable,
			retryAfter: func() string { return "1" },
			wantWait:   time.Second,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			posts := throttle(fc, isMethod(http.MethodPost), 1, tc.status, tc.retryAfter())
			db := fc.open(t, "")

			start := time.Now()
			rows, err := db.Query("SELECT 1")
			elapsed := time.Since(start)

			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
			require.NoError(t, rows.Err())
			assert.EqualValues(t, 2, posts.Load())
			assert.GreaterOrEqual(t, elapsed, tc.wantWait, "the retry must wait as long as Retry-After asks")
		})
	}
}

func TestRoundTripMalformedRetryAfterFallsBackToBackoff(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	posts := throttle(fc, isMethod(http.MethodPost), 1, http.StatusTooManyRequests, "soon")
	db := fc.open(t, "")

	start := time.Now()
	rows, err := db.Query("SELECT 1")
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	assert.EqualValues(t, 2, posts.Load())
	assert.Less(t, elapsed, time.Second, "a malformed Retry-After must not delay the retry beyond the backoff")
}

// A Retry-After beyond request_retry_timeout is cut short, so the query fails
// when the budget runs out instead of waiting as long as the server asks.
func TestRoundTripRetryAfterLongerThanBudget(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	posts := throttle(fc, isMethod(http.MethodPost), math.MaxInt32, http.StatusTooManyRequests, "3600")
	db := fc.open(t, "?request_retry_timeout=300ms")

	start := time.Now()
	_, err := db.Query("SELECT 1")
	elapsed := time.Since(start)

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusTooManyRequests, queryFailed.StatusCode)
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second, "the wait must be capped at the remaining request_retry_timeout")
	assert.EqualValues(t, 2, posts.Load(), "one retry once the budget is spent")
	assert.ErrorContains(t, err, "429 Too Many Requests")
	assert.ErrorContains(t, err, "giving up after 2 attempts")
}

func TestRoundTripTooManyRequestsExhaustsMaxAttempts(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	posts := throttle(fc, isMethod(http.MethodPost), math.MaxInt32, http.StatusTooManyRequests, "0")
	db := fc.open(t, "?request_retry_max_attempts=3&request_retry_timeout=1h")

	_, err := db.Query("SELECT 1")

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusTooManyRequests, queryFailed.StatusCode)
	assert.EqualValues(t, 3, posts.Load())
	assert.ErrorContains(t, err, `trino: query failed (429 Too Many Requests): "giving up after 3 attempts`)
}

func TestRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		status   int
		header   string
		wantWait time.Duration
		wantOK   bool
	}{
		{name: "delta-seconds", status: http.StatusTooManyRequests, header: "120", wantWait: 2 * time.Minute, wantOK: true},
		{name: "zero seconds", status: http.StatusTooManyRequests, header: "0", wantWait: 0, wantOK: true},
		{name: "surrounding whitespace", status: http.StatusTooManyRequests, header: " 5 ", wantWait: 5 * time.Second, wantOK: true},
		{name: "seconds overflowing a duration", status: http.StatusTooManyRequests, header: "9223372036854775807", wantWait: time.Duration(math.MaxInt64), wantOK: true},
		{name: "IMF-fixdate", status: http.StatusTooManyRequests, header: "Sun, 04 Oct 2026 12:00:30 GMT", wantWait: 30 * time.Second, wantOK: true},
		{name: "obsolete RFC 850 date", status: http.StatusTooManyRequests, header: "Sunday, 04-Oct-26 12:01:00 GMT", wantWait: time.Minute, wantOK: true},
		{name: "date in the past", status: http.StatusTooManyRequests, header: "Sun, 04 Oct 2026 11:00:00 GMT", wantWait: 0, wantOK: true},
		{name: "503 with Retry-After", status: http.StatusServiceUnavailable, header: "7", wantWait: 7 * time.Second, wantOK: true},
		{name: "missing", status: http.StatusTooManyRequests},
		{name: "negative seconds", status: http.StatusTooManyRequests, header: "-1"},
		{name: "fractional seconds", status: http.StatusTooManyRequests, header: "1.5"},
		{name: "garbage", status: http.StatusTooManyRequests, header: "soon"},
		{name: "ignored on 502", status: http.StatusBadGateway, header: "7"},
		{name: "ignored on 504", status: http.StatusGatewayTimeout, header: "7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: tc.status, Header: make(http.Header)}
			if tc.header != "" {
				resp.Header.Set("Retry-After", tc.header)
			}
			wait, ok := retryAfter(resp, now)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantWait, wait)
		})
	}
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
		json.MarshalWrite(w, &stmtResponse{})
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
			wantHeader: `catalog1=ROLE%7Brole1%7D,catalog2=ROLE%7Brole2%7D`,
		},
		{
			name:          "override dsn roles with named argument",
			roles:         map[string]string{"catalog1": "role1"},
			namedArgRoles: map[string]string{"catalog3": "role3", "catalog4": "role4", "catalog5": "ALL"},
			wantHeader:    `catalog3=ROLE%7Brole3%7D,catalog4=ROLE%7Brole4%7D,catalog5=ALL`,
		},
		{
			name:       "role name with separators from config",
			roles:      map[string]string{"hive": "admin},system=ROLE{admin"},
			wantHeader: `hive=ROLE%7Badmin%7D%2Csystem%3DROLE%7Badmin%7D`,
		},
		{
			name:          "role name with separators from named argument",
			namedArgRoles: map[string]string{"hive": "admin},system=ROLE{admin"},
			wantHeader:    `hive=ROLE%7Badmin%7D%2Csystem%3DROLE%7Badmin%7D`,
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
		{name: "named role", roles: map[string]string{"hive": "admin"}, want: "hive=ROLE%7Badmin%7D"},
		{name: "all", roles: map[string]string{"hive": "ALL"}, want: "hive=ALL"},
		{name: "none", roles: map[string]string{"hive": "NONE"}, want: "hive=NONE"},
		{name: "sorted by catalog", roles: map[string]string{"tpch": "NONE", "hive": "admin", "memory": "ALL"}, want: "hive=ROLE%7Badmin%7D,memory=ALL,tpch=NONE"},
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

func TestNamedRoleArgumentRejectsInvalidCatalog(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1", sql.Named(trinoRoleHeader, map[string]string{"hive=ALL,system": "admin"}))

	require.EqualError(t, err, `trino: X-Trino-Role key "hive=ALL,system" must not contain '='`)
	assert.Empty(t, fc.capturedRequests(), "the query must be rejected before anything is sent")
}

// Only names are restricted; values are URL-encoded, so the separators the
// server splits on are safe in them.
func TestHeaderValuesKeepSeparators(t *testing.T) {
	t.Parallel()
	const value = "a=b,c%d&e+f;g:h"
	const encodedValue = "a%3Db%2Cc%25d%26e%2Bf%3Bg%3Ah"
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	connector, err := NewConnector(&Config{
		ServerURI:         fc.url(),
		SessionProperties: map[string]string{"hive.max_split_size": value},
		ExtraCredentials:  map[string]string{"aws:access_key": value},
		Roles:             map[string]string{"hive": "admin", "system": "ALL"},
		ClientTags:        []string{"tag1", "tag=2"},
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	header := fc.capturedRequests()[0].header
	assert.Equal(t, []string{"hive.max_split_size=" + encodedValue}, header.Values(trinoSessionHeader))
	assert.Equal(t, []string{"aws:access_key=" + encodedValue}, header.Values(trinoExtraCredentialHeader))
	assert.Equal(t, []string{"hive=ROLE%7Badmin%7D,system=ALL"}, header.Values(trinoRoleHeader))
	assert.Equal(t, []string{"tag1,tag=2"}, header.Values(trinoTagsHeader))
}

func TestDSNHeaderValuesKeepSeparators(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "?"+url.Values{"session_properties": {"query_max_run_time:a=b,c"}}.Encode())

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	assert.Equal(t, []string{"query_max_run_time=a%3Db%2Cc"}, fc.capturedRequests()[0].header.Values(trinoSessionHeader))
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

package trino

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripRetryQueryError(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr string
	}{
		{name: "retry 502 Bad Gateway", status: http.StatusBadGateway, wantErr: "200 OK"},
		{name: "retry 503 Service Unavailable", status: http.StatusServiceUnavailable, wantErr: "200 OK"},
		{name: "retry 504 Gateway Timeout", status: http.StatusGatewayTimeout, wantErr: "200 OK"},
		{name: "no retry 404 Not Found", status: http.StatusNotFound, wantErr: "404 Not Found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			count := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if count == 0 {
					count++
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
			assert.ErrorContains(t, err, tc.wantErr, "unexpected error: %w", err)
		})
	}
}

func TestRoundTripBogusData(t *testing.T) {
	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			count++
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
	assert.Error(t, err, "unexpected query with cancelled context succeeded")
}

func TestAuthFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	assert.NoError(t, db.Close())
}

func TestTokenAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			w.WriteHeader(http.StatusUnauthorized)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?accessToken=token")
	require.NoError(t, err)

	_, err = db.Query("SELECT 1")
	require.Error(t, err, "trino: EOF")

	assert.NoError(t, db.Close())
}

func TestRoleHeader(t *testing.T) {
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
			var receivedHeader string
			var serverURL string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeader = r.Header.Get(trinoRoleHeader)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":"1","nextUri":"` + serverURL + `/1"}`))
			}))
			serverURL = ts.URL
			t.Cleanup(ts.Close)

			c := &Config{
				ServerURI: ts.URL,
				Roles:     tc.roles,
			}

			dsn, err := c.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)

			if tc.namedArgRoles != nil {
				_, _ = db.Query("SELECT 1", sql.Named("X-Trino-Role", tc.namedArgRoles))
			} else {
				_, _ = db.Query("SELECT 1")
			}

			assert.Equal(t, tc.wantHeader, receivedHeader, "expected X-Trino-Role header to match")
		})
	}
}

func TestQueryFailure(t *testing.T) {
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
	assert.IsTypef(t, new(ErrQueryFailed), err, "unexpected error: %w", err)
}

func TestForwardAuthorizationHeader(t *testing.T) {
	var captureAuthHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture the Authorization header for later inspection
		captureAuthHeader = r.Header.Get("Authorization")
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?forwardAuthorizationHeader=true")
	require.NoError(t, err)

	_, _ = db.Query("SELECT 1", sql.Named("accessToken", string("token"))) // Ingore response to focus on header capture
	require.Equal(t, "Bearer token", captureAuthHeader, "Authorization header is incorrect")

	assert.NoError(t, db.Close())
}

func TestForwardAuthorizationHeaderDisabled(t *testing.T) {
	var capturedQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		capturedQuery = string(body)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT ?", sql.Named("accessToken", "token"))
	assert.ErrorIs(t, err, ErrForwardAuthorizationHeaderNotEnabled)
	assert.NotContains(t, capturedQuery, "token", "the access token must never reach the query text")
}

func TestForwardAuthorizationHeaderNonStringToken(t *testing.T) {
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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond) // Simulate slow response
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	cases := []struct {
		name         string
		queryTimeout string
		wantErr      string
	}{
		{name: "with timeout", queryTimeout: "100ms", wantErr: "context deadline exceeded"},
		{name: "without timeout", queryTimeout: "10s", wantErr: "EOF"}, // the empty response
		{name: "bad timeout", queryTimeout: "abc", wantErr: "trino: invalid timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			println(ts.URL + "?query_timeout=" + tc.queryTimeout)
			db, err := sql.Open("trino", ts.URL+"?query_timeout="+tc.queryTimeout)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			_, err = db.Query("SELECT 1")
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

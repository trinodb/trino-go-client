package trino

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: ErrTrino{
				ErrorName: "USER_CANCELLED",
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
	assert.EqualError(t, err, ErrQueryCancelled.Error(), "unexpected error")
}

// This test ensures that the fetch method is not generating stack overflow errors.
// === RUN   TestFetchNoStackOverflow
// runtime: goroutine stack exceeds 1000000000-byte limit
// runtime: sp=0x14037b00390 stack=[0x14037b00000, 0x14057b00000]
// fatal error: stack overflow
func TestFetchNoStackOverflow(t *testing.T) {
	previousSetting := debug.SetMaxStack(50 * 1024)
	defer debug.SetMaxStack(previousSetting)
	count := 0
	var buf *bytes.Buffer
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count <= 50 {
			if buf == nil {
				buf = new(bytes.Buffer)
				json.NewEncoder(buf).Encode(&stmtResponse{
					ID:      "fake-query",
					NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
				})
			}
			w.WriteHeader(http.StatusOK)
			w.Write(buf.Bytes())
			count++
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: ErrTrino{
				ErrorName: "TEST",
			},
		})
	}))

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.IsTypef(t, new(ErrQueryFailed), err, "unexpected error: %w", err)

}

func TestProtocolErrorHandling(t *testing.T) {
	testcases := []struct {
		name          string
		data          interface{}
		expectedError string
	}{
		{
			name: "DirectProtocolInvalidRowType",
			data: []interface{}{
				123,
			},
			expectedError: "unexpected data type for row at index 0: expected []interface{}, got json.Number",
		},
		{
			name: "SpoolingProtocolMissingEncoding",
			data: map[string]interface{}{
				"segments": []interface{}{}, // Missing "encoding" field
			},
			expectedError: "invalid or missing 'encoding' field on spooling protocol, expected string",
		},
		{
			name: "SpoolingProtocolInvalidSegmentsType",
			data: map[string]interface{}{
				"encoding": "json",
				"segments": "invalid", // Invalid type for "segments"
			},
			expectedError: "nvalid or missing 'segments' field on spooling protocol, expected []interface{}",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var ts *httptest.Server

			ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/statement" {
					json.NewEncoder(w).Encode(&stmtResponse{
						ID:      "fake-query",
						NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
					})

					return
				}
				if r.URL.Path == "/v1/statement/20210817_140827_00000_arvdv/1" {
					json.NewEncoder(w).Encode(&queryResponse{
						ID: "fake-query",
						Columns: []queryColumn{
							{
								Name: "_col0",
								Type: "integer",
								TypeSignature: typeSignature{
									RawType:   "integer",
									Arguments: []typeArgument{},
								},
							},
						},
						Data: tc.data,
					})
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
			}))

			defer ts.Close()

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Query("SELECT 1")
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestSetRoleHeader(t *testing.T) {
	var firstRoleHeader string
	var secondRoleHeader string
	var requestCount int
	var baseURL string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		roleHeader := r.Header.Get(trinoRoleHeader)

		if r.URL.Path == "/v1/statement" {
			// Capture the initial role from DSN
			firstRoleHeader = roleHeader
			w.Header().Set(trinoSetRoleHeader, "ROLE%7Badmin%7D")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&stmtResponse{
				ID:      "query1",
				NextURI: baseURL + "/v1/statement/query1/1",
				Stats: stmtStats{
					State: "RUNNING",
				},
			})
		} else if r.URL.Path == "/v1/statement/query1/1" {
			// Capture the role in subsequent request(e.g after server set)
			secondRoleHeader = roleHeader
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&queryResponse{
				ID: "query1",
				Stats: stmtStats{
					State: "FINISHED",
				},
				Data: [][]interface{}{{1}},
				Columns: []queryColumn{
					{
						Name: "_col0",
						Type: "integer",
						TypeSignature: typeSignature{
							RawType:   "integer",
							Arguments: []typeArgument{},
						},
					},
				},
			})
		} else if r.Method == "DELETE" && r.URL.Path == "/v1/query/query1" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&queryResponse{
				ID: "query1",
				Stats: stmtStats{
					State: "FINISHED",
				},
			})
		}
	}))
	baseURL = ts.URL

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL+"?roles=catalog%3Auser")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	assert.Equal(t, `catalog=ROLE{user}`, firstRoleHeader, "initial role from DSN should be sent in first request")
	assert.Equal(t, "ROLE%7Badmin%7D", secondRoleHeader, "server-set role should be sent in subsequent requests")
	assert.NotEqual(t, firstRoleHeader, secondRoleHeader, "role should have changed from DSN value to server-set value")
}

func TestUnsupportedHeader(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(trinoSetPathHeader, "foo.bar")
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.EqualError(t, err, ErrUnsupportedHeader.Error(), "unexpected error")
}

func TestUnsupportedTransaction(t *testing.T) {
	db, err := sql.Open("trino", "http://localhost:9")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Begin()
	require.Error(t, err, "unsupported transaction succeeded with no error")

	expected := "operation not supported"
	assert.Contains(t, err.Error(), expected)
}

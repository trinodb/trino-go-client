package trino

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpoolingProtocolSpooledSegmentDecoders(t *testing.T) {
	testcases := []struct {
		Name           string
		Segments       []map[string]interface{}
		ExpectedResult []int
		Encoding       string
		DownloadedData []byte
	}{
		{
			Name: "noCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"segmentSize": 16, "rowOffset": 0, "rowsCount": 2},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			Encoding:       "json",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: []byte("[[1000],[10001]]"),
		},
		{
			Name: "zstdCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			Name: "spooledSegmentWithoutHeadersOnReponse", // headers are optional
			Segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29},
					"ackUri":   "test",
				},
			},
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			Name: "zlibCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			Encoding:       "json+lz4",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("8AFbWzEwMDBdLFsxMDAwMV1d"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
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
						Data: map[string]interface{}{
							"encoding": tc.Encoding,
							"segments": tc.Segments,
						},
					})
					return
				}
				if r.URL.Path == "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=" {
					w.Write(tc.DownloadedData)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
			}))

			defer ts.Close()

			tc.Segments[0]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc="

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			var results []int
			for rows.Next() {
				var value int
				err := rows.Scan(&value)
				require.NoError(t, err)
				results = append(results, value)
			}

			require.NoError(t, rows.Err())

			assert.Equal(t, tc.ExpectedResult, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolToManyOutOfOrderSegmentDownload(t *testing.T) {
	segments := []map[string]interface{}{
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 30, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 20, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 40, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
	}

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/statement":
			json.NewEncoder(w).Encode(&stmtResponse{
				ID:      "fake-query",
				NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
			})
			return

		case "/v1/statement/20210817_140827_00000_arvdv/1":
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
				Data: map[string]interface{}{
					"encoding": "json",
					"segments": segments,
				},
			})
			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=":
			w.Write([]byte("[[1000]]"))
			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc1=":
			w.Write([]byte("[[1001]]"))

			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc2=":
			w.Write([]byte("[[1002]]"))

			return

		default:
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
		}
	}))
	defer ts.Close()

	// Inject segment URIs into the segment definitions
	segments[0]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc="
	segments[1]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc1="
	segments[2]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc2="

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1", sql.Named(trinoMaxOutOfOrdersSegments, "3"), sql.Named(trinoSpoolingWorkerCount, "2"))
	require.NoError(t, err)

	for rows.Next() {
		var value int
		err := rows.Scan(&value)
		require.NoError(t, err)
	}

	require.Error(t, rows.Err())

	require.ErrorContains(t, rows.Err(), "all 3 out-of-order segments buffered (limit: 3). This indicates a bug or inconsistency in the segments metadata response (e.g., missing, duplicate, or misordered segments, or row offsets not matching the expected sequence)")
}

func TestSpoolingProtocolOutOfOrderSegment(t *testing.T) {
	// Define the segments
	segments := []map[string]interface{}{
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 2, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 1, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
		{
			"type":     "spooled",
			"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1},
			"ackUri":   "test",
			"headers": map[string]interface{}{
				"test": []interface{}{"test"},
			},
		},
	}

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/statement":
			json.NewEncoder(w).Encode(&stmtResponse{
				ID:      "fake-query",
				NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
			})
			return

		case "/v1/statement/20210817_140827_00000_arvdv/1":
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
				Data: map[string]interface{}{
					"encoding": "json",
					"segments": segments,
				},
			})
			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=":
			w.Write([]byte("[[1000]]"))
			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc1=":
			w.Write([]byte("[[1001]]"))

			return

		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc2=":
			w.Write([]byte("[[1002]]"))

			return

		default:
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
		}
	}))
	defer ts.Close()

	// Inject segment URIs into the segment definitions
	segments[2]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc="
	segments[1]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc1="
	segments[0]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc2="

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1", sql.Named(trinoMaxOutOfOrdersSegments, "3"), sql.Named(trinoSpoolingWorkerCount, "1"))
	require.NoError(t, err)

	var results []int
	for rows.Next() {
		var value int
		err := rows.Scan(&value)
		require.NoError(t, err)
		results = append(results, value)
	}

	require.NoError(t, rows.Err())

	expected := []int{1000, 1001, 1002}
	assert.Equal(t, expected, results, "Expected query results to match")
}

func TestSpoolingProtocolSegmentDownloadRetryFails(t *testing.T) {
	testcases := []struct {
		Name              string
		ExpectedErrorMsg  string
		SimulateTimeout   bool
		HttpStatusReponse int
	}{
		{
			Name:              "Test retry 502 Bad Gateway",
			HttpStatusReponse: http.StatusBadGateway,
		},
		{
			Name:              "Test retry 503 Service Unavailable",
			HttpStatusReponse: http.StatusServiceUnavailable,
		},
		{
			Name:              "Test retry 504 Gateway Timeout",
			HttpStatusReponse: http.StatusGatewayTimeout,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			var ts *httptest.Server
			var failCounter int
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
						Data: map[string]interface{}{
							"encoding": "json",
							"segments": []map[string]interface{}{
								{
									"uri":      ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=",
									"type":     "spooled",
									"metadata": map[string]interface{}{"segmentSize": 325, "rowOffset": 0, "rowsCount": 1},
									"ackUri":   "test",
									"headers": map[string]interface{}{
										"test": []interface{}{"test"},
									},
								},
							},
						},
					})
					return
				}
				if r.URL.Path == "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=" {
					if failCounter < 2 {
						failCounter++
						w.WriteHeader(tc.HttpStatusReponse)
						return
					}
					w.WriteHeader(http.StatusOK)
					w.Write([]byte("[[1000]]"))
				}
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
			}))
			defer ts.Close()

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			var results []int
			for rows.Next() {
				var value int
				err := rows.Scan(&value)
				require.NoError(t, err)
				results = append(results, value)
			}

			require.NoError(t, rows.Err())

			assert.Equal(t, []int{1000}, results, "Expected query results to match")
			assert.Equal(t, 2, failCounter, "Expected segment download to fail exactly 2 times before succeeding")
		})
	}
}

func TestSpoolingProtocolSegmentDownloadRetryMaxAttempts(t *testing.T) {
	var ts *httptest.Server
	failCounter := 0
	maxRetries := 6

	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/statement":
			json.NewEncoder(w).Encode(&stmtResponse{
				ID:      "fake-query",
				NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
			})
		case "/v1/statement/20210817_140827_00000_arvdv/1":
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
				Data: map[string]interface{}{
					"encoding": "json",
					"segments": []map[string]interface{}{
						{
							"uri":      ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=",
							"type":     "spooled",
							"metadata": map[string]interface{}{"segmentSize": 325, "rowOffset": 0, "rowsCount": 1},
							"ackUri":   "test",
							"headers": map[string]interface{}{
								"test": []interface{}{"test"},
							},
						},
					},
				},
			})
		case "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=":
			if failCounter <= maxRetries {
				failCounter++
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		default:
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
		}
	}))
	defer ts.Close()

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	for rows.Next() {
	}

	require.Error(t, rows.Err())

	require.ErrorContains(t, rows.Err(), "max retries reached for status code 502")
	assert.Equal(t, maxRetries, failCounter, "Expected segment download to fail exactly 5 times before succeeding")
}

func mustDecodeBase64(encoded string) []byte {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode base64: %v", err))
	}
	return data
}

func TestSpoolingProtocolOnlyWithInlineSegments(t *testing.T) {
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
				Data: map[string]interface{}{
					"encoding": "json",
					"segments": []map[string]interface{}{
						{
							"type":     "inline",
							"data":     "W1sxMDAwXSwgWzEwMDAxXV0=",
							"metadata": map[string]interface{}{"segmentSize": 17, "rowOffset": 0},
						},
						{
							"type":     "inline",
							"data":     "W1sxMDAwXSwgWzEwMDAxXV0=",
							"metadata": map[string]interface{}{"segmentSize": 17, "rowOffset": 2},
						},
						{
							"type":     "inline",
							"data":     "W1sxMDAwXSwgWzEwMDAxXV0=",
							"metadata": map[string]interface{}{"segmentSize": 17, "rowOffset": 4},
						},
						{
							"type":     "inline",
							"data":     "W1sxMDAwXSwgWzEwMDAxXV0=",
							"metadata": map[string]interface{}{"segmentSize": 17, "rowOffset": 6},
						},
					},
				},
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

	rows, err := db.Query("SELECT 1", sql.Named(trinoSpoolingWorkerCount, "2"), sql.Named(trinoMaxOutOfOrdersSegments, "2"))
	require.NoError(t, err)

	var results []int
	for rows.Next() {
		var value int
		err := rows.Scan(&value)
		require.NoError(t, err)
		results = append(results, value)
	}

	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1000, 10001, 1000, 10001, 1000, 10001, 1000, 10001}, results, "Expected query results to match")
}

func TestSpoolingProtocolInlineSegmentDecoders(t *testing.T) {
	testcases := []struct {
		Name           string
		Segments       []map[string]interface{}
		ExpectedResult []int
		Encoding       string
	}{
		{
			Name: "noCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "inline",
					"data":     "W1sxMDAwXSwgWzEwMDAxXV0=",
					"metadata": map[string]interface{}{"segmentSize": 17, "rowOffset": 0},
				},
			},
			Encoding:       "json",
			ExpectedResult: []int{1000, 10001},
		},
		{
			Name: "zstdCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "inline",
					"data":     "KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29},
				},
			},
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
		},
		{
			Name: "zlibCompression",
			Segments: []map[string]interface{}{
				{
					"type":     "inline",
					"data":     "8AFbWzEwMDBdLFsxMDAwMV1d",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18},
				},
			},
			Encoding:       "json+lz4",
			ExpectedResult: []int{1000, 10001},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
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
						Data: map[string]interface{}{
							"encoding": tc.Encoding,
							"segments": tc.Segments,
						},
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

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			var results []int
			for rows.Next() {
				var value int
				err := rows.Scan(&value)
				require.NoError(t, err)
				results = append(results, value)
			}

			require.NoError(t, rows.Err())

			assert.Equal(t, tc.ExpectedResult, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolSpooledSegmentErrorHandling(t *testing.T) {
	testcases := []struct {
		name                          string
		segments                      []map[string]interface{}
		expectedError                 string
		downloadedData                []byte
		downloadedDataStatusCodeError bool
	}{
		{
			name: "MissingRowOffsetMetadata",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "segmentSize": 11},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "rowOffset is missing in segment metadata",
		},
		{
			name: "WrongRowOffsetMetadataType",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": "2", "segmentSize": 11},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "invalid type for rowOffset in segment metadata, expected json.Number",
		},
		{
			name: "MissingSegmentSizeMetadata",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": 2},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "segmentSize is missing in segment metadata",
		},
		{
			name: "WrongSegmentSizeMetadataType",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": "11"},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "invalid type for segmentSize in segment metadata, expected json.Number",
		},
		{
			name: "MissingMetadata",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"ackUri": "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "metadata is missing in segment at index 0",
		},
		{
			name: "WrongMetadataType",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": "fake-metadata",
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError: "metadata is invalid or cannot be parsed as map[string]interface{} in segment at index 0",
		},
		{
			name: "WrongUncompressSize",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError:  "failed to decode spooled segment at index 0: segment size mismatch: expected 11 bytes, got 29 byte",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name: "WrongCompresSize",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 29},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError:  "decompressed size mismatch: expected 2 bytes, got 16 bytes",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name: "MissingUri",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"data":   "fake-data",
					"ackUri": "test",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": map[string][]interface{}{
						"x-amz-server-side-encryption-customer-algorithm": {"AES256"},
						"x-amz-server-side-encryption-customer-key":       {"key"},
						"x-amz-server-side-encryption-customer-key-md5":   {"md5"},
					},
				},
			},
			expectedError: "missing or invalid 'uri' field in spooled segment at index 0",
		},
		{
			name: "MissingUriAck",
			segments: []map[string]interface{}{
				{
					"type": "spooled",
					"data": "fake-data",
					"uri":  "fake-uri",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": map[string][]interface{}{
						"x-amz-server-side-encryption-customer-algorithm": {"AES256"},
						"x-amz-server-side-encryption-customer-key":       {"key"},
						"x-amz-server-side-encryption-customer-key-md5":   {"md5"},
					},
				},
			},
			expectedError: "missing or invalid 'ackUri' field in spooled segment at index 0",
		},
		{
			name: "wrongHeadersFormat",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"data":   "fake-data",
					"uri":    "fake-uri",
					"ackUri": "test",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": [][]string{
						{"x-amz-server-side-encryption-customer-algorithm", "AES256"},
						{"x-amz-server-side-encryption-customer-key", "key"},
					},
				},
			},
			expectedError: "invalid 'headers' field in spooled segment at index 0: expected map[string]interface{}",
		},
		{
			name: "HeadersWithMultipleValues",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"data":   "fake-data",
					"uri":    "fake-uri",
					"ackUri": "test",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": map[string][]interface{}{
						"x-amz-server-side-encryption-customer-algorithm": {"AES256"},
						"x-amz-server-side-encryption-customer-key":       {"key"},
						"x-amz-server-side-encryption-customer-key-md5":   {"md5", "md5"}, // wrong, more then one
					},
				},
			},
			expectedError: "multiple values for header x-amz-server-side-encryption-customer-key-md5",
		},
		{
			name: "HeaderValueWrongType",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"data":   "fake-data",
					"uri":    "fake-uri",
					"ackUri": "test",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": map[string]interface{}{
						"x-amz-server-side-encryption-customer-algorithm": []interface{}{"AES256"},
						"x-amz-server-side-encryption-customer-key":       []interface{}{"key"},
						"x-amz-server-side-encryption-customer-key-md5":   []interface{}{123}, // Wrong type: integer instead of string
					},
				},
			},
			expectedError: "unsupported header value type json.Number",
		},
		{
			name: "HeaderTypeInvalid",
			segments: []map[string]interface{}{
				{
					"type":   "spooled",
					"data":   "fake-data",
					"uri":    "fake-uri",
					"ackUri": "test",
					"metadata": map[string]interface{}{
						"segmentSize":      3679,
						"uncompressedSize": 2,
						"rowOffset":        0,
					},
					"headers": map[string]interface{}{
						"x-amz-server-side-encryption-customer-algorithm": "AES256", // Invalid type: string instead of []interface{}
					},
				},
			},
			expectedError: "unsupported header type string",
		},
		{
			name: "ErrorDownloadingSegment",
			segments: []map[string]interface{}{
				{
					"type":     "spooled",
					"metadata": map[string]interface{}{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11},
					"ackUri":   "test",
					"headers": map[string]interface{}{
						"test": []interface{}{"test"},
					},
				},
			},
			expectedError:                 "trino: query failed (500 Internal Server Error):",
			downloadedData:                mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
			downloadedDataStatusCodeError: true,
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
						Data: map[string]interface{}{
							"encoding": "json+zstd",
							"segments": tc.segments,
						},
					})
					return
				}

				if r.URL.Path == "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc=" {
					if tc.downloadedDataStatusCodeError {
						w.WriteHeader(http.StatusInternalServerError)
					}

					w.Write(tc.downloadedData)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
			}))

			defer ts.Close()

			if tc.name != "MissingUri" {
				tc.segments[0]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc="
			}

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.Query("SELECT 1")

			require.NoError(t, err)
			defer rows.Close()

			for rows.Next() {
				// force segment processing
			}

			err = rows.Err()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestSpoolingProtocolInlineSegmentErrorHandling(t *testing.T) {
	testcases := []struct {
		name          string
		segments      []map[string]interface{}
		expectedError string
	}{
		{
			name: "WrongUncompressSize",
			segments: []map[string]interface{}{
				{
					"type":     "inline",
					"data":     "KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=",
					"metadata": map[string]interface{}{"uncompressedSize": 1, "rowOffset": 2, "segmentSize": 29},
				},
			},
			expectedError: "failed to decode spooled segment at index 0: decompressed size mismatch: expected 1 bytes, got 16 bytes",
		},
		{
			name: "WrongCompresSize",
			segments: []map[string]interface{}{
				{
					"type":     "inline",
					"data":     "KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=",
					"metadata": map[string]interface{}{"uncompressedSize": 16, "rowOffset": 2, "segmentSize": 1},
				},
			},
			expectedError: "failed to decode spooled segment at index 0: segment size mismatch: expected 1 bytes, got 29 bytes",
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
						Data: map[string]interface{}{
							"encoding": "json+zstd",
							"segments": tc.segments,
						},
					})
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
			}))

			defer ts.Close()

			if tc.name != "MissingUri" {
				tc.segments[0]["uri"] = ts.URL + "/v1/spooled/download/jKaLK0aVkNp2ixl6BOuwGMJ0nRjbUVKLHW_f3-I-1Cc="
			}

			db, err := sql.Open("trino", ts.URL)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			for rows.Next() {
				// force segment processing
			}

			err = rows.Err()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func newSpooledSegmentServer(t *testing.T, headHandler func(w http.ResponseWriter, r *http.Request), downloadDelay time.Duration) *httptest.Server {
	t.Helper()
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			if headHandler != nil {
				headHandler(w, r)
			} else {
				w.WriteHeader(http.StatusOK)
			}
			return
		}
		// every response updates the connection headers, so that the race
		// detector sees them being read concurrently by the heartbeat
		switch r.URL.Path {
		case "/v1/statement":
			w.Header().Set(trinoSetSessionHeader, "query_max_run_time=10m")
			json.NewEncoder(w).Encode(&stmtResponse{
				ID:      "fake-query",
				NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
			})
		case "/v1/statement/20210817_140827_00000_arvdv/1":
			w.Header().Set(trinoSetCatalogHeader, "memory")
			json.NewEncoder(w).Encode(&queryResponse{
				ID:      "fake-query",
				NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/2",
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
				Data: map[string]interface{}{
					"encoding": "json",
					"segments": []map[string]interface{}{
						{
							"uri":      ts.URL + "/v1/spooled/download/seg0",
							"type":     "spooled",
							"metadata": map[string]interface{}{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1},
							"ackUri":   ts.URL + "/v1/spooled/ack/seg0",
							"headers":  map[string]interface{}{},
						},
					},
				},
			})
		case "/v1/statement/20210817_140827_00000_arvdv/2":
			w.Header().Set(trinoSetSessionHeader, "query_max_run_time=20m")
			json.NewEncoder(w).Encode(&queryResponse{})
		case "/v1/spooled/download/seg0":
			if downloadDelay > 0 {
				time.Sleep(downloadDelay)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[[1000]]"))
		case "/v1/spooled/ack/seg0":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrTrino{ErrorName: "Unexpected request"})
		}
	}))
	return ts
}

func TestHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name string
		// status returned by the n-th heartbeat; the last one repeats
		statuses      []int
		stopAfter     int32
		atLeast       int32
		downloadDelay time.Duration
	}{
		{
			name:          "sent while a segment is downloading",
			statuses:      []int{http.StatusOK},
			atLeast:       1,
			downloadDelay: 500 * time.Millisecond,
		},
		{
			name:          "disabled when the coordinator rejects HEAD",
			statuses:      []int{http.StatusMethodNotAllowed},
			stopAfter:     1,
			downloadDelay: 800 * time.Millisecond,
		},
		{
			name:          "disabled when the coordinator does not implement heartbeats",
			statuses:      []int{http.StatusNotImplemented},
			stopAfter:     1,
			downloadDelay: 800 * time.Millisecond,
		},
		{
			name:          "disabled after consecutive failures",
			statuses:      []int{http.StatusInternalServerError},
			stopAfter:     maxHeartbeatFailures,
			downloadDelay: 800 * time.Millisecond,
		},
		{
			// a 404 is transient: it's also returned by a coordinator that
			// doesn't know the query, e.g. behind a load balancer
			name:          "retried when the coordinator does not know the query",
			statuses:      []int{http.StatusNotFound, http.StatusNotFound, http.StatusOK},
			atLeast:       maxHeartbeatFailures,
			downloadDelay: 1500 * time.Millisecond,
		},
		{
			name: "failure count reset by a successful heartbeat",
			statuses: []int{
				http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK,
				http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK,
			},
			atLeast:       2 * maxHeartbeatFailures,
			downloadDelay: 1500 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			ts := newSpooledSegmentServer(t, func(w http.ResponseWriter, r *http.Request) {
				attempt := int(attempts.Add(1))
				w.WriteHeader(tc.statuses[min(attempt, len(tc.statuses))-1])
			}, tc.downloadDelay)
			defer ts.Close()

			db, err := sql.Open("trino", ts.URL+"?heartbeat_interval=100ms")
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			defer rows.Close()

			var results []int
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				results = append(results, value)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, []int{1000}, results)

			if tc.stopAfter > 0 {
				assert.Equal(t, tc.stopAfter, attempts.Load(), "Expected the heartbeat to be disabled")
				return
			}
			assert.GreaterOrEqual(t, attempts.Load(), tc.atLeast, "Expected the heartbeat to keep running")
		})
	}
}

func TestHeartbeatDoesNotDelayClose(t *testing.T) {
	heartbeatSent := make(chan struct{}, 1)
	unblockHeartbeat := make(chan struct{})
	ts := newSpooledSegmentServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case heartbeatSent <- struct{}{}:
		default:
		}
		<-unblockHeartbeat
		w.WriteHeader(http.StatusOK)
	}, 0)
	defer ts.Close()
	defer close(unblockHeartbeat)

	db, err := sql.Open("trino", ts.URL+"?heartbeat_interval=50ms")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	require.True(t, rows.Next())
	select {
	case <-heartbeatSent:
	case <-time.NewTimer(5 * time.Second).C:
		require.Fail(t, "Expected a heartbeat to reach the unresponsive coordinator")
	}

	start := time.Now()
	require.NoError(t, rows.Close())
	assert.Less(t, time.Since(start), time.Second, "Close should cancel the in-flight heartbeat instead of waiting for its timeout")
}

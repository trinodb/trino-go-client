package trino

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpoolingProtocolSpooledSegmentDecoders(t *testing.T) {
	testcases := []struct {
		Name           string
		Segment        map[string]any
		ExpectedResult []int
		Encoding       string
		DownloadedData []byte
	}{
		{
			Name:           "noCompression",
			Segment:        spooledSegment("seg", map[string]any{"segmentSize": 16, "rowOffset": 0, "rowsCount": 2}),
			Encoding:       "json",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: []byte("[[1000],[10001]]"),
		},
		{
			Name:           "zstdCompression",
			Segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}),
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			Name:           "spooledSegmentWithoutHeadersOnReponse", // headers are optional
			Segment:        withoutField(spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}), "headers"),
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			Name:           "zlibCompression",
			Segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18}),
			Encoding:       "json+lz4",
			ExpectedResult: []int{1000, 10001},
			DownloadedData: mustDecodeBase64("8AFbWzEwMDBdLFsxMDAwMV1d"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage(tc.Encoding, tc.Segment))
			fc.serveSegment("seg", tc.DownloadedData)
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())

			assert.Equal(t, tc.ExpectedResult, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolToManyOutOfOrderSegmentDownload(t *testing.T) {
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 30, "rowsCount": 1}),
		spooledSegment("seg1", map[string]any{"segmentSize": 8, "rowOffset": 20, "rowsCount": 1}),
		spooledSegment("seg2", map[string]any{"segmentSize": 8, "rowOffset": 40, "rowsCount": 1}),
	))
	fc.serveSegment("seg0", []byte("[[1000]]"))
	fc.serveSegment("seg1", []byte("[[1001]]"))
	fc.serveSegment("seg2", []byte("[[1002]]"))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1", sql.Named(trinoMaxOutOfOrdersSegments, "3"), sql.Named(trinoSpoolingWorkerCount, "2"))
	require.NoError(t, err)

	collectInts(t, rows)

	require.Error(t, rows.Err())

	require.ErrorContains(t, rows.Err(), "all 3 out-of-order segments buffered (limit: 3). This indicates a bug or inconsistency in the segments metadata response (e.g., missing, duplicate, or misordered segments, or row offsets not matching the expected sequence)")
}

func TestSpoolingProtocolOutOfOrderSegment(t *testing.T) {
	fc := newFakeCoordinator(t)
	// the segments are listed in reverse row order
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg2", map[string]any{"segmentSize": 8, "rowOffset": 2, "rowsCount": 1}),
		spooledSegment("seg1", map[string]any{"segmentSize": 8, "rowOffset": 1, "rowsCount": 1}),
		spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.serveSegment("seg0", []byte("[[1000]]"))
	fc.serveSegment("seg1", []byte("[[1001]]"))
	fc.serveSegment("seg2", []byte("[[1002]]"))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1", sql.Named(trinoMaxOutOfOrdersSegments, "3"), sql.Named(trinoSpoolingWorkerCount, "1"))
	require.NoError(t, err)

	results := collectInts(t, rows)
	require.NoError(t, rows.Err())

	expected := []int{1000, 1001, 1002}
	assert.Equal(t, expected, results, "Expected query results to match")
}

func TestSpoolingProtocolSegmentDownloadRetryFails(t *testing.T) {
	testcases := []struct {
		Name              string
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
			var failCounter atomic.Int32
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
			))
			fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
				if failCounter.Load() < 2 {
					failCounter.Add(1)
					w.WriteHeader(tc.HttpStatusReponse)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("[[1000]]"))
			})
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())

			assert.Equal(t, []int{1000}, results, "Expected query results to match")
			assert.Equal(t, int32(2), failCounter.Load(), "Expected segment download to fail exactly 2 times before succeeding")
		})
	}
}

func TestSpoolingProtocolSegmentDownloadRetryMaxAttempts(t *testing.T) {
	var failCounter atomic.Int32
	maxRetries := int32(6)

	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		if failCounter.Load() <= maxRetries {
			failCounter.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
	})
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	collectInts(t, rows)

	require.Error(t, rows.Err())

	require.ErrorContains(t, rows.Err(), "max retries reached for status code 502")
	assert.Equal(t, maxRetries, failCounter.Load(), "Expected segment download to fail exactly 5 times before succeeding")
}

func mustDecodeBase64(encoded string) []byte {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode base64: %v", err))
	}
	return data
}

func TestSpoolingProtocolOnlyWithInlineSegments(t *testing.T) {
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 0}),
		inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 2}),
		inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 4}),
		inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 6}),
	))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1", sql.Named(trinoSpoolingWorkerCount, "2"), sql.Named(trinoMaxOutOfOrdersSegments, "2"))
	require.NoError(t, err)

	results := collectInts(t, rows)
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1000, 10001, 1000, 10001, 1000, 10001, 1000, 10001}, results, "Expected query results to match")
}

func TestSpoolingProtocolInlineSegmentDecoders(t *testing.T) {
	testcases := []struct {
		Name           string
		Segment        map[string]any
		ExpectedResult []int
		Encoding       string
	}{
		{
			Name:           "noCompression",
			Segment:        inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 0}),
			Encoding:       "json",
			ExpectedResult: []int{1000, 10001},
		},
		{
			Name:           "zstdCompression",
			Segment:        inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}),
			Encoding:       "json+zstd",
			ExpectedResult: []int{1000, 10001},
		},
		{
			Name:           "zlibCompression",
			Segment:        inlineSegment("8AFbWzEwMDBdLFsxMDAwMV1d", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18}),
			Encoding:       "json+lz4",
			ExpectedResult: []int{1000, 10001},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage(tc.Encoding, tc.Segment))
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())

			assert.Equal(t, tc.ExpectedResult, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolSpooledSegmentErrorHandling(t *testing.T) {
	validMetadata := map[string]any{"segmentSize": 3679, "uncompressedSize": 2, "rowOffset": 0}
	testcases := []struct {
		name                          string
		segment                       map[string]any
		expectedError                 string
		downloadedData                []byte
		downloadedDataStatusCodeError bool
	}{
		{
			name:          "MissingRowOffsetMetadata",
			segment:       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "segmentSize": 11}),
			expectedError: "rowOffset is missing in segment metadata",
		},
		{
			name:          "WrongRowOffsetMetadataType",
			segment:       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": "2", "segmentSize": 11}),
			expectedError: "invalid type for rowOffset in segment metadata, expected json.Number",
		},
		{
			name:          "MissingSegmentSizeMetadata",
			segment:       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2}),
			expectedError: "segmentSize is missing in segment metadata",
		},
		{
			name:          "WrongSegmentSizeMetadataType",
			segment:       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": "11"}),
			expectedError: "invalid type for segmentSize in segment metadata, expected json.Number",
		},
		{
			name:          "MissingMetadata",
			segment:       withoutField(spooledSegment("seg", nil), "metadata"),
			expectedError: "metadata is missing in segment at index 0",
		},
		{
			name:          "WrongMetadataType",
			segment:       spooledSegment("seg", "fake-metadata"),
			expectedError: "metadata is invalid or cannot be parsed as map[string]interface{} in segment at index 0",
		},
		{
			name:           "WrongUncompressSize",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11}),
			expectedError:  "failed to decode spooled segment at index 0: segment size mismatch: expected 11 bytes, got 29 byte",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:           "WrongCompresSize",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 29}),
			expectedError:  "decompressed size mismatch: expected 2 bytes, got 16 bytes",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:          "MissingUri",
			segment:       withoutField(spooledSegment("seg", validMetadata), "uri"),
			expectedError: "missing or invalid 'uri' field in spooled segment at index 0",
		},
		{
			name:          "MissingUriAck",
			segment:       withoutField(spooledSegment("seg", validMetadata), "ackUri"),
			expectedError: "missing or invalid 'ackUri' field in spooled segment at index 0",
		},
		{
			name: "wrongHeadersFormat",
			segment: withField(spooledSegment("seg", validMetadata), "headers", [][]string{
				{"x-amz-server-side-encryption-customer-algorithm", "AES256"},
				{"x-amz-server-side-encryption-customer-key", "key"},
			}),
			expectedError: "invalid 'headers' field in spooled segment at index 0: expected map[string]interface{}",
		},
		{
			name: "HeadersWithMultipleValues",
			segment: withField(spooledSegment("seg", validMetadata), "headers", map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": []any{"AES256"},
				"x-amz-server-side-encryption-customer-key":       []any{"key"},
				"x-amz-server-side-encryption-customer-key-md5":   []any{"md5", "md5"}, // wrong, more then one
			}),
			expectedError: "multiple values for header x-amz-server-side-encryption-customer-key-md5",
		},
		{
			name: "HeaderValueWrongType",
			segment: withField(spooledSegment("seg", validMetadata), "headers", map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": []any{"AES256"},
				"x-amz-server-side-encryption-customer-key":       []any{"key"},
				"x-amz-server-side-encryption-customer-key-md5":   []any{123}, // Wrong type: integer instead of string
			}),
			expectedError: "unsupported header value type json.Number",
		},
		{
			name: "HeaderTypeInvalid",
			segment: withField(spooledSegment("seg", validMetadata), "headers", map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": "AES256", // Invalid type: string instead of []interface{}
			}),
			expectedError: "unsupported header type string",
		},
		{
			name:                          "ErrorDownloadingSegment",
			segment:                       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11}),
			expectedError:                 "trino: query failed (500 Internal Server Error):",
			downloadedData:                mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
			downloadedDataStatusCodeError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json+zstd", tc.segment))
			fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
				if tc.downloadedDataStatusCodeError {
					w.WriteHeader(http.StatusInternalServerError)
				}
				_, _ = w.Write(tc.downloadedData)
			})
			db := fc.open(t, "")

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
		segment       map[string]any
		expectedError string
	}{
		{
			name:          "WrongUncompressSize",
			segment:       inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 1, "rowOffset": 2, "segmentSize": 29}),
			expectedError: "failed to decode spooled segment at index 0: decompressed size mismatch: expected 1 bytes, got 16 bytes",
		},
		{
			name:          "WrongCompresSize",
			segment:       inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 16, "rowOffset": 2, "segmentSize": 1}),
			expectedError: "failed to decode spooled segment at index 0: segment size mismatch: expected 1 bytes, got 29 bytes",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json+zstd", tc.segment))
			db := fc.open(t, "")

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

// newHeartbeatCoordinator serves a single spooled segment whose download takes
// downloadDelay, so heartbeats are sent while it is in flight. Every page
// updates the connection headers, so that the race detector sees them being
// read concurrently by the heartbeat.
func newHeartbeatCoordinator(t testing.TB, heartbeat http.HandlerFunc, downloadDelay time.Duration) *fakeCoordinator {
	t.Helper()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().withHeader(trinoSetSessionHeader, "query_max_run_time=10m"),
		spooledPage("json", spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1})).
			withHeader(trinoSetCatalogHeader, "memory"),
		emptyPage().withHeader(trinoSetSessionHeader, "query_max_run_time=20m"),
	)
	fc.handleSegment("seg0", func(w http.ResponseWriter, r *http.Request) {
		if downloadDelay > 0 {
			time.Sleep(downloadDelay)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[[1000]]"))
	})
	fc.onHeartbeat(heartbeat)
	return fc
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
			fc := newHeartbeatCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
				attempt := int(attempts.Add(1))
				w.WriteHeader(tc.statuses[min(attempt, len(tc.statuses))-1])
			}, tc.downloadDelay)
			db := fc.open(t, "?heartbeat_interval=100ms")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			defer rows.Close()

			results := collectInts(t, rows)
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
	fc := newHeartbeatCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case heartbeatSent <- struct{}{}:
		default:
		}
		<-unblockHeartbeat
		w.WriteHeader(http.StatusOK)
	}, 0)
	// registered after the server's own cleanup, so it runs first and the
	// parked heartbeat handler cannot block the server from closing
	t.Cleanup(func() { close(unblockHeartbeat) })
	db := fc.open(t, "?heartbeat_interval=50ms")

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

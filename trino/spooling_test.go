package trino

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpoolingProtocolSpooledSegmentDecoders(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		segment        map[string]any
		encoding       string
		downloadedData []byte
		want           []int
	}{
		{
			name:           "noCompression",
			segment:        spooledSegment("seg", map[string]any{"segmentSize": 16, "rowOffset": 0, "rowsCount": 2}),
			encoding:       "json",
			want:           []int{1000, 10001},
			downloadedData: []byte("[[1000],[10001]]"),
		},
		{
			name:           "zstdCompression",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}),
			encoding:       "json+zstd",
			want:           []int{1000, 10001},
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:           "spooledSegmentWithoutHeadersOnReponse", // headers are optional
			segment:        withoutField(spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}), "headers"),
			encoding:       "json+zstd",
			want:           []int{1000, 10001},
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:           "zlibCompression",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18}),
			encoding:       "json+lz4",
			want:           []int{1000, 10001},
			downloadedData: mustDecodeBase64("8AFbWzEwMDBdLFsxMDAwMV1d"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage(tc.encoding, tc.segment))
			fc.serveSegment("seg", tc.downloadedData)
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())

			assert.Equal(t, tc.want, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolToManyOutOfOrderSegmentDownload(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	cases := []struct {
		name   string
		status int
	}{
		{name: "retry 429 Too Many Requests", status: http.StatusTooManyRequests},
		{name: "retry 502 Bad Gateway", status: http.StatusBadGateway},
		{name: "retry 503 Service Unavailable", status: http.StatusServiceUnavailable},
		{name: "retry 504 Gateway Timeout", status: http.StatusGatewayTimeout},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shortenSegmentDownloadRetries(t)
			var failCounter atomic.Int32
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
			))
			fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
				if failCounter.Load() < 2 {
					failCounter.Add(1)
					w.WriteHeader(tc.status)
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

func failingSegmentCoordinator(t *testing.T, failures *atomic.Int32) *fakeCoordinator {
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		failures.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	})
	return fc
}

func TestSpoolingProtocolSegmentDownloadRetriesClosedConnection(t *testing.T) {
	shortenSegmentDownloadRetries(t)
	var downloads atomic.Int32
	fc := newFakeCoordinator(t)
	// A reused connection lets the HTTP transport resend the download
	// itself, which would hide the retry from the count.
	fc.server.Config.SetKeepAlivesEnabled(false)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		if downloads.Add(1) == 1 {
			closeConnection(t, w)
			return
		}
		_, _ = w.Write([]byte("[[1000]]"))
	})
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1000}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	assert.EqualValues(t, 2, downloads.Load())
}

// closedConnection in place of a status closes the connection without a
// response.
const closedConnection = 0

// closeConnection closes the connection without sending a response.
func closeConnection(t testing.TB, w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// Shares the mutable segmentDownloadInitialDelay with the other tests below,
// so it cannot run in parallel with them.
func TestSpoolingProtocolSegmentDownloadRetryTimeout(t *testing.T) {
	shortenSegmentDownloadRetries(t)
	var failures atomic.Int32
	db := failingSegmentCoordinator(t, &failures).open(t, "?request_retry_timeout=200ms&request_retry_max_attempts=100000")

	start := time.Now()
	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	elapsed := time.Since(start)

	require.Error(t, rows.Err())
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
	assert.ErrorContains(t, rows.Err(), fmt.Sprintf("giving up after %d attempts", failures.Load()))
}

func TestSpoolingProtocolSegmentDownloadRetryMaxAttempts(t *testing.T) {
	shortenSegmentDownloadRetries(t)
	var failures atomic.Int32
	db := failingSegmentCoordinator(t, &failures).open(t, "?request_retry_timeout=1h&request_retry_max_attempts=3")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	require.Error(t, rows.Err())
	assert.EqualValues(t, 3, failures.Load())
}

func TestSpoolingProtocolSegmentDownloadErrorKeepsResponseStatus(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, rows.Err(), &queryFailed)
	assert.Equal(t, http.StatusNotFound, queryFailed.StatusCode)
}

func TestSpoolingProtocolExpiredSegment(t *testing.T) {
	t.Parallel()
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	require.NoError(t, err)
	expired := time.Date(2020, time.July, 1, 12, 30, 15, 0, warsaw)
	notExpired := time.Now().Add(time.Hour).In(warsaw)
	cases := []struct {
		name        string
		expiresAt   string
		status      int
		wantExpired bool
	}{
		{name: "expired and missing", expiresAt: expired.Format("2006-01-02T15:04:05"), status: http.StatusNotFound, wantExpired: true},
		{name: "expired but still served", expiresAt: expired.Format("2006-01-02T15:04:05"), status: http.StatusOK},
		{name: "not expired and missing", expiresAt: notExpired.Format("2006-01-02T15:04:05.000"), status: http.StatusNotFound},
		{name: "malformed and missing", expiresAt: "yesterday", status: http.StatusNotFound},
		{name: "malformed but served", expiresAt: "yesterday", status: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1, "expiresAt": tc.expiresAt}),
			))
			fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("[[1000]]"))
			})
			db := fc.open(t, "?timezone=Europe/Warsaw")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			results := collectInts(t, rows)

			if tc.status == http.StatusOK {
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1000}, results)
				return
			}
			var queryFailed *ErrQueryFailed
			require.ErrorAs(t, rows.Err(), &queryFailed)
			assert.Equal(t, http.StatusNotFound, queryFailed.StatusCode)
			var segmentExpired *SegmentExpiredError
			if !tc.wantExpired {
				assert.False(t, errors.As(rows.Err(), &segmentExpired), "unexpected %v", rows.Err())
				assert.ErrorContains(t, rows.Err(), "error fetching segment from uri")
				return
			}
			require.ErrorAs(t, rows.Err(), &segmentExpired)
			assert.True(t, expired.Equal(segmentExpired.ExpiresAt), "expires at %v, want %v", segmentExpired.ExpiresAt, expired)
			assert.ErrorContains(t, rows.Err(), "expired at 2020-07-01T12:30:15+02:00")
		})
	}
}

func TestParseSegmentExpiresAt(t *testing.T) {
	t.Parallel()
	warsaw, err := time.LoadLocation("Europe/Warsaw")
	require.NoError(t, err)
	cases := []struct {
		name      string
		expiresAt string
		want      time.Time
	}{
		{name: "seconds", expiresAt: "2020-07-01T12:30:15", want: time.Date(2020, time.July, 1, 12, 30, 15, 0, warsaw)},
		{name: "fraction", expiresAt: "2020-07-01T12:30:15.123456789", want: time.Date(2020, time.July, 1, 12, 30, 15, 123456789, warsaw)},
		{name: "no seconds", expiresAt: "2020-07-01T12:30", want: time.Date(2020, time.July, 1, 12, 30, 0, 0, warsaw)},
		{name: "offset", expiresAt: "2020-07-01T12:30:15Z", want: time.Date(2020, time.July, 1, 12, 30, 15, 0, time.UTC)},
		{name: "malformed", expiresAt: "2020-07-01 12:30:15"},
		{name: "missing"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseSegmentExpiresAt(tc.expiresAt, warsaw)
			assert.True(t, tc.want.Equal(got), "got %v, want %v", got, tc.want)
		})
	}
}

// shortenSegmentDownloadRetries keeps the retry tests from waiting for the
// real backoff, which adds up to seconds once the retries are exhausted.
func shortenSegmentDownloadRetries(t testing.TB) {
	t.Helper()
	previous := segmentDownloadInitialDelay
	segmentDownloadInitialDelay = time.Millisecond
	t.Cleanup(func() { segmentDownloadInitialDelay = previous })
}

func mustDecodeBase64(encoded string) []byte {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode base64: %v", err))
	}
	return data
}

func TestSpoolingProtocolRejectsMoreWorkersThanOutOfOrderSegments(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1",
		sql.Named(trinoEncoding, "json"),
		sql.Named(trinoSpoolingWorkerCount, "2"),
		sql.Named(trinoMaxOutOfOrdersSegments, "1"))

	require.EqualError(t, err, "spooling worker cannot be greater than max out of order segments allowed. spooling workers: 2, allowed out of order segments: 1")
	assert.Empty(t, fc.capturedRequests(), "the query must be rejected before anything is sent")
}

func TestSpoolingProtocolOnlyWithInlineSegments(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	cases := []struct {
		name     string
		segment  map[string]any
		encoding string
		want     []int
	}{
		{
			name:     "noCompression",
			segment:  inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 0}),
			encoding: "json",
			want:     []int{1000, 10001},
		},
		{
			name:     "zstdCompression",
			segment:  inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 29}),
			encoding: "json+zstd",
			want:     []int{1000, 10001},
		},
		{
			name:     "zlibCompression",
			segment:  inlineSegment("8AFbWzEwMDBdLFsxMDAwMV1d", map[string]any{"uncompressedSize": 16, "rowOffset": 0, "segmentSize": 18}),
			encoding: "json+lz4",
			want:     []int{1000, 10001},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage(tc.encoding, tc.segment))
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())

			assert.Equal(t, tc.want, results, "Expected query results to match")
		})
	}
}

func TestSpoolingProtocolSegmentErrorHandling(t *testing.T) {
	t.Parallel()
	validMetadata := map[string]any{"segmentSize": 3679, "uncompressedSize": 2, "rowOffset": 0}
	cases := []struct {
		name                          string
		segment                       map[string]any
		downloadedData                []byte
		downloadedDataStatusCodeError bool
		wantErr                       string
	}{
		{
			name:    "MissingRowOffsetMetadata",
			segment: spooledSegment("seg", map[string]any{"uncompressedSize": 2, "segmentSize": 11}),
			wantErr: "rowOffset is missing in segment metadata",
		},
		{
			name:    "WrongRowOffsetMetadataType",
			segment: spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": "2", "segmentSize": 11}),
			wantErr: `unmarshal JSON string into Go int64 within "/segments/0/metadata/rowOffset"`,
		},
		{
			name:    "MissingSegmentSizeMetadata",
			segment: spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2}),
			wantErr: "segmentSize is missing in segment metadata",
		},
		{
			name:    "WrongSegmentSizeMetadataType",
			segment: spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": "11"}),
			wantErr: `unmarshal JSON string into Go int64 within "/segments/0/metadata/segmentSize"`,
		},
		{
			name:    "MissingMetadata",
			segment: withoutField(spooledSegment("seg", nil), "metadata"),
			wantErr: "metadata is missing in segment at index 0",
		},
		{
			name:    "WrongMetadataType",
			segment: spooledSegment("seg", "fake-metadata"),
			wantErr: `unmarshal JSON string into Go trino.segmentAttributes within "/segments/0/metadata"`,
		},
		{
			name:           "WrongUncompressSize",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11}),
			wantErr:        "failed to fetch spooled segment at index 0: segment size mismatch: expected 11 bytes, got more",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:           "WrongCompresSize",
			segment:        spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 29}),
			wantErr:        "failed to decompress zstd segment at rowOffset 2: decompressed size exceeds configured limit",
			downloadedData: mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
		},
		{
			name:    "MissingUri",
			segment: withoutField(spooledSegment("seg", validMetadata), "uri"),
			wantErr: "missing 'uri' field in spooled segment at index 0",
		},
		{
			name:    "MissingUriAck",
			segment: withoutField(spooledSegment("seg", validMetadata), "ackUri"),
			wantErr: "missing 'ackUri' field in spooled segment at index 0",
		},
		{
			name: "wrongHeadersFormat",
			segment: withField(spooledSegment("seg", validMetadata), "headers", [][]string{
				{"x-amz-server-side-encryption-customer-algorithm", "AES256"},
				{"x-amz-server-side-encryption-customer-key", "key"},
			}),
			wantErr: `unmarshal JSON array into Go map[string][]string within "/segments/0/headers"`,
		},
		{
			name: "HeaderValueWrongType",
			segment: withField(spooledSegment("seg", validMetadata), "headers", map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": []any{"AES256"},
				"x-amz-server-side-encryption-customer-key":       []any{"key"},
				"x-amz-server-side-encryption-customer-key-md5":   []any{123}, // Wrong type: integer instead of string
			}),
			wantErr: `unmarshal JSON number into Go string within "/segments/0/headers/x-amz-server-side-encryption-customer-key-md5/0"`,
		},
		{
			name: "HeaderTypeInvalid",
			segment: withField(spooledSegment("seg", validMetadata), "headers", map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": "AES256", // Invalid type: string instead of []interface{}
			}),
			wantErr: `unmarshal JSON string into Go []string within "/segments/0/headers/x-amz-server-side-encryption-customer-algorithm"`,
		},
		{
			name:    "InlineDataNotString",
			segment: inlineSegment(123, map[string]any{"rowOffset": 0, "segmentSize": 8}),
			wantErr: `unmarshal JSON number into Go []uint8 within "/segments/0/data"`,
		},
		{
			name:    "RowsCountMismatch",
			segment: inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 0, "rowsCount": 3}),
			wantErr: "segment at rowOffset 0 has 2 rows but its metadata says 3",
		},
		{
			name:    "UnsupportedSegmentType",
			segment: withField(spooledSegment("seg", validMetadata), "type", "remote"),
			wantErr: `unsupported type "remote" of segment at index 0`,
		},
		{
			name:    "InlineDataNotBase64",
			segment: inlineSegment("not base64!", map[string]any{"rowOffset": 0, "segmentSize": 8}),
			wantErr: `unmarshal JSON string into Go []uint8 within "/segments/0/data": illegal base64 data`,
		},
		{
			name:    "FractionalRowOffset",
			segment: spooledSegment("seg", map[string]any{"rowOffset": 1.5, "segmentSize": 8}),
			wantErr: `unmarshal JSON number 1.5 into Go int64 within "/segments/0/metadata/rowOffset"`,
		},
		{
			name:           "MalformedSegmentJson",
			segment:        spooledSegment("seg", map[string]any{"rowOffset": 0, "segmentSize": 6}),
			downloadedData: []byte("[[1000"),
			wantErr:        "failed to decode segment at rowOffset 0: row 0: jsontext: unexpected EOF",
		},
		{
			name:                          "ErrorDownloadingSegment",
			segment:                       spooledSegment("seg", map[string]any{"uncompressedSize": 2, "rowOffset": 2, "segmentSize": 11}),
			wantErr:                       "trino: query failed (500 Internal Server Error):",
			downloadedData:                mustDecodeBase64("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw="),
			downloadedDataStatusCodeError: true,
		},
		{
			name:    "InlineWrongUncompressSize",
			segment: inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 1, "rowOffset": 2, "segmentSize": 29}),
			wantErr: "failed to decode spooled segment at index 0: failed to decompress zstd segment at rowOffset 2: decompressed size exceeds configured limit",
		},
		{
			name:    "NegativeRowsCount",
			segment: inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"rowOffset": 0, "segmentSize": 17, "rowsCount": -1}),
			wantErr: "negative value in segment metadata",
		},
		{
			name:    "NegativeUncompressedSize",
			segment: inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"rowOffset": 0, "segmentSize": 17, "uncompressedSize": -1}),
			wantErr: "negative value in segment metadata",
		},
		{
			// Only the decoded rows may size the result, not the count.
			name:    "RowsCountLargerThanTheData",
			segment: inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"rowOffset": 0, "segmentSize": 17, "rowsCount": int64(1) << 50}),
			wantErr: "segment at rowOffset 0 has 2 rows but its metadata says 1125899906842624",
		},
		{
			name:    "ZstdUncompressedSizeLargerThanPossible",
			segment: inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": int64(1) << 30, "rowOffset": 0, "segmentSize": 29}),
			wantErr: "is not possible for a 29-byte zstd segment",
		},
		{
			name:    "SegmentSizeAboveMaxInt32",
			segment: spooledSegment("seg", map[string]any{"rowOffset": 0, "segmentSize": int64(math.MaxInt32) + 1}),
			wantErr: "size above 2147483647 in segment metadata at index 0: segmentSize 2147483648",
		},
		{
			name:    "UncompressedSizeAboveMaxInt32",
			segment: spooledSegment("seg", map[string]any{"rowOffset": 0, "segmentSize": 29, "uncompressedSize": int64(math.MaxInt32) + 1}),
			wantErr: "size above 2147483647 in segment metadata at index 0: segmentSize 29, uncompressedSize 2147483648",
		},
		{
			name:           "DownloadLongerThanSegmentSize",
			segment:        spooledSegment("seg", map[string]any{"rowOffset": 0, "segmentSize": 4}),
			downloadedData: bytes.Repeat([]byte(" "), 1<<20),
			wantErr:        "failed to fetch spooled segment at index 0: segment size mismatch: expected 4 bytes, got more",
		},
		{
			name:    "InlineWrongCompresSize",
			segment: inlineSegment("KLUv/QQAgQAAW1sxMDAwXSxbMTAwMDFdXZfUttw=", map[string]any{"uncompressedSize": 16, "rowOffset": 2, "segmentSize": 1}),
			wantErr: "failed to decode spooled segment at index 0: segment size mismatch: expected 1 bytes, got 29 bytes",
		},
	}

	for _, tc := range cases {
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

			// A page whose segments do not decode fails the query right
			// away; the other errors surface while reading the rows.
			rows, err := db.Query("SELECT 1")
			if err == nil {
				t.Cleanup(func() { require.NoError(t, rows.Close()) })
				for rows.Next() {
					// force segment processing
				}
				err = rows.Err()
			}

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// A frame header can declare more content than the segment metadata, which
// must not size the decompression buffer. This one declares 4 GiB for a
// 17-byte segment; the buffer is never written, so only the allocation
// statistics show it.
func TestDecompressSegmentIgnoresZstdFrameContentSize(t *testing.T) {
	data := mustDecodeBase64("KLUv/cAAAAAAAAEAAAABAAA=")
	metadata := segmentMetadata{segmentSize: int64(len(data)), uncompressedSize: 17}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := decompressSegment(data, "json+zstd", metadata)
	runtime.ReadMemStats(&after)

	require.ErrorContains(t, err, "failed to decompress zstd segment at rowOffset 0")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<30), "the frame header must not size the buffer")
}

// TestSpoolingProtocolDecodeErrorNamesTheSegmentIndex checks that a segment
// that fails to decode is named by its index in the page, whether it was
// downloaded or travelled inline.
func TestSpoolingProtocolDecodeErrorNamesTheSegmentIndex(t *testing.T) {
	t.Parallel()
	validData := mustDecodeBase64("W1sxMDAwXSwgWzEwMDAxXV0=")
	validMetadata := map[string]any{"segmentSize": len(validData), "rowOffset": 0}
	corruptMetadata := map[string]any{"segmentSize": 6, "rowOffset": 2}
	cases := []struct {
		name     string
		segments []map[string]any
	}{
		{
			name:     "downloaded",
			segments: []map[string]any{spooledSegment("valid", validMetadata), spooledSegment("corrupt", corruptMetadata)},
		},
		{
			name:     "inline",
			segments: []map[string]any{inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", validMetadata), inlineSegment("W1sxMDAw", corruptMetadata)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json", tc.segments...))
			fc.handleSegment("valid", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(validData)
			})
			fc.handleSegment("corrupt", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("[[1000"))
			})
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			if err == nil {
				t.Cleanup(func() { require.NoError(t, rows.Close()) })
				for rows.Next() {
					// force segment processing
				}
				err = rows.Err()
			}

			require.ErrorContains(t, err, "failed to decode spooled segment at index 1: failed to decode segment at rowOffset 2:")
		})
	}
}

// TestSpoolingProtocolWarningsCollectedAcrossPages checks that warnings
// reported only on a later spooled page (consumed by
// proccessSpollingSegments, not by fetch's first page) are still collected.
func TestSpoolingProtocolWarningsCollectedAcrossPages(t *testing.T) {
	t.Parallel()
	firstPage := Warning{Code: 1, Name: "TOO_MANY_STAGES", Message: "Query has too many stages"}
	laterPage := Warning{Code: 5, Name: "DEPRECATED_SYNTAX", Message: "EXPLAIN TYPE LOGICAL is deprecated"}
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		spooledPage("json", spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1})).withWarnings(firstPage),
		spooledPage("json", spooledSegment("seg1", map[string]any{"segmentSize": 8, "rowOffset": 1, "rowsCount": 1})).withWarnings(firstPage, laterPage),
	)
	fc.serveSegment("seg0", []byte("[[1000]]"))
	fc.serveSegment("seg1", []byte("[[1001]]"))
	db := fc.open(t, "")

	var warnings Warnings
	rows, err := db.Query("SELECT 1", sql.Named(trinoWarningsParam, &warnings))
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 1001}, collectInts(t, rows))
	require.NoError(t, rows.Err())

	assert.Equal(t, []Warning{firstPage, laterPage}, warnings.All())
}

func TestSpoolingProtocolAcknowledgesSegments(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
		spooledSegment("seg1", map[string]any{"segmentSize": 8, "rowOffset": 1, "rowsCount": 1}),
	))
	fc.serveSegment("seg0", []byte("[[1000]]"))
	fc.serveSegment("seg1", []byte("[[1001]]"))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 1001}, collectInts(t, rows))
	require.NoError(t, rows.Err())

	// acknowledgements are sent in the background after each download
	require.Eventually(t, func() bool {
		acked := fc.ackedSegments()
		return slices.Contains(acked, "seg0") && slices.Contains(acked, "seg1")
	}, 5*time.Second, time.Millisecond, "every downloaded segment must be acknowledged")
}

// A segment that does not match its metadata fails the query, so it is never
// read again and must not be left in storage until it expires.
func TestSpoolingProtocolAcknowledgesSegmentsOfTheWrongSize(t *testing.T) {
	t.Parallel()
	for name, segmentSize := range map[string]int{"shorter": 9, "longer": 7} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": segmentSize, "rowOffset": 0, "rowsCount": 1}),
			))
			fc.serveSegment("seg", []byte("[[1000]]"))
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			for rows.Next() {
				// force segment processing
			}
			require.ErrorContains(t, rows.Err(), "failed to fetch spooled segment at index 0: segment size mismatch")
			require.NoError(t, rows.Close())

			require.Eventually(t, func() bool {
				return slices.Contains(fc.ackedSegments(), "seg")
			}, 5*time.Second, time.Millisecond, "a segment of the wrong size must be acknowledged")
		})
	}
}

// In the COORDINATOR_STORAGE_REDIRECT and WORKER_PROXY retrieval modes the
// coordinator answers the segment download with a redirect to the storage or
// to a worker, which needs the segment headers to open the segment.
func TestSpoolingProtocolFollowsSegmentRedirects(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusSeeOther, http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var storageRequests []http.Header
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				storageRequests = append(storageRequests, r.Header.Clone())
				mu.Unlock()
				_, _ = w.Write([]byte("[[1000]]"))
			}))
			t.Cleanup(storage.Close)
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
			))
			fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, storage.URL+"/bucket/seg?signature=1", status)
			})
			db := fc.open(t, "?extra_credentials=secret:value")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, []int{1000}, collectInts(t, rows))
			require.NoError(t, rows.Err())

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, storageRequests, 1)
			header := storageRequests[0]
			assert.Equal(t, "test", header.Get("test"), "the redirect target needs the segment headers")
			for name := range header {
				assert.NotContains(t, strings.ToLower(name), "x-trino", "session headers must not reach the redirect target")
			}
			assert.Empty(t, header.Get(authorizationHeader))
			require.Eventually(t, func() bool {
				return slices.Contains(fc.ackedSegments(), "seg")
			}, 5*time.Second, time.Millisecond, "the coordinator still receives the acknowledgment")
		})
	}
}

// The acknowledgment goes where the segment came from, so it follows
// redirects like the download.
func TestSpoolingProtocolFollowsAckRedirects(t *testing.T) {
	t.Parallel()
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json",
				spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
			))
			fc.serveSegment("seg", []byte("[[1000]]"))
			fc.handleAck("seg", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, fc.url()+"/v1/spooled/ack/moved", status)
			})
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, []int{1000}, collectInts(t, rows))
			require.NoError(t, rows.Err())
			require.Eventually(t, func() bool {
				return slices.Equal(fc.ackedSegments(), []string{"moved"})
			}, 5*time.Second, time.Millisecond, "the acknowledgment should reach the redirect target")
		})
	}
}

func TestSpoolingProtocolStopsEndlessSegmentRedirects(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.String(), http.StatusSeeOther)
	})
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.ErrorContains(t, rows.Err(), "stopped after 10 redirects")
}

func TestSpoolingProtocolSendsSegmentHeaders(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		headers any
		want    http.Header
	}{
		{
			name: "multipleValues",
			headers: map[string]any{
				"x-amz-server-side-encryption-customer-algorithm": []any{"AES256"},
				"x-trino-segment-token":                           []any{"first", "second"},
				"x-trino-empty":                                   []any{},
			},
			want: http.Header{
				"X-Amz-Server-Side-Encryption-Customer-Algorithm": {"AES256"},
				"X-Trino-Segment-Token":                           {"first", "second"},
				"X-Trino-Empty":                                   nil,
			},
		},
		{
			name:    "singleValue",
			headers: map[string]any{"x-trino-segment-token": []any{"only"}},
			want:    http.Header{"X-Trino-Segment-Token": {"only"}},
		},
		{
			name: "absent",
			want: http.Header{"Test": nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			segment := withoutField(spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}), "headers")
			if tc.headers != nil {
				segment = withField(segment, "headers", tc.headers)
			}
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json", segment))
			fc.serveSegment("seg", []byte("[[1000]]"))
			db := fc.open(t, "")

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, []int{1000}, collectInts(t, rows))
			require.NoError(t, rows.Err())
			require.Eventually(t, func() bool {
				return slices.Contains(fc.ackedSegments(), "seg")
			}, 5*time.Second, time.Millisecond, "the downloaded segment must be acknowledged")

			sent := map[string]http.Header{}
			for _, r := range fc.capturedRequests() {
				if strings.HasPrefix(r.path, "/v1/spooled/") {
					sent[r.path] = r.header
				}
			}
			for _, path := range []string{"/v1/spooled/download/seg", "/v1/spooled/ack/seg"} {
				require.Contains(t, sent, path)
				for name, values := range tc.want {
					assert.Equal(t, values, sent[path].Values(name), "header %s on %s", name, path)
				}
			}
		})
	}
}

type recordingProgressUpdater struct {
	mu      sync.Mutex
	updates []QueryProgressInfo
}

func (u *recordingProgressUpdater) Update(info QueryProgressInfo) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.updates = append(u.updates, info)
}

func (u *recordingProgressUpdater) reportedStates() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	var states []string
	for _, info := range u.updates {
		states = append(states, info.QueryStats.State)
	}
	return states
}

func (u *recordingProgressUpdater) failedAcks() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	var failed []int64
	for _, info := range u.updates {
		failed = append(failed, info.FailedSegmentAcknowledgments)
	}
	return failed
}

// queryWithProgress reads every row of a spooled query and closes it, which
// closes the statement and so waits for its acknowledgments.
func queryWithProgress(t *testing.T, fc *fakeCoordinator, updater ProgressUpdater) {
	t.Helper()
	db := fc.open(t, "")
	rows, err := db.Query("SELECT 1",
		sql.Named(trinoProgressCallbackParam, updater),
		sql.Named(trinoProgressCallbackPeriodParam, time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 1001}, collectInts(t, rows))
	require.NoError(t, rows.Err(), "a failed acknowledgment must not fail the query")
	require.NoError(t, rows.Close())
}

func twoSegmentCoordinator(t *testing.T) *fakeCoordinator {
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
		spooledSegment("seg1", map[string]any{"segmentSize": 8, "rowOffset": 1, "rowsCount": 1}),
	))
	fc.serveSegment("seg0", []byte("[[1000]]"))
	fc.serveSegment("seg1", []byte("[[1001]]"))
	return fc
}

// An acknowledgment that fails after the last page arrived is still reported,
// in an update sent when the statement closes.
func TestSpoolingProtocolReportsFailedAcknowledgments(t *testing.T) {
	t.Parallel()
	fc := twoSegmentCoordinator(t)
	fc.handleAck("seg1", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	})
	updater := &recordingProgressUpdater{}

	queryWithProgress(t, fc, updater)

	failed := updater.failedAcks()
	require.NotEmpty(t, failed)
	assert.EqualValues(t, 1, failed[len(failed)-1], "the last update should count the failed acknowledgment")
	assert.Contains(t, fc.ackedSegments(), "seg0")
}

// An acknowledgment is sent once, even when the answer would make another
// request retry, and its failure is only counted.
func TestSpoolingProtocolDoesNotRetryAcknowledgments(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusNotFound, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			fc := twoSegmentCoordinator(t)
			var acks atomic.Int32
			fc.handleAck("seg1", func(w http.ResponseWriter, r *http.Request) {
				acks.Add(1)
				w.WriteHeader(status)
			})
			updater := &recordingProgressUpdater{}

			queryWithProgress(t, fc, updater)

			assert.EqualValues(t, 1, acks.Load())
			failed := updater.failedAcks()
			require.NotEmpty(t, failed)
			assert.EqualValues(t, 1, failed[len(failed)-1])
		})
	}
}

func TestSpoolingProtocolNoExtraUpdateWhenAcknowledgmentsSucceed(t *testing.T) {
	t.Parallel()
	fc := twoSegmentCoordinator(t)
	updater := &recordingProgressUpdater{}

	queryWithProgress(t, fc, updater)

	for _, failed := range updater.failedAcks() {
		assert.Zero(t, failed)
	}
}

// newHeartbeatCoordinator serves a single spooled segment whose download
// completes only when release is closed, so heartbeats are sent while it is
// in flight. Every page updates the connection headers, so that the race
// detector sees them being read concurrently by the heartbeat.
func newHeartbeatCoordinator(t testing.TB, heartbeat http.HandlerFunc, release <-chan struct{}) *fakeCoordinator {
	t.Helper()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().withHeader(trinoSetSessionHeader, "query_max_run_time=10m"),
		spooledPage("json", spooledSegment("seg0", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1})).
			withHeader(trinoSetCatalogHeader, "memory"),
		emptyPage().withHeader(trinoSetSessionHeader, "query_max_run_time=20m"),
	)
	fc.handleSegment("seg0", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[[1000]]"))
	})
	fc.onHeartbeat(heartbeat)
	return fc
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()
	const interval = 10 * time.Millisecond
	cases := []struct {
		name string
		// status returned by the n-th heartbeat; the last one repeats
		statuses []int
		// the segment download completes after this many heartbeats
		releaseAfter int32
		// when set, the heartbeat must have stopped after exactly this many attempts
		stopAfter int32
	}{
		{
			name:         "sent while a segment is downloading",
			statuses:     []int{http.StatusOK},
			releaseAfter: 1,
		},
		{
			name:         "disabled when the coordinator rejects HEAD",
			statuses:     []int{http.StatusMethodNotAllowed},
			releaseAfter: 1,
			stopAfter:    1,
		},
		{
			name:         "disabled when the coordinator does not implement heartbeats",
			statuses:     []int{http.StatusNotImplemented},
			releaseAfter: 1,
			stopAfter:    1,
		},
		{
			name:         "disabled after consecutive failures",
			statuses:     []int{http.StatusInternalServerError},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled after consecutive empty responses",
			statuses:     []int{http.StatusNoContent},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled after consecutive redirects",
			statuses:     []int{http.StatusFound},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled after consecutive rejections",
			statuses:     []int{http.StatusForbidden},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled after consecutive rate limits",
			statuses:     []int{http.StatusTooManyRequests},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled while the coordinator is unavailable",
			statuses:     []int{http.StatusServiceUnavailable},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			name:         "disabled after consecutive closed connections",
			statuses:     []int{closedConnection},
			releaseAfter: maxHeartbeatFailures,
			stopAfter:    maxHeartbeatFailures,
		},
		{
			// a 404 is transient: it's also returned by a coordinator that
			// doesn't know the query, e.g. behind a load balancer
			name:         "retried when the coordinator does not know the query",
			statuses:     []int{http.StatusNotFound, http.StatusNotFound, http.StatusOK},
			releaseAfter: maxHeartbeatFailures,
		},
		{
			name: "failure count reset by a successful heartbeat",
			statuses: []int{
				http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK,
				http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK,
			},
			releaseAfter: 2 * maxHeartbeatFailures,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			release := make(chan struct{})
			releaseDownload := sync.OnceFunc(func() { close(release) })
			fc := newHeartbeatCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				if attempt >= tc.releaseAfter {
					defer releaseDownload()
				}
				status := tc.statuses[min(int(attempt), len(tc.statuses))-1]
				if status == closedConnection {
					closeConnection(t, w)
					return
				}
				w.WriteHeader(status)
			}, release)
			// A reused connection lets the HTTP transport resend a heartbeat
			// whose connection was closed, which would add to the count.
			fc.server.Config.SetKeepAlivesEnabled(false)
			// a failing test must not leave the download parked on the fake
			t.Cleanup(releaseDownload)
			db := fc.open(t, "?heartbeat_interval="+interval.String())

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rows.Close()) })

			results := collectInts(t, rows)
			require.NoError(t, rows.Err())
			assert.Equal(t, []int{1000}, results)

			if tc.stopAfter == 0 {
				assert.GreaterOrEqual(t, attempts.Load(), tc.releaseAfter, "Expected the heartbeat to keep running")
				return
			}
			// give a heartbeat that was wrongly kept alive the chance to show up
			time.Sleep(5 * interval)
			assert.Equal(t, tc.stopAfter, attempts.Load(), "Expected the heartbeat to be disabled")
		})
	}
}

func TestHeartbeatDoesNotDelayClose(t *testing.T) {
	t.Parallel()
	heartbeatSent := make(chan struct{}, 1)
	unblockHeartbeat := make(chan struct{})
	downloadReady := make(chan struct{})
	close(downloadReady)
	fc := newHeartbeatCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case heartbeatSent <- struct{}{}:
		default:
		}
		<-unblockHeartbeat
		w.WriteHeader(http.StatusOK)
	}, downloadReady)
	// registered after the server's own cleanup, so it runs first and the
	// parked heartbeat handler cannot block the server from closing
	t.Cleanup(func() { close(unblockHeartbeat) })
	db := fc.open(t, "?heartbeat_interval=10ms")

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

func TestSpoolingProtocolSegmentDownloadRetriesOnTimeout(t *testing.T) {
	shortenSegmentDownloadRetries(t)
	require.NoError(t, RegisterCustomClient("segment-timeout", &http.Client{Timeout: 50 * time.Millisecond}))
	var attempts atomic.Int32
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.handleSegment("seg", func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			// let the client time out
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[[1000]]"))
	})
	db := fc.open(t, "?custom_client=segment-timeout")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	results := collectInts(t, rows)
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1000}, results)
	assert.Equal(t, int32(2), attempts.Load(), "the download must be retried once after the timeout")
}

func TestSpoolingProtocolRejectsUnsupportedEncoding(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json+brotli",
		spooledSegment("seg", map[string]any{"segmentSize": 8, "uncompressedSize": 16, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.serveSegment("seg", []byte("[[1000]]"))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })
	collectInts(t, rows)

	require.ErrorContains(t, rows.Err(), "unsupported segment encoder: json+brotli")
}

func TestSpoolingNamedArgumentsAreValidated(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		arg     sql.NamedArg
		wantErr string
	}{
		{name: "encoding not a string", arg: sql.Named(trinoEncoding, 42), wantErr: "trino: encoding must be a string, got int64"},
		{name: "worker count not a string", arg: sql.Named(trinoSpoolingWorkerCount, 2), wantErr: "trino: spooling_worker_count must be a string, got int64"},
		{name: "worker count not a number", arg: sql.Named(trinoSpoolingWorkerCount, "two"), wantErr: `trino: spooling_worker_count must be an integer, got "two"`},
		{name: "out-of-order segments not a string", arg: sql.Named(trinoMaxOutOfOrdersSegments, 2.5), wantErr: "trino: max_out_of_order_segments must be a string, got float64"},
		{name: "out-of-order segments not a number", arg: sql.Named(trinoMaxOutOfOrdersSegments, "many"), wantErr: `trino: max_out_of_order_segments must be an integer, got "many"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			db := fc.open(t, "")

			_, err := db.Query("SELECT 1", tc.arg)

			require.EqualError(t, err, tc.wantErr)
			assert.Empty(t, fc.capturedRequests(), "the query must be rejected before anything is sent")
		})
	}
}

// A storage response that never completes must not hang Rows.Next past the
// query's deadline.
func TestSpoolingProtocolSegmentDownloadHonorsDeadline(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fc.handleSegment("slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	fc.respond(statementPage(), spooledPage("json", spooledSegment("slow", map[string]any{"rowOffset": 0, "segmentSize": 17})), emptyPage())
	db := fc.open(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	assert.ErrorIs(t, rows.Err(), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 3*time.Second, "reading rows must stop at the context deadline")
}

func inlineRows(t testing.TB, rowOffset int, values ...int) map[string]any {
	t.Helper()
	rows := make([][]int, len(values))
	for i, v := range values {
		rows[i] = []int{v}
	}
	data, err := json.Marshal(rows)
	require.NoError(t, err)
	return inlineSegment(base64.StdEncoding.EncodeToString(data), map[string]any{
		"rowOffset": rowOffset, "segmentSize": len(data), "rowsCount": len(values),
	})
}

// Segments whose row offsets do not follow each other fail the query rather
// than return fewer rows or wait for a segment that never arrives.
func TestSpoolingProtocolSegmentSequenceErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		segments []map[string]any
		wantErr  string
	}{
		{"duplicate offset", []map[string]any{inlineRows(t, 0, 1000), inlineRows(t, 0, 9999), inlineRows(t, 1, 1001)}, "overlaps rows already delivered"},
		{"gap", []map[string]any{inlineRows(t, 0, 1000), inlineRows(t, 2, 1002)}, "spooled segments missing"},
		{"first segment not at offset 0", []map[string]any{inlineRows(t, 5, 1005)}, "spooled segments missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json", tc.segments...), emptyPage())
			db := fc.open(t, "")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			rows, err := db.QueryContext(ctx, "SELECT 1", sql.Named(trinoSpoolingWorkerCount, "1"), sql.Named(trinoMaxOutOfOrdersSegments, "10"))
			require.NoError(t, err)
			collectInts(t, rows)
			assert.ErrorContains(t, rows.Err(), tc.wantErr)
			assert.NoError(t, ctx.Err(), "the query must fail rather than wait for the deadline")
			require.NoError(t, rows.Close())
		})
	}
}

// A segment without rows is skipped; Rows.Next must not index an empty batch.
func TestSpoolingProtocolSkipsSegmentsWithoutRows(t *testing.T) {
	t.Parallel()
	empty := base64.StdEncoding.EncodeToString([]byte("[]"))
	row := func(rowOffset int, value string) map[string]any {
		data := "[[" + value + "]]"
		return inlineSegment(base64.StdEncoding.EncodeToString([]byte(data)), map[string]any{
			"rowOffset": rowOffset, "segmentSize": len(data), "rowsCount": 1,
		})
	}
	cases := []struct {
		name     string
		segments []map[string]any
		want     []int
	}{
		{"only segment", []map[string]any{inlineSegment(empty, map[string]any{"rowOffset": 0, "segmentSize": 2})}, nil},
		{"rowsCount zero", []map[string]any{inlineSegment(empty, map[string]any{"rowOffset": 0, "segmentSize": 2, "rowsCount": 0})}, nil},
		{"between segments", []map[string]any{row(0, "1000"), inlineSegment(empty, map[string]any{"rowOffset": 1, "segmentSize": 2}), row(1, "1001")}, []int{1000, 1001}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), spooledPage("json", tc.segments...), emptyPage())

			rows, err := fc.open(t, "").Query("SELECT 1")
			require.NoError(t, err)
			values := collectInts(t, rows)
			require.NoError(t, rows.Err())
			assert.Equal(t, tc.want, values)
		})
	}
}

// The segments are queued as the caller reads rows, so the result may list
// more segments than the queue and the out-of-order slots hold, in one page
// or across several.
func TestSpoolingProtocolSegmentBacklog(t *testing.T) {
	t.Parallel()
	segments := func(offset, count int) []map[string]any {
		result := make([]map[string]any, count)
		for i := range result {
			data := fmt.Sprintf("[[%d]]", 1000+offset+i)
			result[i] = inlineSegment(base64.StdEncoding.EncodeToString([]byte(data)), map[string]any{
				"rowOffset": offset + i, "segmentSize": len(data), "rowsCount": 1,
			})
		}
		return result
	}
	within := func(t *testing.T, what string, f func()) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			f()
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not return", what)
		}
	}

	t.Run("first page", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(t)
		fc.respond(statementPage(), spooledPage("json", segments(0, 1100)...), emptyPage())
		db := fc.open(t, "")
		within(t, "reading the result", func() {
			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			assert.Len(t, collectInts(t, rows), 1100)
			assert.NoError(t, rows.Err())
		})
	})
	t.Run("close with a backlog across pages", func(t *testing.T) {
		t.Parallel()
		pages := []page{statementPage(), spooledPage("json", segments(0, 1)...)}
		for offset := 1; offset < 1201; offset += 100 {
			pages = append(pages, spooledPage("json", segments(offset, 100)...))
		}
		pages = append(pages, emptyPage())
		fc := newFakeCoordinator(t)
		fc.respond(pages...)
		// A page is requested once the one before it is parsed, which is
		// after the page before that was handed to the queueing goroutine,
		// so the last request means all but the last two pages, more
		// segments than fit, are being queued.
		lastPageRequested := make(chan struct{}, 1)
		fc.onPage(func(index int, r *http.Request) {
			if index == len(pages)-1 {
				select {
				case lastPageRequested <- struct{}{}:
				default:
				}
			}
		})
		db := fc.open(t, "")
		rows, err := db.Query("SELECT 1")
		require.NoError(t, err)
		select {
		case <-lastPageRequested:
		case <-time.After(10 * time.Second):
			t.Fatal("the last page was not requested")
		}
		within(t, "rows.Close", func() { assert.NoError(t, rows.Close()) })
	})
	t.Run("context ends with a backlog", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(t)
		fc.respond(statementPage(), spooledPage("json", segments(0, 1100)...), emptyPage())
		conn, err := fc.open(t, "").Conn(context.Background())
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, conn.Close()) })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// The driver's rows are read directly, because database/sql
		// reports the canceled context itself.
		within(t, "reading the result", func() {
			require.NoError(t, conn.Raw(func(driverConn any) error {
				stmt, err := driverConn.(*Conn).PrepareContext(ctx, "SELECT 1")
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
				require.NoError(t, err)
				defer rows.Close()

				row := make([]driver.Value, 1)
				require.NoError(t, rows.Next(row))
				cancel()
				read := 1
				for err = rows.Next(row); err == nil; err = rows.Next(row) {
					read++
				}
				assert.ErrorIs(t, err, context.Canceled, "the result ended after %d of 1100 rows", read)
				return nil
			}))
		})
	})
}

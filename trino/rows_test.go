package trino

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryCancellation(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.MarshalWrite(w, &stmtResponse{
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
	t.Cleanup(func() { debug.SetMaxStack(previousSetting) })
	var count atomic.Int32
	var nextPage bytes.Buffer
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count.Add(1) <= 51 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(nextPage.Bytes())
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.MarshalWrite(w, &stmtResponse{
			Error: ErrTrino{
				ErrorName: "TEST",
			},
		})
	}))
	t.Cleanup(ts.Close)
	require.NoError(t, json.MarshalWrite(&nextPage, &stmtResponse{
		ID:      "fake-query",
		NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
	}))

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	var queryFailed *ErrQueryFailed
	assert.ErrorAs(t, err, &queryFailed)
}

func TestProtocolErrorHandling(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		data    interface{}
		wantErr string
	}{
		{
			name:    "direct protocol invalid row type",
			data:    []interface{}{123},
			wantErr: "trino: row 0: expected an array of column values, got a number",
		},
		{
			name:    "spooling protocol missing encoding",
			data:    map[string]interface{}{"segments": []interface{}{}},
			wantErr: "trino: missing 'encoding' field on spooling protocol",
		},
		{
			name:    "spooling protocol invalid segments type",
			data:    map[string]interface{}{"encoding": "json", "segments": "invalid"},
			wantErr: `unmarshal JSON string into Go []trino.dataSegment within "/segments"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage(tc.data))
			db := fc.open(t, "")

			_, err := db.Query("SELECT 1")
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestPageKeepsNumbersExact covers numbers a float64 cannot hold: integers
// beyond 2^53, also inside an array scanned as []interface{}, where callers
// get an int64, and decimals, which the server sends as strings.
func TestPageKeepsNumbersExact(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	decimalType := typeSignature{RawType: "decimal", Arguments: []typeArgument{
		{Kind: KIND_LONG, Value: jsontext.Value("38")},
		{Kind: KIND_LONG, Value: jsontext.Value("2")},
	}}
	columns := []queryColumn{
		column("bigint", scalarType("bigint")),
		column("array(bigint)", arrayType(scalarType("bigint"))),
		column("decimal(38,2)", decimalType),
	}
	fc.respond(statementPage(), columnsPage(columns, [][]any{
		{int64(math.MaxInt64), []any{int64(1<<53 + 1)}, "123456789012345678901234567890.12"},
	}))
	db := fc.open(t, "")

	var bigint int64
	var array NullSlice[interface{}]
	var decimal string
	require.NoError(t, db.QueryRow("SELECT x").Scan(&bigint, &array, &decimal))
	assert.Equal(t, int64(math.MaxInt64), bigint)
	assert.Equal(t, NullSlice[interface{}]{Slice: []interface{}{int64(9007199254740993)}, Valid: true}, array)
	assert.Equal(t, "123456789012345678901234567890.12", decimal)
}

func TestSetRoleHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?roles=catalog%3Auser")

	fc.respond(
		statementPage().withHeader(trinoSetRoleHeader, "hive=ROLE%7Badmin%7D"),
		resultPage([][]any{{1}}),
	)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.QueryContext(context.Background(), "SET ROLE admin IN hive")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	fc.respond(
		statementPage().
			withHeader(trinoSetRoleHeader, "iceberg=ROLE%7Bwriter%7D").
			withHeader(trinoSetRoleHeader, "catalog=NONE"),
		resultPage([][]any{{1}}),
	)
	rows, err = conn.QueryContext(context.Background(), "SET ROLE writer IN iceberg")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "catalog=ROLE%7Buser%7D", requests[0].header.Get(trinoRoleHeader), "initial role from DSN should be sent in first request")
	assert.Equal(t, "catalog=ROLE%7Buser%7D,hive=ROLE%7Badmin%7D", requests[1].header.Get(trinoRoleHeader), "server-set role should be added to the DSN role")
	assert.Equal(t, "catalog=ROLE%7Buser%7D,hive=ROLE%7Badmin%7D", requests[2].header.Get(trinoRoleHeader), "roles should carry over to the next statement")
	assert.Equal(t, "catalog=NONE,hive=ROLE%7Badmin%7D,iceberg=ROLE%7Bwriter%7D", requests[3].header.Get(trinoRoleHeader), "every Set-Role value should be applied and roles of other catalogs kept")
}

// A role selected through a plain db.Query does not follow the pooled
// connection to the next caller.
func TestPooledQueryDoesNotLeakRole(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?roles=catalog%3Auser")
	db.SetMaxOpenConns(1)

	fc.respond(statementPage().withHeader(trinoSetRoleHeader, "hive=ROLE%7Badmin%7D"), emptyPage())
	_, err := db.Exec("SET ROLE admin IN hive")
	require.NoError(t, err)

	fc.respond(statementPage(), emptyPage())
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "catalog=ROLE%7Buser%7D", requests[2].header.Get(trinoRoleHeader))
}

func TestClientMetadataHeaders(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "?trace_token=trace-123&client_info=batch+job&language=en-US")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	for _, request := range fc.capturedRequests() {
		assert.Equal(t, "trace-123", request.header.Get(trinoTraceTokenHeader), "%s %s", request.method, request.path)
		assert.Equal(t, "batch job", request.header.Get(trinoClientInfoHeader), "%s %s", request.method, request.path)
		assert.Equal(t, "en-US", request.header.Get(trinoLanguageHeader), "%s %s", request.method, request.path)
	}
}

func TestTimeZoneHeaderSentOnEveryRequest(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{name: "default is the local zone", params: "", want: localTimeZoneName()},
		{name: "named zone", params: "?timezone=Asia%2FTokyo", want: "Asia/Tokyo"},
		{name: "offset", params: "?timezone=%2B05%3A30", want: "+05:30"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			db := fc.open(t, tc.params)

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			collectInts(t, rows)
			require.NoError(t, rows.Err())

			requests := fc.capturedRequests()
			require.NotEmpty(t, requests)
			for _, request := range requests {
				assert.Equal(t, tc.want, request.header.Get(trinoTimeZoneHeader), "%s %s", request.method, request.path)
			}
		})
	}
}

func TestInvalidTimeZoneRejected(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?timezone=Mars%2FOlympus_Mons")

	err := db.Ping()

	require.ErrorContains(t, err, `trino: invalid timezone "Mars/Olympus_Mons"`)
	assert.Empty(t, fc.capturedRequests(), "no request made with an invalid zone")
}

// A timestamp without a zone is read in the zone the server was told to use.
func TestTimestampReadInConnectionTimeZone(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), timestampPage("2017-07-10 01:02:03.000"))
	db := fc.open(t, "?timezone=Asia%2FTokyo")

	var got time.Time
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&got))

	assertTimeIn(t, "Asia/Tokyo", got)
}

func TestTimeZoneNamedArgOverridesTheConnection(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), timestampPage("2017-07-10 01:02:03.000"))
	db := fc.open(t, "?timezone=UTC")
	db.SetMaxOpenConns(1)

	var overridden time.Time
	require.NoError(t, db.QueryRow("SELECT 1", sql.Named(trinoTimeZoneHeader, "Europe/Paris")).Scan(&overridden))
	var unchanged time.Time
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&unchanged))

	assertTimeIn(t, "Europe/Paris", overridden)
	assertTimeIn(t, "UTC", unchanged)
	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "Europe/Paris", requests[0].header.Get(trinoTimeZoneHeader), "statement with the named argument")
	assert.Equal(t, "UTC", requests[2].header.Get(trinoTimeZoneHeader), "statement without the named argument")

	_, err := db.Query("SELECT 1", sql.Named(trinoTimeZoneHeader, "Mars/Olympus_Mons"))
	require.ErrorContains(t, err, `trino: invalid timezone "Mars/Olympus_Mons"`)
}

// SET TIME ZONE comes back as the time_zone_id session property; values are
// then read in that zone until the property is cleared.
func TestTimestampReadInSessionTimeZone(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?timezone=UTC")
	db.SetMaxOpenConns(1)

	fc.respond(statementPage().withHeader(trinoSetSessionHeader, "time_zone_id=America%2FNew_York"), timestampPage("2017-07-10 01:02:03.000"))
	var afterSet time.Time
	require.NoError(t, db.QueryRow("SET TIME ZONE 'America/New_York'").Scan(&afterSet))
	fc.respond(statementPage().withHeader(trinoClearSessionHeader, "time_zone_id"), timestampPage("2017-07-10 01:02:03.000"))
	var afterClear time.Time
	require.NoError(t, db.QueryRow("SET TIME ZONE LOCAL").Scan(&afterClear))

	assertTimeIn(t, "America/New_York", afterSet)
	assertTimeIn(t, "UTC", afterClear)
	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, []string{"time_zone_id=America%2FNew_York"}, requests[1].header.Values(trinoSessionHeader), "session property forwarded")
	assert.Equal(t, "UTC", requests[1].header.Get(trinoTimeZoneHeader), "connection zone header left alone")
}

// A value without a zone in a daylight saving time gap of the session zone
// does not exist there, so it only reads back unchanged in a zone without
// transitions, chosen with naiveTimeZone.
func TestNaiveTimeZoneReadsValuesWithoutAZone(t *testing.T) {
	t.Parallel()
	timestamp := timestampColumn("_col0")
	date := column("date", scalarType("date"))
	timeOfDay := column("time(3)", scalarType("time"))
	timestampWithTimeZone := column("timestamp(3) with time zone", scalarType("timestamp with time zone"))
	cases := []struct {
		name   string
		params string
		column queryColumn
		value  string
		want   string
	}{
		{name: "timestamp in a gap of the session zone", params: "?timezone=America%2FNew_York", column: timestamp, value: "2024-03-10 02:30:00.000", want: "2024-03-10 01:30:00 -0500 EST"},
		{name: "timestamp in a gap with naiveTimeZone", params: "?timezone=America%2FNew_York&naiveTimeZone=UTC", column: timestamp, value: "2024-03-10 02:30:00.000", want: "2024-03-10 02:30:00 +0000 UTC"},
		{name: "timestamp with an offset naiveTimeZone", params: "?timezone=America%2FNew_York&naiveTimeZone=%2B05%3A30", column: timestamp, value: "2024-03-10 02:30:00.000", want: "2024-03-10 02:30:00 +0530 +05:30"},
		{name: "date at a midnight transition of the session zone", params: "?timezone=America%2FHavana", column: date, value: "2024-03-10", want: "2024-03-09 23:00:00 -0500 CST"},
		{name: "date at a midnight transition with naiveTimeZone", params: "?timezone=America%2FHavana&naiveTimeZone=UTC", column: date, value: "2024-03-10", want: "2024-03-10 00:00:00 +0000 UTC"},
		{name: "time in the session zone", params: "?timezone=America%2FNew_York", column: timeOfDay, value: "03:00:07.000", want: "0000-01-01 03:00:07 -0456 LMT"},
		{name: "time with naiveTimeZone", params: "?timezone=America%2FNew_York&naiveTimeZone=UTC", column: timeOfDay, value: "03:00:07.000", want: "0000-01-01 03:00:07 +0000 UTC"},
		{name: "timestamp with a zone keeps it", params: "?timezone=America%2FNew_York&naiveTimeZone=UTC", column: timestampWithTimeZone, value: "2017-07-10 01:02:03.000 Asia/Tokyo", want: "2017-07-10 01:02:03 +0900 JST"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), columnsPage([]queryColumn{tc.column}, [][]any{{tc.value}}))
			db := fc.open(t, tc.params)

			var got time.Time
			require.NoError(t, db.QueryRow("SELECT 1").Scan(&got))

			assert.Equal(t, tc.want, got.String())
		})
	}
}

// naiveTimeZone only changes how values are read; the server keeps the
// session zone, and neither SET TIME ZONE nor the named argument override it.
func TestNaiveTimeZoneWinsOverTheSessionTimeZone(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?timezone=Asia%2FTokyo&naiveTimeZone=UTC")
	db.SetMaxOpenConns(1)

	fc.respond(statementPage(), timestampPage("2017-07-10 01:02:03.000"))
	var fromConnection time.Time
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&fromConnection))
	fc.respond(statementPage(), timestampPage("2017-07-10 01:02:03.000"))
	var fromNamedArg time.Time
	require.NoError(t, db.QueryRow("SELECT 1", sql.Named(trinoTimeZoneHeader, "Europe/Paris")).Scan(&fromNamedArg))
	fc.respond(statementPage().withHeader(trinoSetSessionHeader, "time_zone_id=America%2FNew_York"), timestampPage("2017-07-10 01:02:03.000"))
	var afterSet time.Time
	require.NoError(t, db.QueryRow("SET TIME ZONE 'America/New_York'").Scan(&afterSet))
	fc.respond(statementPage(), columnsPage(
		[]queryColumn{column("array(timestamp(3))", arrayType(scalarType("timestamp")))},
		[][]any{{[]any{"2017-07-10 01:02:03.000"}}},
	))
	var nested NullSlice[time.Time]
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&nested))

	assertTimeIn(t, "UTC", fromConnection)
	assertTimeIn(t, "UTC", fromNamedArg)
	assertTimeIn(t, "UTC", afterSet)
	require.Len(t, nested.Slice, 1)
	assertTimeIn(t, "UTC", nested.Slice[0])
	requests := fc.capturedRequests()
	require.Len(t, requests, 8)
	assert.Equal(t, "Asia/Tokyo", requests[0].header.Get(trinoTimeZoneHeader), "session zone sent to the server")
	assert.Equal(t, "Europe/Paris", requests[2].header.Get(trinoTimeZoneHeader), "named argument sent to the server")
}

func TestInvalidNaiveTimeZoneRejected(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?naiveTimeZone=Mars%2FOlympus_Mons")

	err := db.Ping()

	require.ErrorContains(t, err, `trino: invalid timezone "Mars/Olympus_Mons"`)
	require.ErrorContains(t, err, "(set by naiveTimeZone)")
	assert.Empty(t, fc.capturedRequests(), "no request made with an invalid zone")
}

func timestampPage(value string) page {
	return columnsPage([]queryColumn{timestampColumn("_col0")}, [][]any{{value}})
}

// assertTimeIn checks that got is 2017-07-10 01:02:03 on the wall clock of zone.
func assertTimeIn(t *testing.T, zone string, got time.Time) {
	t.Helper()
	location, err := time.LoadLocation(zone)
	require.NoError(t, err)
	assert.True(t, got.Equal(time.Date(2017, 7, 10, 1, 2, 3, 0, location)), "got %v", got)
	assert.Equal(t, zone, got.Location().String())
}

func TestExtraCredentialsSentOnlyWithTheStatement(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}), emptyPage())
	db := fc.open(t, "?extra_credentials=token%3Asecret%3Bother%3Avalue")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.Len(t, requests, 3)
	assert.ElementsMatch(t, []string{"token=secret", "other=value"}, requests[0].header.Values(trinoExtraCredentialHeader), "credentials sent with the statement")
	for _, request := range requests[1:] {
		assert.Empty(t, request.header.Values(trinoExtraCredentialHeader), "credentials sent with %s %s", request.method, request.path)
	}
}

func TestResourceEstimatesSentWithTheStatement(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}), emptyPage())
	db := fc.open(t, "?resourceEstimates=PEAK_MEMORY%3A1.5GB%3BEXECUTION_TIME%3A10m")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.Len(t, requests, 3)
	assert.Equal(t, []string{"EXECUTION_TIME=10m", "PEAK_MEMORY=1.5GB"}, requests[0].header.Values(trinoResourceEstimateHeader), "estimates sent with the statement")
	for _, request := range requests[1:] {
		assert.Empty(t, request.header.Values(trinoResourceEstimateHeader), "estimates sent with %s %s", request.method, request.path)
	}
}

// The server URL-decodes each estimate, as the Java client encodes it.
func TestResourceEstimatesAreURLEncoded(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	connector, err := NewConnector(&Config{
		ServerURI:         fc.url(),
		ResourceEstimates: map[string]string{"EXECUTION_TIME": "1h;2h%,+"},
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)

	assert.Equal(t, []string{"EXECUTION_TIME=1h%3B2h%25%2C%2B"}, fc.capturedRequests()[0].header.Values(trinoResourceEstimateHeader))
}

func TestResourceEstimatesQueryArgument(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		dsn      string
		arg      any
		want     []string
		wantNext []string
	}{
		{
			name:     "map adds to and replaces the connection estimates",
			dsn:      "?resourceEstimates=EXECUTION_TIME%3A10m%3BPEAK_MEMORY%3A1GB",
			arg:      map[string]string{"PEAK_MEMORY": "2GB", "CPU_TIME": "1h"},
			want:     []string{"CPU_TIME=1h", "EXECUTION_TIME=10m", "PEAK_MEMORY=2GB"},
			wantNext: []string{"EXECUTION_TIME=10m", "PEAK_MEMORY=1GB"},
		},
		{
			name: "map without connection estimates",
			arg:  map[string]string{"CPU_TIME": "1h"},
			want: []string{"CPU_TIME=1h"},
		},
		{
			// The server keeps the last value of a repeated estimate.
			name:     "string is sent verbatim after the connection estimates",
			dsn:      "?resourceEstimates=EXECUTION_TIME%3A10m",
			arg:      "EXECUTION_TIME=1h",
			want:     []string{"EXECUTION_TIME=10m", "EXECUTION_TIME=1h"},
			wantNext: []string{"EXECUTION_TIME=10m"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			db := fc.open(t, tc.dsn)

			rows, err := db.Query("SELECT 1", sql.Named(trinoResourceEstimateHeader, tc.arg))
			require.NoError(t, err)
			collectInts(t, rows)
			rows, err = db.Query("SELECT 1")
			require.NoError(t, err)
			collectInts(t, rows)

			var statements []capturedRequest
			for _, request := range fc.capturedRequests() {
				if request.method == http.MethodPost {
					statements = append(statements, request)
				}
			}
			require.Len(t, statements, 2)
			assert.Equal(t, tc.want, statements[0].header.Values(trinoResourceEstimateHeader))
			assert.Equal(t, tc.wantNext, statements[1].header.Values(trinoResourceEstimateHeader), "the next query sends only the connection estimates")
		})
	}
}

func TestResourceEstimatesQueryArgumentRejected(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1", sql.Named(trinoResourceEstimateHeader, map[string]string{"EXECUTION=TIME": "1h"}))

	assert.EqualError(t, err, `trino: resourceEstimates key "EXECUTION=TIME" must not contain '='`)
	assert.Empty(t, fc.capturedRequests())
}

func TestSetPathHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().withHeader(trinoSetPathHeader, "memory.default,tpch.tiny"),
		resultPage([][]any{{1}}),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SET PATH memory.default, tpch.tiny")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, clientCapabilities, requests[0].header.Get(trinoClientCapabilitiesHeader), "the statement should announce the PATH capability")
	assert.Empty(t, requests[0].header.Get(trinoPathHeader), "no path before the server sets one")
	assert.Equal(t, "memory.default,tpch.tiny", requests[1].header.Get(trinoPathHeader), "server-set path should be sent in subsequent requests")
}

func TestResponseHeadersUpdateFollowingRequests(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().
			withHeader(trinoSetSessionHeader, "query_max_run_time=10m").
			withHeader(trinoSetSessionHeader, "query_priority=1").
			withHeader(trinoSetSessionHeader, "join_distribution_type=BROADCAST").
			withHeader(trinoSetCatalogHeader, "memory").
			withHeader(trinoSetSchemaHeader, "default").
			withHeader(trinoAddedPrepareHeader, "stmt1=SELECT 1").
			withHeader(trinoAddedPrepareHeader, "stmt2=SELECT 2"),
		resultPage([][]any{{1}}).
			withHeader(trinoSetSessionHeader, "query_max_run_time=20m").
			withHeader(trinoClearSessionHeader, "query_priority").
			withHeader(trinoClearSessionHeader, "join_distribution_type").
			withHeader(trinoDeallocatedPrepareHeader, "stmt1").
			withHeader(trinoDeallocatedPrepareHeader, "stmt2"),
		emptyPage(),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.Len(t, requests, 3)
	afterFirstPage := requests[1].header
	assert.Equal(t, []string{"query_max_run_time=10m", "query_priority=1", "join_distribution_type=BROADCAST"}, afterFirstPage.Values(trinoSessionHeader), "every session property set by the first page")
	assert.Equal(t, "memory", afterFirstPage.Get(trinoCatalogHeader), "catalog set by the first page")
	assert.Equal(t, "default", afterFirstPage.Get(trinoSchemaHeader), "schema set by the first page")
	assert.Equal(t, []string{"stmt1=SELECT 1", "stmt2=SELECT 2"}, afterFirstPage.Values(preparedStatementHeader), "every statement prepared by the first page")
	afterSecondPage := requests[2].header
	assert.Equal(t, []string{"query_max_run_time=20m"}, afterSecondPage.Values(trinoSessionHeader), "property replaced and the others cleared by the second page")
	assert.Empty(t, afterSecondPage.Values(preparedStatementHeader), "every statement deallocated by the second page")
	assert.Equal(t, "memory", afterSecondPage.Get(trinoCatalogHeader), "catalog kept by the second page")
}

func TestRowsCloseCancelsUnfinishedQuery(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		resultPage([][]any{{1}}),
		resultPage([][]any{{2}}),
		resultPage([][]any{{3}}),
		emptyPage(),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Close())

	var cancelled bool
	for _, request := range fc.capturedRequests() {
		if request.method == http.MethodDelete && request.path == "/v1/query/"+fakeQueryID {
			cancelled = true
		}
	}
	assert.True(t, cancelled, "closing the rows before the last page must cancel the query")
}

func TestNextReturnsContextError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	firstRowRead := make(chan struct{})
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		resultPage([][]any{{1}}),
		resultPage([][]any{{2}}),
		emptyPage(),
	)
	fc.onPage(func(index int, r *http.Request) {
		if index != 2 {
			return
		}
		// cancel the query while its next page is being fetched
		<-firstRowRead
		cancel()
		<-r.Context().Done()
	})
	db := fc.open(t, "")

	rows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	close(firstRowRead)

	assert.False(t, rows.Next())
	assert.ErrorIs(t, rows.Err(), context.Canceled)
}

// TestQueryWarningsCollectedAcrossPages exercises the "warnings" named
// argument: both pages of a two-page query repeat the first warning, the
// second page adds a new one, and the sink ends up with each warning once,
// in the order the coordinator first reported it.
func TestQueryWarningsCollectedAcrossPages(t *testing.T) {
	t.Parallel()
	deprecated := Warning{Code: 3, Name: "DEPRECATED_FUNCTION", Message: "Use of deprecated function: foo"}
	tooManyStages := Warning{Code: 1, Name: "TOO_MANY_STAGES", Message: "Query has too many stages"}
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		pageOf(&queryResponse{
			ID:       fakeQueryID,
			Columns:  []queryColumn{integerColumn("_col0")},
			Data:     jsonData([][]any{{1}}),
			Warnings: []Warning{deprecated},
		}),
		pageOf(&queryResponse{
			ID:       fakeQueryID,
			Warnings: []Warning{deprecated, tooManyStages},
		}),
	)
	db := fc.open(t, "")

	var warnings Warnings
	rows, err := db.Query("SELECT 1", sql.Named(trinoWarningsParam, &warnings))
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	require.NoError(t, rows.Err())

	assert.Equal(t, []Warning{deprecated, tooManyStages}, warnings.All())
}

// A query without a "warnings" sink drops the warnings the coordinator sent.
func TestQueryWarningsIgnoredWithoutSink(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		pageOf(&queryResponse{
			ID:       fakeQueryID,
			Columns:  []queryColumn{integerColumn("_col0")},
			Data:     jsonData([][]any{{1}}),
			Warnings: []Warning{{Code: 3, Name: "DEPRECATED_FUNCTION", Message: "Use of deprecated function: foo"}},
		}),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	require.NoError(t, rows.Err())
}

// TestRowColumnScansThroughTheWireFormat exercises a ROW column end to end,
// through the same JSON encoding and decoding a real coordinator response
// goes through, covering a named field, an anonymous field, and a NULL row.
func TestRowColumnScansThroughTheWireFormat(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	col := rowColumn("_col0", "row(x integer, varchar)",
		namedField("x", typeSignature{RawType: "integer"}),
		namedField("", typeSignature{RawType: "varchar"}),
	)
	fc.respond(statementPage(), columnsPage([]queryColumn{col}, [][]any{
		{[]any{1, "a"}},
		{nil},
	}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT x")
	require.NoError(t, err)
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	assert.Equal(t, "ROW(x INTEGER, VARCHAR)", columnTypes[0].DatabaseTypeName())
	assert.Equal(t, reflect.TypeOf(Row{}), columnTypes[0].ScanType())

	require.True(t, rows.Next())
	var got Row
	require.NoError(t, rows.Scan(&got))
	assert.Equal(t, Row{names: []string{"x", "field1"}, values: []interface{}{int64(1), "a"}, Valid: true}, got)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&got))
	assert.Equal(t, Row{}, got, "a NULL row scans as a zero Row")
	assert.False(t, got.Valid, "a NULL row is not valid")

	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

// The coordinator reads the statement as text; the JDBC client labels it the
// same way, so proxies and gateways in front of Trino see the same request.
func TestStatementSentAsPlainText(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.NotEmpty(t, requests)
	assert.Equal(t, http.MethodPost, requests[0].method)
	assert.Equal(t, "/v1/statement", requests[0].path)
	assert.Equal(t, "text/plain; charset=utf-8", requests[0].header.Get("Content-Type"))
}

func TestColumnTypeNullable(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columnTypes, 1)
	nullable, ok := columnTypes[0].Nullable()
	assert.True(t, ok, "nullability must be reported")
	assert.True(t, nullable, "every column must be reported as nullable")
}

func TestNamedWarningsArgumentMustBeWarningsPointer(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")

	_, err := db.Query("SELECT 1", sql.Named(trinoWarningsParam, "not-a-sink"))

	require.EqualError(t, err, "trino: warnings must be a *trino.Warnings, got string")
	assert.Empty(t, fc.capturedRequests(), "the query must be rejected before anything is sent")
}

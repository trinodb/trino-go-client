package trino

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationTypeConversion(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	var (
		goTime            time.Time
		nullTime          NullTime
		goBytes           []byte
		nullBytes         []byte
		goString          string
		nullString        sql.NullString
		nullStringSlice   NullSliceString
		nullStringSlice2  NullSlice2String
		nullStringSlice3  NullSlice3String
		nullInt64Slice    NullSliceInt64
		nullInt64Slice2   NullSlice2Int64
		nullInt64Slice3   NullSlice3Int64
		nullFloat64Slice  NullSliceFloat64
		nullFloat64Slice2 NullSlice2Float64
		nullFloat64Slice3 NullSlice3Float64
		goMap             map[string]interface{}
		nullMap           NullMap
		goRow             []interface{}
	)
	err = db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST(X'FFFF0FFF3FFFFFFF' AS VARBINARY),
			CAST(NULL AS VARBINARY),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY['A', 'B', NULL],
			ARRAY[ARRAY['A'], NULL],
			ARRAY[ARRAY[ARRAY['A'], NULL], NULL],
			ARRAY[1, 2, NULL],
			ARRAY[ARRAY[1, 1, 1], NULL],
			ARRAY[ARRAY[ARRAY[1, 1, 1], NULL], NULL],
			ARRAY[1.0, 2.0, NULL],
			ARRAY[ARRAY[1.1, 1.1, 1.1], NULL],
			ARRAY[ARRAY[ARRAY[1.1, 1.1, 1.1], NULL], NULL],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER))),
			ROW(1, 'a', CAST('2017-07-10 01:02:03.004 UTC' AS TIMESTAMP(6) WITH TIME ZONE), ARRAY['c'])
	`).Scan(
		&goTime,
		&nullTime,
		&goBytes,
		&nullBytes,
		&goString,
		&nullString,
		&nullStringSlice,
		&nullStringSlice2,
		&nullStringSlice3,
		&nullInt64Slice,
		&nullInt64Slice2,
		&nullInt64Slice3,
		&nullFloat64Slice,
		&nullFloat64Slice2,
		&nullFloat64Slice3,
		&goMap,
		&nullMap,
		&goRow,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Compare the actual and expected values.
	expectedTime := time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC)
	if !goTime.Equal(expectedTime) {
		t.Errorf("expected GoTime to be %v, got %v", expectedTime, goTime)
	}

	expectedBytes := []byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff}
	if !bytes.Equal(goBytes, expectedBytes) {
		t.Errorf("expected GoBytes to be %v, got %v", expectedBytes, goBytes)
	}

	if nullBytes != nil {
		t.Errorf("expected NullBytes to be nil, got %v", nullBytes)
	}

	if goString != "string" {
		t.Errorf("expected GoString to be %q, got %q", "string", goString)
	}

	if nullString.Valid {
		t.Errorf("expected NullString.Valid to be false, got true")
	}

	if !reflect.DeepEqual(nullStringSlice.SliceString, []sql.NullString{{String: "A", Valid: true}, {String: "B", Valid: true}, {Valid: false}}) {
		t.Errorf("expected NullStringSlice.SliceString to be %v, got %v",
			[]sql.NullString{{String: "A", Valid: true}, {String: "B", Valid: true}, {Valid: false}},
			nullStringSlice.SliceString)
	}
	if !nullStringSlice.Valid {
		t.Errorf("expected NullStringSlice.Valid to be true, got false")
	}

	expectedSlice2String := [][]sql.NullString{{{String: "A", Valid: true}}, {}}
	if !reflect.DeepEqual(nullStringSlice2.Slice2String, expectedSlice2String) {
		t.Errorf("expected NullStringSlice2.Slice2String to be %v, got %v", expectedSlice2String, nullStringSlice2.Slice2String)
	}
	if !nullStringSlice2.Valid {
		t.Errorf("expected NullStringSlice2.Valid to be true, got false")
	}

	expectedSlice3String := [][][]sql.NullString{{{{String: "A", Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullStringSlice3.Slice3String, expectedSlice3String) {
		t.Errorf("expected NullStringSlice3.Slice3String to be %v, got %v", expectedSlice3String, nullStringSlice3.Slice3String)
	}
	if !nullStringSlice3.Valid {
		t.Errorf("expected NullStringSlice3.Valid to be true, got false")
	}

	expectedSliceInt64 := []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 2, Valid: true}, {Valid: false}}
	if !reflect.DeepEqual(nullInt64Slice.SliceInt64, expectedSliceInt64) {
		t.Errorf("expected NullInt64Slice.SliceInt64 to be %v, got %v", expectedSliceInt64, nullInt64Slice.SliceInt64)
	}
	if !nullInt64Slice.Valid {
		t.Errorf("expected NullInt64Slice.Valid to be true, got false")
	}

	expectedSlice2Int64 := [][]sql.NullInt64{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}
	if !reflect.DeepEqual(nullInt64Slice2.Slice2Int64, expectedSlice2Int64) {
		t.Errorf("expected NullInt64Slice2.Slice2Int64 to be %v, got %v", expectedSlice2Int64, nullInt64Slice2.Slice2Int64)
	}
	if !nullInt64Slice2.Valid {
		t.Errorf("expected NullInt64Slice2.Valid to be true, got false")
	}

	expectedSlice3Int64 := [][][]sql.NullInt64{{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullInt64Slice3.Slice3Int64, expectedSlice3Int64) {
		t.Errorf("expected NullInt64Slice3.Slice3Int64 to be %v, got %v", expectedSlice3Int64, nullInt64Slice3.Slice3Int64)
	}
	if !nullInt64Slice3.Valid {
		t.Errorf("expected NullInt64Slice3.Valid to be true, got false")
	}

	expectedSliceFloat64 := []sql.NullFloat64{{Float64: 1.0, Valid: true}, {Float64: 2.0, Valid: true}, {Valid: false}}
	if !reflect.DeepEqual(nullFloat64Slice.SliceFloat64, expectedSliceFloat64) {
		t.Errorf("expected NullFloat64Slice.SliceFloat64 to be %v, got %v", expectedSliceFloat64, nullFloat64Slice.SliceFloat64)
	}
	if !nullFloat64Slice.Valid {
		t.Errorf("expected NullFloat64Slice.Valid to be true, got false")
	}

	expectedSlice2Float64 := [][]sql.NullFloat64{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}
	if !reflect.DeepEqual(nullFloat64Slice2.Slice2Float64, expectedSlice2Float64) {
		t.Errorf("expected NullFloat64Slice2.Slice2Float64 to be %v, got %v", expectedSlice2Float64, nullFloat64Slice2.Slice2Float64)
	}
	if !nullFloat64Slice2.Valid {
		t.Errorf("expected NullFloat64Slice2.Valid to be true, got false")
	}

	expectedSlice3Float64 := [][][]sql.NullFloat64{{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullFloat64Slice3.Slice3Float64, expectedSlice3Float64) {
		t.Errorf("expected NullFloat64Slice3.Slice3Float64 to be %v, got %v", expectedSlice3Float64, nullFloat64Slice3.Slice3Float64)
	}
	if !nullFloat64Slice3.Valid {
		t.Errorf("expected NullFloat64Slice3.Valid to be true, got false")
	}

	expectedMap := map[string]interface{}{"a": "c", "b": "d"}
	if !reflect.DeepEqual(goMap, expectedMap) {
		t.Errorf("expected GoMap to be %v, got %v", expectedMap, goMap)
	}

	if nullMap.Valid {
		t.Errorf("expected NullMap.Valid to be false, got true")
	}

	expectedRow := []interface{}{json.Number("1"), "a", "2017-07-10 01:02:03.004000 UTC", []interface{}{"c"}}
	if !reflect.DeepEqual(goRow, expectedRow) {
		t.Errorf("expected GoRow to be %v, got %v", expectedRow, goRow)
	}
}

func TestComplexTypes(t *testing.T) {
	// This test has been created to showcase some issues with parsing
	// complex types. It is not intended to be a comprehensive test of
	// the parsing logic, but rather to provide a reference for future
	// changes to the parsing logic.
	//
	// The current implementation of the parsing logic reads the value
	// in the same format as the JSON response from Trino. This means
	// that we don't go further to parse values as their structured types.
	// For example, a row like `ROW(1, X'0000')` is read as
	// a list of a `json.Number(1)` and a base64-encoded string.
	t.Skip("skipping failing test")

	dsn := *integrationServerFlag
	db := integrationOpen(t, dsn)

	for _, tt := range []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "row containing scalar values",
			query:    `SELECT ROW(1, 'a', X'0000')`,
			expected: []interface{}{1, "a", []byte{0x00, 0x00}},
		},
		{
			name:     "nested row",
			query:    `SELECT ROW(ROW(1, 'a'), ROW(2, 'b'))`,
			expected: []interface{}{[]interface{}{1, "a"}, []interface{}{2, "b"}},
		},
		{
			name:     "map with scalar values",
			query:    `SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2])`,
			expected: map[string]interface{}{"a": 1, "b": 2},
		},
		{
			name:     "map with nested row",
			query:    `SELECT MAP(ARRAY['a', 'b'], ARRAY[ROW(1, 'a'), ROW(2, 'b')])`,
			expected: map[string]interface{}{"a": []interface{}{1, "a"}, "b": []interface{}{2, "b"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIntegrationArgsConversion(t *testing.T) {
	dsn := *integrationServerFlag
	db := integrationOpen(t, dsn)
	value := 0
	err := db.QueryRow(`
		SELECT 1 FROM (VALUES (
			CAST(1 AS TINYINT),
			CAST(1 AS SMALLINT),
			CAST(1 AS INTEGER),
			CAST(1 AS BIGINT),
			CAST(1 AS REAL),
			CAST(1 AS DOUBLE),
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST('string' AS VARCHAR),
			CAST(X'FFFF0FFF3FFFFFFF' AS VARBINARY),
			ARRAY['A', 'B']
			)) AS t(col_tiny, col_small, col_int, col_big, col_real, col_double, col_ts, col_varchar, col_varbinary, col_array )
			WHERE 1=1
			AND col_tiny = ?
			AND col_small = ?
			AND col_int = ?
			AND col_big = ?
			AND col_real = cast(? as real)
			AND col_double = cast(? as double)
			AND col_ts = ?
			AND col_varchar = ?
			AND col_varbinary = ?
			AND col_array = ?`,
		int16(1),
		int16(1),
		int32(1),
		int64(1),
		Numeric("1"),
		Numeric("1"),
		time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC),
		"string",
		[]byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff},
		[]string{"A", "B"},
	).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationIntervalArgs(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	for _, tc := range []struct {
		arg     time.Duration
		literal string
	}{
		{-500 * time.Millisecond, "INTERVAL '-0.5' SECOND"},
		{-5 * time.Millisecond, "INTERVAL '-0.005' SECOND"},
		{-(10*time.Second + 5*time.Millisecond), "INTERVAL '-10.005' SECOND"},
		{500 * time.Millisecond, "INTERVAL '0.5' SECOND"},
	} {
		var equal bool
		err := db.QueryRow("SELECT ? = "+tc.literal, tc.arg).Scan(&equal)
		require.NoError(t, err, tc.literal)
		require.True(t, equal, "%v did not round-trip as %s", tc.arg, tc.literal)
	}
}

func TestIntegrationTimeTzArgs(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	for _, tc := range []struct {
		arg     trinoTimeTz
		literal string
	}{
		{TimeTz(11, 34, 25, 123456, time.UTC), "TIME '11:34:25.000123456 +00:00'"},
		{TimeTz(11, 34, 25, 123456, nil), "TIME '11:34:25.000123456 +00:00'"},
		{TimeTz(11, 34, 25, 123456, time.FixedZone("test zone", +2*3600)), "TIME '11:34:25.000123456 +02:00'"},
	} {
		var equal bool
		err := db.QueryRow("SELECT ? = "+tc.literal, tc.arg).Scan(&equal)
		require.NoError(t, err, tc.literal)
		require.True(t, equal, "%v did not round-trip as %s", time.Time(tc.arg), tc.literal)
	}
}

func TestIntegrationNumericArgs(t *testing.T) {
	dsns := []string{*integrationServerFlag}
	// EXECUTE IMMEDIATE, used when explicit prepare is disabled, needs Trino 418 or later.
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if (err != nil && *trinoImageTagFlag == "latest") || (err == nil && version >= 418) {
		dsns = append(dsns, *integrationServerFlag+"?explicitPrepare=false")
	}
	for _, dsn := range dsns {
		db := integrationOpen(t, dsn)
		defer db.Close()

		for _, tc := range []struct {
			arg     Numeric
			literal string
		}{
			{Numeric("-1.5"), "DECIMAL '-1.5'"},
			{Numeric("1e3"), "DOUBLE '1000'"},
			{Numeric(".5"), "DECIMAL '0.5'"},
		} {
			var equal bool
			err := db.QueryRow("SELECT ? = "+tc.literal, tc.arg).Scan(&equal)
			require.NoError(t, err, "%s with %s", dsn, tc.arg)
			require.True(t, equal, "%s did not round-trip as %s", tc.arg, tc.literal)
		}

		// Rejected client-side, before anything is sent to the server.
		var value int
		err := db.QueryRow("SELECT * FROM (VALUES (99)) AS t(nan) WHERE nan = ?", Numeric("NaN")).Scan(&value)
		require.ErrorContains(t, err, `Numeric "NaN" is not a decimal or scientific number literal`, dsn)
	}
}

func TestIntgrationNumberType(t *testing.T) {
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if (err != nil && *trinoImageTagFlag != "latest") || (err == nil && version < 480) {
		t.Skip("Skipping test when using a custom integration server.")
	}

	db := integrationOpen(t)
	defer db.Close()

	rows, err := db.Query("SELECT NUMBER '3.14159' AS num, NUMBER 'NaN' as nan_val, CAST(NULL AS NUMBER) as null_num")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	for _, col := range columnTypes {
		if col.DatabaseTypeName() != "NUMBER" {
			t.Errorf("expected DatabaseTypeName Number for column %s, got %s", col.Name(), col.DatabaseTypeName())
		}
	}

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var num, nanVal sql.NullString
	var nullNum sql.NullString
	if err := rows.Scan(&num, &nanVal, &nullNum); err != nil {
		t.Fatal(err)
	}

	if !num.Valid || num.String != "3.14159" {
		t.Errorf("expected num to be 3.14159, got %s", num.String)
	}

	if !nanVal.Valid || nanVal.String != "NaN" {
		t.Errorf("expected nanVal to be NaN, got %s", nanVal.String)
	}

	if nullNum.Valid {
		t.Errorf("expected nullNum to be invalid, got %s", nullNum.String)
	}
}

func TestIntegrationDayToHourIntervalMilliPrecision(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	tests := []struct {
		name    string
		arg     time.Duration
		wantErr bool
	}{
		{
			name:    "valid 1234567891s",
			arg:     time.Duration(1234567891) * time.Second,
			wantErr: false,
		},
		{
			name:    "valid 123456789.1s",
			arg:     time.Duration(123456789100) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid 12345678.91s",
			arg:     time.Duration(12345678910) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid 1234567.891s",
			arg:     time.Duration(1234567891) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -1234567891s",
			arg:     time.Duration(-1234567891) * time.Second,
			wantErr: false,
		},
		{
			name:    "valid -123456789.1s",
			arg:     time.Duration(-123456789100) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -12345678.91s",
			arg:     time.Duration(-12345678910) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -1234567.891s",
			arg:     time.Duration(-1234567891) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "invalid 1234567891.2s",
			arg:     time.Duration(1234567891200) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid 123456789.12s",
			arg:     time.Duration(123456789120) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid 12345678.912s",
			arg:     time.Duration(12345678912) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -1234567891.2s",
			arg:     time.Duration(-1234567891200) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -123456789.12s",
			arg:     time.Duration(-123456789120) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -12345678.912s",
			arg:     time.Duration(-12345678912) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid max seconds (9223372036)",
			arg:     time.Duration(math.MaxInt64) / time.Second * time.Second,
			wantErr: true,
		},
		{
			name:    "invalid min seconds (-9223372036)",
			arg:     time.Duration(math.MinInt64) / time.Second * time.Second,
			wantErr: true,
		},
		{
			name: "valid max seconds (2147483647)",
			arg:  math.MaxInt32 * time.Second,
		},
		{
			name: "valid min seconds (-2147483647)",
			arg:  -math.MaxInt32 * time.Second,
		},
		{
			name: "valid max minutes (153722867)",
			arg:  time.Duration(math.MaxInt64) / time.Minute * time.Minute,
		},
		{
			name: "valid min minutes (-153722867)",
			arg:  time.Duration(math.MinInt64) / time.Minute * time.Minute,
		},
		{
			name: "valid max hours (2562047)",
			arg:  time.Duration(math.MaxInt64) / time.Hour * time.Hour,
		},
		{
			name: "valid min hours (-2562047)",
			arg:  time.Duration(math.MinInt64) / time.Hour * time.Hour,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Exec("SELECT ?", test.arg)
			if (err != nil) != test.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, test.wantErr)
				return
			}
		})
	}
}

func TestQueryColumns(t *testing.T) {
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	rows, err := db.Query(`SELECT
  true AS bool,
  cast(123 AS tinyint) AS tinyint,
  cast(456 AS smallint) AS smallint,
  cast(678 AS integer) AS integer,
  cast(1234 AS bigint) AS bigint,
  cast(1.23 AS real) AS real,
  cast(1.23 AS double) AS double,
  cast(1.23 as decimal(10,5)) AS decimal,
  cast('aaa' as varchar) AS vunbounded,
  cast('bbb' as varchar(10)) AS vbounded,
  cast('ccc' AS char) AS cunbounded,
  cast('ddd' as char(10)) AS cbounded,
  cast('ddd' as varbinary) AS varbinary,
  cast('{"aaa": 1}' as json) AS json,
  current_date AS date,
  cast(current_time as time) AS time,
  cast(current_time as time(6)) AS timep,
  cast(current_time as time with time zone) AS timetz,
  cast(current_time as timestamp) AS ts,
  cast(current_time as timestamp(6)) AS tsp,
  cast(current_time as timestamp with time zone) AS tstz,
  cast(current_time as timestamp(6) with time zone) AS tsptz,
  interval '3' month AS ytm,
  interval '2' day AS dts,
  array['a', 'b'] AS varray,
  array[array['a'], array['b']] AS v2array,
  array[array[array['a'], array['b']]] AS v3array,
  map(array['a'], array[1]) AS map,
  array[map(array['a'], array[1]), map(array['b'], array[2])] AS marray,
  row('a', 1) AS row,
  cast(row('a', 1.23) AS row(x varchar, y double)) AS named_row,
  ipaddress '10.0.0.1' AS ip,
  uuid '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS uuid`)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	columns, err := rows.Columns()
	require.NoError(t, err, "Failed reading result columns")

	assert.Equal(t, 33, len(columns), "Expected 33 result column")
	expectedNames := []string{
		"bool",
		"tinyint",
		"smallint",
		"integer",
		"bigint",
		"real",
		"double",
		"decimal",
		"vunbounded",
		"vbounded",
		"cunbounded",
		"cbounded",
		"varbinary",
		"json",
		"date",
		"time",
		"timep",
		"timetz",
		"ts",
		"tsp",
		"tstz",
		"tsptz",
		"ytm",
		"dts",
		"varray",
		"v2array",
		"v3array",
		"map",
		"marray",
		"row",
		"named_row",
		"ip",
		"uuid",
	}
	assert.Equal(t, expectedNames, columns)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err, "Failed reading result column types")

	assert.Equal(t, 33, len(columnTypes), "Expected 33 result column type")

	type columnType struct {
		typeName  string
		hasScale  bool
		precision int64
		scale     int64
		hasLength bool
		length    int64
		scanType  reflect.Type
	}
	expectedTypes := []columnType{
		{
			"BOOLEAN",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullBool{}),
		},
		{
			"TINYINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"SMALLINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"INTEGER",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt32{}),
		},
		{
			"BIGINT",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullInt64{}),
		},
		{
			"REAL",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullFloat64{}),
		},
		{
			"DOUBLE",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullFloat64{}),
		},
		{
			"DECIMAL",
			true,
			10,
			5,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARCHAR",
			false,
			0,
			0,
			true,
			math.MaxInt32,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARCHAR",
			false,
			0,
			0,
			true,
			10,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"CHAR",
			false,
			0,
			0,
			true,
			1,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"CHAR",
			false,
			0,
			0,
			true,
			10,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"VARBINARY",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf([]byte{}),
		},
		{
			"JSON",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"DATE",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME WITH TIME ZONE",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			3,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			6,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"INTERVAL YEAR TO MONTH",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"INTERVAL DAY TO SECOND",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"ARRAY(VARCHAR(1))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSliceString{}),
		},
		{
			"ARRAY(ARRAY(VARCHAR(1)))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSlice2String{}),
		},
		{
			"ARRAY(ARRAY(ARRAY(VARCHAR(1))))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSlice3String{}),
		},
		{
			"MAP(VARCHAR(1), INTEGER)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullMap{}),
		},
		{
			"ARRAY(MAP(VARCHAR(1), INTEGER))",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(NullSliceMap{}),
		},
		{
			"ROW(VARCHAR(1), INTEGER)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(new(interface{})).Elem(),
		},
		{
			"ROW(X VARCHAR, Y DOUBLE)",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(new(interface{})).Elem(),
		},
		{
			"IPADDRESS",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
		{
			"UUID",
			false,
			0,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullString{}),
		},
	}
	actualTypes := make([]columnType, 33)
	for i, column := range columnTypes {
		actualTypes[i].typeName = column.DatabaseTypeName()
		actualTypes[i].precision, actualTypes[i].scale, actualTypes[i].hasScale = column.DecimalSize()
		actualTypes[i].length, actualTypes[i].hasLength = column.Length()
		actualTypes[i].scanType = column.ScanType()
	}

	assert.Equal(t, actualTypes, expectedTypes)
}

func TestMaxGoPrecisionDateTime(t *testing.T) {
	c := &Config{
		ServerURI:         *integrationServerFlag,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	rows, err := db.Query(`SELECT
  cast(current_time as time(9)) AS timep,
  cast(current_time as time(9) with time zone) AS timeptz,
  cast(current_time as timestamp(9)) AS tsp,
  cast(current_time as timestamp(9) with time zone) AS tsptz`)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	columns, err := rows.Columns()
	require.NoError(t, err, "Failed reading result columns")

	assert.Equal(t, 4, len(columns), "Expected 4 result column")
	expectedNames := []string{
		"timep",
		"timeptz",
		"tsp",
		"tsptz",
	}
	assert.Equal(t, expectedNames, columns)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err, "Failed reading result column types")

	assert.Equal(t, 4, len(columnTypes), "Expected 4 result column type")

	type columnType struct {
		typeName  string
		hasScale  bool
		precision int64
		scale     int64
		hasLength bool
		length    int64
		scanType  reflect.Type
	}
	expectedTypes := []columnType{
		{
			"TIME",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIME WITH TIME ZONE",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
		{
			"TIMESTAMP WITH TIME ZONE",
			true,
			9,
			0,
			false,
			0,
			reflect.TypeOf(sql.NullTime{}),
		},
	}
	actualTypes := make([]columnType, 4)
	for i, column := range columnTypes {
		actualTypes[i].typeName = column.DatabaseTypeName()
		actualTypes[i].precision, actualTypes[i].scale, actualTypes[i].hasScale = column.DecimalSize()
		actualTypes[i].length, actualTypes[i].hasLength = column.Length()
		actualTypes[i].scanType = column.ScanType()
	}

	assert.Equal(t, actualTypes, expectedTypes)

	assert.True(t, rows.Next())
	require.NoError(t, rows.Err())

}

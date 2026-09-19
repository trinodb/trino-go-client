package trino

import (
	"database/sql"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationTypeConversion(t *testing.T) {
	db := integrationOpen(t, integrationDSN(t)+"?custom_client="+uncompressedClient)
	for _, protocol := range queryProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			testIntegrationTypeConversion(t, db, protocol.args...)
		})
	}
}

func testIntegrationTypeConversion(t *testing.T, db *sql.DB, args ...any) {
	t.Helper()
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
	err := db.QueryRow(`
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
	`, args...).Scan(
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
	require.NoError(t, err)

	assert.WithinDuration(t, time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC), goTime, 0, "GoTime")
	assert.Equal(t, []byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff}, goBytes, "GoBytes")
	assert.Nil(t, nullBytes, "NullBytes")
	assert.Equal(t, "string", goString, "GoString")
	assert.False(t, nullString.Valid, "NullString.Valid")

	assert.Equal(t, []sql.NullString{{String: "A", Valid: true}, {String: "B", Valid: true}, {Valid: false}}, nullStringSlice.SliceString)
	assert.True(t, nullStringSlice.Valid, "NullStringSlice.Valid")
	assert.Equal(t, [][]sql.NullString{{{String: "A", Valid: true}}, {}}, nullStringSlice2.Slice2String)
	assert.True(t, nullStringSlice2.Valid, "NullStringSlice2.Valid")
	assert.Equal(t, [][][]sql.NullString{{{{String: "A", Valid: true}}, {}}, {}}, nullStringSlice3.Slice3String)
	assert.True(t, nullStringSlice3.Valid, "NullStringSlice3.Valid")

	assert.Equal(t, []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 2, Valid: true}, {Valid: false}}, nullInt64Slice.SliceInt64)
	assert.True(t, nullInt64Slice.Valid, "NullInt64Slice.Valid")
	assert.Equal(t, [][]sql.NullInt64{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}, nullInt64Slice2.Slice2Int64)
	assert.True(t, nullInt64Slice2.Valid, "NullInt64Slice2.Valid")
	assert.Equal(t, [][][]sql.NullInt64{{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}, {}}, nullInt64Slice3.Slice3Int64)
	assert.True(t, nullInt64Slice3.Valid, "NullInt64Slice3.Valid")

	assert.Equal(t, []sql.NullFloat64{{Float64: 1.0, Valid: true}, {Float64: 2.0, Valid: true}, {Valid: false}}, nullFloat64Slice.SliceFloat64)
	assert.True(t, nullFloat64Slice.Valid, "NullFloat64Slice.Valid")
	assert.Equal(t, [][]sql.NullFloat64{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}, nullFloat64Slice2.Slice2Float64)
	assert.True(t, nullFloat64Slice2.Valid, "NullFloat64Slice2.Valid")
	assert.Equal(t, [][][]sql.NullFloat64{{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}, {}}, nullFloat64Slice3.Slice3Float64)
	assert.True(t, nullFloat64Slice3.Valid, "NullFloat64Slice3.Valid")

	assert.Equal(t, map[string]interface{}{"a": "c", "b": "d"}, goMap, "GoMap")
	assert.False(t, nullMap.Valid, "NullMap.Valid")
	assert.Equal(t, []interface{}{json.Number("1"), "a", "2017-07-10 01:02:03.004000 UTC", []interface{}{"c"}}, goRow, "GoRow")
}

// TestComplexTypes pins down how ROW and MAP values decode when nested,
// which TestIntegrationTypeConversion does not exercise: the driver does not
// parse these into structured Go types, it passes through whatever shape the
// JSON response used. A VARBINARY column decodes to []byte at the top level
// (see TestIntegrationTypeConversion), but the same VARBINARY nested inside a
// ROW stays a base64 string, because ConvertValue never recurses into a row
// or map to convert its elements.
func TestComplexTypes(t *testing.T) {
	db := integrationOpen(t)

	for _, tt := range []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "row containing a binary value",
			query:    `SELECT ROW(1, 'a', X'0000')`,
			expected: []interface{}{json.Number("1"), "a", "AAA="},
		},
		{
			name:  "nested row",
			query: `SELECT ROW(ROW(1, 'a'), ROW(2, 'b'))`,
			expected: []interface{}{
				[]interface{}{json.Number("1"), "a"},
				[]interface{}{json.Number("2"), "b"},
			},
		},
		{
			name:     "map with scalar values",
			query:    `SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2])`,
			expected: map[string]interface{}{"a": json.Number("1"), "b": json.Number("2")},
		},
		{
			name:  "map with row values",
			query: `SELECT MAP(ARRAY['a', 'b'], ARRAY[ROW(1, 'a'), ROW(2, 'b')])`,
			expected: map[string]interface{}{
				"a": []interface{}{json.Number("1"), "a"},
				"b": []interface{}{json.Number("2"), "b"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			require.NoError(t, db.QueryRow(tt.query).Scan(&result))

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIntegrationArgsConversion(t *testing.T) {
	dsn := integrationDSN(t)
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
			AND col_real = ?
			AND col_double = ?
			AND col_ts = ?
			AND col_varchar = ?
			AND col_varbinary = ?
			AND col_array = ?`,
		int16(1),
		int16(1),
		int32(1),
		int64(1),
		float32(1),
		float64(1),
		time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC),
		"string",
		[]byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff},
		[]string{"A", "B"},
	).Scan(&value)
	require.NoError(t, err)
}

func TestIntegrationIntervalArgs(t *testing.T) {
	db := integrationOpen(t)
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
	dsns := []string{integrationDSN(t)}
	// EXECUTE IMMEDIATE, used when explicit prepare is disabled, needs Trino 418 or later.
	if serverVersion >= 418 {
		dsns = append(dsns, integrationDSN(t)+"?explicitPrepare=false")
	}
	for _, dsn := range dsns {
		db := integrationOpen(t, dsn)

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

func TestIntegrationFloatArgs(t *testing.T) {
	db := integrationOpen(t)
	for _, tc := range []struct {
		arg     any
		literal string
	}{
		{float32(0.1), "REAL '0.1'"},
		{float32(-1.5), "REAL '-1.5'"},
		{0.1, "DOUBLE '0.1'"},
		{1e-7, "DOUBLE '1e-7'"},
		{math.MaxFloat64, "DOUBLE '1.7976931348623157e308'"},
		{math.Inf(1), "infinity()"},
		{math.Inf(-1), "-infinity()"},
		{float32(math.Inf(1)), "CAST(infinity() AS REAL)"},
	} {
		var equal bool
		err := db.QueryRow("SELECT ? = "+tc.literal, tc.arg).Scan(&equal)
		require.NoError(t, err, tc.literal)
		require.True(t, equal, "%v did not round-trip as %s", tc.arg, tc.literal)
	}

	var isNaN bool
	err := db.QueryRow("SELECT is_nan(?)", math.NaN()).Scan(&isNaN)
	require.NoError(t, err)
	require.True(t, isNaN, "NaN did not round-trip")
}

func TestIntgrationNumberType(t *testing.T) {
	requireServerVersion(t, 480)

	db := integrationOpen(t)

	rows, err := db.Query("SELECT NUMBER '3.14159' AS num, NUMBER 'NaN' as nan_val, CAST(NULL AS NUMBER) as null_num")
	require.NoError(t, err)
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)

	for _, col := range columnTypes {
		assert.Equal(t, "NUMBER", col.DatabaseTypeName(), "DatabaseTypeName of column %s", col.Name())
	}

	require.True(t, rows.Next(), "expected at least one row")

	var num, nanVal sql.NullString
	var nullNum sql.NullString
	require.NoError(t, rows.Scan(&num, &nanVal, &nullNum))

	assert.Equal(t, sql.NullString{String: "3.14159", Valid: true}, num, "num")
	assert.Equal(t, sql.NullString{String: "NaN", Valid: true}, nanVal, "nanVal")
	assert.False(t, nullNum.Valid, "nullNum.Valid")
}

func TestIntegrationDayToHourIntervalMilliPrecision(t *testing.T) {
	db := integrationOpen(t)
	cases := []struct {
		name string
		arg  time.Duration
	}{
		{name: "1234567891s", arg: time.Duration(1234567891) * time.Second},
		{name: "123456789.1s", arg: time.Duration(123456789100) * time.Millisecond},
		{name: "12345678.91s", arg: time.Duration(12345678910) * time.Millisecond},
		{name: "1234567.891s", arg: time.Duration(1234567891) * time.Millisecond},
		{name: "-1234567891s", arg: time.Duration(-1234567891) * time.Second},
		{name: "-123456789.1s", arg: time.Duration(-123456789100) * time.Millisecond},
		{name: "-12345678.91s", arg: time.Duration(-12345678910) * time.Millisecond},
		{name: "-1234567.891s", arg: time.Duration(-1234567891) * time.Millisecond},
		{name: "max seconds (2147483647)", arg: math.MaxInt32 * time.Second},
		{name: "min seconds (-2147483647)", arg: -math.MaxInt32 * time.Second},
		{name: "max minutes (153722867)", arg: time.Duration(math.MaxInt64) / time.Minute * time.Minute},
		{name: "min minutes (-153722867)", arg: time.Duration(math.MinInt64) / time.Minute * time.Minute},
		{name: "max hours (2562047)", arg: time.Duration(math.MaxInt64) / time.Hour * time.Hour},
		{name: "min hours (-2562047)", arg: time.Duration(math.MinInt64) / time.Hour * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec("SELECT ?", tc.arg)
			assert.NoError(t, err)
		})
	}
}

func TestQueryColumns(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

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

	assert.Len(t, columns, 33)
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

	assert.Len(t, columnTypes, 33)

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

	assert.Equal(t, expectedTypes, actualTypes)
}

func TestMaxGoPrecisionDateTime(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

	rows, err := db.Query(`SELECT
  cast(current_time as time(9)) AS timep,
  cast(current_time as time(9) with time zone) AS timeptz,
  cast(current_time as timestamp(9)) AS tsp,
  cast(current_time as timestamp(9) with time zone) AS tsptz`)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	columns, err := rows.Columns()
	require.NoError(t, err, "Failed reading result columns")

	assert.Len(t, columns, 4)
	expectedNames := []string{
		"timep",
		"timeptz",
		"tsp",
		"tsptz",
	}
	assert.Equal(t, expectedNames, columns)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err, "Failed reading result column types")

	assert.Len(t, columnTypes, 4)

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

	assert.Equal(t, expectedTypes, actualTypes)

	assert.True(t, rows.Next())
	require.NoError(t, rows.Err())

}

func TestIntegrationScanValues(t *testing.T) {
	db := integrationOpen(t)
	nanos := time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.Local)
	cases := []struct {
		name  string
		query string
		want  any
		check func(t *testing.T, got any)
	}{
		{name: "char is padded", query: "CAST('ddd' AS CHAR(5))", want: "ddd  "},
		{name: "decimal keeps its scale", query: "CAST(1.23 AS DECIMAL(10,5))", want: "1.23000"},
		{name: "json", query: `JSON '{"a":1}'`, want: `{"a":1}`},
		{name: "uuid", query: "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'", want: "12151fd2-7586-11e9-8f9e-2a86e4085a59"},
		{name: "ipaddress", query: "IPADDRESS '10.0.0.1'", want: "10.0.0.1"},
		{name: "interval year to month", query: "INTERVAL '3' MONTH", want: "0-3"},
		{name: "interval day to second", query: "INTERVAL '2' DAY", want: "2 00:00:00.000"},
		{name: "null", query: "CAST(NULL AS VARCHAR)", want: nil},
		{name: "real NaN", query: "CAST(nan() AS REAL)", check: func(t *testing.T, got any) {
			assert.True(t, math.IsNaN(got.(float64)), "got %v", got)
		}},
		{name: "double infinity", query: "infinity()", check: func(t *testing.T, got any) {
			assert.True(t, math.IsInf(got.(float64), 1), "got %v", got)
		}},
		{name: "timestamp with nanoseconds", query: "TIMESTAMP '2017-07-10 01:02:03.123456789'", check: func(t *testing.T, got any) {
			assert.WithinDuration(t, nanos, got.(time.Time), 0)
		}},
		// time.Time cannot hold more than nanoseconds, so the rest is dropped
		{name: "timestamp with picoseconds is truncated", query: "TIMESTAMP '2017-07-10 01:02:03.123456789012'", check: func(t *testing.T, got any) {
			assert.WithinDuration(t, nanos, got.(time.Time), 0)
		}},
		{name: "timestamp in a named zone", query: "TIMESTAMP '2017-07-10 01:02:03 Europe/Paris'", check: func(t *testing.T, got any) {
			paris, err := time.LoadLocation("Europe/Paris")
			require.NoError(t, err)
			assert.WithinDuration(t, time.Date(2017, 7, 10, 1, 2, 3, 0, paris), got.(time.Time), 0)
			assert.Equal(t, "Europe/Paris", got.(time.Time).Location().String())
		}},
		{name: "time with an offset", query: "TIME '01:02:03.123456789 +05:30'", check: func(t *testing.T, got any) {
			value := got.(time.Time)
			assert.WithinDuration(t, time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 5*3600+30*60)), value, 0)
			_, offset := value.Zone()
			assert.Equal(t, 5*3600+30*60, offset, "zone offset")
		}},
		{name: "date before the epoch", query: "DATE '1969-12-31'", check: func(t *testing.T, got any) {
			assert.WithinDuration(t, time.Date(1969, 12, 31, 0, 0, 0, 0, time.Local), got.(time.Time), 0)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got any
			require.NoError(t, db.QueryRow("SELECT "+tc.query).Scan(&got))

			if tc.check != nil {
				tc.check(t, got)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

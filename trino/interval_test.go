package trino

import (
	"database/sql"
	"encoding/json/v2"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalScan(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		value   any
		want    Interval
		wantErr string
	}{
		{name: "NULL", value: nil, want: Interval{}},
		{name: "year to month", value: "1-2", want: Interval{Months: 14, Valid: true}},
		{name: "year to month as bytes", value: []byte("1-2"), want: Interval{Months: 14, Valid: true}},
		{name: "negative year to month", value: "-1-2", want: Interval{Months: -14, Valid: true}},
		{name: "negative months only", value: "-0-5", want: Interval{Months: -5, Valid: true}},
		{name: "zero year to month", value: "0-0", want: Interval{Valid: true}},
		{name: "largest year to month", value: "178956970-7", want: Interval{Months: math.MaxInt32, Valid: true}},
		{name: "smallest year to month", value: "-178956970-8", want: Interval{Months: math.MinInt32, Valid: true}},
		{name: "day to second", value: "3 04:05:06.789", want: Interval{Duration: 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, Valid: true}},
		{name: "negative day to second", value: "-3 04:05:06.789", want: Interval{Duration: -(3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond), Valid: true}},
		{name: "negative day", value: "-1 00:00:00.000", want: Interval{Duration: -24 * time.Hour, Valid: true}},
		{name: "negative fraction of a second", value: "-0 00:00:00.500", want: Interval{Duration: -500 * time.Millisecond, Valid: true}},
		{name: "day to second without fraction", value: "0 23:59:59", want: Interval{Duration: 24*time.Hour - time.Second, Valid: true}},
		{name: "day to second with microseconds", value: "0 00:00:01.123456", want: Interval{Duration: time.Second + 123456*time.Microsecond, Valid: true}},
		{name: "day to second with nanoseconds", value: "0 00:00:00.000000001", want: Interval{Duration: time.Nanosecond, Valid: true}},
		{name: "day to second with trailing zeros", value: "3 04:05:06.789000000", want: Interval{Duration: 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, Valid: true}},
		{name: "day to second with ten digits", value: "0 00:00:01.1234567891", want: Interval{Duration: time.Second + 123456789*time.Nanosecond, Picoseconds: 100, Valid: true}},
		{name: "day to second with picoseconds", value: "0 00:00:01.123456789012", want: Interval{Duration: time.Second + 123456789*time.Nanosecond, Picoseconds: 12, Valid: true}},
		{name: "negative day to second with picoseconds", value: "-0 00:00:01.000000000001", want: Interval{Duration: -time.Second, Picoseconds: -1, Valid: true}},
		{name: "negative picoseconds only", value: "-0 00:00:00.000000000999", want: Interval{Picoseconds: -999, Valid: true}},
		{name: "largest duration", value: "106751 23:47:16.854775807", want: Interval{Duration: math.MaxInt64, Valid: true}},
		{name: "largest duration with picoseconds", value: "106751 23:47:16.854775807999", want: Interval{Duration: math.MaxInt64, Picoseconds: 999, Valid: true}},
		{name: "smallest duration", value: "-106751 23:47:16.854775808", want: Interval{Duration: math.MinInt64, Valid: true}},
		{name: "smallest duration with picoseconds", value: "-106751 23:47:16.854775808999", want: Interval{Duration: math.MinInt64, Picoseconds: -999, Valid: true}},
		{name: "day to second overflowing a duration", value: "106751 23:47:16.854775808", wantErr: "overflows time.Duration"},
		{name: "negative day to second overflowing a duration", value: "-106751 23:47:16.854775809", wantErr: "overflows time.Duration"},
		{name: "days overflowing a duration", value: "106752 00:00:00.000", wantErr: "overflows time.Duration"},
		{name: "days overflowing int64", value: "9223372036854775808 00:00:00", wantErr: "cannot parse"},
		{name: "sign on a field", value: "1--2", wantErr: "cannot parse"},
		{name: "month out of range", value: "1-12", wantErr: "cannot parse"},
		{name: "hour out of range", value: "1 24:00:00", wantErr: "cannot parse"},
		{name: "missing seconds", value: "1 04:05", wantErr: "cannot parse"},
		{name: "empty fraction", value: "0 00:00:00.", wantErr: "cannot parse"},
		{name: "fraction beyond picoseconds", value: "0 00:00:00.0000000000001", wantErr: "cannot parse"},
		{name: "empty", value: "", wantErr: "cannot parse"},
		{name: "plain number", value: "5", wantErr: "cannot parse"},
		{name: "unsupported type", value: int64(5), wantErr: "cannot convert 5 (int64) to Interval"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Interval{Months: 99, Duration: time.Hour, Picoseconds: 7, Valid: true}
			err := got.Scan(tc.value)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestClientCapabilitiesIncludeParametricInterval(t *testing.T) {
	t.Parallel()
	assert.Contains(t, strings.Split(clientCapabilities, commaSeparator), "PARAMETRIC_INTERVAL")
}

// TestIntervalColumnsScanThroughTheWireFormat serves the column types and
// values Trino 484 sends once PARAMETRIC_INTERVAL is announced, on their own
// and nested in an ARRAY, a MAP and a ROW, next to a column the way older
// servers send it.
func TestIntervalColumnsScanThroughTheWireFormat(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	columns := []queryColumn{
		intervalColumn(t, "picoseconds", "INTERVAL SECOND(12, 12)", `{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":5},{"kind":"LONG","value":12},{"kind":"LONG","value":12}]}`),
		intervalColumn(t, "whole_seconds", "INTERVAL SECOND(2, 0)", `{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":5},{"kind":"LONG","value":2},{"kind":"LONG","value":0}]}`),
		intervalColumn(t, "day_to_second", "INTERVAL DAY(3) TO SECOND(9)", `{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":2},{"kind":"LONG","value":5},{"kind":"LONG","value":3},{"kind":"LONG","value":9}]}`),
		intervalColumn(t, "day_to_hour", "INTERVAL DAY(1) TO HOUR", `{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":2},{"kind":"LONG","value":3},{"kind":"LONG","value":1},{"kind":"LONG","value":0}]}`),
		intervalColumn(t, "year_to_month", "INTERVAL YEAR(5) TO MONTH", `{"rawType":"interval year to month","arguments":[{"kind":"LONG","value":0},{"kind":"LONG","value":1},{"kind":"LONG","value":5}]}`),
		intervalColumn(t, "year", "INTERVAL YEAR(1)", `{"rawType":"interval year to month","arguments":[{"kind":"LONG","value":0},{"kind":"LONG","value":0},{"kind":"LONG","value":1}]}`),
		intervalColumn(t, "legacy", "interval day to second", `{"rawType":"interval day to second","arguments":[]}`),
		intervalColumn(t, "array", "array(INTERVAL SECOND(12, 12))", `{"rawType":"array","arguments":[{"kind":"TYPE","value":{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":5},{"kind":"LONG","value":12},{"kind":"LONG","value":12}]}}]}`),
		intervalColumn(t, "map", "map(INTERVAL SECOND(1, 9), INTERVAL YEAR(3) TO MONTH)", `{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":5},{"kind":"LONG","value":1},{"kind":"LONG","value":9}]}},{"kind":"TYPE","value":{"rawType":"interval year to month","arguments":[{"kind":"LONG","value":0},{"kind":"LONG","value":1},{"kind":"LONG","value":3}]}}]}`),
		intervalColumn(t, "row", "row(a INTERVAL SECOND(1, 6), b INTERVAL MINUTE(2))", `{"rawType":"row","arguments":[{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"a"},"typeSignature":{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":5},{"kind":"LONG","value":1},{"kind":"LONG","value":6}]}}},{"kind":"NAMED_TYPE","value":{"fieldName":{"name":"b"},"typeSignature":{"rawType":"interval day to second","arguments":[{"kind":"LONG","value":4},{"kind":"LONG","value":4},{"kind":"LONG","value":2},{"kind":"LONG","value":0}]}}}]}`),
	}
	fc.respond(statementPage(), columnsPage(columns, [][]any{{
		"0 00:00:01.123456789012",
		"0 00:00:01",
		"3 04:05:06.789000000",
		"1 02:00:00",
		"12345-0",
		"2-0",
		"0 00:00:01.123",
		[]any{"-0 00:00:01.000000000001", nil},
		map[string]any{"0 00:00:01.500000000": "2-0"},
		[]any{"0 00:00:00.000001", "0 00:01:00"},
	}}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT intervals")
	require.NoError(t, err)
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	type metadata struct {
		name             string
		precision, scale int64
		ok               bool
	}
	got := make([]metadata, len(columnTypes))
	for i, columnType := range columnTypes {
		precision, scale, ok := columnType.DecimalSize()
		got[i] = metadata{columnType.DatabaseTypeName(), precision, scale, ok}
	}
	assert.Equal(t, []metadata{
		{"INTERVAL SECOND(12, 12)", 12, 12, true},
		{"INTERVAL SECOND(2, 0)", 2, 0, true},
		{"INTERVAL DAY(3) TO SECOND(9)", 3, 9, true},
		{"INTERVAL DAY(1) TO HOUR", 1, 0, true},
		{"INTERVAL YEAR(5) TO MONTH", 5, 0, true},
		{"INTERVAL YEAR(1)", 1, 0, true},
		{"INTERVAL DAY TO SECOND", 0, 0, false},
		{"ARRAY(INTERVAL SECOND(12, 12))", 0, 0, false},
		{"MAP(INTERVAL SECOND(1, 9), INTERVAL YEAR(3) TO MONTH)", 0, 0, false},
		{"ROW(A INTERVAL SECOND(1, 6), B INTERVAL MINUTE(2))", 0, 0, false},
	}, got)
	assert.Equal(t, reflect.TypeOf(sql.NullString{}), columnTypes[0].ScanType())
	assert.Equal(t, reflect.TypeOf(NullSlice[sql.NullString]{}), columnTypes[7].ScanType())

	require.True(t, rows.Next())
	var picoseconds, wholeSeconds, dayToSecond, dayToHour, yearToMonth, year, legacy Interval
	var array NullSlice[Interval]
	var mapValue NullMap[Interval, Interval]
	var row NullRow[struct {
		A Interval
		B Interval
	}]
	require.NoError(t, rows.Scan(&picoseconds, &wholeSeconds, &dayToSecond, &dayToHour, &yearToMonth, &year, &legacy, &array, &mapValue, &row))
	assert.Equal(t, Interval{Duration: time.Second + 123456789*time.Nanosecond, Picoseconds: 12, Valid: true}, picoseconds)
	assert.Equal(t, Interval{Duration: time.Second, Valid: true}, wholeSeconds)
	assert.Equal(t, Interval{Duration: 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, Valid: true}, dayToSecond)
	assert.Equal(t, Interval{Duration: 26 * time.Hour, Valid: true}, dayToHour)
	assert.Equal(t, Interval{Months: 12345 * 12, Valid: true}, yearToMonth)
	assert.Equal(t, Interval{Months: 24, Valid: true}, year)
	assert.Equal(t, Interval{Duration: time.Second + 123*time.Millisecond, Valid: true}, legacy)
	assert.Equal(t, NullSlice[Interval]{Slice: []Interval{{Duration: -time.Second, Picoseconds: -1, Valid: true}, {}}, Valid: true}, array)
	assert.Equal(t, map[Interval]Interval{{Duration: 1500 * time.Millisecond, Valid: true}: {Months: 24, Valid: true}}, mapValue.Map)
	assert.Equal(t, Interval{Duration: time.Microsecond, Valid: true}, row.Row.A)
	assert.Equal(t, Interval{Duration: time.Minute, Valid: true}, row.Row.B)

	var picosecondsText string
	var arrayElements interface{}
	require.NoError(t, rows.Scan(&picosecondsText, new(Interval), new(Interval), new(Interval), new(Interval), new(Interval), new(Interval), &arrayElements, new(NullMap[Interval, Interval]), new(Row)))
	assert.Equal(t, "0 00:00:01.123456789012", picosecondsText, "a string keeps every digit the server sent")
	assert.Equal(t, []interface{}{"-0 00:00:01.000000000001", nil}, arrayElements)

	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestNewColumnTypeRejectsInvalidIntervalArguments(t *testing.T) {
	t.Parallel()
	long := func(value int64) typeArgument { return typeArgument{Kind: KIND_LONG, long: value} }
	cases := []struct {
		name      string
		rawType   string
		arguments []typeArgument
	}{
		{name: "too few arguments", rawType: "interval day to second", arguments: []typeArgument{long(5), long(5)}},
		{name: "too many arguments", rawType: "interval day to second", arguments: []typeArgument{long(5), long(5), long(2), long(6), long(0)}},
		{name: "fractional precision of a year to month interval", rawType: "interval year to month", arguments: []typeArgument{long(0), long(1), long(2), long(0)}},
		{name: "argument that is not a number", rawType: "interval day to second", arguments: []typeArgument{long(5), long(5), {Kind: KIND_TYPE}, long(6)}},
		{name: "day field of a year to month interval", rawType: "interval year to month", arguments: []typeArgument{long(0), long(2), long(2)}},
		{name: "month field of a day to second interval", rawType: "interval day to second", arguments: []typeArgument{long(1), long(5), long(2), long(6)}},
		{name: "field beyond second", rawType: "interval day to second", arguments: []typeArgument{long(2), long(6), long(2), long(6)}},
		{name: "negative field", rawType: "interval year to month", arguments: []typeArgument{long(-1), long(1), long(2)}},
		{name: "fields out of order", rawType: "interval day to second", arguments: []typeArgument{long(5), long(2), long(2), long(6)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newColumnType(tc.rawType, typeSignature{RawType: tc.rawType, Arguments: tc.arguments})

			require.ErrorIs(t, err, ErrInvalidResponseType)
		})
	}
}

// intervalColumn builds a column from the type signature JSON Trino sends,
// so its arguments go through the same decoding a real response does.
func intervalColumn(t testing.TB, name, dataType, signatureJSON string) queryColumn {
	t.Helper()
	var signature typeSignature
	require.NoError(t, json.Unmarshal([]byte(signatureJSON), &signature))
	return queryColumn{Name: name, Type: dataType, TypeSignature: signature}
}

package trino

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRowsDecoderProducesTheDocumentedTypes decodes a row holding a value of
// every kind, and a row of NULLs, into the Go types the README documents.
func TestRowsDecoderProducesTheDocumentedTypes(t *testing.T) {
	t.Parallel()
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	pointType := rowType(namedField("x", scalarType("integer")), namedField("", scalarType("varbinary")))
	columns := []struct {
		column queryColumn
		value  string
		want   any
	}{
		{column: column("boolean", scalarType("boolean")), value: `true`, want: true},
		{column: column("tinyint", scalarType("tinyint")), value: `-128`, want: int64(-128)},
		{column: column("smallint", scalarType("smallint")), value: `32767`, want: int64(32767)},
		{column: column("integer", scalarType("integer")), value: `42`, want: int64(42)},
		{column: column("bigint", scalarType("bigint")), value: `9223372036854775807`, want: int64(9223372036854775807)},
		{column: column("real", scalarType("real")), value: `1.5`, want: 1.5},
		{column: column("double", scalarType("double")), value: `"-Infinity"`, want: math.Inf(-1)},
		{column: column("decimal(38,2)", scalarType("decimal")), value: `"123456789012345678901234567890.12"`, want: "123456789012345678901234567890.12"},
		{column: column("varchar", scalarType("varchar")), value: `"a\"b"`, want: `a"b`},
		{column: column("json", scalarType("json")), value: `"{\"a\":1}"`, want: `{"a":1}`},
		{column: column("uuid", scalarType("uuid")), value: `"12151fd2-7586-11e9-8f9e-2a86e4085a59"`, want: "12151fd2-7586-11e9-8f9e-2a86e4085a59"},
		{column: column("interval day to second", scalarType("interval day to second")), value: `"2 00:00:00.000"`, want: "2 00:00:00.000"},
		{column: column("varbinary", scalarType("varbinary")), value: `"AAE="`, want: []byte{0, 1}},
		{column: column("HyperLogLog", scalarType("HyperLogLog")), value: `"AAI="`, want: []byte{0, 2}},
		{column: column("date", scalarType("date")), value: `"2017-07-10"`, want: time.Date(2017, 7, 10, 0, 0, 0, 0, tokyo)},
		{column: column("timestamp(3)", scalarType("timestamp")), value: `"2017-07-10 01:02:03.000"`, want: time.Date(2017, 7, 10, 1, 2, 3, 0, tokyo)},
		{column: column("timestamp(3) with time zone", scalarType("timestamp with time zone")), value: `"2017-07-10 01:02:03.000 UTC"`, want: time.Date(2017, 7, 10, 1, 2, 3, 0, time.UTC)},
		{column: column("BingTile", scalarType("BingTile")), value: `{"x":1,"y":2,"zoom":3}`, want: map[string]any{"x": 1.0, "y": 2.0, "zoom": 3.0}},
		{
			column: column("array(row(x integer, varbinary))", arrayType(pointType)),
			value:  `[[1, "AAE="], null, [null, null]]`,
			want: []any{
				Row{names: []string{"x", "field1"}, values: []any{int64(1), []byte{0, 1}}, Valid: true},
				nil,
				Row{names: []string{"x", "field1"}, values: []any{nil, nil}, Valid: true},
			},
		},
		{
			column: column("map(varchar,array(timestamp(3)))", mapType(scalarType("varchar"), arrayType(scalarType("timestamp")))),
			value:  `{"a": ["2017-07-10 01:02:03.000", null], "b": null, "c": []}`,
			want: map[string]any{
				"a": []any{time.Date(2017, 7, 10, 1, 2, 3, 0, tokyo), nil},
				"b": nil,
				"c": []any{},
			},
		},
	}
	queryColumns := make([]queryColumn, len(columns))
	values, nulls := "[", "["
	for i, c := range columns {
		queryColumns[i] = c.column
		queryColumns[i].Name = c.column.Type
		if i > 0 {
			values, nulls = values+",", nulls+","
		}
		values, nulls = values+c.value, nulls+"null"
	}

	rows, err := decodeTestRows(t, queryColumns, tokyo, "["+values+"],"+nulls+"]]")

	require.NoError(t, err)
	require.Len(t, rows, 2)
	for i, c := range columns {
		assert.Equal(t, c.want, rows[0][i], "column %s", c.column.Type)
		assert.Nil(t, rows[1][i], "NULL in column %s", c.column.Type)
	}
}

func TestRowsDecoderNamesTheColumnOfAnError(t *testing.T) {
	t.Parallel()
	columns := []queryColumn{
		{Name: "id", Type: "bigint", TypeSignature: scalarType("bigint")},
		{Name: "tags", Type: "array(varchar)", TypeSignature: arrayType(scalarType("varchar"))},
	}
	cases := []struct {
		name    string
		page    string
		wantErr string
	}{
		{name: "too few values", page: `[[1, []], [2]]`, wantErr: `row 1: no value for column "tags"`},
		{name: "too many values", page: `[[1, [], 3]]`, wantErr: "row 0: more values than the 2 columns"},
		{name: "wrong kind", page: `[["1", []]]`, wantErr: `row 0: column "id": expected an integer, got a string`},
		{name: "wrong kind of element", page: `[[1, ["a", 2]]]`, wantErr: `row 0: column "tags": element 1: expected a string, got a number`},
		{name: "row not an array", page: `[{"id": 1}]`, wantErr: "row 0: expected an array of column values, got an object"},
		{name: "not an array of rows", page: `{}`, wantErr: "expected an array of rows, got an object"},
		{name: "trailing data", page: `[] []`, wantErr: "unexpected data after the rows"},
		{name: "truncated", page: `[[1, [`, wantErr: "unexpected EOF"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeTestRows(t, columns, time.UTC, tc.page)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestRowsDecoderToleratesAWrongRowCount decodes the same rows with no row
// count, the right one, and counts that are too low or too high, since the
// count only sizes the allocation of the values.
func TestRowsDecoderToleratesAWrongRowCount(t *testing.T) {
	t.Parallel()
	decoder, err := newRowsDecoder([]queryColumn{
		{Name: "id", Type: "bigint", TypeSignature: scalarType("bigint")},
		{Name: "name", Type: "varchar", TypeSignature: scalarType("varchar")},
	}, time.UTC)
	require.NoError(t, err)
	want := []queryData{{int64(1), "a"}, {int64(2), "b"}, {int64(3), nil}}

	for _, expectedRows := range []int{0, 1, 2, 3, 4, 1_000_000} {
		t.Run(fmt.Sprint(expectedRows), func(t *testing.T) {
			rows, err := decoder.decodeRows([]byte(`[[1, "a"], [2, "b"], [3, null]]`), expectedRows)

			require.NoError(t, err)
			require.Equal(t, want, rows)
			rows[0] = append(rows[0], "appended")
			assert.Equal(t, want[1], rows[1], "an append to a row must not overwrite the next one")
		})
	}
}

// decodeTestRows decodes page with the decoders of columns, whose type
// arguments are the raw JSON the server sends.
func decodeTestRows(t *testing.T, columns []queryColumn, location *time.Location, page string) ([]queryData, error) {
	t.Helper()
	for i := range columns {
		require.NoError(t, unmarshalArguments(&columns[i].TypeSignature))
	}
	decoder, err := newRowsDecoder(columns, location)
	require.NoError(t, err)
	return decoder.decodeRows([]byte(page), 0)
}

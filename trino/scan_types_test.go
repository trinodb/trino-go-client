package trino

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetScanType(t *testing.T) {
	t.Parallel()
	pointType := rowType(namedField("x", scalarType("integer")))
	cases := []struct {
		name      string
		signature typeSignature
		want      reflect.Type
	}{
		{name: "boolean", signature: scalarType("boolean"), want: reflect.TypeFor[sql.NullBool]()},
		{name: "varchar", signature: scalarType("varchar"), want: reflect.TypeFor[sql.NullString]()},
		{name: "varbinary", signature: scalarType("varbinary"), want: reflect.TypeFor[[]byte]()},
		{name: "tinyint", signature: scalarType("tinyint"), want: reflect.TypeFor[sql.NullInt32]()},
		{name: "bigint", signature: scalarType("bigint"), want: reflect.TypeFor[sql.NullInt64]()},
		{name: "real", signature: scalarType("real"), want: reflect.TypeFor[sql.NullFloat64]()},
		{name: "date", signature: scalarType("date"), want: reflect.TypeFor[sql.NullTime]()},
		{name: "KdbTree", signature: scalarType("KdbTree"), want: anyScanType},
		{name: "HyperLogLog", signature: scalarType("HyperLogLog"), want: reflect.TypeFor[[]byte]()},
		{name: "row", signature: pointType, want: reflect.TypeFor[Row]()},
		{name: "array of integer", signature: arrayType(scalarType("integer")), want: reflect.TypeFor[NullSlice[sql.NullInt32]]()},
		{name: "array of smallint", signature: arrayType(scalarType("smallint")), want: reflect.TypeFor[NullSlice[sql.NullInt32]]()},
		{name: "array of bigint", signature: arrayType(scalarType("bigint")), want: reflect.TypeFor[NullSlice[sql.NullInt64]]()},
		{name: "array of double", signature: arrayType(scalarType("double")), want: reflect.TypeFor[NullSlice[sql.NullFloat64]]()},
		{name: "array of boolean", signature: arrayType(scalarType("boolean")), want: reflect.TypeFor[NullSlice[sql.NullBool]]()},
		{name: "array of decimal", signature: arrayType(scalarType("decimal")), want: reflect.TypeFor[NullSlice[sql.NullString]]()},
		{name: "array of varbinary", signature: arrayType(scalarType("varbinary")), want: reflect.TypeFor[NullSlice[[]byte]]()},
		{name: "array of HyperLogLog", signature: arrayType(scalarType("HyperLogLog")), want: reflect.TypeFor[NullSlice[[]byte]]()},
		{name: "array of timestamp", signature: arrayType(scalarType("timestamp")), want: reflect.TypeFor[NullSlice[sql.NullTime]]()},
		{name: "array of KdbTree", signature: arrayType(scalarType("KdbTree")), want: reflect.TypeFor[NullSlice[interface{}]]()},
		{name: "array of row", signature: arrayType(pointType), want: reflect.TypeFor[NullSlice[Row]]()},
		{name: "array of array of row", signature: arrayType(arrayType(pointType)), want: reflect.TypeFor[NullSlice[NullSlice[Row]]]()},
		{
			name:      "three dimensional array",
			signature: arrayType(arrayType(arrayType(scalarType("varchar")))),
			want:      reflect.TypeFor[NullSlice[NullSlice[NullSlice[sql.NullString]]]](),
		},
		{
			name:      "four dimensional array reports interface{} below three dimensions",
			signature: arrayType(arrayType(arrayType(arrayType(scalarType("bigint"))))),
			want:      reflect.TypeFor[NullSlice[NullSlice[NullSlice[interface{}]]]](),
		},
		{name: "map of varchar to bigint", signature: mapType(scalarType("varchar"), scalarType("bigint")), want: reflect.TypeFor[NullMapOf[sql.NullString, sql.NullInt64]]()},
		{name: "map of integer to varchar", signature: mapType(scalarType("integer"), scalarType("varchar")), want: reflect.TypeFor[NullMapOf[sql.NullInt32, sql.NullString]]()},
		{name: "map of boolean to double", signature: mapType(scalarType("boolean"), scalarType("double")), want: reflect.TypeFor[NullMapOf[sql.NullBool, sql.NullFloat64]]()},
		{name: "map of date to varbinary", signature: mapType(scalarType("date"), scalarType("varbinary")), want: reflect.TypeFor[NullMapOf[sql.NullTime, []byte]]()},
		{name: "map of varchar to row", signature: mapType(scalarType("varchar"), pointType), want: reflect.TypeFor[NullMapOf[sql.NullString, Row]]()},
		{name: "map with a varbinary key", signature: mapType(scalarType("varbinary"), scalarType("varchar")), want: reflect.TypeFor[NullMapOf[interface{}, sql.NullString]]()},
		{name: "map with an array key", signature: mapType(arrayType(scalarType("integer")), scalarType("varchar")), want: reflect.TypeFor[NullMapOf[interface{}, sql.NullString]]()},
		{name: "map with a row key", signature: mapType(pointType, scalarType("varchar")), want: reflect.TypeFor[NullMapOf[interface{}, sql.NullString]]()},
		{
			name:      "map with named type arguments",
			signature: typeSignature{RawType: "map", Arguments: []typeArgument{namedField("", scalarType("varchar")), namedField("", scalarType("bigint"))}},
			want:      reflect.TypeFor[NullMapOf[sql.NullString, sql.NullInt64]](),
		},
		{
			name:      "map of varchar to array reports interface{} values",
			signature: mapType(scalarType("varchar"), arrayType(scalarType("bigint"))),
			want:      reflect.TypeFor[NullMapOf[sql.NullString, interface{}]](),
		},
		{
			name:      "map of maps reports interface{} values",
			signature: mapType(scalarType("varchar"), mapType(scalarType("varchar"), scalarType("bigint"))),
			want:      reflect.TypeFor[NullMapOf[sql.NullString, interface{}]](),
		},
		{
			name:      "array of map",
			signature: arrayType(mapType(scalarType("varchar"), scalarType("bigint"))),
			want:      reflect.TypeFor[NullSlice[NullMapOf[sql.NullString, sql.NullInt64]]](),
		},
		{
			name:      "array of map of varchar to array reports interface{} values",
			signature: arrayType(mapType(scalarType("varchar"), arrayType(scalarType("bigint")))),
			want:      reflect.TypeFor[NullSlice[NullMapOf[sql.NullString, interface{}]]](),
		},
		{
			name:      "array of array of map reports interface{} elements",
			signature: arrayType(arrayType(mapType(scalarType("varchar"), scalarType("bigint")))),
			want:      reflect.TypeFor[NullSlice[NullSlice[interface{}]]](),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scanType, err := parsedScanType(t, tc.signature)

			require.NoError(t, err)
			assert.Equal(t, tc.want, scanType)
		})
	}
}

func TestGetScanTypeRejectsMalformedSignatures(t *testing.T) {
	t.Parallel()
	for name, signature := range map[string]typeSignature{
		"array without element":   {RawType: "array"},
		"nested array":            arrayType(arrayType(typeSignature{RawType: "array"})),
		"map without value":       {RawType: "map", Arguments: []typeArgument{typeArg(scalarType("varchar"))}},
		"map in array":            arrayType(typeSignature{RawType: "map"}),
		"array of a long literal": {RawType: "array", Arguments: []typeArgument{{Kind: KIND_LONG, Value: json.RawMessage("1")}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parsedScanType(t, signature)

			require.ErrorIs(t, err, ErrInvalidResponseType)
		})
	}
}

func TestContainerScanTypesAreScanners(t *testing.T) {
	t.Parallel()
	scanner := reflect.TypeFor[sql.Scanner]()
	for element, sliceType := range containerScanTypes.slices {
		assert.Truef(t, reflect.PointerTo(sliceType).Implements(scanner), "%s", sliceType)
		assert.Equal(t, element, sliceType.Field(0).Type.Elem())
	}
	for key, mapType := range containerScanTypes.maps {
		assert.Truef(t, reflect.PointerTo(mapType).Implements(scanner), "%s", mapType)
		assert.Equal(t, key.key, mapType.Field(0).Type.Key())
		assert.Equal(t, key.value, mapType.Field(0).Type.Elem())
	}
}

// TestScanTypeScansEveryColumn scans a row of values and a row of NULLs into
// what reflect.New returns for each reported scan type, the way generic
// database tools read results they know nothing about.
func TestScanTypeScansEveryColumn(t *testing.T) {
	t.Parallel()
	pointType := rowType(namedField("x", scalarType("integer")))
	point := Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true}
	date := time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local)
	instant := time.Date(2017, 7, 10, 1, 2, 3, 0, time.UTC)
	columns := []struct {
		signature typeSignature
		value     any
		want      any
	}{
		{signature: scalarType("boolean"), value: true, want: sql.NullBool{Bool: true, Valid: true}},
		{signature: scalarType("varchar"), value: "a", want: sql.NullString{String: "a", Valid: true}},
		{signature: scalarType("decimal"), value: "1.10", want: sql.NullString{String: "1.10", Valid: true}},
		{signature: scalarType("varbinary"), value: "YQ==", want: []byte("a")},
		{signature: scalarType("HyperLogLog"), value: "YQ==", want: []byte("a")},
		{signature: scalarType("tinyint"), value: 1, want: sql.NullInt32{Int32: 1, Valid: true}},
		{signature: scalarType("integer"), value: 1, want: sql.NullInt32{Int32: 1, Valid: true}},
		{signature: scalarType("bigint"), value: 1, want: sql.NullInt64{Int64: 1, Valid: true}},
		{signature: scalarType("double"), value: 1.5, want: sql.NullFloat64{Float64: 1.5, Valid: true}},
		{signature: scalarType("timestamp with time zone"), value: "2017-07-10 01:02:03.000 UTC", want: sql.NullTime{Time: instant, Valid: true}},
		{signature: scalarType("KdbTree"), value: map[string]any{"id": "a"}, want: map[string]interface{}{"id": "a"}},
		{signature: pointType, value: []any{1}, want: point},
		{
			signature: arrayType(scalarType("integer")),
			value:     []any{1, nil},
			want:      NullSlice[sql.NullInt32]{Slice: []sql.NullInt32{{Int32: 1, Valid: true}, {}}, Valid: true},
		},
		{
			signature: arrayType(scalarType("varbinary")),
			value:     []any{"YQ==", nil},
			want:      NullSlice[[]byte]{Slice: [][]byte{[]byte("a"), nil}, Valid: true},
		},
		{
			signature: arrayType(scalarType("date")),
			value:     []any{"2017-07-10"},
			want:      NullSlice[sql.NullTime]{Slice: []sql.NullTime{{Time: date, Valid: true}}, Valid: true},
		},
		{
			signature: arrayType(scalarType("KdbTree")),
			value:     []any{"a"},
			want:      NullSlice[interface{}]{Slice: []interface{}{"a"}, Valid: true},
		},
		{
			signature: arrayType(pointType),
			value:     []any{[]any{1}, nil},
			want:      NullSlice[Row]{Slice: []Row{point, {}}, Valid: true},
		},
		{
			signature: arrayType(arrayType(arrayType(scalarType("bigint")))),
			value:     []any{[]any{[]any{1}, nil}},
			want: NullSlice[NullSlice[NullSlice[sql.NullInt64]]]{Slice: []NullSlice[NullSlice[sql.NullInt64]]{{
				Slice: []NullSlice[sql.NullInt64]{{Slice: []sql.NullInt64{{Int64: 1, Valid: true}}, Valid: true}, {}},
				Valid: true,
			}}, Valid: true},
		},
		{
			signature: arrayType(arrayType(arrayType(arrayType(scalarType("bigint"))))),
			value:     []any{[]any{[]any{[]any{1}}}},
			want: NullSlice[NullSlice[NullSlice[interface{}]]]{Slice: []NullSlice[NullSlice[interface{}]]{{
				Slice: []NullSlice[interface{}]{{Slice: []interface{}{[]interface{}{json.Number("1")}}, Valid: true}},
				Valid: true,
			}}, Valid: true},
		},
		{
			signature: mapType(scalarType("varchar"), scalarType("bigint")),
			value:     map[string]any{"a": 1, "b": nil},
			want: NullMapOf[sql.NullString, sql.NullInt64]{Map: map[sql.NullString]sql.NullInt64{
				{String: "a", Valid: true}: {Int64: 1, Valid: true},
				{String: "b", Valid: true}: {},
			}, Valid: true},
		},
		{
			signature: mapType(scalarType("integer"), scalarType("varchar")),
			value:     map[string]any{"1": "a"},
			want:      NullMapOf[sql.NullInt32, sql.NullString]{Map: map[sql.NullInt32]sql.NullString{{Int32: 1, Valid: true}: {String: "a", Valid: true}}, Valid: true},
		},
		{
			signature: mapType(scalarType("boolean"), scalarType("double")),
			value:     map[string]any{"true": 1.5},
			want:      NullMapOf[sql.NullBool, sql.NullFloat64]{Map: map[sql.NullBool]sql.NullFloat64{{Bool: true, Valid: true}: {Float64: 1.5, Valid: true}}, Valid: true},
		},
		{
			signature: mapType(scalarType("double"), scalarType("boolean")),
			value:     map[string]any{"1.5": true},
			want:      NullMapOf[sql.NullFloat64, sql.NullBool]{Map: map[sql.NullFloat64]sql.NullBool{{Float64: 1.5, Valid: true}: {Bool: true, Valid: true}}, Valid: true},
		},
		{
			signature: mapType(scalarType("date"), scalarType("varbinary")),
			value:     map[string]any{"2017-07-10": "YQ=="},
			want:      NullMapOf[sql.NullTime, []byte]{Map: map[sql.NullTime][]byte{{Time: date, Valid: true}: []byte("a")}, Valid: true},
		},
		{
			signature: mapType(scalarType("varbinary"), scalarType("varchar")),
			value:     map[string]any{"YQ==": "a"},
			want:      NullMapOf[interface{}, sql.NullString]{Map: map[interface{}]sql.NullString{"YQ==": {String: "a", Valid: true}}, Valid: true},
		},
		{
			signature: mapType(scalarType("varchar"), pointType),
			value:     map[string]any{"a": []any{1}},
			want:      NullMapOf[sql.NullString, Row]{Map: map[sql.NullString]Row{{String: "a", Valid: true}: point}, Valid: true},
		},
		{
			signature: mapType(scalarType("varchar"), arrayType(scalarType("bigint"))),
			value:     map[string]any{"a": []any{1}, "b": nil},
			want: NullMapOf[sql.NullString, interface{}]{Map: map[sql.NullString]interface{}{
				{String: "a", Valid: true}: []interface{}{json.Number("1")},
				{String: "b", Valid: true}: nil,
			}, Valid: true},
		},
		{
			signature: mapType(scalarType("varchar"), mapType(scalarType("varchar"), scalarType("bigint"))),
			value:     map[string]any{"a": map[string]any{"b": 1}},
			want: NullMapOf[sql.NullString, interface{}]{Map: map[sql.NullString]interface{}{
				{String: "a", Valid: true}: map[string]interface{}{"b": json.Number("1")},
			}, Valid: true},
		},
		{
			signature: arrayType(mapType(scalarType("varchar"), scalarType("bigint"))),
			value:     []any{map[string]any{"a": 1}, nil},
			want: NullSlice[NullMapOf[sql.NullString, sql.NullInt64]]{Slice: []NullMapOf[sql.NullString, sql.NullInt64]{
				{Map: map[sql.NullString]sql.NullInt64{{String: "a", Valid: true}: {Int64: 1, Valid: true}}, Valid: true},
				{},
			}, Valid: true},
		},
		{
			signature: arrayType(arrayType(mapType(scalarType("varchar"), scalarType("bigint")))),
			value:     []any{[]any{map[string]any{"a": 1}}},
			want: NullSlice[NullSlice[interface{}]]{Slice: []NullSlice[interface{}]{{
				Slice: []interface{}{map[string]interface{}{"a": json.Number("1")}},
				Valid: true,
			}}, Valid: true},
		},
	}
	queryColumns := make([]queryColumn, len(columns))
	values := make([]any, len(columns))
	nulls := make([]any, len(columns))
	for i, c := range columns {
		queryColumns[i] = column(c.signature.RawType, c.signature)
		queryColumns[i].Name = c.signature.RawType
		values[i] = c.value
	}
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), columnsPage(queryColumns, [][]any{values, nulls}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT x")
	require.NoError(t, err)
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columnTypes, len(columns))

	for _, wantNull := range []bool{false, true} {
		require.True(t, rows.Next())
		dests := make([]any, len(columnTypes))
		for i, columnType := range columnTypes {
			dests[i] = reflect.New(columnType.ScanType()).Interface()
			if _, ok := dests[i].(sql.Scanner); !ok {
				assert.Contains(t, []reflect.Type{reflect.TypeFor[[]byte](), anyScanType}, columnType.ScanType(), "a scan type that is not a sql.Scanner must be one database/sql scans into natively")
			}
		}
		require.NoError(t, rows.Scan(dests...))

		for i, c := range columns {
			got := reflect.ValueOf(dests[i]).Elem().Interface()
			want := c.want
			if wantNull {
				want = reflect.Zero(columnTypes[i].ScanType()).Interface()
			}
			assert.Equalf(t, want, got, "column %d of type %s", i, columnTypes[i].DatabaseTypeName())
		}
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

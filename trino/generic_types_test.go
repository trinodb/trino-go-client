package trino

import (
	"database/sql"
	"encoding/json/v2"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type point struct {
	X     int64 `trino:"x"`
	Label sql.NullString
}

type anonymousPair struct {
	Field0 int64
	Second string `trino:"field1"`
}

type outer struct {
	ID    int64          `trino:"id"`
	Inner NullRow[point] `trino:"inner"`
}

type everyKind struct {
	Tags    NullSlice[string]         `trino:"tags"`
	Scores  NullMap[string, float64]  `trino:"scores"`
	Created time.Time                 `trino:"created"`
	Payload []byte                    `trino:"payload"`
	Points  NullSlice[NullRow[point]] `trino:"points"`
	Raw     interface{}               `trino:"raw"`
	Ignored string                    `trino:"-"`
	hidden  string
}

// TestGenericScannersThroughTheWireFormat scans every generic scanner from a
// response that went through the same JSON encoding and decoding a real
// coordinator's does, so elements arrive decoded the way a plain column of
// their type would be.
func TestGenericScannersThroughTheWireFormat(t *testing.T) {
	t.Parallel()
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	pointType := rowType(namedField("x", scalarType("integer")), namedField("label", scalarType("varchar")))

	cases := []struct {
		name   string
		column queryColumn
		value  any
		dest   func() any
		want   any
	}{
		{
			name:   "array of nullable bigint",
			column: column("array(bigint)", arrayType(scalarType("bigint"))),
			value:  []any{1, nil, 3},
			dest:   func() any { return &NullSlice[sql.NullInt64]{} },
			want:   NullSlice[sql.NullInt64]{Slice: []sql.NullInt64{{Int64: 1, Valid: true}, {}, {Int64: 3, Valid: true}}, Valid: true},
		},
		{
			name:   "array of plain integers",
			column: column("array(integer)", arrayType(scalarType("integer"))),
			value:  []any{1, -2},
			dest:   func() any { return &NullSlice[int32]{} },
			want:   NullSlice[int32]{Slice: []int32{1, -2}, Valid: true},
		},
		{
			name:   "NULL array",
			column: column("array(bigint)", arrayType(scalarType("bigint"))),
			value:  nil,
			dest:   func() any { return &NullSlice[int64]{Slice: []int64{1}, Valid: true} },
			want:   NullSlice[int64]{},
		},
		{
			name:   "array of booleans",
			column: column("array(boolean)", arrayType(scalarType("boolean"))),
			value:  []any{true, nil},
			dest:   func() any { return &NullSlice[sql.NullBool]{} },
			want:   NullSlice[sql.NullBool]{Slice: []sql.NullBool{{Bool: true, Valid: true}, {}}, Valid: true},
		},
		{
			name:   "array of doubles",
			column: column("array(double)", arrayType(scalarType("double"))),
			value:  []any{1.5, "Infinity"},
			dest:   func() any { return &NullSlice[float64]{} },
			want:   NullSlice[float64]{Slice: []float64{1.5, math.Inf(1)}, Valid: true},
		},
		{
			name:   "array of varbinary",
			column: column("array(varbinary)", arrayType(scalarType("varbinary"))),
			value:  []any{"AAE=", nil},
			dest:   func() any { return &NullSlice[[]byte]{} },
			want:   NullSlice[[]byte]{Slice: [][]byte{{0, 1}, nil}, Valid: true},
		},
		{
			name:   "array of anything",
			column: column("array(bigint)", arrayType(scalarType("bigint"))),
			value:  []any{1, nil},
			dest:   func() any { return &NullSlice[interface{}]{} },
			want:   NullSlice[interface{}]{Slice: []interface{}{int64(1), nil}, Valid: true},
		},
		{
			name:   "nested array keeps a NULL inner array apart from an empty one",
			column: column("array(array(varchar))", arrayType(arrayType(scalarType("varchar")))),
			value:  []any{[]any{"a", nil}, nil, []any{}},
			dest:   func() any { return &NullSlice[NullSlice[sql.NullString]]{} },
			want: NullSlice[NullSlice[sql.NullString]]{Slice: []NullSlice[sql.NullString]{
				{Slice: []sql.NullString{{String: "a", Valid: true}, {}}, Valid: true},
				{},
				{Slice: []sql.NullString{}, Valid: true},
			}, Valid: true},
		},
		{
			name:   "four dimensional array",
			column: column("array(array(array(array(varchar))))", arrayType(arrayType(arrayType(arrayType(scalarType("varchar")))))),
			value:  nest(4, "a"),
			dest:   func() any { return &NullSlice[NullSlice[NullSlice[NullSlice[string]]]]{} },
			want: NullSlice[NullSlice[NullSlice[NullSlice[string]]]]{Slice: []NullSlice[NullSlice[NullSlice[string]]]{
				{Slice: []NullSlice[NullSlice[string]]{
					{Slice: []NullSlice[string]{
						{Slice: []string{"a"}, Valid: true},
					}, Valid: true},
				}, Valid: true},
			}, Valid: true},
		},
		{
			name:   "nested array of timestamps in the connection's zone",
			column: column("array(array(timestamp(3)))", arrayType(arrayType(scalarType("timestamp")))),
			value:  nest(2, "2017-07-10 01:02:03.000"),
			dest:   func() any { return &NullSlice[NullSlice[time.Time]]{} },
			want: NullSlice[NullSlice[time.Time]]{Slice: []NullSlice[time.Time]{
				{Slice: []time.Time{time.Date(2017, 7, 10, 1, 2, 3, 0, tokyo)}, Valid: true},
			}, Valid: true},
		},
		{
			name:   "map of arrays",
			column: column("map(varchar,array(bigint))", mapType(scalarType("varchar"), arrayType(scalarType("bigint")))),
			value:  map[string]any{"a": []any{1, nil}, "b": nil},
			dest:   func() any { return &NullMap[string, NullSlice[sql.NullInt64]]{} },
			want: NullMap[string, NullSlice[sql.NullInt64]]{Map: map[string]NullSlice[sql.NullInt64]{
				"a": {Slice: []sql.NullInt64{{Int64: 1, Valid: true}, {}}, Valid: true},
				"b": {},
			}, Valid: true},
		},
		{
			name:   "map with integer keys",
			column: column("map(integer,varchar)", mapType(scalarType("integer"), scalarType("varchar"))),
			value:  map[string]any{"1": "a", "-2": "b"},
			dest:   func() any { return &NullMap[int64, string]{} },
			want:   NullMap[int64, string]{Map: map[int64]string{1: "a", -2: "b"}, Valid: true},
		},
		{
			name:   "map with boolean keys",
			column: column("map(boolean,double)", mapType(scalarType("boolean"), scalarType("double"))),
			value:  map[string]any{"true": 1, "false": 0},
			dest:   func() any { return &NullMap[bool, float64]{} },
			want:   NullMap[bool, float64]{Map: map[bool]float64{true: 1, false: 0}, Valid: true},
		},
		{
			name:   "NULL map",
			column: column("map(varchar,varchar)", mapType(scalarType("varchar"), scalarType("varchar"))),
			value:  nil,
			dest:   func() any { return &NullMap[string, string]{} },
			want:   NullMap[string, string]{},
		},
		{
			name:   "array of maps",
			column: column("array(map(varchar,bigint))", arrayType(mapType(scalarType("varchar"), scalarType("bigint")))),
			value:  []any{map[string]any{"a": 1}, nil},
			dest:   func() any { return &NullSlice[NullMap[string, int64]]{} },
			want: NullSlice[NullMap[string, int64]]{Slice: []NullMap[string, int64]{
				{Map: map[string]int64{"a": 1}, Valid: true},
				{},
			}, Valid: true},
		},
		{
			name:   "row matched by tag and by name ignoring case",
			column: rowColumn("_col0", "row(x integer, label varchar)", pointType.Arguments...),
			value:  []any{1, "a"},
			dest:   func() any { return &NullRow[point]{} },
			want:   NullRow[point]{Row: point{X: 1, Label: sql.NullString{String: "a", Valid: true}}, Valid: true},
		},
		{
			name:   "NULL row",
			column: rowColumn("_col0", "row(x integer, label varchar)", pointType.Arguments...),
			value:  nil,
			dest:   func() any { return &NullRow[point]{Row: point{X: 1}, Valid: true} },
			want:   NullRow[point]{},
		},
		{
			name:   "row with anonymous fields",
			column: rowColumn("_col0", "row(integer, varchar)", namedField("", scalarType("integer")), namedField("", scalarType("varchar"))),
			value:  []any{1, "a"},
			dest:   func() any { return &NullRow[anonymousPair]{} },
			want:   NullRow[anonymousPair]{Row: anonymousPair{Field0: 1, Second: "a"}, Valid: true},
		},
		{
			name:   "row in row",
			column: rowColumn("_col0", "row(id bigint, inner row(x integer, label varchar))", namedField("id", scalarType("bigint")), namedField("inner", pointType)),
			value:  []any{7, []any{1, nil}},
			dest:   func() any { return &NullRow[outer]{} },
			want:   NullRow[outer]{Row: outer{ID: 7, Inner: NullRow[point]{Row: point{X: 1}, Valid: true}}, Valid: true},
		},
		{
			name:   "NULL row in row",
			column: rowColumn("_col0", "row(id bigint, inner row(x integer, label varchar))", namedField("id", scalarType("bigint")), namedField("inner", pointType)),
			value:  []any{7, nil},
			dest:   func() any { return &NullRow[outer]{} },
			want:   NullRow[outer]{Row: outer{ID: 7}, Valid: true},
		},
		{
			name:   "row in array",
			column: column("array(row(x integer, label varchar))", arrayType(pointType)),
			value:  []any{[]any{1, "a"}, nil},
			dest:   func() any { return &NullSlice[NullRow[point]]{} },
			want: NullSlice[NullRow[point]]{Slice: []NullRow[point]{
				{Row: point{X: 1, Label: sql.NullString{String: "a", Valid: true}}, Valid: true},
				{},
			}, Valid: true},
		},
		{
			name:   "row in map",
			column: column("map(varchar,row(x integer, label varchar))", mapType(scalarType("varchar"), pointType)),
			value:  map[string]any{"a": []any{1, "b"}},
			dest:   func() any { return &NullMap[string, NullRow[point]]{} },
			want: NullMap[string, NullRow[point]]{Map: map[string]NullRow[point]{
				"a": {Row: point{X: 1, Label: sql.NullString{String: "b", Valid: true}}, Valid: true},
			}, Valid: true},
		},
		{
			name: "row with every kind of field",
			column: rowColumn("_col0", "row(...)",
				namedField("tags", arrayType(scalarType("varchar"))),
				namedField("scores", mapType(scalarType("varchar"), scalarType("double"))),
				namedField("created", scalarType("timestamp with time zone")),
				namedField("payload", scalarType("varbinary")),
				namedField("points", arrayType(pointType)),
				namedField("raw", scalarType("bigint")),
			),
			value: []any{[]any{"a"}, map[string]any{"b": 1.5}, "2017-07-10 01:02:03.000 UTC", "AAE=", []any{[]any{2, nil}}, 3},
			dest:  func() any { return &NullRow[everyKind]{} },
			want: NullRow[everyKind]{Row: everyKind{
				Tags:    NullSlice[string]{Slice: []string{"a"}, Valid: true},
				Scores:  NullMap[string, float64]{Map: map[string]float64{"b": 1.5}, Valid: true},
				Created: time.Date(2017, 7, 10, 1, 2, 3, 0, time.UTC),
				Payload: []byte{0, 1},
				Points:  NullSlice[NullRow[point]]{Slice: []NullRow[point]{{Row: point{X: 2}, Valid: true}}, Valid: true},
				Raw:     int64(3),
				// Unexported fields are never populated, so Equal pins it at zero.
				hidden: "",
			}, Valid: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dest := tc.dest()

			require.NoError(t, scanOneValue(t, tc.column, tc.value, dest))

			assert.Equal(t, tc.want, reflect.ValueOf(dest).Elem().Interface())
		})
	}
}

func TestGenericScannersRejectMismatchedValues(t *testing.T) {
	t.Parallel()
	pointType := rowType(namedField("x", scalarType("integer")), namedField("label", scalarType("varchar")))

	cases := []struct {
		name    string
		column  queryColumn
		value   any
		dest    any
		wantErr string
	}{
		{
			name:    "scalar into a slice",
			column:  integerColumn("_col0"),
			value:   1,
			dest:    &NullSlice[int64]{},
			wantErr: "trino: cannot convert 1 (int64) to []int64",
		},
		{
			name:    "map into a slice",
			column:  column("map(varchar,varchar)", mapType(scalarType("varchar"), scalarType("varchar"))),
			value:   map[string]any{"a": "b"},
			dest:    &NullSlice[string]{},
			wantErr: "trino: cannot convert map[a:b] (map[string]interface {}) to []string",
		},
		{
			name:    "string element into an integer",
			column:  column("array(varchar)", arrayType(scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullSlice[int64]{},
			wantErr: "trino: element 0: cannot convert a (string) to int64",
		},
		{
			name:    "NULL element into a plain type",
			column:  column("array(bigint)", arrayType(scalarType("bigint"))),
			value:   []any{1, nil},
			dest:    &NullSlice[int64]{},
			wantErr: "trino: element 1: cannot convert NULL to int64, use a nullable element type",
		},
		{
			name:    "integer overflowing a narrower type",
			column:  column("array(integer)", arrayType(scalarType("integer"))),
			value:   []any{300},
			dest:    &NullSlice[int8]{},
			wantErr: "trino: element 0: value 300 overflows int8",
		},
		{
			name:    "mismatch at depth",
			column:  column("array(array(bigint))", arrayType(arrayType(scalarType("bigint")))),
			value:   []any{[]any{nil}, []any{nil, 1}},
			dest:    &NullSlice[NullSlice[sql.NullString]]{},
			wantErr: "trino: element 1: element 1: cannot convert 1 (int64) to string",
		},
		{
			name:    "unsupported element type",
			column:  column("array(bigint)", arrayType(scalarType("bigint"))),
			value:   []any{1},
			dest:    &NullSlice[complex128]{},
			wantErr: "trino: element 0: unsupported element type complex128",
		},
		{
			name:    "slice into a map",
			column:  column("array(varchar)", arrayType(scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullMap[string, string]{},
			wantErr: "trino: cannot convert [a] ([]interface {}) to map[string]string",
		},
		{
			name:    "map key of the wrong type",
			column:  column("map(varchar,varchar)", mapType(scalarType("varchar"), scalarType("varchar"))),
			value:   map[string]any{"a": "b"},
			dest:    &NullMap[int64, string]{},
			wantErr: `trino: key "a": strconv.ParseInt: parsing "a": invalid syntax`,
		},
		{
			name:    "map value of the wrong type",
			column:  column("map(varchar,varchar)", mapType(scalarType("varchar"), scalarType("varchar"))),
			value:   map[string]any{"a": "b"},
			dest:    &NullMap[string, bool]{},
			wantErr: `trino: value for key "a": cannot convert b (string) to bool`,
		},
		{
			name:    "array into a row",
			column:  column("array(varchar)", arrayType(scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullRow[point]{},
			wantErr: "trino: cannot convert [a] ([]interface {}) to trino.point",
		},
		{
			name:    "row type parameter that is not a struct",
			column:  rowColumn("_col0", "row(x integer)", namedField("x", scalarType("integer"))),
			value:   []any{1},
			dest:    &NullRow[int64]{},
			wantErr: "trino: NullRow type parameter must be a struct, got int64",
		},
		{
			name:    "row field without a struct field",
			column:  rowColumn("_col0", "row(x integer, z varchar)", namedField("x", scalarType("integer")), namedField("z", scalarType("varchar"))),
			value:   []any{1, "a"},
			dest:    &NullRow[point]{},
			wantErr: `trino: row field "z" has no matching field in trino.point`,
		},
		{
			name:    "row field matching two struct fields ignoring case",
			column:  rowColumn("_col0", "row(ab integer)", namedField("ab", scalarType("integer"))),
			value:   []any{1},
			dest:    &NullRow[struct{ Ab, AB int64 }]{},
			wantErr: `trino: row field "ab" matches both struct { Ab int64; AB int64 }.Ab and struct { Ab int64; AB int64 }.AB ignoring case`,
		},
		{
			name:    "two row fields mapping to one struct field",
			column:  rowColumn("_col0", "row(x integer, x integer)", namedField("x", scalarType("integer")), namedField("x", scalarType("integer"))),
			value:   []any{1, 2},
			dest:    &NullRow[point]{},
			wantErr: `trino: row fields "x" and "x" both map to trino.point.X`,
		},
		{
			name:    "skipped struct field",
			column:  rowColumn("_col0", "row(ignored varchar)", namedField("ignored", scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullRow[everyKind]{},
			wantErr: `trino: row field "ignored" has no matching field in trino.everyKind`,
		},
		{
			name:    "unexported struct field",
			column:  rowColumn("_col0", "row(hidden varchar)", namedField("hidden", scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullRow[everyKind]{},
			wantErr: `trino: row field "hidden" has no matching field in trino.everyKind`,
		},
		{
			name:    "row field of the wrong type",
			column:  rowColumn("_col0", "row(x varchar)", namedField("x", scalarType("varchar"))),
			value:   []any{"a"},
			dest:    &NullRow[point]{},
			wantErr: `trino: field "x": cannot convert a (string) to int64`,
		},
		{
			name:    "mismatch in a row in an array",
			column:  column("array(row(x integer, label varchar))", arrayType(pointType)),
			value:   []any{[]any{1, "a"}, []any{2, "b"}},
			dest:    &NullSlice[NullRow[struct{ X int64 }]]{},
			wantErr: `trino: element 0: row field "label" has no matching field in struct { X int64 }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := scanOneValue(t, tc.column, tc.value, tc.dest)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// Every generic scanner can be used directly, outside of rows.Scan, for
// values a caller already holds, like a Row field read with Row.Value.
func TestGenericScannersScanDirectly(t *testing.T) {
	t.Parallel()

	var slice NullSlice[NullSlice[sql.NullInt64]]
	require.NoError(t, slice.Scan([]interface{}{nil, []interface{}{int64(1)}}))
	assert.Equal(t, NullSlice[NullSlice[sql.NullInt64]]{Slice: []NullSlice[sql.NullInt64]{{}, {Slice: []sql.NullInt64{{Int64: 1, Valid: true}}, Valid: true}}, Valid: true}, slice)

	var row NullRow[point]
	require.NoError(t, row.Scan(Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true}))
	assert.Equal(t, NullRow[point]{Row: point{X: 1}, Valid: true}, row)
	require.NoError(t, row.Scan(Row{}))
	assert.Equal(t, NullRow[point]{}, row, "a zero Row is a NULL row")
}

// TestScanElementConversions covers the conversions between element types
// that do not hold the same Go type.
func TestScanElementConversions(t *testing.T) {
	t.Parallel()
	variant, err := newVariant([]byte{1, 0, 0}, []byte{1 << 2}, time.UTC)
	require.NoError(t, err)
	cases := []struct {
		name    string
		value   interface{}
		dest    sql.Scanner
		want    interface{}
		wantErr string
	}{
		{name: "integer into float64", value: int64(3), dest: &NullSlice[float64]{}, want: []float64{3}},
		{name: "integer into float32", value: int64(3), dest: &NullSlice[float32]{}, want: []float32{3}},
		{name: "decimal into float64", value: "1.25", dest: &NullSlice[sql.NullFloat64]{}, want: []sql.NullFloat64{{Float64: 1.25, Valid: true}}},
		{name: "variant into string", value: variant, dest: &NullSlice[string]{}, want: []string{"true"}},
		{name: "variant", value: variant, dest: &NullSlice[Variant]{}, want: []Variant{variant}},
		{name: "integer into int8", value: int64(-128), dest: &NullSlice[int8]{}, want: []int8{-128}},
		{name: "float into integer", value: 1.0, dest: &NullSlice[int64]{}, wantErr: "trino: element 0: cannot convert 1 (float64) to int64"},
		{name: "string into bytes", value: "AAE=", dest: &NullSlice[[]byte]{}, wantErr: "trino: element 0: cannot convert AAE= (string) to []uint8"},
		{name: "string into float64", value: "one", dest: &NullSlice[float64]{}, wantErr: `trino: element 0: cannot convert one (string) to float64: strconv.ParseFloat: parsing "one": invalid syntax`},
		{name: "string into time", value: "2017-07-10", dest: &NullSlice[time.Time]{}, wantErr: "trino: element 0: cannot convert 2017-07-10 (string) to time.Time"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.dest.Scan([]interface{}{tc.value})

			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, reflect.ValueOf(tc.dest).Elem().FieldByName("Slice").Interface())
		})
	}
}

// scanOneValue serves a single row with a single column holding value from a
// fake coordinator, whose connection is in the Asia/Tokyo zone, and scans it
// into dest.
func scanOneValue(t *testing.T, col queryColumn, value any, dest any) error {
	t.Helper()
	fc := newFakeCoordinator(t)
	col.Name = "_col0"
	fc.respond(statementPage(), columnsPage([]queryColumn{col}, [][]any{{value}}))
	db := fc.open(t, "?timezone=Asia%2FTokyo")
	return db.QueryRow("SELECT x").Scan(dest)
}

func column(dataType string, signature typeSignature) queryColumn {
	return queryColumn{Type: dataType, TypeSignature: signature}
}

func scalarType(rawType string) typeSignature {
	return typeSignature{RawType: rawType, Arguments: []typeArgument{}}
}

func arrayType(element typeSignature) typeSignature {
	return typeSignature{RawType: "array", Arguments: []typeArgument{typeArg(element)}}
}

func mapType(key, value typeSignature) typeSignature {
	return typeSignature{RawType: "map", Arguments: []typeArgument{typeArg(key), typeArg(value)}}
}

func rowType(fields ...typeArgument) typeSignature {
	return typeSignature{RawType: "row", Arguments: fields}
}

// typeArg builds a type argument carrying its signature as the raw JSON the
// server sends, decoded back by unmarshalArguments.
func typeArg(signature typeSignature) typeArgument {
	value, err := json.Marshal(signature)
	if err != nil {
		panic(err)
	}
	return typeArgument{Kind: KIND_TYPE, Value: value}
}

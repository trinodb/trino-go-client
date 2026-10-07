package trino

import (
	"cmp"
	"database/sql"
	"encoding/json/jsontext"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeConversion(t *testing.T) {
	t.Parallel()
	utc, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)

	cases := []struct {
		dataType  string
		rawType   string
		arguments []typeArgument
		// sample is the JSON form of a value; bogus is one of the wrong
		// kind, an object unless the type accepts objects.
		sample string
		bogus  string
		want   interface{}
	}{
		{
			dataType: "boolean",
			rawType:  "boolean",
			sample:   `true`,
			want:     true,
		},
		{
			dataType: "varchar(1)",
			rawType:  "varchar",
			sample:   `"hello"`,
			want:     "hello",
		},
		{
			dataType: "bigint",
			rawType:  "bigint",
			sample:   `1234516165077230279`,
			want:     int64(1234516165077230279),
		},
		{
			dataType: "double",
			rawType:  "double",
			sample:   `1.0`,
			want:     float64(1),
		},
		{
			dataType: "date",
			rawType:  "date",
			sample:   `"2017-07-10"`,
			want:     time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local),
		},
		{
			dataType: "time",
			rawType:  "time",
			sample:   `"01:02:03.000"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.Local),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.000 UTC"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.000 +03:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.000+03:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.000 -05:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.000-05:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time",
			rawType:  "time",
			sample:   `"01:02:03.123456789"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.Local),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789 UTC"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, utc),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789 +03:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789+03:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789 -05:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789-05:00"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   `"01:02:03.123456789 Europe/Paris"`,
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, paris),
		},
		{
			dataType: "timestamp",
			rawType:  "timestamp",
			sample:   `"2017-07-10 01:02:03.000"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.Local),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.000 UTC"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.000 +03:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.000+03:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.000 -04:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.000-04:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp",
			rawType:  "timestamp",
			sample:   `"2017-07-10 01:02:03.123456789"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.Local),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789 UTC"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, utc),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789 +03:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789+03:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789 -04:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789-04:00"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   `"2017-07-10 01:02:03.123456789 Europe/Paris"`,
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, paris),
		},
		{
			dataType: "map(varchar,varchar)",
			rawType:  "map",
			arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
			},
			sample: `null`,
			bogus:  `[]`,
			want:   nil,
		},
		{
			// arrays decode their elements like plain columns
			dataType: "array(varchar)",
			rawType:  "array",
			arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
			},
			sample: `["a", null]`,
			want:   []interface{}{"a", nil},
		},
		{
			// rows convert into a Row, field by field, the same way a plain column would
			dataType: "row(int, varchar(1), timestamp, array(varchar(1)))",
			rawType:  "row",
			arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "integer",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
							Arguments: []typeArgument{
								{
									Kind: "LONG",
									long: 1,
								},
							},
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "timestamp",
						},
					},
				},
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "array",
							Arguments: []typeArgument{
								{
									Kind: "TYPE",
									typeSignature: typeSignature{
										RawType: "varchar",
										Arguments: []typeArgument{
											{
												Kind: "LONG",
												long: 1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			sample: `[1, "a", "2017-07-10 01:02:03.000 UTC", ["b"]]`,
			want: Row{
				names: []string{"field0", "field1", "field2", "field3"},
				values: []interface{}{
					int64(1),
					"a",
					time.Date(2017, 7, 10, 1, 2, 3, 0, time.UTC),
					[]interface{}{"b"},
				},
				Valid: true,
			},
		},
		{
			// an unnamed field is named after its position, like in the Java client
			dataType: "row(varchar)",
			rawType:  "row",
			arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{RawType: "varchar"},
					},
				},
			},
			sample: `["a"]`,
			want:   Row{names: []string{"field0"}, values: []interface{}{"a"}, Valid: true},
		},
		{
			// a ROW field can itself be a ROW
			dataType: "row(row(integer))",
			rawType:  "row",
			arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						FieldName: rowFieldName{Name: "inner"},
						TypeSignature: typeSignature{
							RawType: "row",
							Arguments: []typeArgument{
								{
									Kind: "NAMED_TYPE",
									namedTypeSignature: namedTypeSignature{
										FieldName:     rowFieldName{Name: "x"},
										TypeSignature: typeSignature{RawType: "integer"},
									},
								},
							},
						},
					},
				},
			},
			sample: `[[1]]`,
			want: Row{
				names:  []string{"inner"},
				values: []interface{}{Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true}},
				Valid:  true,
			},
		},
		{
			// a ROW inside an ARRAY, including a NULL row
			dataType: "array(row(integer))",
			rawType:  "array",
			arguments: []typeArgument{
				{
					Kind: "TYPE",
					typeSignature: typeSignature{
						RawType: "row",
						Arguments: []typeArgument{
							{
								Kind: "NAMED_TYPE",
								namedTypeSignature: namedTypeSignature{
									FieldName:     rowFieldName{Name: "x"},
									TypeSignature: typeSignature{RawType: "integer"},
								},
							},
						},
					},
				},
			},
			sample: `[[1], null]`,
			want: []interface{}{
				Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true},
				nil,
			},
		},
		{
			// a ROW inside a MAP's value type
			dataType: "map(varchar,row(integer))",
			rawType:  "map",
			arguments: []typeArgument{
				{Kind: "TYPE", typeSignature: typeSignature{RawType: "varchar"}},
				{
					Kind: "TYPE",
					typeSignature: typeSignature{
						RawType: "row",
						Arguments: []typeArgument{
							{
								Kind: "NAMED_TYPE",
								namedTypeSignature: namedTypeSignature{
									FieldName:     rowFieldName{Name: "x"},
									TypeSignature: typeSignature{RawType: "integer"},
								},
							},
						},
					},
				},
			},
			sample: `{"k": [1]}`,
			bogus:  `[]`,
			want: map[string]interface{}{
				"k": Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true},
			},
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   `"3.1415926535897932384626433832795028841971693993751"`,
			want:     "3.1415926535897932384626433832795028841971693993751",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   `"12345678901234567890123456789012345678901234567890"`,
			want:     "12345678901234567890123456789012345678901234567890",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   `"NaN"`,
			want:     "NaN",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   `"Infinity"`,
			want:     "Infinity",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   `"-Infinity"`,
			want:     "-Infinity",
		},
		{
			dataType: "Geometry",
			rawType:  "Geometry",
			sample:   `"Point (0 0)"`,
			want:     "Point (0 0)",
		},
		{dataType: "tinyint", rawType: "tinyint", sample: `-128`, want: int64(-128)},
		{dataType: "smallint", rawType: "smallint", sample: `32767`, want: int64(32767)},
		{dataType: "integer", rawType: "integer", sample: `42`, want: int64(42)},
		{dataType: "real", rawType: "real", sample: `1.5`, want: float64(1.5)},
		{dataType: "decimal(10,5)", rawType: "decimal", sample: `"1.23000"`, want: "1.23000"},
		{dataType: "varbinary", rawType: "varbinary", sample: `"//8P/z////8="`, want: []byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff}},
		{dataType: "json", rawType: "json", sample: `"{\"aaa\": 1}"`, want: `{"aaa": 1}`},
		{dataType: "ipaddress", rawType: "ipaddress", sample: `"10.0.0.1"`, want: "10.0.0.1"},
		{dataType: "uuid", rawType: "uuid", sample: `"12151fd2-7586-11e9-8f9e-2a86e4085a59"`, want: "12151fd2-7586-11e9-8f9e-2a86e4085a59"},
		{dataType: "interval year to month", rawType: "interval year to month", sample: `"0-3"`, want: "0-3"},
		{dataType: "interval day to second", rawType: "interval day to second", sample: `"2 00:00:00.000"`, want: "2 00:00:00.000"},
		{dataType: "unknown", rawType: "unknown", sample: `null`, want: nil},

		{
			dataType: "SphericalGeography",
			rawType:  "SphericalGeography",
			sample:   `"Point (0 0)"`,
			want:     "Point (0 0)",
		},
		{dataType: "color", rawType: "color", sample: `"red"`, want: "red"},
		{dataType: "HyperLogLog", rawType: "HyperLogLog", sample: `"AAI="`, want: []byte{0x00, 0x02}},
		{dataType: "SetDigest", rawType: "SetDigest", sample: `"AAI="`, want: []byte{0x00, 0x02}},
		{dataType: "qdigest(double)", rawType: "qdigest", sample: `"AAI="`, want: []byte{0x00, 0x02}},
		{dataType: "tdigest", rawType: "tdigest", sample: `"AAI="`, want: []byte{0x00, 0x02}},
		{dataType: "ObjectId", rawType: "ObjectId", sample: `"AAI="`, want: []byte{0x00, 0x02}},
		{
			dataType: "BingTile",
			rawType:  "BingTile",
			sample:   `{"x": 1, "y": 2, "zoom": 3}`,
			bogus:    `{`,
			want:     map[string]interface{}{"x": float64(1), "y": float64(2), "zoom": float64(3)},
		},
		{
			dataType: "KdbTree",
			rawType:  "KdbTree",
			sample:   `{"root": {"leafId": 0}}`,
			bogus:    `{`,
			want:     map[string]interface{}{"root": map[string]interface{}{"leafId": float64(0)}},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s %v", tc.dataType, tc.sample), func(t *testing.T) {
			signature := typeSignature{RawType: tc.rawType, Arguments: tc.arguments}

			t.Run("null", func(t *testing.T) {
				got, err := decodeJSON(t, signature, `null`, time.Local)
				assert.NoError(t, err)
				assert.Nil(t, got)
			})

			t.Run("bogus", func(t *testing.T) {
				bogus := cmp.Or(tc.bogus, `{}`)
				_, err := decodeJSON(t, signature, bogus, time.Local)
				assert.Error(t, err, "bogus data decoded with no error")
			})

			t.Run("sample", func(t *testing.T) {
				got, err := decodeJSON(t, signature, tc.sample, time.Local)
				require.NoError(t, err)

				assert.Equal(t, tc.want, got)
			})
		})
	}
}

// decodeJSON decodes text, the JSON form of one value, the way a value of
// the type signature describes is decoded from a page.
func decodeJSON(t testing.TB, signature typeSignature, text string, location *time.Location) (any, error) {
	t.Helper()
	decoder, err := newValueDecoder(signature, location)
	require.NoError(t, err)
	return decodeValue(jsontext.NewDecoder(strings.NewReader(text)), decoder)
}

// nest wraps value in depth levels of []interface{}, the shape Trino arrays
// have after JSON decoding.
func nest(depth int, value interface{}) interface{} {
	for range depth {
		value = []interface{}{value}
	}
	return value
}

func TestSliceTypeConversion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		scanner sql.Scanner
		depth   int
		sample  interface{}
	}{
		{name: "[]bool", scanner: &NullSlice[sql.NullBool]{}, depth: 1, sample: true},
		{name: "[]string", scanner: &NullSlice[sql.NullString]{}, depth: 1, sample: "hello"},
		{name: "[]int64", scanner: &NullSlice[sql.NullInt64]{}, depth: 1, sample: int64(1)},
		{name: "[]float64", scanner: &NullSlice[sql.NullFloat64]{}, depth: 1, sample: 1.5},
		{name: "[]time.Time", scanner: &NullSlice[NullTime]{}, depth: 1, sample: time.Date(2017, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "[]map[string]interface{}", scanner: &NullSlice[NullMap[string, interface{}]]{}, depth: 1, sample: map[string]interface{}{"hello": "world"}},
		{name: "[][]bool", scanner: &NullSlice[NullSlice[sql.NullBool]]{}, depth: 2, sample: true},
		{name: "[][]string", scanner: &NullSlice[NullSlice[sql.NullString]]{}, depth: 2, sample: "hello"},
		{name: "[][]int64", scanner: &NullSlice[NullSlice[sql.NullInt64]]{}, depth: 2, sample: int64(1)},
		{name: "[][]float64", scanner: &NullSlice[NullSlice[sql.NullFloat64]]{}, depth: 2, sample: 1.5},
		{name: "[][]time.Time", scanner: &NullSlice[NullSlice[NullTime]]{}, depth: 2, sample: time.Date(2017, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "[][]map[string]interface{}", scanner: &NullSlice[NullSlice[NullMap[string, interface{}]]]{}, depth: 2, sample: map[string]interface{}{"hello": "world"}},
		{name: "[][][]bool", scanner: &NullSlice[NullSlice[NullSlice[sql.NullBool]]]{}, depth: 3, sample: true},
		{name: "[][][]string", scanner: &NullSlice[NullSlice[NullSlice[sql.NullString]]]{}, depth: 3, sample: "hello"},
		{name: "[][][]int64", scanner: &NullSlice[NullSlice[NullSlice[sql.NullInt64]]]{}, depth: 3, sample: int64(1)},
		{name: "[][][]float64", scanner: &NullSlice[NullSlice[NullSlice[sql.NullFloat64]]]{}, depth: 3, sample: 1.5},
		{name: "[][][]time.Time", scanner: &NullSlice[NullSlice[NullSlice[NullTime]]]{}, depth: 3, sample: time.Date(2017, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "[][][]map[string]interface{}", scanner: &NullSlice[NullSlice[NullSlice[NullMap[string, interface{}]]]]{}, depth: 3, sample: map[string]interface{}{"hello": "world"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("nil", func(t *testing.T) {
				assert.NoError(t, tc.scanner.Scan(nil))
				assert.NoError(t, tc.scanner.Scan(nest(tc.depth-1, nil)))
			})

			t.Run("bogus", func(t *testing.T) {
				for depth := 0; depth <= tc.depth; depth++ {
					assert.Error(t, tc.scanner.Scan(nest(depth, struct{}{})), "bogus data at depth %d scanned with no error", depth)
				}
			})

			t.Run("sample", func(t *testing.T) {
				require.NoError(t, tc.scanner.Scan(nest(tc.depth, tc.sample)))
				assert.True(t, scannerValid(t, tc.scanner), "scanner should be valid after scanning a value")
				require.NoError(t, tc.scanner.Scan(nil))
				assert.False(t, scannerValid(t, tc.scanner), "scanner should be invalid after scanning nil")
			})
		})
	}
}

// scannerValid reads the Valid field every Null* scanner in this package has.
func scannerValid(t testing.TB, scanner sql.Scanner) bool {
	t.Helper()
	field := reflect.ValueOf(scanner).Elem().FieldByName("Valid")
	require.True(t, field.IsValid(), "%T has no Valid field", scanner)
	return field.Bool()
}

func TestDecodeFloatSpecialValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sample  string
		check   func(float64) bool
		wantErr string
	}{
		{sample: `"NaN"`, check: math.IsNaN},
		{sample: `"Infinity"`, check: func(f float64) bool { return math.IsInf(f, 1) }},
		{sample: `"-Infinity"`, check: func(f float64) bool { return math.IsInf(f, -1) }},
		{sample: `"1.5"`, check: func(f float64) bool { return f == 1.5 }},
		{sample: `1.5`, check: func(f float64) bool { return f == 1.5 }},
		{sample: `"one"`, wantErr: `strconv.ParseFloat: parsing "one": invalid syntax`},
		{sample: `true`, wantErr: "expected a number, got a boolean"},
	}

	for _, rawType := range []string{"real", "double"} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s %v", rawType, tc.sample), func(t *testing.T) {
				got, err := decodeJSON(t, typeSignature{RawType: rawType}, tc.sample, time.Local)

				if tc.wantErr != "" {
					require.EqualError(t, err, tc.wantErr)
					return
				}
				require.NoError(t, err)
				assert.True(t, tc.check(got.(float64)), "unexpected value %v", got)
			})
		}
	}
}

// Types the driver does not know by name decode as base64, the same as the
// Java client's default, so a value that is not base64 is the only error path.
func TestDecodeRejectsInvalidBase64(t *testing.T) {
	t.Parallel()
	for _, rawType := range []string{"varbinary", "HyperLogLog"} {
		t.Run(rawType, func(t *testing.T) {
			_, err := decodeJSON(t, typeSignature{RawType: rawType}, `"not base64!"`, time.Local)

			require.ErrorContains(t, err, "cannot decode base64 string")
		})
	}
}

func TestDecodeRejectsWrongKinds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		rawType string
		sample  string
		wantErr string
	}{
		{rawType: "bigint", sample: `"1"`, wantErr: "expected an integer, got a string"},
		{rawType: "bigint", sample: `1.5`, wantErr: "expected an integer, got 1.5"},
		{rawType: "bigint", sample: `9223372036854775808`, wantErr: "integer 9223372036854775808 is out of range"},
		{rawType: "boolean", sample: `1`, wantErr: "expected a boolean, got a number"},
		{rawType: "varchar", sample: `[]`, wantErr: "expected a string, got an array"},
		{rawType: "timestamp", sample: `1`, wantErr: "expected a date or time string, got a number"},
		{rawType: "timestamp", sample: `"yesterday"`, wantErr: `parsing time "yesterday"`},
		{rawType: "varbinary", sample: `true`, wantErr: "expected a base64 string, got a boolean"},
	}

	for _, tc := range cases {
		t.Run(tc.rawType+" "+tc.sample, func(t *testing.T) {
			_, err := decodeJSON(t, typeSignature{RawType: tc.rawType}, tc.sample, time.Local)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestNewColumnTypeRejectsWrongArgumentKinds(t *testing.T) {
	t.Parallel()
	typeArg := typeArgument{Kind: KIND_TYPE}
	longArg := typeArgument{Kind: KIND_LONG, long: 10}
	cases := []struct {
		name      string
		rawType   string
		arguments []typeArgument
	}{
		{name: "varchar length", rawType: "varchar", arguments: []typeArgument{typeArg}},
		{name: "char length", rawType: "char", arguments: []typeArgument{typeArg}},
		{name: "decimal precision", rawType: "decimal", arguments: []typeArgument{typeArg}},
		{name: "decimal scale", rawType: "decimal", arguments: []typeArgument{longArg, typeArg}},
		{name: "time precision", rawType: "time", arguments: []typeArgument{typeArg}},
		{name: "timestamp with time zone precision", rawType: "timestamp with time zone", arguments: []typeArgument{typeArg}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newColumnType(tc.rawType, typeSignature{RawType: tc.rawType, Arguments: tc.arguments})

			require.ErrorIs(t, err, ErrInvalidResponseType)
		})
	}
}

func TestNewValueDecoderRejectsMissingArguments(t *testing.T) {
	t.Parallel()
	for _, signature := range []typeSignature{
		{RawType: "array"},
		{RawType: "map", Arguments: []typeArgument{{Kind: KIND_TYPE}}},
		{RawType: "array", Arguments: []typeArgument{{Kind: KIND_LONG, long: 1}}},
	} {
		_, err := newValueDecoder(signature, time.Local)

		require.ErrorIs(t, err, ErrInvalidResponseType, "%+v", signature)
	}
}

func TestGetScanTypeForNonStandardTypes(t *testing.T) {
	t.Parallel()
	for typeName, want := range map[string]reflect.Type{
		"Geometry":           reflect.TypeOf(sql.NullString{}),
		"SphericalGeography": reflect.TypeOf(sql.NullString{}),
		"color":              reflect.TypeOf(sql.NullString{}),
		"BingTile":           reflect.TypeOf(new(interface{})).Elem(),
		"KdbTree":            reflect.TypeOf(new(interface{})).Elem(),
		"row":                reflect.TypeOf(Row{}),
		"HyperLogLog":        reflect.TypeOf([]byte{}),
		"SetDigest":          reflect.TypeOf([]byte{}),
		"qdigest":            reflect.TypeOf([]byte{}),
		"tdigest":            reflect.TypeOf([]byte{}),
	} {
		t.Run(typeName, func(t *testing.T) {
			scanType, err := parsedScanType(t, scalarType(typeName))

			require.NoError(t, err)
			assert.Equal(t, want, scanType)
		})
	}
}

// parsedScanType decodes the type arguments of signature the way a response
// is decoded, before passing it to getScanType.
func parsedScanType(t *testing.T, signature typeSignature) (reflect.Type, error) {
	t.Helper()
	require.NoError(t, unmarshalArguments(&signature))
	return getScanType(signature)
}

func TestRowScan(t *testing.T) {
	t.Parallel()
	var r Row

	require.NoError(t, r.Scan(nil))
	assert.Equal(t, Row{}, r)

	want := Row{names: []string{"x"}, values: []interface{}{int64(1)}, Valid: true}
	require.NoError(t, r.Scan(want))
	assert.Equal(t, want, r)

	require.ErrorContains(t, r.Scan("bogus"), "cannot convert bogus (string) to Row")
}

func TestRowAccessors(t *testing.T) {
	t.Parallel()
	r := Row{names: []string{"x", "field1"}, values: []interface{}{int64(1), "a"}, Valid: true}

	assert.Equal(t, 2, r.Len())
	assert.Equal(t, "x", r.Name(0))
	assert.Equal(t, "a", r.Value(1))

	value, ok := r.Field("field1")
	assert.True(t, ok)
	assert.Equal(t, "a", value)

	value, ok = r.Field("missing")
	assert.False(t, ok)
	assert.Nil(t, value)

	assert.Zero(t, Row{}.Len(), "a NULL row has no fields")
}

// The field count comes from the column's type, not the data, so a mismatch
// is reported rather than silently misaligning names and values.
func TestRowFieldCountMismatch(t *testing.T) {
	t.Parallel()
	signature := typeSignature{
		RawType: "row",
		Arguments: []typeArgument{
			{
				Kind: "NAMED_TYPE",
				namedTypeSignature: namedTypeSignature{
					FieldName:     rowFieldName{Name: "x"},
					TypeSignature: typeSignature{RawType: "integer"},
				},
			},
		},
	}

	_, err := decodeJSON(t, signature, `[1, 2]`, time.Local)
	require.EqualError(t, err, "row has 2 fields but its type has 1")

	_, err = decodeJSON(t, signature, `[]`, time.Local)
	require.EqualError(t, err, "row has 0 fields but its type has 1")
}

// NullTime.Scan accepts only time.Time and NullTime; anything else leaves
// the value untouched and invalid instead of failing.
func TestNullTimeScanLeavesOtherTypesInvalid(t *testing.T) {
	t.Parallel()
	var value NullTime

	require.NoError(t, value.Scan("2017-07-10"))

	assert.False(t, value.Valid)
}

func TestParseZonedTimeRejectsBadInput(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "no zone", input: "2017-07-10T01:02:03", wantErr: "cannot convert 2017-07-10T01:02:03 (string) to time+zone"},
		{name: "unknown zone", input: "2017-07-10 01:02:03.000 Mars/Olympus_Mons", wantErr: `cannot load timezone "Mars/Olympus_Mons"`},
		{name: "bad offset", input: "2017-07-10 01:02:03.000 +25:00", wantErr: "parsing time"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseZonedTime(tc.input)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// Values without a zone are interpreted in the decoder's location, values
// carrying a zone keep it.
func TestTypeConversionUsesLocation(t *testing.T) {
	t.Parallel()
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	cases := []struct {
		dataType string
		sample   string
		want     time.Time
	}{
		{dataType: "date", sample: "2017-07-10", want: time.Date(2017, 7, 10, 0, 0, 0, 0, tokyo)},
		{dataType: "time", sample: "01:02:03.000", want: time.Date(0, 1, 1, 1, 2, 3, 0, tokyo)},
		{dataType: "timestamp", sample: "2017-07-10 01:02:03.000", want: time.Date(2017, 7, 10, 1, 2, 3, 0, tokyo)},
		{dataType: "timestamp with time zone", sample: "2017-07-10 01:02:03.000 UTC", want: time.Date(2017, 7, 10, 1, 2, 3, 0, time.UTC)},
	}

	for _, tc := range cases {
		t.Run(tc.dataType, func(t *testing.T) {
			got, err := decodeJSON(t, typeSignature{RawType: tc.dataType}, `"`+tc.sample+`"`, tokyo)

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.want.Location().String(), got.(time.Time).Location().String())
		})
	}
}

// Elements arrive in the connection's zone already; Location only parses
// map keys, which the server sends as strings, and is passed down.
func TestNullMapLocation(t *testing.T) {
	t.Parallel()
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	want := time.Date(2017, 7, 10, 1, 2, 3, 0, tokyo)
	sample := map[string]interface{}{"2017-07-10 01:02:03.000": "a"}

	t.Run("map", func(t *testing.T) {
		scanner := NullMap[time.Time, string]{Location: tokyo}
		require.NoError(t, scanner.Scan(sample))
		assert.Equal(t, map[time.Time]string{want: "a"}, scanner.Map)
	})

	t.Run("map in an array", func(t *testing.T) {
		scanner := NullSlice[NullMap[NullTime, string]]{Location: tokyo}
		require.NoError(t, scanner.Scan([]interface{}{sample}))
		assert.Equal(t, map[NullTime]string{{Time: want, Valid: true}: "a"}, scanner.Slice[0].Map)
	})

	t.Run("nil location means time.Local", func(t *testing.T) {
		var scanner NullMap[time.Time, string]
		require.NoError(t, scanner.Scan(sample))
		for key := range scanner.Map {
			assert.Equal(t, time.Local, key.Location())
		}
	})

	t.Run("key with a zone keeps it", func(t *testing.T) {
		var scanner NullMap[time.Time, string]
		require.NoError(t, scanner.Scan(map[string]interface{}{"2017-07-10 01:02:03.000 Asia/Tokyo": "a"}))
		require.Len(t, scanner.Map, 1)
		for key := range scanner.Map {
			assert.True(t, want.Equal(key), "got %v", key)
			assert.Equal(t, "Asia/Tokyo", key.Location().String())
		}
	})
}

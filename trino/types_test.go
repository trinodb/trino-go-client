package trino

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeConversion(t *testing.T) {
	utc, err := time.LoadLocation("UTC")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)

	cases := []struct {
		dataType  string
		rawType   string
		arguments []typeArgument
		sample    interface{}
		want      interface{}
	}{
		{
			dataType: "boolean",
			rawType:  "boolean",
			sample:   true,
			want:     true,
		},
		{
			dataType: "varchar(1)",
			rawType:  "varchar",
			sample:   "hello",
			want:     "hello",
		},
		{
			dataType: "bigint",
			rawType:  "bigint",
			sample:   json.Number("1234516165077230279"),
			want:     int64(1234516165077230279),
		},
		{
			dataType: "double",
			rawType:  "double",
			sample:   json.Number("1.0"),
			want:     float64(1),
		},
		{
			dataType: "date",
			rawType:  "date",
			sample:   "2017-07-10",
			want:     time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local),
		},
		{
			dataType: "time",
			rawType:  "time",
			sample:   "01:02:03.000",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.Local),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.000 UTC",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.000 +03:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.000+03:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.000 -05:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.000-05:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time",
			rawType:  "time",
			sample:   "01:02:03.123456789",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.Local),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789 UTC",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, utc),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789 +03:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789+03:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789 -05:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789-05:00",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			dataType: "time with time zone",
			rawType:  "time with time zone",
			sample:   "01:02:03.123456789 Europe/Paris",
			want:     time.Date(0, 1, 1, 1, 2, 3, 123456789, paris),
		},
		{
			dataType: "timestamp",
			rawType:  "timestamp",
			sample:   "2017-07-10 01:02:03.000",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.Local),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.000 UTC",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.000 +03:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.000+03:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.000 -04:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.000-04:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp",
			rawType:  "timestamp",
			sample:   "2017-07-10 01:02:03.123456789",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.Local),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789 UTC",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, utc),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789 +03:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789+03:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789 -04:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789-04:00",
			want:     time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			dataType: "timestamp with time zone",
			rawType:  "timestamp with time zone",
			sample:   "2017-07-10 01:02:03.123456789 Europe/Paris",
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
			sample: nil,
			want:   nil,
		},
		{
			// arrays return data as-is for slice scanners
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
			sample: nil,
			want:   nil,
		},
		{
			// rows return data as-is for slice scanners
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
			sample: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
			want: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   "3.1415926535897932384626433832795028841971693993751",
			want:     "3.1415926535897932384626433832795028841971693993751",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   "12345678901234567890123456789012345678901234567890",
			want:     "12345678901234567890123456789012345678901234567890",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   "NaN",
			want:     "NaN",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   "Infinity",
			want:     "Infinity",
		},
		{
			dataType: "number",
			rawType:  "number",
			sample:   "-Infinity",
			want:     "-Infinity",
		},
		{
			dataType: "Geometry",
			rawType:  "Geometry",
			sample:   "Point (0 0)",
			want:     "Point (0 0)",
		},

		{
			dataType: "SphericalGeography",
			rawType:  "SphericalGeography",
			sample:   "Point (0 0)",
			want:     "Point (0 0)",
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s %v", tc.dataType, tc.sample), func(t *testing.T) {
			converter, err := newTypeConverter(tc.dataType, typeSignature{RawType: tc.rawType, Arguments: tc.arguments})
			require.NoError(t, err)

			t.Run("nil", func(t *testing.T) {
				_, err := converter.ConvertValue(nil)
				assert.NoError(t, err)
			})

			t.Run("bogus", func(t *testing.T) {
				_, err := converter.ConvertValue(struct{}{})
				assert.Error(t, err, "bogus data scanned with no error")
			})

			t.Run("sample", func(t *testing.T) {
				got, err := converter.ConvertValue(tc.sample)
				require.NoError(t, err)

				assert.Equal(t, tc.want, got)
			})
		})
	}
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
	cases := []struct {
		name    string
		scanner sql.Scanner
		depth   int
		sample  interface{}
	}{
		{name: "[]bool", scanner: &NullSliceBool{}, depth: 1, sample: true},
		{name: "[]string", scanner: &NullSliceString{}, depth: 1, sample: "hello"},
		{name: "[]int64", scanner: &NullSliceInt64{}, depth: 1, sample: json.Number("1")},
		{name: "[]float64", scanner: &NullSliceFloat64{}, depth: 1, sample: json.Number("1.0")},
		{name: "[]time.Time", scanner: &NullSliceTime{}, depth: 1, sample: "2017-07-01"},
		{name: "[]map[string]interface{}", scanner: &NullSliceMap{}, depth: 1, sample: map[string]interface{}{"hello": "world"}},
		{name: "[][]bool", scanner: &NullSlice2Bool{}, depth: 2, sample: true},
		{name: "[][]string", scanner: &NullSlice2String{}, depth: 2, sample: "hello"},
		{name: "[][]int64", scanner: &NullSlice2Int64{}, depth: 2, sample: json.Number("1")},
		{name: "[][]float64", scanner: &NullSlice2Float64{}, depth: 2, sample: json.Number("1.0")},
		{name: "[][]time.Time", scanner: &NullSlice2Time{}, depth: 2, sample: "2017-07-01"},
		{name: "[][]map[string]interface{}", scanner: &NullSlice2Map{}, depth: 2, sample: map[string]interface{}{"hello": "world"}},
		{name: "[][][]bool", scanner: &NullSlice3Bool{}, depth: 3, sample: true},
		{name: "[][][]string", scanner: &NullSlice3String{}, depth: 3, sample: "hello"},
		{name: "[][][]int64", scanner: &NullSlice3Int64{}, depth: 3, sample: json.Number("1")},
		{name: "[][][]float64", scanner: &NullSlice3Float64{}, depth: 3, sample: json.Number("1.0")},
		{name: "[][][]time.Time", scanner: &NullSlice3Time{}, depth: 3, sample: "2017-07-01"},
		{name: "[][][]map[string]interface{}", scanner: &NullSlice3Map{}, depth: 3, sample: map[string]interface{}{"hello": "world"}},
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

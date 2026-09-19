package trino

import (
	"database/sql"
	"encoding/json"
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

	testcases := []struct {
		DataType                   string
		RawType                    string
		Arguments                  []typeArgument
		ResponseUnmarshalledSample interface{}
		ExpectedGoValue            interface{}
	}{
		{
			DataType:                   "boolean",
			RawType:                    "boolean",
			ResponseUnmarshalledSample: true,
			ExpectedGoValue:            true,
		},
		{
			DataType:                   "varchar(1)",
			RawType:                    "varchar",
			ResponseUnmarshalledSample: "hello",
			ExpectedGoValue:            "hello",
		},
		{
			DataType:                   "bigint",
			RawType:                    "bigint",
			ResponseUnmarshalledSample: json.Number("1234516165077230279"),
			ExpectedGoValue:            int64(1234516165077230279),
		},
		{
			DataType:                   "double",
			RawType:                    "double",
			ResponseUnmarshalledSample: json.Number("1.0"),
			ExpectedGoValue:            float64(1),
		},
		{
			DataType:                   "date",
			RawType:                    "date",
			ResponseUnmarshalledSample: "2017-07-10",
			ExpectedGoValue:            time.Date(2017, 7, 10, 0, 0, 0, 0, time.Local),
		},
		{
			DataType:                   "time",
			RawType:                    "time",
			ResponseUnmarshalledSample: "01:02:03.000",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 +03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000+03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000 -05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.000-05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time",
			RawType:                    "time",
			ResponseUnmarshalledSample: "01:02:03.123456789",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.Local),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 UTC",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, utc),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 +03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789+03:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 -05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789-05:00",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, time.FixedZone("", -5*3600)),
		},
		{
			DataType:                   "time with time zone",
			RawType:                    "time with time zone",
			ResponseUnmarshalledSample: "01:02:03.123456789 Europe/Paris",
			ExpectedGoValue:            time.Date(0, 1, 1, 1, 2, 3, 123456789, paris),
		},
		{
			DataType:                   "timestamp",
			RawType:                    "timestamp",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.Local),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 UTC",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, utc),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 +03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000+03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000 -04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.000-04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 0, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp",
			RawType:                    "timestamp",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.Local),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 UTC",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, utc),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 +03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789+03:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", 3*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 -04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789-04:00",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, time.FixedZone("", -4*3600)),
		},
		{
			DataType:                   "timestamp with time zone",
			RawType:                    "timestamp with time zone",
			ResponseUnmarshalledSample: "2017-07-10 01:02:03.123456789 Europe/Paris",
			ExpectedGoValue:            time.Date(2017, 7, 10, 1, 2, 3, 123456789, paris),
		},
		{
			DataType: "map(varchar,varchar)",
			RawType:  "map",
			Arguments: []typeArgument{
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
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// arrays return data as-is for slice scanners
			DataType: "array(varchar)",
			RawType:  "array",
			Arguments: []typeArgument{
				{
					Kind: "NAMED_TYPE",
					namedTypeSignature: namedTypeSignature{
						TypeSignature: typeSignature{
							RawType: "varchar",
						},
					},
				},
			},
			ResponseUnmarshalledSample: nil,
			ExpectedGoValue:            nil,
		},
		{
			// rows return data as-is for slice scanners
			DataType: "row(int, varchar(1), timestamp, array(varchar(1)))",
			RawType:  "row",
			Arguments: []typeArgument{
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
			ResponseUnmarshalledSample: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
			ExpectedGoValue: []interface{}{
				json.Number("1"),
				"a",
				"2017-07-10 01:02:03.000 UTC",
				[]interface{}{"b"},
			},
		},
		{
			DataType:                   "number",
			RawType:                    "number",
			ResponseUnmarshalledSample: "3.1415926535897932384626433832795028841971693993751",
			ExpectedGoValue:            "3.1415926535897932384626433832795028841971693993751",
		},
		{
			DataType:                   "number",
			RawType:                    "number",
			ResponseUnmarshalledSample: "12345678901234567890123456789012345678901234567890",
			ExpectedGoValue:            "12345678901234567890123456789012345678901234567890",
		},
		{
			DataType:                   "number",
			RawType:                    "number",
			ResponseUnmarshalledSample: "NaN",
			ExpectedGoValue:            "NaN",
		},
		{
			DataType:                   "number",
			RawType:                    "number",
			ResponseUnmarshalledSample: "Infinity",
			ExpectedGoValue:            "Infinity",
		},
		{
			DataType:                   "number",
			RawType:                    "number",
			ResponseUnmarshalledSample: "-Infinity",
			ExpectedGoValue:            "-Infinity",
		},
		{
			DataType:                   "Geometry",
			RawType:                    "Geometry",
			ResponseUnmarshalledSample: "Point (0 0)",
			ExpectedGoValue:            "Point (0 0)",
		},

		{
			DataType:                   "SphericalGeography",
			RawType:                    "SphericalGeography",
			ResponseUnmarshalledSample: "Point (0 0)",
			ExpectedGoValue:            "Point (0 0)",
		},
	}

	for _, tc := range testcases {
		converter, err := newTypeConverter(tc.DataType, typeSignature{RawType: tc.RawType, Arguments: tc.Arguments})
		assert.NoError(t, err)

		t.Run(tc.DataType+":nil", func(t *testing.T) {
			_, err := converter.ConvertValue(nil)
			assert.NoError(t, err)
		})

		t.Run(tc.DataType+":bogus", func(t *testing.T) {
			_, err := converter.ConvertValue(struct{}{})
			assert.Error(t, err, "bogus data scanned with no error")
		})

		t.Run(tc.DataType+":sample", func(t *testing.T) {
			v, err := converter.ConvertValue(tc.ResponseUnmarshalledSample)
			require.NoError(t, err)

			require.Equal(t,
				v, tc.ExpectedGoValue,
				"unexpected data from sample:\nhave %+v\nwant %+v", v, tc.ExpectedGoValue)
		})
	}
}

func TestSliceTypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[]bool",
			Scanner:                         &NullSliceBool{},
			TrinoResponseUnmarshalledSample: []interface{}{true},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceBool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]string",
			Scanner:                         &NullSliceString{},
			TrinoResponseUnmarshalledSample: []interface{}{"hello"},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceString)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]int64",
			Scanner:                         &NullSliceInt64{},
			TrinoResponseUnmarshalledSample: []interface{}{json.Number("1")},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceInt64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},

		{
			GoType:                          "[]float64",
			Scanner:                         &NullSliceFloat64{},
			TrinoResponseUnmarshalledSample: []interface{}{json.Number("1.0")},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceFloat64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]time.Time",
			Scanner:                         &NullSliceTime{},
			TrinoResponseUnmarshalledSample: []interface{}{"2017-07-01"},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceTime)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[]map[string]interface{}",
			Scanner:                         &NullSliceMap{},
			TrinoResponseUnmarshalledSample: []interface{}{map[string]interface{}{"hello": "world"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSliceMap)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}))
			assert.Error(t, tc.Scanner.Scan([]interface{}{struct{}{}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

func TestSlice2TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[][]bool",
			Scanner:                         &NullSlice2Bool{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{true}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Bool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]string",
			Scanner:                         &NullSlice2String{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{"hello"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2String)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]int64",
			Scanner:                         &NullSlice2Int64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{json.Number("1")}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Int64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]float64",
			Scanner:                         &NullSlice2Float64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{json.Number("1.0")}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Float64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]time.Time",
			Scanner:                         &NullSlice2Time{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{"2017-07-01"}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Time)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][]map[string]interface{}",
			Scanner:                         &NullSlice2Map{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice2Map)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
			assert.NoError(t, tc.Scanner.Scan([]interface{}{nil}))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{struct{}{}}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

func TestSlice3TypeConversion(t *testing.T) {
	testcases := []struct {
		GoType                          string
		Scanner                         sql.Scanner
		TrinoResponseUnmarshalledSample interface{}
		TestScanner                     func(t *testing.T, s sql.Scanner, isValid bool)
	}{
		{
			GoType:                          "[][][]bool",
			Scanner:                         &NullSlice3Bool{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{true}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Bool)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]string",
			Scanner:                         &NullSlice3String{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"hello"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3String)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]int64",
			Scanner:                         &NullSlice3Int64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{json.Number("1")}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Int64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]float64",
			Scanner:                         &NullSlice3Float64{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{json.Number("1.0")}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Float64)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]time.Time",
			Scanner:                         &NullSlice3Time{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{"2017-07-01"}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Time)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
		{
			GoType:                          "[][][]map[string]interface{}",
			Scanner:                         &NullSlice3Map{},
			TrinoResponseUnmarshalledSample: []interface{}{[]interface{}{[]interface{}{map[string]interface{}{"hello": "world"}}}},
			TestScanner: func(t *testing.T, s sql.Scanner, isValid bool) {
				v, _ := s.(*NullSlice3Map)
				assert.Equal(t, isValid, v.Valid, "scanner failed")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.GoType+":nil", func(t *testing.T) {
			assert.NoError(t, tc.Scanner.Scan(nil))
			assert.NoError(t, tc.Scanner.Scan([]interface{}{[]interface{}{nil}}))
		})

		t.Run(tc.GoType+":bogus", func(t *testing.T) {
			assert.Error(t, tc.Scanner.Scan(struct{}{}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{struct{}{}}}), "bogus data scanned with no error")
			assert.Error(t, tc.Scanner.Scan([]interface{}{[]interface{}{[]interface{}{struct{}{}}}}), "bogus data scanned with no error")
		})

		t.Run(tc.GoType+":sample", func(t *testing.T) {
			require.NoError(t, tc.Scanner.Scan(tc.TrinoResponseUnmarshalledSample))
			tc.TestScanner(t, tc.Scanner, true)
			require.NoError(t, tc.Scanner.Scan(nil))
			tc.TestScanner(t, tc.Scanner, false)
		})
	}
}

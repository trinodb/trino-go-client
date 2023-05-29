// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
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

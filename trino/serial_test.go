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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSerial(t *testing.T) {
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	scenarios := []struct {
		name           string
		value          interface{}
		expectedError  bool
		expectedSerial string
	}{
		{
			name:           "basic string",
			value:          "hello world",
			expectedSerial: `'hello world'`,
		},
		{
			name:           "single quoted string",
			value:          "hello world's",
			expectedSerial: `'hello world''s'`,
		},
		{
			name:           "double quoted string",
			value:          `hello "world"`,
			expectedSerial: `'hello "world"'`,
		},
		{
			name:           "int8",
			value:          int8(100),
			expectedSerial: "100",
		},
		{
			name:           "int16",
			value:          int16(100),
			expectedSerial: "100",
		},
		{
			name:           "int32",
			value:          int32(100),
			expectedSerial: "100",
		},
		{
			name:           "int",
			value:          int(100),
			expectedSerial: "100",
		},
		{
			name:           "int64",
			value:          int64(100),
			expectedSerial: "100",
		},
		{
			name:          "uint8",
			value:         uint8(100),
			expectedError: true,
		},
		{
			name:           "uint16",
			value:          uint16(100),
			expectedSerial: "100",
		},
		{
			name:           "uint32",
			value:          uint32(100),
			expectedSerial: "100",
		},
		{
			name:           "uint",
			value:          uint(100),
			expectedSerial: "100",
		},
		{
			name:           "uint64",
			value:          uint64(100),
			expectedSerial: "100",
		},
		{
			name:          "byte",
			value:         byte('a'),
			expectedError: true,
		},
		{
			name:           "valid Numeric",
			value:          Numeric("10"),
			expectedSerial: "10",
		},
		{
			name:          "invalid Numeric",
			value:         Numeric("not-a-number"),
			expectedError: true,
		},
		{
			name:           "bool true",
			value:          true,
			expectedSerial: "true",
		},
		{
			name:           "bool false",
			value:          false,
			expectedSerial: "false",
		},
		{
			name:           "date",
			value:          Date(2017, 7, 10),
			expectedSerial: "DATE '2017-07-10'",
		},
		{
			name:           "time without timezone",
			value:          Time(11, 34, 25, 123456),
			expectedSerial: "TIME '11:34:25.000123456'",
		},
		{
			name:           "time with timezone",
			value:          TimeTz(11, 34, 25, 123456, time.FixedZone("test zone", +2*3600)),
			expectedSerial: "TIME '11:34:25.000123456 +02:00'",
		},
		{
			name:           "time with timezone",
			value:          TimeTz(11, 34, 25, 123456, nil),
			expectedSerial: "TIME '11:34:25.000123456 Z'",
		},
		{
			name:           "timestamp without timezone",
			value:          Timestamp(2017, 7, 10, 11, 34, 25, 123456),
			expectedSerial: "TIMESTAMP '2017-07-10 11:34:25.000123456'",
		},
		{
			name:           "timestamp with time zone in Fixed Zone",
			value:          time.Date(2017, 7, 10, 11, 34, 25, 123456, time.FixedZone("test zone", +2*3600)),
			expectedSerial: "TIMESTAMP '2017-07-10 11:34:25.000123456 +02:00'",
		},
		{
			name:           "timestamp with time zone in Named Zone",
			value:          time.Date(2017, 7, 10, 11, 34, 25, 123456, paris),
			expectedSerial: "TIMESTAMP '2017-07-10 11:34:25.000123456 +02:00'",
		},
		{
			name:           "timestamp with time zone in UTC",
			value:          time.Date(2017, 7, 10, 11, 34, 25, 123456, time.UTC),
			expectedSerial: "TIMESTAMP '2017-07-10 11:34:25.000123456 Z'",
		},
		{
			name:           "duration",
			value:          10*time.Second + 5*time.Millisecond,
			expectedSerial: "INTERVAL '10.005' SECOND",
		},
		{
			name:           "duration with negative value",
			value:          -(10*time.Second + 5*time.Millisecond),
			expectedSerial: "INTERVAL '-10.005' SECOND",
		},
		{
			name:           "minute duration",
			value:          10 * time.Minute,
			expectedSerial: "INTERVAL '10' MINUTE",
		},
		{
			name:           "hour duration",
			value:          23 * time.Hour,
			expectedSerial: "INTERVAL '23' HOUR",
		},
		{
			name:           "max hour duration",
			value:          (math.MaxInt64 / time.Hour) * time.Hour,
			expectedSerial: "INTERVAL '2562047' HOUR",
		},
		{
			name:           "min hour duration",
			value:          (math.MinInt64 / time.Hour) * time.Hour,
			expectedSerial: "INTERVAL '-2562047' HOUR",
		},
		{
			name:           "max minute duration",
			value:          (math.MaxInt64 / time.Minute) * time.Minute,
			expectedSerial: "INTERVAL '153722867' MINUTE",
		},
		{
			name:           "min minute duration",
			value:          (math.MinInt64 / time.Minute) * time.Minute,
			expectedSerial: "INTERVAL '-153722867' MINUTE",
		},
		{
			name:          "too big second duration",
			value:         (math.MaxInt64 / time.Second) * time.Second,
			expectedError: true,
		},
		{
			name:          "too small second duration",
			value:         (math.MinInt64 / time.Second) * time.Second,
			expectedError: true,
		},
		{
			name:          "too big millisecond duration",
			value:         time.Millisecond*912 + time.Second*12345678,
			expectedError: true,
		},
		{
			name:          "too small millisecond duration",
			value:         -(time.Millisecond*910 + time.Second*123456789),
			expectedError: true,
		},
		{
			name:           "max allowed second duration",
			value:          math.MaxInt32 * time.Second,
			expectedSerial: "INTERVAL '2147483647' SECOND",
		},
		{
			name:           "min allowed second duration",
			value:          -math.MaxInt32 * time.Second,
			expectedSerial: "INTERVAL '-2147483647' SECOND",
		},
		{
			name:           "max allowed second with milliseconds duration",
			value:          999999999*time.Second + 900*time.Millisecond,
			expectedSerial: "INTERVAL '999999999.9' SECOND",
		},
		{
			name:           "min allowed second with milliseconds duration",
			value:          -999999999*time.Second - 900*time.Millisecond,
			expectedSerial: "INTERVAL '-999999999.9' SECOND",
		},
		{
			name:           "nil",
			value:          nil,
			expectedSerial: "NULL",
		},
		{
			name:          "slice typed nil",
			value:         []interface{}(nil),
			expectedError: true,
		},
		{
			name:           "valid slice",
			value:          []interface{}{1, 2},
			expectedSerial: "ARRAY[1, 2]",
		},
		{
			name:           "valid empty",
			value:          []interface{}{},
			expectedSerial: "ARRAY[]",
		},
		{
			name:          "invalid slice contents",
			value:         []interface{}{1, byte('a')},
			expectedError: true,
		},
	}

	for i := range scenarios {
		scenario := scenarios[i]

		t.Run(scenario.name, func(t *testing.T) {
			s, err := Serial(scenario.value)
			if err != nil {
				if scenario.expectedError {
					return
				}
				t.Fatal(err)
			}

			if scenario.expectedError {
				t.Fatal("missing an expected error")
			}

			if scenario.expectedSerial != s {
				t.Fatalf("mismatched serial, got %q expected %q", s, scenario.expectedSerial)
			}
		})
	}
}

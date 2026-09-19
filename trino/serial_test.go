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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerial(t *testing.T) {
	t.Parallel()
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	cases := []struct {
		name    string
		value   interface{}
		want    string
		wantErr string
	}{
		{name: "basic string", value: "hello world", want: `'hello world'`},
		{name: "single quoted string", value: "hello world's", want: `'hello world''s'`},
		{name: "double quoted string", value: `hello "world"`, want: `'hello "world"'`},
		{name: "empty string", value: "", want: `''`},
		{name: "basic binary", value: []byte{0x01, 0x02, 0x03}, want: `X'010203'`},
		{name: "empty binary", value: []byte{}, want: `X''`},
		{name: "nil binary", value: []byte(nil), want: `NULL`},
		{name: "int8", value: int8(100), want: "100"},
		{name: "int16", value: int16(100), want: "100"},
		{name: "int32", value: int32(100), want: "100"},
		{name: "int", value: int(100), want: "100"},
		{name: "int64", value: int64(100), want: "100"},
		{name: "uint8", value: uint8(100), wantErr: "trino: unsupported arg type: byte/uint8"},
		{name: "uint16", value: uint16(100), want: "100"},
		{name: "uint32", value: uint32(100), want: "100"},
		{name: "uint", value: uint(100), want: "100"},
		{name: "uint64", value: uint64(100), want: "100"},
		{name: "byte", value: byte('a'), wantErr: "trino: unsupported arg type: byte/uint8"},
		{name: "float32", value: float32(1.5), wantErr: "trino: unsupported arg type: float32"},
		{name: "float64", value: float64(1.5), wantErr: "trino: unsupported arg type: float64"},
		{name: "valid Numeric", value: Numeric("10"), want: "10"},
		{name: "Numeric with sign, fraction and exponent", value: Numeric("-1.5e+3"), want: "-1.5e+3"},
		{name: "Numeric with leading dot", value: Numeric(".5"), want: ".5"},
		{name: "Numeric with trailing dot", value: Numeric("1."), want: "1."},
		{name: "Numeric with plus sign", value: Numeric("+1"), want: "+1"},
		{name: "Numeric with capital exponent", value: Numeric("1E5"), want: "1E5"},
		{name: "empty Numeric", value: Numeric(""), wantErr: `trino: Numeric "" is not a decimal or scientific number literal`},
		{name: "Numeric NaN", value: Numeric("NaN"), wantErr: `trino: Numeric "NaN" is not a decimal or scientific number literal`},
		{name: "Numeric infinity", value: Numeric("-Inf"), wantErr: `trino: Numeric "-Inf" is not a decimal or scientific number literal`},
		{name: "Numeric with digit separators", value: Numeric("1_000"), wantErr: `trino: Numeric "1_000" is not a decimal or scientific number literal`},
		{name: "Numeric hexadecimal float", value: Numeric("0x1p-2"), wantErr: `trino: Numeric "0x1p-2" is not a decimal or scientific number literal`},
		{name: "Numeric with surrounding space", value: Numeric(" 1"), wantErr: `trino: Numeric " 1" is not a decimal or scientific number literal`},
		{name: "invalid Numeric", value: Numeric("not-a-number"), wantErr: `trino: Numeric "not-a-number" is not a decimal or scientific number literal`},
		{name: "bool true", value: true, want: "true"},
		{name: "bool false", value: false, want: "false"},
		{name: "date", value: Date(2017, 7, 10), want: "DATE '2017-07-10'"},
		{name: "time without timezone", value: Time(11, 34, 25, 123456), want: "TIME '11:34:25.000123456'"},
		{name: "time with timezone", value: TimeTz(11, 34, 25, 123456, time.FixedZone("test zone", +2*3600)), want: "TIME '11:34:25.000123456 +02:00'"},
		{name: "time with negative half-hour offset", value: TimeTz(11, 34, 25, 123456, time.FixedZone("test zone", -5*3600-30*60)), want: "TIME '11:34:25.000123456 -05:30'"},
		{name: "time with quarter-hour offset", value: TimeTz(11, 34, 25, 123456, time.FixedZone("test zone", +5*3600+45*60)), want: "TIME '11:34:25.000123456 +05:45'"},
		// a nil location means UTC, and UTC is written as an offset since Trino rejects Z
		{name: "time with nil timezone", value: TimeTz(11, 34, 25, 123456, nil), want: "TIME '11:34:25.000123456 +00:00'"},
		{name: "time with UTC timezone", value: TimeTz(11, 34, 25, 123456, time.UTC), want: "TIME '11:34:25.000123456 +00:00'"},
		{name: "timestamp without timezone", value: Timestamp(2017, 7, 10, 11, 34, 25, 123456), want: "TIMESTAMP '2017-07-10 11:34:25.000123456'"},
		{name: "timestamp with time zone in Fixed Zone", value: time.Date(2017, 7, 10, 11, 34, 25, 123456, time.FixedZone("test zone", +2*3600)), want: "TIMESTAMP '2017-07-10 11:34:25.000123456 +02:00'"},
		{name: "timestamp with time zone in Named Zone", value: time.Date(2017, 7, 10, 11, 34, 25, 123456, paris), want: "TIMESTAMP '2017-07-10 11:34:25.000123456 +02:00'"},
		{name: "timestamp with time zone in UTC", value: time.Date(2017, 7, 10, 11, 34, 25, 123456, time.UTC), want: "TIMESTAMP '2017-07-10 11:34:25.000123456 Z'"},
		{name: "timestamp before the epoch", value: time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), want: "TIMESTAMP '1969-12-31 23:59:59 Z'"},
		{name: "zero timestamp", value: time.Time{}, want: "TIMESTAMP '0001-01-01 00:00:00 Z'"},
		{name: "duration", value: 10*time.Second + 5*time.Millisecond, want: "INTERVAL '10.005' SECOND"},
		{name: "duration with negative value", value: -(10*time.Second + 5*time.Millisecond), want: "INTERVAL '-10.005' SECOND"},
		{name: "negative duration shorter than a second", value: -500 * time.Millisecond, want: "INTERVAL '-0.5' SECOND"},
		{name: "negative duration of a few milliseconds", value: -5 * time.Millisecond, want: "INTERVAL '-0.005' SECOND"},
		{name: "zero duration", value: time.Duration(0), want: "INTERVAL '0' HOUR"},
		{name: "minute duration", value: 10 * time.Minute, want: "INTERVAL '10' MINUTE"},
		{name: "hour duration", value: 23 * time.Hour, want: "INTERVAL '23' HOUR"},
		{name: "max hour duration", value: (math.MaxInt64 / time.Hour) * time.Hour, want: "INTERVAL '2562047' HOUR"},
		{name: "min hour duration", value: (math.MinInt64 / time.Hour) * time.Hour, want: "INTERVAL '-2562047' HOUR"},
		{name: "max minute duration", value: (math.MaxInt64 / time.Minute) * time.Minute, want: "INTERVAL '153722867' MINUTE"},
		{name: "min minute duration", value: (math.MinInt64 / time.Minute) * time.Minute, want: "INTERVAL '-153722867' MINUTE"},
		{name: "too big second duration", value: (math.MaxInt64 / time.Second) * time.Second, wantErr: "out of range for interval of seconds type"},
		{name: "too small second duration", value: (math.MinInt64 / time.Second) * time.Second, wantErr: "out of range for interval of seconds type"},
		{name: "second duration with too many digits", value: time.Duration(1234567891200) * time.Millisecond, wantErr: "out of range for interval of seconds with millis type"},
		{name: "negative second duration with too many digits", value: time.Duration(-1234567891200) * time.Millisecond, wantErr: "out of range for interval of seconds with millis type"},
		{name: "millisecond duration with too many digits", value: time.Duration(123456789120) * time.Millisecond, wantErr: "out of range for interval of seconds with millis type"},
		{name: "negative millisecond duration with too many digits", value: time.Duration(-123456789120) * time.Millisecond, wantErr: "out of range for interval of seconds with millis type"},
		{name: "too big millisecond duration", value: time.Millisecond*912 + time.Second*12345678, wantErr: "out of range for interval of seconds with millis type"},
		{name: "too small millisecond duration", value: -(time.Millisecond*910 + time.Second*123456789), wantErr: "out of range for interval of seconds with millis type"},
		{name: "sub-millisecond duration", value: 1500 * time.Microsecond, wantErr: "is not a multiple of hours, minutes, seconds or milliseconds"},
		{name: "max allowed second duration", value: math.MaxInt32 * time.Second, want: "INTERVAL '2147483647' SECOND"},
		{name: "min allowed second duration", value: -math.MaxInt32 * time.Second, want: "INTERVAL '-2147483647' SECOND"},
		{name: "max allowed second with milliseconds duration", value: 999999999*time.Second + 900*time.Millisecond, want: "INTERVAL '999999999.9' SECOND"},
		{name: "min allowed second with milliseconds duration", value: -999999999*time.Second - 900*time.Millisecond, want: "INTERVAL '-999999999.9' SECOND"},
		{name: "nil", value: nil, want: "NULL"},
		{name: "slice typed nil", value: []interface{}(nil), wantErr: "trino: unsupported arg type: []<nil>"},
		{name: "valid slice", value: []interface{}{1, 2}, want: "ARRAY[1, 2]"},
		{name: "valid empty", value: []interface{}{}, want: "ARRAY[]"},
		{name: "string slice", value: []string{"a", "b'c"}, want: "ARRAY['a', 'b''c']"},
		{name: "int slice", value: []int{1, 2}, want: "ARRAY[1, 2]"},
		{name: "nested slice", value: [][]int{{1}, {2, 3}}, want: "ARRAY[ARRAY[1], ARRAY[2, 3]]"},
		{name: "binary slice", value: [][]byte{{0x01}, nil}, want: "ARRAY[X'01', NULL]"},
		{name: "invalid slice contents", value: []interface{}{1, byte('a')}, wantErr: "trino: unsupported arg type: byte/uint8"},
		{name: "json", value: json.RawMessage(`{}`), wantErr: "trino: unsupported arg type: json.RawMessage"},
		{name: "map", value: map[string]string{"a": "b"}, wantErr: "trino: unsupported arg type: map"},
		{name: "struct", value: struct{ A int }{1}, wantErr: "trino: unsupported arg type: struct { A int }"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Serial(tc.value)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

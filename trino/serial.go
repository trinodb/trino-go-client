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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type UnsupportedArgError struct {
	t string
}

func (e UnsupportedArgError) Error() string {
	return fmt.Sprintf("trino: unsupported arg type: %s", e.t)
}

// Numeric is a string representation of a number, such as "10", "5.5" or in scientific form
// If another string format is used it will error to serialise
type Numeric string

// trinoDate represents a Date type in Trino.
type trinoDate struct {
	year  int
	month time.Month
	day   int
}

// Date creates a representation of a Trino Date type.
func Date(year int, month time.Month, day int) trinoDate {
	return trinoDate{year, month, day}
}

// trinoTime represents a Time type in Trino.
type trinoTime struct {
	hour       int
	minute     int
	second     int
	nanosecond int
}

// Time creates a representation of a Trino Time type. To represent time with precision higher than nanoseconds, pass the value as a string and use a cast in the query.
func Time(hour int,
	minute int,
	second int,
	nanosecond int) trinoTime {
	return trinoTime{hour, minute, second, nanosecond}
}

// trinoTimeTz represents a Time(9) With Timezone type in Trino.
type trinoTimeTz time.Time

// TimeTz creates a representation of a Trino Time(9) With Timezone type.
func TimeTz(hour int,
	minute int,
	second int,
	nanosecond int,
	location *time.Location) trinoTimeTz {
	// When reading a time, a nil location indicates UTC.
	// However, passing nil to time.Date() panics.
	if location == nil {
		location = time.UTC
	}
	return trinoTimeTz(time.Date(0, 0, 0, hour, minute, second, nanosecond, location))
}

// Timestamp indicates we want a TimeStamp type WITHOUT a time zone in Trino from a Golang time.
type trinoTimestamp time.Time

// Timestamp creates a representation of a Trino Timestamp(9) type.
func Timestamp(year int,
	month time.Month,
	day int,
	hour int,
	minute int,
	second int,
	nanosecond int) trinoTimestamp {
	return trinoTimestamp(time.Date(year, month, day, hour, minute, second, nanosecond, time.UTC))
}

// Serial converts any supported value to its equivalent string for as a Trino parameter
// See https://trino.io/docs/current/language/types.html
func Serial(v interface{}) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil

	// numbers convertible to int
	case int8:
		return strconv.Itoa(int(x)), nil
	case int16:
		return strconv.Itoa(int(x)), nil
	case int32:
		return strconv.Itoa(int(x)), nil
	case int:
		return strconv.Itoa(x), nil
	case uint16:
		return strconv.Itoa(int(x)), nil

	case int64:
		return strconv.FormatInt(x, 10), nil

	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil

		// float32, float64 not supported because digit precision will easily cause large problems
	case float32:
		return "", UnsupportedArgError{"float32"}
	case float64:
		return "", UnsupportedArgError{"float64"}

	case Numeric:
		if _, err := strconv.ParseFloat(string(x), 64); err != nil {
			return "", err
		}
		return string(x), nil

		// note byte and uint are not supported, this is because byte is an alias for uint8
		// if you were to use uint8 (as a number) it could be interpreted as a byte, so it is unsupported
		// use string instead of byte and any other uint/int type for uint8
	case byte:
		return "", UnsupportedArgError{"byte/uint8"}

	case bool:
		return strconv.FormatBool(x), nil

	case string:
		return "'" + strings.Replace(x, "'", "''", -1) + "'", nil

		// TODO - []byte should probably be matched to 'VARBINARY' in trino
	case []byte:
		return "", UnsupportedArgError{"[]byte"}

	case trinoDate:
		return fmt.Sprintf("DATE '%04d-%02d-%02d'", x.year, x.month, x.day), nil
	case trinoTime:
		return fmt.Sprintf("TIME '%02d:%02d:%02d.%09d'", x.hour, x.minute, x.second, x.nanosecond), nil
	case trinoTimeTz:
		return "TIME " + time.Time(x).Format("'15:04:05.999999999 Z07:00'"), nil
	case trinoTimestamp:
		return "TIMESTAMP " + time.Time(x).Format("'2006-01-02 15:04:05.999999999'"), nil
	case time.Time:
		return "TIMESTAMP " + time.Time(x).Format("'2006-01-02 15:04:05.999999999 Z07:00'"), nil

	case time.Duration:
		return serialDuration(x)

		// TODO - json.RawMesssage should probably be matched to 'JSON' in Trino
	case json.RawMessage:
		return "", UnsupportedArgError{"json.RawMessage"}
	}

	if reflect.TypeOf(v).Kind() == reflect.Slice {
		x := reflect.ValueOf(v)
		if x.IsNil() {
			return "", UnsupportedArgError{"[]<nil>"}
		}

		slice := make([]interface{}, x.Len())

		for i := 0; i < x.Len(); i++ {
			slice[i] = x.Index(i).Interface()
		}

		return serialSlice(slice)
	}

	if reflect.TypeOf(v).Kind() == reflect.Map {
		// are Trino MAPs indifferent to order? Golang maps are, if Trino aren't then the two types can't be compatible
		return "", UnsupportedArgError{"map"}
	}

	// TODO - consider the remaining types in https://trino.io/docs/current/language/types.html (Row, IP, ...)

	return "", UnsupportedArgError{fmt.Sprintf("%T", v)}
}

func serialSlice(v []interface{}) (string, error) {
	ss := make([]string, len(v))

	for i, x := range v {
		s, err := Serial(x)
		if err != nil {
			return "", err
		}
		ss[i] = s
	}

	return "ARRAY[" + strings.Join(ss, ", ") + "]", nil
}

const (
	// For seconds with milliseconds there is a maximum length of 10 digits
	// or 11 characters with the dot and 12 characters with the minus sign and dot
	maxIntervalStrLenWithDot = 11 // 123456789.1 and 12345678.91 are valid
)

func serialDuration(dur time.Duration) (string, error) {
	switch {
	case dur%time.Hour == 0:
		return serialHoursInterval(dur), nil
	case dur%time.Minute == 0:
		return serialMinutesInterval(dur), nil
	case dur%time.Second == 0:
		return serialSecondsInterval(dur)
	case dur%time.Millisecond == 0:
		return serialMillisecondsInterval(dur)
	default:
		return "", fmt.Errorf("trino: duration %v is not a multiple of hours, minutes, seconds or milliseconds", dur)
	}
}

func serialHoursInterval(dur time.Duration) string {
	return "INTERVAL '" + strconv.Itoa(int(dur/time.Hour)) + "' HOUR"
}

func serialMinutesInterval(dur time.Duration) string {
	return "INTERVAL '" + strconv.Itoa(int(dur/time.Minute)) + "' MINUTE"
}

func serialSecondsInterval(dur time.Duration) (string, error) {
	seconds := int64(dur / time.Second)
	if seconds <= math.MinInt32 || seconds > math.MaxInt32 {
		return "", fmt.Errorf("trino: duration %v is out of range for interval of seconds type", dur)
	}
	return "INTERVAL '" + strconv.FormatInt(seconds, 10) + "' SECOND", nil
}

func serialMillisecondsInterval(dur time.Duration) (string, error) {
	seconds := int64(dur / time.Second)
	millisInSecond := dur.Abs().Milliseconds() % 1000
	intervalNr := strings.TrimRight(fmt.Sprintf("%d.%03d", seconds, millisInSecond), "0")
	if seconds > 0 && len(intervalNr) > maxIntervalStrLenWithDot ||
		seconds < 0 && len(intervalNr) > maxIntervalStrLenWithDot+1 { // +1 for the minus sign
		return "", fmt.Errorf("trino: duration %v is out of range for interval of seconds with millis type", dur)
	}
	return "INTERVAL '" + intervalNr + "' SECOND", nil
}

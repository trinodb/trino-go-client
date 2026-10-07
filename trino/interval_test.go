package trino

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalScan(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		value   any
		want    Interval
		wantErr string
	}{
		{name: "NULL", value: nil, want: Interval{}},
		{name: "year to month", value: "1-2", want: Interval{Months: 14, Valid: true}},
		{name: "year to month as bytes", value: []byte("1-2"), want: Interval{Months: 14, Valid: true}},
		{name: "negative year to month", value: "-1-2", want: Interval{Months: -14, Valid: true}},
		{name: "negative months only", value: "-0-5", want: Interval{Months: -5, Valid: true}},
		{name: "zero year to month", value: "0-0", want: Interval{Valid: true}},
		{name: "largest year to month", value: "178956970-7", want: Interval{Months: math.MaxInt32, Valid: true}},
		{name: "smallest year to month", value: "-178956970-8", want: Interval{Months: math.MinInt32, Valid: true}},
		{name: "day to second", value: "3 04:05:06.789", want: Interval{Duration: 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond, Valid: true}},
		{name: "negative day to second", value: "-3 04:05:06.789", want: Interval{Duration: -(3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond), Valid: true}},
		{name: "negative day", value: "-1 00:00:00.000", want: Interval{Duration: -24 * time.Hour, Valid: true}},
		{name: "negative fraction of a second", value: "-0 00:00:00.500", want: Interval{Duration: -500 * time.Millisecond, Valid: true}},
		{name: "day to second without fraction", value: "0 23:59:59", want: Interval{Duration: 24*time.Hour - time.Second, Valid: true}},
		{name: "day to second with nanoseconds", value: "0 00:00:00.000000001", want: Interval{Duration: time.Nanosecond, Valid: true}},
		{name: "largest duration", value: "106751 23:47:16.854775807", want: Interval{Duration: math.MaxInt64, Valid: true}},
		{name: "day to second overflowing a duration", value: "106751 23:47:16.854775808", wantErr: "overflows time.Duration"},
		{name: "days overflowing a duration", value: "106752 00:00:00.000", wantErr: "overflows time.Duration"},
		{name: "days overflowing int64", value: "9223372036854775808 00:00:00", wantErr: "cannot parse"},
		{name: "sign on a field", value: "1--2", wantErr: "cannot parse"},
		{name: "month out of range", value: "1-12", wantErr: "cannot parse"},
		{name: "hour out of range", value: "1 24:00:00", wantErr: "cannot parse"},
		{name: "missing seconds", value: "1 04:05", wantErr: "cannot parse"},
		{name: "fraction beyond nanoseconds", value: "0 00:00:00.0000000001", wantErr: "cannot parse"},
		{name: "empty", value: "", wantErr: "cannot parse"},
		{name: "plain number", value: "5", wantErr: "cannot parse"},
		{name: "unsupported type", value: int64(5), wantErr: "cannot convert 5 (int64) to Interval"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Interval{Months: 99, Duration: time.Hour, Valid: true}
			err := got.Scan(tc.value)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

package trino

import (
	"math"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// airliftValuePattern is the pattern that io.airlift.units.Duration.valueOf
// and io.airlift.units.DataSize.valueOf accept.
var airliftValuePattern = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$`)

func TestFormatDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		duration time.Duration
		expected string
	}{
		{0, "0.00ns"},
		{time.Nanosecond, "1.00ns"},
		{999 * time.Nanosecond, "999.00ns"},
		{1500 * time.Nanosecond, "1.50us"},
		{1234 * time.Nanosecond, "1234.00ns"},
		{250 * time.Millisecond, "250.00ms"},
		{1010 * time.Millisecond, "1.01s"},
		{1001 * time.Millisecond, "1001.00ms"},
		{90 * time.Second, "1.50m"},
		{10 * time.Minute, "10.00m"},
		{90 * time.Minute, "1.50h"},
		{100 * time.Minute, "100.00m"},
		{time.Hour + time.Second, "3601.00s"},
		{36 * time.Hour, "1.50d"},
		{24 * time.Hour, "1.00d"},
		{400 * 24 * time.Hour, "400.00d"},
		{math.MaxInt64, "9223372036854775807.00ns"},
	} {
		t.Run(tc.expected, func(t *testing.T) {
			formatted := FormatDuration(tc.duration)

			assert.Equal(t, tc.expected, formatted)
			assert.Equal(t, big.NewRat(int64(tc.duration), 1), parseAirliftValue(t, formatted, map[string]int64{
				"ns": int64(time.Nanosecond),
				"us": int64(time.Microsecond),
				"ms": int64(time.Millisecond),
				"s":  int64(time.Second),
				"m":  int64(time.Minute),
				"h":  int64(time.Hour),
				"d":  int64(24 * time.Hour),
			}))
		})
	}
}

func TestFormatDataSize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		bytes    int64
		expected string
	}{
		{0, "0B"},
		{1, "1B"},
		{1000, "1000B"},
		{1024, "1kB"},
		{1536, "1.50kB"},
		{1025, "1025B"},
		{512 << 20, "512MB"},
		{1 << 30, "1GB"},
		{3 << 29, "1.50GB"},
		{5 << 28, "1.25GB"},
		{1<<30 + 1, "1073741825B"},
		{1<<30 + 1<<20, "1025MB"},
		{1 << 40, "1TB"},
		{1 << 50, "1PB"},
		{3000 << 50, "3000PB"},
		{math.MaxInt64, "9223372036854775807B"},
	} {
		t.Run(tc.expected, func(t *testing.T) {
			formatted := FormatDataSize(tc.bytes)

			assert.Equal(t, tc.expected, formatted)
			assert.Equal(t, big.NewRat(tc.bytes, 1), parseAirliftValue(t, formatted, map[string]int64{
				"B":  1,
				"kB": 1 << 10,
				"MB": 1 << 20,
				"GB": 1 << 30,
				"TB": 1 << 40,
				"PB": 1 << 50,
			}))
		})
	}
}

func TestFormatNegativeValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "-1.50h", FormatDuration(-90*time.Minute))
	assert.Equal(t, "-9223372036854775808.00ns", FormatDuration(math.MinInt64))
	assert.Equal(t, "-1GB", FormatDataSize(-1<<30))
	assert.Equal(t, "-8192PB", FormatDataSize(math.MinInt64))
	assert.False(t, airliftValuePattern.MatchString(FormatDuration(-time.Second)))
	assert.False(t, airliftValuePattern.MatchString(FormatDataSize(-1)))
}

// parseAirliftValue checks that formatted matches the airlift pattern and
// returns its exact value in the smallest unit.
func parseAirliftValue(t *testing.T, formatted string, units map[string]int64) *big.Rat {
	t.Helper()
	match := airliftValuePattern.FindStringSubmatch(formatted)
	require.NotNil(t, match, "%q does not match the airlift pattern", formatted)
	unit, ok := units[match[2]]
	require.True(t, ok, "unknown unit in %q", formatted)
	value, ok := new(big.Rat).SetString(match[1])
	require.True(t, ok)
	return value.Mul(value, big.NewRat(unit, 1))
}

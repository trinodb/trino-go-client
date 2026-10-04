package trino

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type unitSize struct {
	suffix string
	size   uint64
}

// durationUnits and dataSizeUnits are the units of io.airlift.units.Duration
// and io.airlift.units.DataSize, from the largest to the smallest.
var durationUnits = []unitSize{
	{"d", uint64(24 * time.Hour)},
	{"h", uint64(time.Hour)},
	{"m", uint64(time.Minute)},
	{"s", uint64(time.Second)},
	{"ms", uint64(time.Millisecond)},
	{"us", uint64(time.Microsecond)},
	{"ns", uint64(time.Nanosecond)},
}

var dataSizeUnits = []unitSize{
	{"PB", 1 << 50},
	{"TB", 1 << 40},
	{"GB", 1 << 30},
	{"MB", 1 << 20},
	{"kB", 1 << 10},
	{"B", 1},
}

// FormatDuration formats d as an airlift Duration string, such as 1.50h or
// 250.00ms, which the server accepts in the EXECUTION_TIME and CPU_TIME
// resource estimates. The value of time.Duration.String(), such as 1h30m0s,
// is rejected by the server.
//
// Like airlift, it uses the largest unit that d is at least one of, with two
// decimal places. Unlike airlift, it never rounds: when d is not a whole
// number of hundredths of that unit, it uses the next smaller unit that
// represents d exactly, so 1h0m1s is formatted as 3601.00s, not 1.00h.
// Negative durations are formatted with a minus sign, which the server
// rejects.
func FormatDuration(d time.Duration) string {
	sign, nanos := splitSign(int64(d))
	value, suffix := formatExactly(nanos, durationUnits)
	if value == "" {
		return sign + "0.00ns"
	}
	return sign + value + suffix
}

// FormatDataSize formats a number of bytes as an airlift DataSize string,
// such as 1GB, 1.50GB or 1000B, which the server accepts in the PEAK_MEMORY
// resource estimate. Units are powers of 1024.
//
// Like airlift, it uses the largest unit that bytes is at least one of, with
// two decimal places, omitted for a whole number. Unlike airlift, it never
// rounds: when bytes is not a whole number of hundredths of that unit, it
// uses the next smaller unit that represents bytes exactly, so 1GB plus one
// byte is formatted as 1073741825B, not 1GB. Negative sizes are formatted
// with a minus sign, which the server rejects.
func FormatDataSize(bytes int64) string {
	sign, magnitude := splitSign(bytes)
	value, suffix := formatExactly(magnitude, dataSizeUnits)
	if value == "" {
		return sign + "0B"
	}
	return sign + strings.TrimSuffix(value, ".00") + suffix
}

// airliftDurationPattern matches io.airlift.units.Duration strings, such as
// 3.00d or 12.50ms.
var airliftDurationPattern = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$`)

// parseAirliftDuration reads a Duration string the server produces, such as
// the uptime at /v1/info, using the same units FormatDuration writes.
func parseAirliftDuration(s string) (time.Duration, error) {
	match := airliftDurationPattern.FindStringSubmatch(s)
	if match == nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	unit, ok := durationUnit(match[2])
	if !ok {
		return 0, fmt.Errorf("unknown time unit in duration %q", s)
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return time.Duration(value * float64(unit)), nil
}

func durationUnit(suffix string) (time.Duration, bool) {
	for _, unit := range durationUnits {
		if unit.suffix == suffix {
			return time.Duration(unit.size), true
		}
	}
	return 0, false
}

func splitSign(value int64) (string, uint64) {
	if value < 0 {
		// Negating in uint64 keeps the magnitude of math.MinInt64.
		return "-", -uint64(value)
	}
	return "", uint64(value)
}

// formatExactly returns value in the largest unit that it is at least one of
// and that represents it with two decimal places without rounding, or an
// empty string for zero.
func formatExactly(value uint64, units []unitSize) (string, string) {
	for _, unit := range units {
		if value < unit.size {
			continue
		}
		// The remainder is smaller than the largest unit, under 2^50, so
		// multiplying it by 100 cannot overflow.
		remainder := value % unit.size
		if remainder*100%unit.size != 0 {
			continue
		}
		return fmt.Sprintf("%d.%02d", value/unit.size, remainder*100/unit.size), unit.suffix
	}
	return "", ""
}

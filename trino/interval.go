package trino

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Interval holds an INTERVAL YEAR TO MONTH as Months, or an INTERVAL DAY TO
// SECOND as Duration. Valid is false for SQL NULL. As an argument, an
// Interval with Months set is passed as an INTERVAL YEAR TO MONTH and any
// other as an INTERVAL DAY TO SECOND, like a time.Duration; setting both is
// an error, since Trino has no interval type that holds both.
type Interval struct {
	Months   int64
	Duration time.Duration
	Valid    bool
}

// Scan implements the sql.Scanner interface. It accepts the text the server
// sends for both interval types: years-months, as in 1-2, and days followed
// by hours, minutes and seconds, as in 3 04:05:06.789. A leading minus sign
// applies to the whole value.
func (i *Interval) Scan(value interface{}) error {
	var text string
	switch v := value.(type) {
	case nil:
		*i = Interval{}
		return nil
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return fmt.Errorf("trino: cannot convert %v (%T) to Interval", value, value)
	}

	negative := strings.HasPrefix(text, "-")
	magnitude := strings.TrimPrefix(text, "-")
	if days, clock, ok := strings.Cut(magnitude, " "); ok {
		duration, err := parseDayToSecond(text, days, clock)
		if err != nil {
			return err
		}
		if negative {
			duration = -duration
		}
		*i = Interval{Duration: duration, Valid: true}
		return nil
	}

	months, err := parseYearToMonth(text, magnitude)
	if err != nil {
		return err
	}
	if negative {
		months = -months
	}
	*i = Interval{Months: months, Valid: true}
	return nil
}

func parseYearToMonth(text, magnitude string) (int64, error) {
	yearsText, monthsText, ok := strings.Cut(magnitude, "-")
	if !ok {
		return 0, invalidIntervalError(text)
	}
	years, err := parseIntervalField(text, yearsText)
	if err != nil {
		return 0, err
	}
	months, err := parseIntervalField(text, monthsText)
	if err != nil {
		return 0, err
	}
	if months > 11 || years > (math.MaxInt64-months)/12 {
		return 0, invalidIntervalError(text)
	}
	return years*12 + months, nil
}

func parseDayToSecond(text, daysText, clock string) (time.Duration, error) {
	days, err := parseIntervalField(text, daysText)
	if err != nil {
		return 0, err
	}
	parts := strings.Split(clock, ":")
	if len(parts) != 3 {
		return 0, invalidIntervalError(text)
	}
	secondsText, fractionText, hasFraction := strings.Cut(parts[2], ".")
	var fields [3]int64
	for index, field := range []string{parts[0], parts[1], secondsText} {
		if fields[index], err = parseIntervalField(text, field); err != nil {
			return 0, err
		}
	}
	hours, minutes, seconds := fields[0], fields[1], fields[2]
	if hours > 23 || minutes > 59 || seconds > 59 {
		return 0, invalidIntervalError(text)
	}
	var nanoseconds int64
	if hasFraction {
		if len(fractionText) > 9 {
			return 0, invalidIntervalError(text)
		}
		fraction, err := parseIntervalField(text, fractionText)
		if err != nil {
			return 0, err
		}
		nanoseconds = fraction * int64(math.Pow10(9-len(fractionText)))
	}

	withinDay := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second + time.Duration(nanoseconds)
	const day = 24 * time.Hour
	if days > int64(math.MaxInt64/day) || time.Duration(days)*day > math.MaxInt64-withinDay {
		return 0, fmt.Errorf("trino: interval %q overflows time.Duration", text)
	}
	return time.Duration(days)*day + withinDay, nil
}

// parseIntervalField reads an unsigned decimal field; the only sign an
// interval has is the one in front of the whole value.
func parseIntervalField(text, field string) (int64, error) {
	if field == "" || strings.TrimLeft(field, "0123456789") != "" {
		return 0, invalidIntervalError(text)
	}
	value, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		return 0, invalidIntervalError(text)
	}
	return value, nil
}

func invalidIntervalError(text string) error {
	return fmt.Errorf("trino: cannot parse %q as an interval year to month or day to second", text)
}

func serialInterval(i Interval) (string, error) {
	switch {
	case !i.Valid:
		return "NULL", nil
	case i.Months != 0 && i.Duration != 0:
		return "", fmt.Errorf("trino: interval with both months (%d) and a duration (%v) has no Trino type", i.Months, i.Duration)
	case i.Months != 0:
		return serialYearToMonth(i.Months)
	default:
		return serialDuration(i.Duration)
	}
}

// serialYearToMonth keeps the sign inside the quotes: Trino negates a sign
// written before them only after reading the value, so -'178956970-8' would
// overflow while '-178956970-8', the smallest INTERVAL YEAR TO MONTH, does not.
func serialYearToMonth(months int64) (string, error) {
	if months < math.MinInt32 || months > math.MaxInt32 {
		return "", fmt.Errorf("trino: %d months is out of range for an interval year to month", months)
	}
	sign := ""
	if months < 0 {
		sign = "-"
		months = -months
	}
	return fmt.Sprintf("INTERVAL '%s%d-%d' YEAR TO MONTH", sign, months/12, months%12), nil
}

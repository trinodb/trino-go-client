package trino

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Interval holds an INTERVAL YEAR TO MONTH as Months, or an INTERVAL DAY TO
// SECOND as Duration and Picoseconds. Valid is false for SQL NULL. As an
// argument, an Interval with Months set is passed as an INTERVAL YEAR TO
// MONTH and any other as an INTERVAL DAY TO SECOND, like a time.Duration;
// setting Months together with Duration or Picoseconds is an error, since
// Trino has no interval type that holds both.
//
// Trino 484 and later send up to twelve fractional-second digits. Duration
// holds the value truncated toward zero to whole nanoseconds, and
// Picoseconds the rest, from -999 to 999, with the sign of the value.
type Interval struct {
	Months      int64
	Duration    time.Duration
	Picoseconds int
	Valid       bool
}

const (
	maxIntervalFractionDigits = 12
	picosecondsPerNanosecond  = 1000
)

// Scan implements the sql.Scanner interface. It accepts the text the server
// sends for both interval types: years-months, as in 1-2, and days followed
// by hours, minutes and seconds, with up to twelve fractional digits or
// none, as in 3 04:05:06.789. A leading minus sign applies to the whole
// value.
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
		duration, picoseconds, err := parseDayToSecond(text, negative, days, clock)
		if err != nil {
			return err
		}
		*i = Interval{Duration: duration, Picoseconds: picoseconds, Valid: true}
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

// parseDayToSecond accumulates the magnitude unsigned, so the smallest
// time.Duration, whose magnitude is one more than the largest, still fits.
func parseDayToSecond(text string, negative bool, daysText, clock string) (time.Duration, int, error) {
	days, err := parseIntervalField(text, daysText)
	if err != nil {
		return 0, 0, err
	}
	parts := strings.Split(clock, ":")
	if len(parts) != 3 {
		return 0, 0, invalidIntervalError(text)
	}
	secondsText, fractionText, hasFraction := strings.Cut(parts[2], ".")
	var fields [3]int64
	for index, field := range []string{parts[0], parts[1], secondsText} {
		if fields[index], err = parseIntervalField(text, field); err != nil {
			return 0, 0, err
		}
	}
	hours, minutes, seconds := fields[0], fields[1], fields[2]
	if hours > 23 || minutes > 59 || seconds > 59 {
		return 0, 0, invalidIntervalError(text)
	}
	var nanoseconds, picoseconds int64
	if hasFraction {
		if len(fractionText) > maxIntervalFractionDigits {
			return 0, 0, invalidIntervalError(text)
		}
		fraction, err := parseIntervalField(text, fractionText)
		if err != nil {
			return 0, 0, err
		}
		fractionPicoseconds := fraction * int64(math.Pow10(maxIntervalFractionDigits-len(fractionText)))
		nanoseconds, picoseconds = fractionPicoseconds/picosecondsPerNanosecond, fractionPicoseconds%picosecondsPerNanosecond
	}

	withinDay := uint64(time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second + time.Duration(nanoseconds))
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	const day = uint64(24 * time.Hour)
	if uint64(days) > limit/day || uint64(days)*day > limit-withinDay {
		return 0, 0, fmt.Errorf("trino: interval %q overflows time.Duration", text)
	}
	magnitude := uint64(days)*day + withinDay
	if negative {
		return -time.Duration(magnitude), -int(picoseconds), nil
	}
	return time.Duration(magnitude), int(picoseconds), nil
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
	case i.Months != 0 && (i.Duration != 0 || i.Picoseconds != 0):
		return "", fmt.Errorf("trino: interval with both months (%d) and a duration (%v and %d picoseconds) has no Trino type", i.Months, i.Duration, i.Picoseconds)
	case i.Months != 0:
		return serialYearToMonth(i.Months)
	case i.Picoseconds <= -picosecondsPerNanosecond || i.Picoseconds >= picosecondsPerNanosecond:
		return "", fmt.Errorf("trino: interval picoseconds %d are out of range, they must be between -999 and 999", i.Picoseconds)
	case i.Duration < 0 && i.Picoseconds > 0 || i.Duration > 0 && i.Picoseconds < 0:
		return "", fmt.Errorf("trino: interval duration %v and picoseconds %d have different signs", i.Duration, i.Picoseconds)
	case i.Picoseconds != 0:
		return serialFractionalSecondsInterval(i.Duration, i.Picoseconds), nil
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

package trino

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// localtimePath is where the operating system keeps the local time zone when
// TZ is not set. It is a variable so tests can point it elsewhere.
var localtimePath = "/etc/localtime"

var zoneOffsetPattern = regexp.MustCompile(`^([+-])(\d{2}):(\d{2})$`)

const sessionTimeZoneProperty = "time_zone_id"

// resolveTimeZone loads the location for a time zone id in one of the forms
// Trino accepts: an IANA name, UTC, or a ±HH:MM offset.
func resolveTimeZone(name string) (*time.Location, error) {
	// Go resolves both of these to time.Local, which Trino does not know.
	if name == "" || name == "Local" {
		return nil, fmt.Errorf("trino: invalid timezone %q", name)
	}
	if match := zoneOffsetPattern.FindStringSubmatch(name); match != nil {
		hours, _ := strconv.Atoi(match[2])
		minutes, _ := strconv.Atoi(match[3])
		if hours > 18 || minutes > 59 {
			return nil, fmt.Errorf("trino: invalid timezone %q: offset out of range", name)
		}
		offset := hours*3600 + minutes*60
		if match[1] == "-" {
			offset = -offset
		}
		return time.FixedZone(name, offset), nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("trino: invalid timezone %q: %w", name, err)
	}
	return location, nil
}

// localTimeZoneName returns the id of the process time zone in a form Trino
// accepts. time.Local is only named after its zone when TZ is set, so the
// name is recovered from TZ or from the localtime symlink; when neither
// names a zone, the current UTC offset is used instead.
func localTimeZoneName() string {
	if tz, ok := os.LookupEnv("TZ"); ok {
		name := strings.TrimPrefix(tz, ":")
		if name == "" {
			name = "UTC"
		}
		if isKnownTimeZone(name) {
			return name
		}
	}
	if name, ok := timeZoneNameFromLocaltime(); ok {
		return name
	}
	if name := time.Local.String(); isKnownTimeZone(name) {
		return name
	}
	return time.Now().Format("-07:00")
}

func timeZoneNameFromLocaltime() (string, bool) {
	target, err := os.Readlink(localtimePath)
	if err != nil {
		return "", false
	}
	const zoneinfoDir = "zoneinfo/"
	idx := strings.LastIndex(target, zoneinfoDir)
	if idx == -1 {
		return "", false
	}
	name := target[idx+len(zoneinfoDir):]
	name = strings.TrimPrefix(name, "posix/")
	name = strings.TrimPrefix(name, "right/")
	return name, isKnownTimeZone(name)
}

func isKnownTimeZone(name string) bool {
	_, err := resolveTimeZone(name)
	return err == nil
}

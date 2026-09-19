package trino

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveTimeZone(t *testing.T) {
	t.Parallel()
	t.Run("accepted", func(t *testing.T) {
		for _, name := range []string{"UTC", "Europe/Warsaw", "+02:00", "-07:00", "+05:30"} {
			t.Run(name, func(t *testing.T) {
				location, err := resolveTimeZone(name)

				require.NoError(t, err)
				assert.Equal(t, name, location.String())
			})
		}
	})

	t.Run("offset", func(t *testing.T) {
		location, err := resolveTimeZone("-05:30")

		require.NoError(t, err)
		_, offset := time.Date(2017, 7, 10, 0, 0, 0, 0, location).Zone()
		assert.Equal(t, -(5*3600 + 30*60), offset)
	})

	t.Run("rejected", func(t *testing.T) {
		for _, name := range []string{"", "Local", "Mars/Olympus_Mons", "+2:00", "+25:00", "+02:60", "CEST"} {
			t.Run(name, func(t *testing.T) {
				_, err := resolveTimeZone(name)

				require.ErrorContains(t, err, "trino: invalid timezone")
			})
		}
	})
}

// These tests change TZ and the localtime path, so they cannot run in parallel.
func TestLocalTimeZoneName(t *testing.T) {
	t.Run("from TZ", func(t *testing.T) {
		t.Setenv("TZ", ":Asia/Tokyo")

		assert.Equal(t, "Asia/Tokyo", localTimeZoneName())
	})

	t.Run("empty TZ means UTC", func(t *testing.T) {
		t.Setenv("TZ", "")

		assert.Equal(t, "UTC", localTimeZoneName())
	})

	t.Run("unknown TZ falls through to the localtime link", func(t *testing.T) {
		t.Setenv("TZ", "Not/AZone")
		useLocaltimeLink(t, "/usr/share/zoneinfo/Europe/Warsaw")

		assert.Equal(t, "Europe/Warsaw", localTimeZoneName())
	})

	t.Run("localtime link", func(t *testing.T) {
		unsetTZ(t)
		for target, want := range map[string]string{
			"/usr/share/zoneinfo/Europe/Warsaw":          "Europe/Warsaw",
			"../usr/share/zoneinfo/America/New_York":     "America/New_York",
			"/var/db/timezone/zoneinfo/Asia/Tokyo":       "Asia/Tokyo",
			"/usr/share/zoneinfo/posix/Europe/Warsaw":    "Europe/Warsaw",
			"/usr/share/zoneinfo/right/Australia/Sydney": "Australia/Sydney",
		} {
			t.Run(target, func(t *testing.T) {
				useLocaltimeLink(t, target)

				assert.Equal(t, want, localTimeZoneName())
			})
		}
	})

	t.Run("localtime that is not a link", func(t *testing.T) {
		unsetTZ(t)
		path := filepath.Join(t.TempDir(), "localtime")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		useLocaltimePath(t, path)

		assertFixedOffset(t, localTimeZoneName())
	})

	t.Run("localtime link outside zoneinfo", func(t *testing.T) {
		unsetTZ(t)
		useLocaltimeLink(t, "/somewhere/else/Europe/Warsaw")

		assertFixedOffset(t, localTimeZoneName())
	})

	t.Run("always resolves", func(t *testing.T) {
		_, err := resolveTimeZone(localTimeZoneName())

		require.NoError(t, err)
	})
}

// assertFixedOffset checks the fallback used when no zone name can be found.
func assertFixedOffset(t *testing.T, name string) {
	t.Helper()
	assert.Regexp(t, `^[+-]\d{2}:\d{2}$`, name)
	assert.Equal(t, time.Now().Format("-07:00"), name)
}

func unsetTZ(t *testing.T) {
	t.Helper()
	// t.Setenv registers the restore; the unset makes LookupEnv report absence.
	t.Setenv("TZ", "")
	require.NoError(t, os.Unsetenv("TZ"))
}

func useLocaltimeLink(t *testing.T, target string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "localtime")
	require.NoError(t, os.Symlink(target, path))
	useLocaltimePath(t, path)
}

func useLocaltimePath(t *testing.T, path string) {
	t.Helper()
	previous := localtimePath
	localtimePath = path
	t.Cleanup(func() { localtimePath = previous })
}

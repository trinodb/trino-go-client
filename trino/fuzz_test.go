package trino

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FuzzParseDSN checks that a DSN ParseDSN accepts survives FormatDSN and
// ParseDSN again unchanged, and that the header keys it accepts cannot split
// into several entries on the server.
func FuzzParseDSN(f *testing.F) {
	for _, dsn := range []string{
		"http://localhost:8080",
		"http://user@localhost:8080?source=test",
		"https://user:secret@trino.example.com:8443/hive/default?source=app&query_timeout=30s",
		"http://user@localhost/hive/default?session_properties=query_max_run_time%3A10m%3Bjoin_distribution_type%3AAUTOMATIC",
		"http://user@localhost?extra_credentials=foo%3Abar%3Bbaz%3Aqux&roles=hive%3Aadmin%3Bsystem%3ALL",
		"http://user@localhost?clientTags=a%2Cb%2Cc&catalog=hive&schema=default",
		"http://user@localhost?resource_estimates=EXECUTION_TIME%3A2h%3BCPU_TIME%3A1h",
		"https://trino.example.com?externalAuthentication=true&externalAuthenticationTimeout=5m",
		"https://trino.example.com?SSLVerification=NONE&http_proxy=http%3A%2F%2Fproxy%3A3128",
		"http://user@localhost?timezone=Europe%2FWarsaw&trace_token=t&client_info=i&language=pl",
		"http://user@localhost?request_retry_timeout=1m&request_retry_max_attempts=5&heartbeat_interval=10s",
		"http://user@localhost?session_properties=a%3Ab%3Bc&roles=%3A",
		"http://user@localhost/hive/default/extra",
		"::not a url",
	} {
		f.Add(dsn)
	}
	f.Fuzz(func(t *testing.T, dsn string) {
		config, err := ParseDSN(dsn)
		if err != nil {
			return
		}
		for _, entries := range []map[string]string{config.SessionProperties, config.ExtraCredentials, config.Roles, config.ResourceEstimates} {
			for key := range entries {
				assert.NotContains(t, key, "=")
				assert.NotContains(t, key, ",")
			}
		}
		for _, tag := range config.ClientTags {
			assert.NotContains(t, tag, ",")
		}

		formatted, err := config.FormatDSN()
		if err != nil {
			// ParseDSN only reads the DSN. FormatDSN also checks the
			// combination of settings, for instance that SSLVerification
			// needs an https server, as NewConnector does.
			return
		}
		reparsed, err := ParseDSN(formatted)
		require.NoError(t, err, "formatted DSN %q", formatted)
		assert.Equal(t, config, reparsed, "formatted DSN %q", formatted)

		again, err := reparsed.FormatDSN()
		require.NoError(t, err)
		assert.Equal(t, formatted, again)
	})
}

// FuzzFormatDSN checks that a Config FormatDSN accepts is read back by
// ParseDSN as the same Config.
func FuzzFormatDSN(f *testing.F) {
	f.Add("http://localhost:8080", "app", "hive", "default", "query_max_run_time", "10m", "token", "abc", "hive", "admin", "tag")
	f.Add("https://user:pw@example.com", "", "", "", "a:b", "c;d", "k", "v w", "c", "r,s", "x,y")
	f.Fuzz(func(t *testing.T, serverURI, source, catalog, schema, sessionKey, sessionValue, credKey, credValue, roleCatalog, role, tag string) {
		config := &Config{
			ServerURI:         serverURI,
			Source:            source,
			Catalog:           catalog,
			Schema:            schema,
			SessionProperties: map[string]string{sessionKey: sessionValue},
			ExtraCredentials:  map[string]string{credKey: credValue},
			Roles:             map[string]string{roleCatalog: role},
			ClientTags:        []string{tag},
		}
		formatted, err := config.FormatDSN()
		if err != nil {
			return
		}
		reparsed, err := ParseDSN(formatted)
		if err != nil {
			t.Fatalf("ParseDSN rejected the DSN %q that FormatDSN produced from %+v: %v", formatted, config, err)
		}
		assert.Equal(t, config.SessionProperties, reparsed.SessionProperties, "DSN %q", formatted)
		assert.Equal(t, config.ExtraCredentials, reparsed.ExtraCredentials, "DSN %q", formatted)
		assert.Equal(t, config.Roles, reparsed.Roles, "DSN %q", formatted)
		assert.Equal(t, config.ClientTags, reparsed.ClientTags, "DSN %q", formatted)
		assert.Equal(t, config.Catalog, reparsed.Catalog, "DSN %q", formatted)
		assert.Equal(t, config.Schema, reparsed.Schema, "DSN %q", formatted)
		assert.Equal(t, config.Source, reparsed.Source, "DSN %q", formatted)
	})
}

// FuzzHeaderKeys checks that a session property, extra credential or role key
// the driver accepts becomes exactly one name=value entry, as the server
// splits the header on ',' and each entry on '='.
func FuzzHeaderKeys(f *testing.F) {
	for _, key := range []string{"query_max_run_time", "hive.insert_existing_partitions_behavior", "a=b", "a,b", "a b", "", "é", "x;y", "x:y"} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key string) {
		const value = "value"

		config := &Config{
			ServerURI:         "http://user@localhost",
			SessionProperties: map[string]string{key: value},
			ExtraCredentials:  map[string]string{key: value},
			Roles:             map[string]string{key: value},
		}
		connector, err := NewConnector(config)
		if err != nil {
			return
		}
		conn, err := newConnFromConfig(connector.conf, connector.externalAuth)
		require.NoError(t, err)
		assert.NotContains(t, key, "=")
		assert.NotContains(t, key, ",")

		entries := conn.httpHeaderValues(trinoSessionHeader)
		require.Len(t, entries, 1)
		name, _, ok := strings.Cut(entries[0], "=")
		assert.True(t, ok)
		assert.Equal(t, key, name)

		credentials := conn.extraCredentials
		require.Len(t, credentials, 1)
		name, _, ok = strings.Cut(credentials[0], "=")
		assert.True(t, ok)
		assert.Equal(t, key, name)

		roles := strings.Split(conn.httpHeaderValue(trinoRoleHeader), commaSeparator)
		require.Len(t, roles, 1)
		name, _, ok = strings.Cut(roles[0], "=")
		assert.True(t, ok)
		assert.Equal(t, key, name)
	})
}

// FuzzApplyResponseHeaders feeds the session headers of a response to a
// connection. The server is trusted to follow the protocol, but a proxy in
// between can send anything, and none of it may panic or make the stored
// session state ambiguous.
func FuzzApplyResponseHeaders(f *testing.F) {
	f.Add("query_max_run_time=10m", "stmt1=SELECT+1", "hive=ROLE%7Badmin%7D", "hive/default", "query_max_run_time", "stmt1")
	f.Add("time_zone=Europe%2FWarsaw", "s=SELECT+%3F", "system=ALL", "/a/b", "time_zone", "s")
	f.Add("a=b,c=d", "no equals sign", "=x", "", "", "")
	f.Add("=", "=", "a==b", "\x00", "a=b", "=")
	f.Fuzz(func(t *testing.T, setSession, addedPrepare, setRole, setPath, clearSession, deallocated string) {
		conn, err := newConn("http://alice@localhost?roles=hive%3Aadmin&session_properties=join_distribution_type%3AAUTOMATIC&timezone=UTC")
		require.NoError(t, err)

		conn.applyResponseHeaders(http.Header{
			trinoSetSessionHeader:         {setSession},
			trinoAddedPrepareHeader:       {addedPrepare},
			trinoSetRoleHeader:            {setRole},
			trinoSetPathHeader:            {setPath},
			trinoClearSessionHeader:       {clearSession},
			trinoDeallocatedPrepareHeader: {deallocated},
		})
		// A second pass with the same input must not corrupt the state
		// the first one left behind.
		conn.applyResponseHeaders(http.Header{
			trinoSetSessionHeader:   {setSession},
			trinoAddedPrepareHeader: {addedPrepare},
			trinoSetRoleHeader:      {setRole},
		})

		// Entries of the same name replace each other.
		for _, header := range []string{trinoSessionHeader, preparedStatementHeader} {
			seen := map[string]bool{}
			for _, entry := range conn.httpHeaderValues(header) {
				name, _, ok := strings.Cut(entry, "=")
				if !ok {
					continue
				}
				assert.False(t, seen[name], "%s has two entries named %q: %q", header, name, conn.httpHeaderValues(header))
				seen[name] = true
			}
		}
		// The role header is rebuilt from catalog=role pairs only.
		if roles := conn.httpHeaderValue(trinoRoleHeader); roles != "" {
			for _, entry := range strings.Split(roles, commaSeparator) {
				catalog, _, ok := strings.Cut(entry, "=")
				assert.True(t, ok && catalog != "", "entry %q of roles header %q", entry, roles)
			}
		}
	})
}

// FuzzMergeRoles checks that merging role updates is idempotent and keeps
// one entry per catalog.
func FuzzMergeRoles(f *testing.F) {
	f.Add("hive=ROLE%7Badmin%7D,system=ALL", "hive=NONE")
	f.Add("", "a=b")
	f.Add("garbage,a=b", "=")
	f.Fuzz(func(t *testing.T, current, update string) {
		merged := mergeRoles(current, []string{update})
		assert.Equal(t, merged, mergeRoles(merged, nil))
		seen := map[string]bool{}
		for _, entry := range strings.Split(merged, commaSeparator) {
			if entry == "" {
				continue
			}
			catalog, _, ok := strings.Cut(entry, "=")
			require.True(t, ok, "entry %q of %q", entry, merged)
			assert.NotEmpty(t, catalog, "entry %q of %q", entry, merged)
			assert.False(t, seen[catalog], "catalog %q twice in %q", catalog, merged)
			seen[catalog] = true
		}
	})
}

// FuzzRetryAfter checks that retryAfter never panics, never returns a negative
// wait, and reads delta-seconds as written.
func FuzzRetryAfter(f *testing.F) {
	for _, value := range []string{"0", "1", "120", "-1", "1.5", "", " 7 ", "9223372036854775807", "99999999999999999999",
		"Sun, 04 Oct 2026 12:00:30 GMT", "Sunday, 04-Oct-26 12:01:00 GMT", "Sun Oct  4 12:00:30 2026", "soon"} {
		f.Add(http.StatusTooManyRequests, value)
		f.Add(http.StatusServiceUnavailable, value)
	}
	f.Add(http.StatusBadGateway, "7")
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, status int, value string) {
		resp := &http.Response{StatusCode: status, Header: http.Header{}}
		resp.Header.Set("Retry-After", value)
		wait, ok := retryAfter(resp, now)
		if !ok {
			assert.Zero(t, wait)
			return
		}
		assert.Contains(t, []int{http.StatusTooManyRequests, http.StatusServiceUnavailable}, status)
		assert.GreaterOrEqual(t, wait, time.Duration(0))
		if seconds, err := strconv.ParseInt(strings.TrimSpace(resp.Header.Get("Retry-After")), 10, 64); err == nil && seconds >= 0 && seconds < 1<<31 {
			assert.Equal(t, time.Duration(seconds)*time.Second, wait)
		}
	})
}

// FuzzParseExternalAuthChallenge checks that whatever a server sends in
// WWW-Authenticate is either rejected or yields absolute http(s) URLs.
func FuzzParseExternalAuthChallenge(f *testing.F) {
	for _, value := range []string{
		`Bearer x_redirect_server="https://trino.example.com/oauth2/token/initiate/abc", x_token_server="https://trino.example.com/oauth2/token/abc"`,
		`Bearer x_token_server="https://trino.example.com/oauth2/token/abc"`,
		`Basic realm="Trino", Bearer x_token_server="http://localhost/token"`,
		`bearer X_TOKEN_SERVER=https://a/b`,
		`Bearer x_token_server="relative/path"`,
		`Bearer x_token_server="ftp://example.com"`,
		`Negotiate`,
		`Bearer x_token_server="https://a/\"b"`,
		`Bearer x_token_server="unterminated`,
		`="" , = ,"`,
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		challenge, err := parseExternalAuthChallenge(http.Header{"Www-Authenticate": {value}})
		if err != nil {
			assert.Nil(t, challenge)
			return
		}
		if challenge == nil {
			return
		}
		require.NotNil(t, challenge.tokenURL)
		assert.Contains(t, []string{"http", "https"}, challenge.tokenURL.Scheme)
		assert.NotEmpty(t, challenge.tokenURL.Host)
		if challenge.redirectURL != nil {
			assert.Contains(t, []string{"http", "https"}, challenge.redirectURL.Scheme)
			assert.NotEmpty(t, challenge.redirectURL.Host)
		}
	})
}

// FuzzResolveTimeZone checks that a time zone name from a DSN or from an
// X-Trino-Set-Session entry either fails or yields a usable location.
func FuzzResolveTimeZone(f *testing.F) {
	for _, name := range []string{"UTC", "Europe/Warsaw", "+01:00", "-08:30", "+18:00", "+19:00", "+01:60", "Local", "", "../etc/passwd", "America/Argentina/Buenos_Aires"} {
		f.Add(name)
	}
	f.Fuzz(func(t *testing.T, name string) {
		location, err := resolveTimeZone(name)
		if err != nil {
			assert.Nil(t, location)
			return
		}
		require.NotNil(t, location)
		_ = time.Unix(0, 0).In(location).String()
	})
}

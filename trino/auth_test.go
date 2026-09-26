package trino

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeExternalAuth makes a fake coordinator answer requests without its
// current token with Trino's external authentication challenge, and serves
// the token from /oauth2/token after pending polls that return a nextUri.
type fakeExternalAuth struct {
	fc      *fakeCoordinator
	mu      sync.Mutex
	token   string
	pending int
	failure string
	// unavailable is the number of polls answered with 503 first.
	unavailable int
	// status, body, contentType and nextURI replace the token server's
	// answer when set.
	status      int
	body        string
	contentType string
	nextURI     string
	// challenge replaces the WWW-Authenticate Bearer challenge; noChallenge
	// sends none.
	challenge   string
	noChallenge bool
	// rejectAll rejects every token.
	rejectAll bool
	polls     int
	deletes   int
	rejected  chan struct{}
}

func newFakeExternalAuth(fc *fakeCoordinator, token string) *fakeExternalAuth {
	a := &fakeExternalAuth{fc: fc, token: token, rejected: make(chan struct{}, 100)}
	fc.onRequest(a.serve)
	return a
}

func (a *fakeExternalAuth) serve(w http.ResponseWriter, r *http.Request) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if index, ok := strings.CutPrefix(r.URL.Path, "/oauth2/token/"); ok {
		if r.Method == http.MethodDelete {
			a.deletes++
			w.WriteHeader(http.StatusNoContent)
			return true
		}
		a.polls++
		if a.unavailable != 0 {
			a.unavailable--
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		n, _ := strconv.Atoi(index)
		response := map[string]string{"token": a.token}
		switch {
		case a.failure != "":
			response = map[string]string{"error": a.failure}
		case a.nextURI != "":
			response = map[string]string{"nextUri": a.nextURI}
		case a.pending != 0:
			a.pending--
			response = map[string]string{"nextUri": a.fc.url() + "/oauth2/token/" + strconv.Itoa(n+1)}
		}
		contentType := "application/json; charset=utf-8"
		if a.contentType != "" {
			contentType = a.contentType
		}
		w.Header().Set("Content-Type", contentType)
		if a.status != 0 {
			w.WriteHeader(a.status)
		}
		if a.body != "" {
			_, _ = w.Write([]byte(a.body))
		} else {
			_ = json.NewEncoder(w).Encode(response)
		}
		return true
	}
	if !a.rejectAll && r.Header.Get(authorizationHeader) == "Bearer "+a.token {
		return false
	}
	w.Header().Add("WWW-Authenticate", `Basic realm="Trino"`)
	switch {
	case a.noChallenge:
	case a.challenge != "":
		w.Header().Add("WWW-Authenticate", a.challenge)
	default:
		w.Header().Add("WWW-Authenticate", `Bearer x_redirect_server="`+a.fc.url()+`/oauth2/initiate/1", x_token_server="`+a.fc.url()+`/oauth2/token/1"`)
	}
	w.WriteHeader(http.StatusUnauthorized)
	select {
	case a.rejected <- struct{}{}:
	default:
	}
	return true
}

// expire makes the coordinator reject the current token and hand out next.
func (a *fakeExternalAuth) expire(next string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = next
}

func (a *fakeExternalAuth) counts() (polls, deletes int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.polls, a.deletes
}

type recordingRedirects struct {
	mu   sync.Mutex
	urls []string
}

func (r *recordingRedirects) handle(_ context.Context, u *url.URL) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, u.String())
	return nil
}

func (r *recordingRedirects) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.urls)
}

func openExternalAuth(t *testing.T, fc *fakeCoordinator, conf *Config) *sql.DB {
	t.Helper()
	conf.ServerURI = fc.url()
	conf.ExternalAuthentication = true
	conf.HTTPClient = fc.server.Client()
	if conf.ExternalAuthenticationTimeout == nil {
		// fail fast instead of waiting the default two minutes
		conf.ExternalAuthenticationTimeout = ptr(5 * time.Second)
	}
	if conf.RedirectHandler == nil {
		conf.RedirectHandler = (&recordingRedirects{}).handle
	}
	connector, err := NewConnector(conf)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestExternalAuthentication(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	auth.pending = 2
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{RedirectHandler: redirects.handle})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	assert.Equal(t, []string{fc.url() + "/oauth2/initiate/1"}, redirects.urls)
	polls, deletes := auth.counts()
	assert.Equal(t, 3, polls, "two pending polls, then the token")
	assert.Equal(t, 1, deletes, "the token server is told the token arrived")
	var statements []capturedRequest
	for _, r := range fc.capturedRequests() {
		if r.path == "/v1/statement" {
			statements = append(statements, r)
		}
	}
	require.Len(t, statements, 2)
	assert.Equal(t, "SELECT 1", string(statements[1].body), "the retried statement keeps its body")
	assert.Equal(t, "Bearer token1", statements[1].header.Get(authorizationHeader))
}

func TestExternalAuthenticationSharesTokenAcrossConnections(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	// fresh responses, since the fake sets nextUri on the value it serves
	fc.respond(
		page{response: func(string) any { return &stmtResponse{ID: fakeQueryID} }},
		page{response: func(base string) any { return resultPage([][]any{{1}}).response(base) }},
	)
	auth := newFakeExternalAuth(fc, "token1")
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{RedirectHandler: func(ctx context.Context, u *url.URL) error {
		// wait until the other connection was rejected too
		for range 2 {
			select {
			case <-auth.rejected:
			case <-time.After(5 * time.Second):
				return errors.New("the second connection was not rejected")
			}
		}
		return redirects.handle(ctx, u)
	}})

	ctx := context.Background()
	conns := make([]*sql.Conn, 2)
	for i := range conns {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		conns[i] = conn
	}
	var wg sync.WaitGroup
	errs := make([]error, len(conns))
	for i, conn := range conns {
		wg.Go(func() {
			var n int
			errs[i] = conn.QueryRowContext(ctx, "SELECT 1").Scan(&n)
		})
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Equal(t, 1, redirects.count())
}

func TestExternalAuthenticationRenewsTokenDuringQuery(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	fc.onPage(func(index int, r *http.Request) {
		if index == 0 {
			auth.expire("token2")
		}
	})
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{RedirectHandler: redirects.handle})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Equal(t, 2, redirects.count())
}

func TestExternalAuthenticationUsesTokenCache(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "cached")
	cache := &memoryTokenCache{}
	cache.SetToken("cached")
	db := openExternalAuth(t, fc, &Config{
		TokenCache: cache,
		RedirectHandler: func(context.Context, *url.URL) error {
			return errors.New("the cached token should have been used")
		},
	})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Empty(t, auth.rejected, "the first request carries the cached token")
}

func TestExternalAuthenticationStoresTokenInCache(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	newFakeExternalAuth(fc, "token1")
	cache := &memoryTokenCache{}
	db := openExternalAuth(t, fc, &Config{TokenCache: cache, RedirectHandler: (&recordingRedirects{}).handle})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	assert.Equal(t, "token1", cache.Token())
}

func TestExternalAuthenticationFailures(t *testing.T) {
	t.Parallel()
	redirectErr := errors.New("no browser")
	tests := []struct {
		name    string
		setup   func(*fakeExternalAuth)
		conf    Config
		wantErr string
		wantIs  error
	}{
		{
			name:    "token server error",
			setup:   func(a *fakeExternalAuth) { a.failure = "access denied" },
			wantErr: "access denied",
		},
		{
			name:   "redirect handler error",
			conf:   Config{RedirectHandler: func(context.Context, *url.URL) error { return redirectErr }},
			wantIs: redirectErr,
		},
		{
			name:   "timeout",
			setup:  func(a *fakeExternalAuth) { a.pending = 1 << 30 },
			conf:   Config{ExternalAuthenticationTimeout: ptr(50 * time.Millisecond)},
			wantIs: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeExternalAuth(fc, "token1")
			if tt.setup != nil {
				tt.setup(auth)
			}
			conf := tt.conf
			if conf.RedirectHandler == nil {
				conf.RedirectHandler = (&recordingRedirects{}).handle
			}
			db := openExternalAuth(t, fc, &conf)

			_, err := db.Query("SELECT 1")
			require.Error(t, err)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			}
			if tt.wantIs != nil {
				assert.ErrorIs(t, err, tt.wantIs)
			}
		})
	}
}

func TestExternalAuthenticationDisabled(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	connector, err := NewConnector(&Config{ServerURI: fc.url(), HTTPClient: fc.server.Client()})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")
	var qf *ErrQueryFailed
	require.ErrorAs(t, err, &qf)
	assert.Equal(t, http.StatusUnauthorized, qf.StatusCode)
	polls, _ := auth.counts()
	assert.Zero(t, polls)
}

func TestExternalAuthenticationRequiresTLS(t *testing.T) {
	t.Parallel()
	_, err := ParseDSN("http://localhost:8080?externalAuthentication=true")
	assert.ErrorIs(t, err, errExternalAuthenticationNeedsTLS)
	_, err = NewConnector(&Config{ServerURI: "http://localhost:8080", ExternalAuthentication: true})
	assert.ErrorIs(t, err, errExternalAuthenticationNeedsTLS)
}

func TestExternalAuthenticationConflicts(t *testing.T) {
	t.Parallel()
	for name, conf := range map[string]*Config{
		"password":                   {ServerURI: "https://user:secret@localhost"},
		"Kerberos":                   {ServerURI: "https://localhost", KerberosEnabled: true},
		"forwardAuthorizationHeader": {ServerURI: "https://localhost", ForwardAuthorizationHeader: true},
	} {
		conf.ExternalAuthentication = true
		_, err := NewConnector(conf)
		assert.ErrorContains(t, err, "cannot be combined", name)
	}

	db, err := sql.Open("trino", "https://localhost?externalAuthentication=true&forwardAuthorizationHeader=true")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.ErrorContains(t, db.Ping(), "cannot be combined")
}

func TestExternalAuthenticationDSNRoundTrip(t *testing.T) {
	t.Parallel()
	conf := &Config{ServerURI: "https://localhost:8443", ExternalAuthentication: true, ExternalAuthenticationTimeout: ptr(time.Minute)}
	dsn, err := conf.FormatDSN()
	require.NoError(t, err)
	parsed, err := ParseDSN(dsn)
	require.NoError(t, err)
	assert.True(t, parsed.ExternalAuthentication)
	require.NotNil(t, parsed.ExternalAuthenticationTimeout)
	assert.Equal(t, time.Minute, *parsed.ExternalAuthenticationTimeout)

	_, err = ParseDSN("https://localhost:8443?externalAuthenticationTimeout=0s")
	assert.ErrorContains(t, err, "externalAuthenticationTimeout must be positive")
	_, err = ParseDSN("https://localhost:8443?externalAuthenticationTimeout=nope")
	assert.ErrorContains(t, err, "invalid duration for externalAuthenticationTimeout")
}

func TestFormatDSNRejectsExternalAuthenticationCallbacks(t *testing.T) {
	t.Parallel()
	for name, conf := range map[string]*Config{
		"RedirectHandler": {ServerURI: "https://localhost", RedirectHandler: OpenBrowser},
		"TokenCache":      {ServerURI: "https://localhost", TokenCache: &memoryTokenCache{}},
	} {
		_, err := conf.FormatDSN()
		assert.ErrorContains(t, err, name+" cannot be expressed in a DSN", name)
	}
}

func TestParseExternalAuthChallenge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		headers      []string
		wantToken    string
		wantRedirect string
	}{
		{
			name:         "redirect and token servers",
			headers:      []string{`Basic realm="Trino"`, `Bearer x_redirect_server="https://t/initiate", x_token_server="https://t/token"`},
			wantToken:    "https://t/token",
			wantRedirect: "https://t/initiate",
		},
		{
			name:      "token server only",
			headers:   []string{`Bearer x_token_server="https://t/token"`},
			wantToken: "https://t/token",
		},
		{
			name:    "plain bearer challenge",
			headers: []string{`Bearer realm="Trino"`},
		},
		{
			name:         "challenges folded into one value",
			headers:      []string{`Basic realm="Trino", Bearer x_redirect_server="https://t/initiate", x_token_server="https://t/token"`},
			wantToken:    "https://t/token",
			wantRedirect: "https://t/initiate",
		},
		{
			name:      "parameter names in any case",
			headers:   []string{`bearer X_Token_Server="https://t/token"`},
			wantToken: "https://t/token",
		},
		{
			name:         "comma and escaped quote in a quoted value",
			headers:      []string{`Bearer x_redirect_server="https://t/initiate?a=1,2&b=\"x\"", x_token_server=https://t/token`},
			wantToken:    "https://t/token",
			wantRedirect: `https://t/initiate?a=1,2&b="x"`,
		},
		{
			name:      "token68 before the challenge",
			headers:   []string{`Negotiate abc==, Bearer x_token_server="https://t/token"`},
			wantToken: "https://t/token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			header := http.Header{"Www-Authenticate": tt.headers}
			challenge, err := parseExternalAuthChallenge(header)
			require.NoError(t, err)
			if tt.wantToken == "" {
				assert.Nil(t, challenge)
				return
			}
			require.NotNil(t, challenge)
			assert.Equal(t, tt.wantToken, challenge.tokenURL.String())
			if tt.wantRedirect == "" {
				assert.Nil(t, challenge.redirectURL)
			} else {
				assert.Equal(t, tt.wantRedirect, challenge.redirectURL.String())
			}
		})
	}
}

func TestParseExternalAuthChallengeRejectsNonHTTPURLs(t *testing.T) {
	t.Parallel()
	for _, header := range []string{
		`Bearer x_redirect_server="file:///etc/passwd", x_token_server="https://t/token"`,
		`Bearer x_redirect_server="-a Calculator", x_token_server="https://t/token"`,
		`Bearer x_token_server="smb://t/token"`,
	} {
		_, err := parseExternalAuthChallenge(http.Header{"Www-Authenticate": []string{header}})
		assert.ErrorContains(t, err, "not an absolute http or https URL", header)
	}
}

func TestNewConnectorCopiesExternalAuthenticationTimeout(t *testing.T) {
	t.Parallel()
	conf := &Config{ServerURI: "https://localhost", ExternalAuthentication: true, ExternalAuthenticationTimeout: ptr(time.Minute)}
	connector, err := NewConnector(conf)
	require.NoError(t, err)
	*conf.ExternalAuthenticationTimeout = time.Second
	require.NotNil(t, connector.conf.ExternalAuthenticationTimeout)
	assert.Equal(t, time.Minute, *connector.conf.ExternalAuthenticationTimeout)
}

func TestExternalAuthenticationRetriesUnavailableTokenServer(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	auth.unavailable = 2
	db := openExternalAuth(t, fc, &Config{})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	polls, _ := auth.counts()
	assert.Equal(t, 3, polls, "two unavailable polls, then the token")
}

func TestExternalAuthenticationLogsInOncePerRequest(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	auth.rejectAll = true
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{RedirectHandler: redirects.handle})

	_, err := db.Query("SELECT 1")
	var qf *ErrQueryFailed
	require.ErrorAs(t, err, &qf)
	assert.Equal(t, http.StatusUnauthorized, qf.StatusCode)
	assert.Equal(t, 1, redirects.count(), "a token rejected right after the login is not retried")
}

// staleTokenCache has no token for the first request, then returns a token
// another writer stored, which the server rejects.
type staleTokenCache struct {
	memoryTokenCache
	reads int
}

func (c *staleTokenCache) Token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if c.reads == 1 {
		return ""
	}
	return c.token
}

func TestExternalAuthenticationLogsInAfterRejectedCachedToken(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	newFakeExternalAuth(fc, "token1")
	cache := &staleTokenCache{}
	cache.SetToken("stale")
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{TokenCache: cache, RedirectHandler: redirects.handle})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Equal(t, 1, redirects.count())
}

// churningTokenCache returns a new token, which the server rejects, on every
// read, as a cache shared with another writer might.
type churningTokenCache struct {
	mu    sync.Mutex
	reads int
}

func (c *churningTokenCache) Token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	return "stale-" + strconv.Itoa(c.reads)
}

func (c *churningTokenCache) SetToken(string) {}

func TestExternalAuthenticationLogsInWhenCacheKeepsFailing(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	newFakeExternalAuth(fc, "token1")
	redirects := &recordingRedirects{}
	db := openExternalAuth(t, fc, &Config{TokenCache: &churningTokenCache{}, RedirectHandler: redirects.handle})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Positive(t, redirects.count())
}

func TestExternalAuthenticationTokenServerOnly(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeExternalAuth(fc, "token1")
	auth.challenge = `Bearer x_token_server="` + fc.url() + `/oauth2/token/1"`
	db := openExternalAuth(t, fc, &Config{RedirectHandler: func(context.Context, *url.URL) error {
		return errors.New("no redirect without x_redirect_server")
	}})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
}

func TestExternalAuthenticationSendsAccessTokenUntilLogin(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	newFakeExternalAuth(fc, "token1")
	db := openExternalAuth(t, fc, &Config{AccessToken: "access"})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	var auths []string
	for _, r := range fc.capturedRequests() {
		if r.path == "/v1/statement" {
			auths = append(auths, r.header.Get(authorizationHeader))
		}
	}
	assert.Equal(t, []string{"Bearer access", "Bearer token1"}, auths)
}

func TestExternalAuthenticationRejectsTokenServerResponses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(a *fakeExternalAuth, base string)
		wantErr string
	}{
		{
			name:    "no challenge",
			setup:   func(a *fakeExternalAuth, _ string) { a.noChallenge = true },
			wantErr: "401 Unauthorized",
		},
		{
			name:    "malformed challenge",
			setup:   func(a *fakeExternalAuth, _ string) { a.challenge = `Bearer x_token_server="ftp://t/token"` },
			wantErr: "invalid x_token_server: not an absolute http or https URL",
		},
		{
			name: "token server on another host",
			setup: func(a *fakeExternalAuth, _ string) {
				a.challenge = `Bearer x_token_server="https://other.invalid/oauth2/token/1"`
			},
			wantErr: "x_token_server is on https://other.invalid:443, not on the server",
		},
		{
			name:    "nextUri on another host",
			setup:   func(a *fakeExternalAuth, _ string) { a.nextURI = "https://other.invalid/oauth2/token/2" },
			wantErr: "token server nextUri is on https://other.invalid:443",
		},
		{
			name:    "relative nextUri",
			setup:   func(a *fakeExternalAuth, _ string) { a.nextURI = "/oauth2/token/2" },
			wantErr: "invalid token server nextUri",
		},
		{
			name:    "empty response",
			setup:   func(a *fakeExternalAuth, _ string) { a.body = "{}" },
			wantErr: "empty token server response",
		},
		{
			name:    "field names in another case",
			setup:   func(a *fakeExternalAuth, _ string) { a.body = `{"Token":"token1"}` },
			wantErr: "empty token server response",
		},
		{
			name:    "undecodable response",
			setup:   func(a *fakeExternalAuth, _ string) { a.body = "not json" },
			wantErr: "decoding token server response",
		},
		{
			name:    "not JSON",
			setup:   func(a *fakeExternalAuth, _ string) { a.contentType = "text/plain" },
			wantErr: "not application/json",
		},
		{
			name: "status that is not retried",
			setup: func(a *fakeExternalAuth, _ string) {
				a.status = http.StatusBadRequest
				a.body = `{"token":"secret"}`
			},
			wantErr: "token server returned 400 Bad Request",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeExternalAuth(fc, "token1")
			tt.setup(auth, fc.url())
			db := openExternalAuth(t, fc, &Config{})

			_, err := db.Query("SELECT 1")
			require.ErrorContains(t, err, tt.wantErr)
			assert.NotErrorIs(t, err, context.DeadlineExceeded, "fails without waiting for the timeout")
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

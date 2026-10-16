package trino

import (
	"context"
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testClientID     = "client-id"
	testClientSecret = "client-secret"
)

// fakeClientCredentials makes a fake coordinator reject requests without a
// token its fake token endpoint issued, with the challenge Trino 484 sends,
// and serves that token endpoint over HTTPS with the same test certificate.
type fakeClientCredentials struct {
	fc       *fakeCoordinator
	endpoint *httptest.Server

	mu sync.Mutex
	// challenge replaces the OAuth2 Bearer challenge.
	challenge string
	// rejectAll rejects every token.
	rejectAll bool
	accepted  string
	issued    int
	// expiresIn is sent as is in the token response when set, and left out
	// with omitExpiresIn.
	expiresIn     jsontext.Value
	omitExpiresIn bool
	// status and body replace the token endpoint's answer when set.
	status   int
	body     string
	location string
	// beforeToken runs before the token endpoint answers.
	beforeToken   func()
	tokenRequests []capturedRequest
	rejected      chan struct{}
}

func newFakeClientCredentials(t *testing.T, fc *fakeCoordinator) *fakeClientCredentials {
	t.Helper()
	a := &fakeClientCredentials{fc: fc, rejected: make(chan struct{}, 100)}
	a.endpoint = httptest.NewTLSServer(http.HandlerFunc(a.serveToken))
	t.Cleanup(a.endpoint.Close)
	fc.onRequest(a.serveCoordinator)
	return a
}

func (a *fakeClientCredentials) tokenURL() string {
	return a.endpoint.URL + "/oauth2/token"
}

func (a *fakeClientCredentials) serveToken(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	a.mu.Lock()
	a.tokenRequests = append(a.tokenRequests, capturedRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body})
	beforeToken := a.beforeToken
	a.mu.Unlock()
	if beforeToken != nil {
		beforeToken()
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.location != "" {
		w.Header().Set("Location", a.location)
	}
	if a.status != 0 {
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
		return
	}
	if a.body != "" {
		_, _ = w.Write([]byte(a.body))
		return
	}
	a.issued++
	a.accepted = "token-" + strconv.Itoa(a.issued)
	response := map[string]any{"access_token": a.accepted, "token_type": "Bearer"}
	switch {
	case a.omitExpiresIn:
	case a.expiresIn != nil:
		response["expires_in"] = a.expiresIn
	default:
		response["expires_in"] = 3600
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, response)
}

func (a *fakeClientCredentials) serveCoordinator(w http.ResponseWriter, r *http.Request) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.rejectAll && a.accepted != "" && r.Header.Get(authorizationHeader) == "Bearer "+a.accepted {
		return false
	}
	challenge := a.challenge
	if challenge == "" {
		base := a.fc.url()
		challenge = `Bearer x_redirect_server="` + base + `/oauth2/initiate/1", x_token_server="` + base + `/oauth2/token/1", x_token_endpoint="` + a.tokenURL() + `", scope="openid profile"`
	}
	// the order Trino uses with PASSWORD,JWT,OAUTH2 authenticators
	w.Header().Add("WWW-Authenticate", `Basic realm="Trino"`)
	w.Header().Add("WWW-Authenticate", `Bearer realm="Trino", token_type="JWT"`)
	w.Header().Add("WWW-Authenticate", challenge)
	w.WriteHeader(http.StatusUnauthorized)
	select {
	case a.rejected <- struct{}{}:
	default:
	}
	return true
}

// revoke makes the coordinator reject the token it accepts now.
func (a *fakeClientCredentials) revoke() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.accepted = ""
}

func (a *fakeClientCredentials) requests() []capturedRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]capturedRequest(nil), a.tokenRequests...)
}

func (a *fakeClientCredentials) forms(t *testing.T) []url.Values {
	t.Helper()
	var forms []url.Values
	for _, r := range a.requests() {
		form, err := url.ParseQuery(string(r.body))
		require.NoError(t, err)
		forms = append(forms, form)
	}
	return forms
}

func openClientCredentials(t *testing.T, fc *fakeCoordinator, conf *Config) *sql.DB {
	t.Helper()
	conf.ServerURI = fc.url()
	conf.OAuth2ClientID = testClientID
	conf.OAuth2ClientSecret = testClientSecret
	conf.HTTPClient = fc.server.Client()
	connector, err := NewConnector(conf)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// freshPages serves a new response value for every query, since the fake
// sets nextUri on the value it serves.
func freshPages() []page {
	return []page{
		{response: func(string) any { return &stmtResponse{ID: fakeQueryID} }},
		{response: func(base string) any { return resultPage([][]any{{1}}).response(base) }},
	}
}

func queryFailureReason(t *testing.T, err error) error {
	t.Helper()
	var qf *ErrQueryFailed
	require.ErrorAs(t, err, &qf)
	return qf.Reason
}

func statementAuthorizations(fc *fakeCoordinator) []string {
	var authorizations []string
	for _, r := range fc.capturedRequests() {
		if r.path == "/v1/statement" {
			authorizations = append(authorizations, r.header.Get(authorizationHeader))
		}
	}
	return authorizations
}

func TestClientCredentials(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeClientCredentials(t, fc)
	db := openClientCredentials(t, fc, &Config{})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	requests := auth.requests()
	require.Len(t, requests, 1)
	assert.Equal(t, http.MethodPost, requests[0].method)
	assert.Equal(t, "/oauth2/token", requests[0].path)
	assert.Equal(t, "application/x-www-form-urlencoded", requests[0].header.Get("Content-Type"))
	assert.Empty(t, requests[0].header.Get(authorizationHeader), "the credentials travel in the form, not in a Basic header")
	assert.Equal(t, url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {testClientID},
		"client_secret": {testClientSecret},
		"scope":         {"openid profile"},
	}, auth.forms(t)[0])

	assert.Equal(t, []string{"", "Bearer token-1"}, statementAuthorizations(fc))
	for _, r := range fc.capturedRequests() {
		if r.path == "/v1/statement" {
			assert.Equal(t, "SELECT 1", string(r.body), "the retried statement keeps its body")
		}
		for name, values := range r.header {
			for _, value := range values {
				assert.NotContains(t, value, testClientSecret, "header %s sent to the coordinator", name)
			}
		}
		assert.NotContains(t, string(r.body), testClientSecret)
	}
}

func TestClientCredentialsScope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		scope     string
		challenge func(a *fakeClientCredentials) string
		want      []string
	}{
		{
			name:  "configured scope overrides the advertised one",
			scope: "custom offline_access",
			want:  []string{"custom offline_access"},
		},
		{
			name: "no scope advertised or configured",
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="` + a.tokenURL() + `"`
			},
		},
		{
			name:  "configured scope without an advertised one",
			scope: "custom",
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="` + a.tokenURL() + `"`
			},
			want: []string{"custom"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeClientCredentials(t, fc)
			if tt.challenge != nil {
				auth.challenge = tt.challenge(auth)
			}
			db := openClientCredentials(t, fc, &Config{OAuth2Scope: tt.scope})

			rows, err := db.Query("SELECT 1")
			require.NoError(t, err)
			collectInts(t, rows)
			forms := auth.forms(t)
			require.Len(t, forms, 1)
			assert.Equal(t, tt.want, forms[0]["scope"])
		})
	}
}

func TestClientCredentialsCachesToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		expiresIn     jsontext.Value
		omitExpiresIn bool
	}{
		{name: "expires_in in seconds", expiresIn: jsontext.Value(`3600`)},
		{name: "expires_in as a string", expiresIn: jsontext.Value(`"3600"`)},
		{name: "null expires_in", expiresIn: jsontext.Value(`null`)},
		{name: "no expires_in", omitExpiresIn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(freshPages()...)
			auth := newFakeClientCredentials(t, fc)
			auth.expiresIn = tt.expiresIn
			auth.omitExpiresIn = tt.omitExpiresIn
			db := openClientCredentials(t, fc, &Config{})

			for range 2 {
				var n int
				require.NoError(t, db.QueryRow("SELECT 1").Scan(&n))
			}
			assert.Len(t, auth.requests(), 1)
			assert.Equal(t, []string{"", "Bearer token-1", "Bearer token-1"}, statementAuthorizations(fc), "the second query sends the cached token at once")
		})
	}
}

func TestClientCredentialsRenewsExpiredToken(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(freshPages()...)
	auth := newFakeClientCredentials(t, fc)
	// within the expiry margin, so the token counts as expired at once
	auth.expiresIn = jsontext.Value(`20`)
	db := openClientCredentials(t, fc, &Config{})

	for range 2 {
		var n int
		require.NoError(t, db.QueryRow("SELECT 1").Scan(&n))
	}
	assert.Len(t, auth.requests(), 4, "the statement and the page request of each query start without a token")
	assert.Equal(t, []string{"", "Bearer token-1", "", "Bearer token-3"}, statementAuthorizations(fc))
}

func TestClientCredentialsRenewsRejectedToken(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeClientCredentials(t, fc)
	fc.onPage(func(index int, r *http.Request) {
		if index == 0 {
			auth.revoke()
		}
	})
	db := openClientCredentials(t, fc, &Config{})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Len(t, auth.requests(), 2)
}

func TestClientCredentialsRequestsTokenOncePerRequest(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeClientCredentials(t, fc)
	auth.rejectAll = true
	db := openClientCredentials(t, fc, &Config{})

	_, err := db.Query("SELECT 1")
	var qf *ErrQueryFailed
	require.ErrorAs(t, err, &qf)
	assert.Equal(t, http.StatusUnauthorized, qf.StatusCode)
	assert.Len(t, auth.requests(), 1, "a token rejected right after the grant is not retried")
}

func TestClientCredentialsSharesTokenAcrossConnections(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(freshPages()...)
	auth := newFakeClientCredentials(t, fc)
	auth.beforeToken = func() {
		// wait until the other connection was rejected too
		for range 2 {
			select {
			case <-auth.rejected:
			case <-time.After(5 * time.Second):
				t.Error("the second connection was not rejected")
				return
			}
		}
	}
	db := openClientCredentials(t, fc, &Config{})

	ctx := t.Context()
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
	assert.Len(t, auth.requests(), 1)
}

func TestClientCredentialsConfiguredTokenEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		configure func(a *fakeClientCredentials) string
		challenge func(a *fakeClientCredentials) string
		wantErr   string
	}{
		{
			name:      "matching the advertised endpoint",
			configure: func(a *fakeClientCredentials) string { return a.tokenURL() },
		},
		{
			name: "matching after normalization",
			configure: func(a *fakeClientCredentials) string {
				return strings.Replace(a.tokenURL(), "https://", "HTTPS://", 1) + "/?tenant=1"
			},
		},
		{
			name:      "without an advertised endpoint",
			configure: func(a *fakeClientCredentials) string { return a.tokenURL() },
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_redirect_server="` + a.fc.url() + `/oauth2/initiate/1", x_token_server="` + a.fc.url() + `/oauth2/token/1"`
			},
		},
		{
			name:      "advertised endpoint on another host",
			configure: func(a *fakeClientCredentials) string { return a.tokenURL() },
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="https://attacker.invalid/oauth2/token"`
			},
			wantErr: `server-provided OAuth2 token endpoint "https://attacker.invalid/oauth2/token" does not match the configured oauth2TokenEndpoint`,
		},
		{
			name:      "advertised endpoint with another path",
			configure: func(a *fakeClientCredentials) string { return a.tokenURL() },
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="` + a.endpoint.URL + `/other"`
			},
			wantErr: "refusing to send client credentials",
		},
		{
			name:      "advertised endpoint with another scheme",
			configure: func(a *fakeClientCredentials) string { return a.tokenURL() },
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="` + strings.Replace(a.tokenURL(), "https://", "http://", 1) + `"`
			},
			wantErr: "does not match the configured oauth2TokenEndpoint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeClientCredentials(t, fc)
			if tt.challenge != nil {
				auth.challenge = tt.challenge(auth)
			}
			db := openClientCredentials(t, fc, &Config{OAuth2TokenEndpoint: tt.configure(auth)})

			rows, err := db.Query("SELECT 1")
			if tt.wantErr != "" {
				assert.ErrorContains(t, queryFailureReason(t, err), tt.wantErr)
				assert.Empty(t, auth.requests(), "the credentials are not sent anywhere")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
			assert.Len(t, auth.requests(), 1)
		})
	}
}

func TestClientCredentialsRejectsAdvertisedEndpoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		challenge func(a *fakeClientCredentials) string
		wantErr   string
	}{
		{
			name: "server without the client credentials challenge",
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_redirect_server="` + a.fc.url() + `/oauth2/initiate/1", x_token_server="` + a.fc.url() + `/oauth2/token/1"`
			},
			wantErr: "OAuth2 token endpoint is not available; the server did not return x_token_endpoint",
		},
		{
			name: "plain http endpoint",
			challenge: func(a *fakeClientCredentials) string {
				return `Bearer x_token_endpoint="` + strings.Replace(a.tokenURL(), "https://", "http://", 1) + `"`
			},
			wantErr: "is not https; refusing to send client credentials over an insecure connection",
		},
		{
			name:      "relative endpoint",
			challenge: func(*fakeClientCredentials) string { return `Bearer x_token_endpoint="/oauth2/token"` },
			wantErr:   `server-provided OAuth2 token endpoint "/oauth2/token" is not a valid URL`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeClientCredentials(t, fc)
			auth.challenge = tt.challenge(auth)
			db := openClientCredentials(t, fc, &Config{})

			_, err := db.Query("SELECT 1")
			var qf *ErrQueryFailed
			require.ErrorAs(t, err, &qf)
			assert.Equal(t, http.StatusUnauthorized, qf.StatusCode)
			assert.ErrorContains(t, qf.Reason, tt.wantErr)
			assert.Empty(t, auth.requests())
		})
	}
}

func TestClientCredentialsTokenEndpointFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(a *fakeClientCredentials)
		wantErr string
	}{
		{
			name: "rejected client",
			setup: func(a *fakeClientCredentials) {
				a.status = http.StatusUnauthorized
				a.body = `{"error":"invalid_client","error_description":"bad secret"}`
			},
			wantErr: "failed to obtain OAuth2 client credentials token: OAuth2 Client Credentials authentication failed, HTTP 401: invalid_client",
		},
		{
			name: "error without a body",
			setup: func(a *fakeClientCredentials) {
				a.status = http.StatusInternalServerError
			},
			wantErr: "OAuth2 Client Credentials authentication failed, HTTP 500",
		},
		{
			name: "redirect",
			setup: func(a *fakeClientCredentials) {
				a.status = http.StatusTemporaryRedirect
				a.location = "https://attacker.invalid/token"
			},
			wantErr: "OAuth2 Client Credentials authentication failed, HTTP 307",
		},
		{
			name:    "no access token",
			setup:   func(a *fakeClientCredentials) { a.body = `{"token_type":"Bearer"}` },
			wantErr: "token endpoint response has no access_token",
		},
		{
			name:    "not JSON",
			setup:   func(a *fakeClientCredentials) { a.body = `access_token=token-1` },
			wantErr: "decoding token endpoint response",
		},
		{
			name:    "invalid expires_in",
			setup:   func(a *fakeClientCredentials) { a.body = `{"access_token":"token-1","expires_in":"soon"}` },
			wantErr: `token endpoint response has an invalid expires_in: "soon"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeTLSCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))
			auth := newFakeClientCredentials(t, fc)
			tt.setup(auth)
			db := openClientCredentials(t, fc, &Config{})

			_, err := db.Query("SELECT 1")
			assert.ErrorContains(t, queryFailureReason(t, err), tt.wantErr)
			assert.NotContains(t, err.Error(), testClientSecret)
			assert.Len(t, auth.requests(), 1, "a redirect is not followed")
		})
	}
}

func TestClientCredentialsHonorsContext(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeClientCredentials(t, fc)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	auth.beforeToken = func() { <-release }
	db := openClientCredentials(t, fc, &Config{})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := db.QueryContext(ctx, "SELECT 1")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestClientCredentialsDisabled(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	auth := newFakeClientCredentials(t, fc)
	connector, err := NewConnector(&Config{ServerURI: fc.url(), HTTPClient: fc.server.Client()})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.Query("SELECT 1")
	var qf *ErrQueryFailed
	require.ErrorAs(t, err, &qf)
	assert.Equal(t, http.StatusUnauthorized, qf.StatusCode)
	assert.Empty(t, auth.requests())
}

func TestClientCredentialsValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{
			name:    "client ID without a secret",
			dsn:     "https://localhost?oauth2ClientId=id",
			wantErr: "trino: oauth2ClientId requires oauth2ClientSecret to be set",
		},
		{
			name:    "secret without a client ID",
			dsn:     "https://localhost?oauth2ClientSecret=secret",
			wantErr: "trino: oauth2ClientSecret requires oauth2ClientId to be set",
		},
		{
			name:    "token endpoint without a client ID",
			dsn:     "https://localhost?oauth2TokenEndpoint=https%3A%2F%2Fidp%2Ftoken",
			wantErr: "trino: oauth2TokenEndpoint requires oauth2ClientId to be set",
		},
		{
			name:    "scope without a client ID",
			dsn:     "https://localhost?oauth2Scope=openid",
			wantErr: "trino: oauth2Scope requires oauth2ClientId to be set",
		},
		{
			name:    "plain http server",
			dsn:     "http://localhost:8080?oauth2ClientId=id&oauth2ClientSecret=secret",
			wantErr: errClientCredentialsNeedsTLS.Error(),
		},
		{
			name:    "plain http token endpoint",
			dsn:     "https://localhost?oauth2ClientId=id&oauth2ClientSecret=secret&oauth2TokenEndpoint=http%3A%2F%2Fidp%2Ftoken",
			wantErr: "trino: oauth2TokenEndpoint must be a valid https:// URL",
		},
		{
			name:    "relative token endpoint",
			dsn:     "https://localhost?oauth2ClientId=id&oauth2ClientSecret=secret&oauth2TokenEndpoint=%2Ftoken",
			wantErr: "trino: oauth2TokenEndpoint must be a valid https:// URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseDSN(tt.dsn)
			assert.EqualError(t, err, tt.wantErr)

			conf := &Config{ServerURI: tt.dsn[:strings.Index(tt.dsn, "?")]}
			query, err := url.ParseQuery(tt.dsn[strings.Index(tt.dsn, "?")+1:])
			require.NoError(t, err)
			conf.OAuth2ClientID = query.Get(oauth2ClientIDConfig)
			conf.OAuth2ClientSecret = query.Get(oauth2ClientSecretConfig)
			conf.OAuth2TokenEndpoint = query.Get(oauth2TokenEndpointConfig)
			conf.OAuth2Scope = query.Get(oauth2ScopeConfig)
			_, err = NewConnector(conf)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestClientCredentialsConflicts(t *testing.T) {
	t.Parallel()
	for name, conf := range map[string]*Config{
		"password":                   {ServerURI: "https://user:secret@localhost"},
		"access token":               {ServerURI: "https://localhost", AccessToken: "token"},
		"Kerberos":                   {ServerURI: "https://localhost", KerberosEnabled: true},
		"external authentication":    {ServerURI: "https://localhost", ExternalAuthentication: true},
		"forwardAuthorizationHeader": {ServerURI: "https://localhost", ForwardAuthorizationHeader: true},
	} {
		conf.OAuth2ClientID = testClientID
		conf.OAuth2ClientSecret = testClientSecret
		_, err := NewConnector(conf)
		assert.ErrorContains(t, err, "oauth2ClientId cannot be combined", name)
	}

	db, err := sql.Open("trino", "https://localhost?oauth2ClientId=id&oauth2ClientSecret=secret&accessToken=token")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.ErrorContains(t, db.Ping(), "oauth2ClientId cannot be combined")
}

func TestClientCredentialsDSNRoundTrip(t *testing.T) {
	t.Parallel()
	conf := &Config{
		ServerURI:           "https://localhost:8443",
		OAuth2ClientID:      "id",
		OAuth2ClientSecret:  "s&cr=t",
		OAuth2TokenEndpoint: "https://idp.example.com/token?tenant=1",
		OAuth2Scope:         "openid profile",
	}
	dsn, err := conf.FormatDSN()
	require.NoError(t, err)
	parsed, err := ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, conf, parsed)
}

func TestParseClientCredentialsChallenge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		headers []string
		want    clientCredentialsChallenge
	}{
		{
			name:    "Trino 484",
			headers: []string{`Bearer x_redirect_server="https://t/initiate", x_token_server="https://t/token", x_token_endpoint="https://idp/token", scope="openid profile"`},
			want:    clientCredentialsChallenge{tokenEndpoint: "https://idp/token", scope: "openid profile"},
		},
		{
			name:    "after the JWT challenge",
			headers: []string{`Basic realm="Trino"`, `Bearer realm="Trino", token_type="JWT"`, `Bearer x_token_server="https://t/token", x_token_endpoint="https://idp/token", scope="openid"`},
			want:    clientCredentialsChallenge{tokenEndpoint: "https://idp/token", scope: "openid"},
		},
		{
			name:    "challenges folded into one value",
			headers: []string{`Bearer realm="Trino", token_type="JWT", Bearer x_token_endpoint="https://idp/token?a=1,2"`},
			want:    clientCredentialsChallenge{tokenEndpoint: "https://idp/token?a=1,2"},
		},
		{
			name:    "parameter names in any case",
			headers: []string{`bearer X_Token_Endpoint="https://idp/token", Scope="openid"`},
			want:    clientCredentialsChallenge{tokenEndpoint: "https://idp/token", scope: "openid"},
		},
		{
			name:    "before Trino 484",
			headers: []string{`Bearer x_redirect_server="https://t/initiate", x_token_server="https://t/token"`},
		},
		{
			name:    "scope without a token endpoint",
			headers: []string{`Bearer scope="openid"`},
		},
		{
			name:    "no Bearer challenge",
			headers: []string{`Basic realm="Trino", x_token_endpoint="https://idp/token"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, parseClientCredentialsChallenge(http.Header{"Www-Authenticate": tt.headers}))
		})
	}
}

func TestSameTokenEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		configured string
		advertised string
		want       bool
	}{
		{"https://idp/token", "https://idp/token", true},
		{"https://idp/token", "https://idp/token/", true},
		{"https://idp/token/", "https://idp/token", true},
		{"https://IdP/token", "https://idp/token", true},
		{"https://idp/token", "https://idp:443/token", true},
		{"https://idp", "https://idp/", true},
		{"https://idp/token?tenant=1", "https://idp/token#x", true},
		{"https://idp/token", "https://idp/Token", false},
		{"https://idp/token", "https://idp:8443/token", false},
		{"https://idp/token", "http://idp/token", false},
		{"https://idp/token", "https://idp.attacker/token", false},
		{"https://idp/token", "https://idp/token/extra", false},
	}
	for _, tt := range tests {
		configured, err := url.Parse(tt.configured)
		require.NoError(t, err)
		advertised, err := url.Parse(tt.advertised)
		require.NoError(t, err)
		assert.Equal(t, tt.want, sameTokenEndpoint(configured, advertised), "%s and %s", tt.configured, tt.advertised)
	}
}

func TestParseExpiresIn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value   string
		want    time.Duration
		wantErr bool
	}{
		{value: "", want: defaultTokenLifetime},
		{value: "null", want: defaultTokenLifetime},
		{value: "3600", want: time.Hour},
		{value: `"3600"`, want: time.Hour},
		{value: "0", want: 0},
		{value: "-5", want: 0},
		{value: "99999999999999999", want: maxTokenLifetime},
		{value: "1.5", wantErr: true},
		{value: `"soon"`, wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseExpiresIn(jsontext.Value(tt.value))
		if tt.wantErr {
			assert.Error(t, err, tt.value)
			continue
		}
		require.NoError(t, err, tt.value)
		assert.Equal(t, tt.want, got, tt.value)
	}
}

func TestClientCredentialsAuthenticatorRequiresContextForLock(t *testing.T) {
	t.Parallel()
	a := newClientCredentialsAuthenticator(&Config{OAuth2ClientID: testClientID, OAuth2ClientSecret: testClientSecret})
	a.lock <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := a.authenticate(ctx, http.DefaultClient, clientCredentialsChallenge{}, "")
	assert.ErrorIs(t, err, context.Canceled)
}

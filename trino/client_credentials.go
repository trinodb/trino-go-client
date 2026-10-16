package trino

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	oauth2ClientIDConfig      = "oauth2ClientId"
	oauth2ClientSecretConfig  = "oauth2ClientSecret"
	oauth2TokenEndpointConfig = "oauth2TokenEndpoint"
	oauth2ScopeConfig         = "oauth2Scope"

	// defaultTokenLifetime applies when the token endpoint leaves out
	// expires_in, as in the JDBC driver.
	defaultTokenLifetime = 300 * time.Second
	// tokenExpiryMargin renews a token before it expires, so a request does
	// not carry a token that runs out on its way to the coordinator.
	tokenExpiryMargin = 30 * time.Second
	// maxTokenLifetime keeps a huge expires_in from overflowing time.Duration.
	maxTokenLifetime = 100 * 365 * 24 * time.Hour
	// maxTokenResponseSize bounds what is read from the token endpoint.
	maxTokenResponseSize = 1 << 20
)

var errClientCredentialsNeedsTLS = errors.New("trino: TLS/SSL is required for authentication with client credentials")

// validateClientCredentials mirrors the JDBC driver, which rejects the other
// ways to authenticate, since each of them sends its own Authorization header.
func (c *Config) validateClientCredentials(serverURL *url.URL) error {
	if err := c.validateClientCredentialsOptions(serverURL); err != nil {
		return err
	}
	if c.OAuth2ClientID == "" {
		return nil
	}
	password, _ := serverURL.User.Password()
	if c.ExternalAuthentication || password != "" || c.AccessToken != "" || c.KerberosEnabled || c.ForwardAuthorizationHeader {
		return fmt.Errorf("trino: %s cannot be combined with %s, a password, an %s, Kerberos or %s", oauth2ClientIDConfig, externalAuthenticationConfig, accessTokenConfig, forwardAuthorizationHeaderConfig)
	}
	return nil
}

// validateClientCredentialsOptions checks the client credentials options on
// their own, like ParseDSN does for every option; the conflicts with other
// options are reported when connecting.
func (c *Config) validateClientCredentialsOptions(serverURL *url.URL) error {
	if c.OAuth2ClientID == "" {
		for _, option := range []struct{ name, value string }{
			{oauth2ClientSecretConfig, c.OAuth2ClientSecret},
			{oauth2TokenEndpointConfig, c.OAuth2TokenEndpoint},
			{oauth2ScopeConfig, c.OAuth2Scope},
		} {
			if option.value != "" {
				return fmt.Errorf("trino: %s requires %s to be set", option.name, oauth2ClientIDConfig)
			}
		}
		return nil
	}
	if c.OAuth2ClientSecret == "" {
		return fmt.Errorf("trino: %s requires %s to be set", oauth2ClientIDConfig, oauth2ClientSecretConfig)
	}
	if serverURL.Scheme != "https" {
		return errClientCredentialsNeedsTLS
	}
	if c.OAuth2TokenEndpoint == "" {
		return nil
	}
	_, err := parseConfiguredTokenEndpoint(c.OAuth2TokenEndpoint)
	return err
}

// parseConfiguredTokenEndpoint accepts only https URLs, since the client
// secret is sent there.
func parseConfiguredTokenEndpoint(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("trino: %s must be a valid https:// URL", oauth2TokenEndpointConfig)
	}
	return u, nil
}

// clientCredentialsAuthenticator obtains tokens with the OAuth2 client
// credentials grant for the connections of one Connector. The client secret
// is only sent to an https token endpoint, never to the coordinator, and,
// when OAuth2TokenEndpoint is set, never to another endpoint, so a
// coordinator cannot redirect it to a host it controls.
type clientCredentialsAuthenticator struct {
	clientID           string
	clientSecret       string
	configuredEndpoint string
	configuredScope    string
	// lock lets one connection obtain a token while the others wait for it.
	lock chan struct{}

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func newClientCredentialsAuthenticator(conf *Config) *clientCredentialsAuthenticator {
	if conf.OAuth2ClientID == "" {
		return nil
	}
	return &clientCredentialsAuthenticator{
		clientID:           conf.OAuth2ClientID,
		clientSecret:       conf.OAuth2ClientSecret,
		configuredEndpoint: conf.OAuth2TokenEndpoint,
		configuredScope:    conf.OAuth2Scope,
		lock:               make(chan struct{}, 1),
	}
}

// cachedToken returns the token to send with a request, or "" when there is
// none that is still valid, so the coordinator's challenge starts a new grant.
func (a *clientCredentialsAuthenticator) cachedToken() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token == "" || !time.Now().Before(a.expiry) {
		return ""
	}
	return a.token
}

// authenticate returns a token to use instead of rejected, the token the
// coordinator refused, or "" when the request carried none. A valid token
// another connection obtained in the meantime is returned as is.
func (a *clientCredentialsAuthenticator) authenticate(ctx context.Context, client *http.Client, challenge clientCredentialsChallenge, rejected string) (string, error) {
	select {
	case a.lock <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-a.lock }()

	if token := a.cachedToken(); token != "" && token != rejected {
		return token, nil
	}
	a.setToken("", time.Time{})

	endpoint, err := a.resolveTokenEndpoint(challenge.tokenEndpoint)
	if err != nil {
		return "", err
	}
	token, lifetime, err := a.requestToken(ctx, client, endpoint, cmp.Or(a.configuredScope, challenge.scope))
	if err != nil {
		return "", fmt.Errorf("trino: failed to obtain OAuth2 client credentials token: %w", err)
	}
	a.setToken(token, time.Now().Add(max(lifetime-tokenExpiryMargin, 0)))
	return token, nil
}

func (a *clientCredentialsAuthenticator) setToken(token string, expiry time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = token
	a.expiry = expiry
}

// resolveTokenEndpoint picks the endpoint the client credentials are sent to:
// the configured one, which an advertised endpoint must match, or else the
// advertised one, which must use https.
func (a *clientCredentialsAuthenticator) resolveTokenEndpoint(advertised string) (*url.URL, error) {
	if a.configuredEndpoint != "" {
		configured, err := parseConfiguredTokenEndpoint(a.configuredEndpoint)
		if err != nil {
			return nil, err
		}
		if advertised == "" {
			return configured, nil
		}
		if advertisedURL, err := url.Parse(advertised); err != nil || !sameTokenEndpoint(configured, advertisedURL) {
			return nil, fmt.Errorf("trino: server-provided OAuth2 token endpoint %q does not match the configured %s %q; refusing to send client credentials", advertised, oauth2TokenEndpointConfig, a.configuredEndpoint)
		}
		return configured, nil
	}

	if advertised == "" {
		return nil, errors.New("trino: OAuth2 token endpoint is not available; the server did not return x_token_endpoint in the WWW-Authenticate challenge, set " + oauth2TokenEndpointConfig + " or use Trino 484 or later")
	}
	advertisedURL, err := url.Parse(advertised)
	if err != nil || advertisedURL.Host == "" {
		return nil, fmt.Errorf("trino: server-provided OAuth2 token endpoint %q is not a valid URL", advertised)
	}
	if advertisedURL.Scheme != "https" {
		return nil, fmt.Errorf("trino: server-provided OAuth2 token endpoint %q is not https; refusing to send client credentials over an insecure connection", advertised)
	}
	return advertisedURL, nil
}

// sameTokenEndpoint compares the scheme, host, port and path of two
// endpoints, ignoring a trailing slash, the query and the fragment, which do
// not identify the endpoint.
func sameTokenEndpoint(configured, advertised *url.URL) bool {
	return origin(configured) == origin(advertised) && normalizedPath(configured) == normalizedPath(advertised)
}

func normalizedPath(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	if len(path) > 1 {
		return strings.TrimSuffix(path, "/")
	}
	return path
}

type clientCredentialsTokenResponse struct {
	AccessToken string         `json:"access_token"`
	ExpiresIn   jsontext.Value `json:"expires_in"`
	Error       string         `json:"error"`
}

// requestToken posts the client credentials grant to endpoint, sending the
// credentials as form fields (client_secret_post), as the JDBC driver does.
func (a *clientCredentialsAuthenticator) requestToken(ctx context.Context, client *http.Client, endpoint *url.URL, scope string) (string, time.Duration, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {a.clientID},
		"client_secret": {a.clientSecret},
	}
	if scope != "" {
		form.Set("scope", scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(userAgentHeader, userAgent)

	// A redirect could send the credentials to a host the server chose.
	resp, err := withoutRedirects(client).Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	var response clientCredentialsTokenResponse
	decodeErr := json.UnmarshalRead(io.LimitReader(resp.Body, maxTokenResponseSize), &response)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if decodeErr == nil && response.Error != "" {
			return "", 0, fmt.Errorf("OAuth2 Client Credentials authentication failed, HTTP %d: %s", resp.StatusCode, response.Error)
		}
		return "", 0, fmt.Errorf("OAuth2 Client Credentials authentication failed, HTTP %d", resp.StatusCode)
	}
	if decodeErr != nil {
		return "", 0, fmt.Errorf("decoding token endpoint response: %w", decodeErr)
	}
	if response.AccessToken == "" {
		return "", 0, errors.New("token endpoint response has no access_token")
	}
	lifetime, err := parseExpiresIn(response.ExpiresIn)
	if err != nil {
		return "", 0, err
	}
	return response.AccessToken, lifetime, nil
}

// parseExpiresIn reads the token lifetime in seconds, which some providers
// send as a string, as the Jackson binding of the JDBC driver accepts.
func parseExpiresIn(value jsontext.Value) (time.Duration, error) {
	if len(value) == 0 || string(value) == "null" {
		return defaultTokenLifetime, nil
	}
	seconds, err := strconv.ParseInt(strings.Trim(string(value), `"`), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("token endpoint response has an invalid expires_in: %s", value)
	}
	return time.Duration(min(max(seconds, 0), int64(maxTokenLifetime/time.Second))) * time.Second, nil
}

type clientCredentialsChallenge struct {
	tokenEndpoint string
	scope         string
}

// parseClientCredentialsChallenge reads the token endpoint and scope that
// Trino 484 and later add to the OAuth2 Bearer challenge. Another Bearer
// challenge, like the JWT authenticator's, can come first, so the one naming
// a token endpoint is taken.
func parseClientCredentialsChallenge(header http.Header) clientCredentialsChallenge {
	for _, value := range header.Values("WWW-Authenticate") {
		for _, c := range parseChallenges(value) {
			if !strings.EqualFold(c.scheme, "Bearer") || c.params["x_token_endpoint"] == "" {
				continue
			}
			return clientCredentialsChallenge{tokenEndpoint: c.params["x_token_endpoint"], scope: c.params["scope"]}
		}
	}
	return clientCredentialsChallenge{}
}

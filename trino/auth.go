package trino

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	externalAuthenticationConfig         = "externalAuthentication"
	externalAuthenticationTimeoutConfig  = "externalAuthenticationTimeout"
	defaultExternalAuthenticationTimeout = 2 * time.Minute
)

var errExternalAuthenticationNeedsTLS = errors.New("trino: TLS/SSL is required for external authentication")

// validateExternalAuthentication rejects the settings that send their own
// Authorization header with every request, which a token must not silently
// replace. An AccessToken is allowed: it is sent until a token is cached.
func (c *Config) validateExternalAuthentication(serverURL *url.URL) error {
	if !c.ExternalAuthentication {
		return nil
	}
	if serverURL.Scheme != "https" {
		return errExternalAuthenticationNeedsTLS
	}
	password, _ := serverURL.User.Password()
	if password != "" || c.KerberosEnabled || c.ForwardAuthorizationHeader {
		return errors.New("trino: external authentication cannot be combined with a password, Kerberos or " + forwardAuthorizationHeaderConfig)
	}
	return nil
}

// RedirectHandler sends the user to redirectURL to authenticate, when the
// server asks for external authentication.
type RedirectHandler func(ctx context.Context, redirectURL *url.URL) error

// TokenCache keeps the token obtained through external authentication. It is
// used by all connections of a Connector, so it must be safe for concurrent use.
//
// A Connector makes its connections wait while one of them logs in, but it
// cannot coordinate with other Connectors or processes: each that shares the
// cache and finds its token rejected starts its own login. Use one Connector
// per cache to get a single login.
type TokenCache interface {
	// Token returns the cached token, or "" when there is none.
	Token() string
	SetToken(token string)
}

// OpenBrowser is a RedirectHandler that opens redirectURL in the default browser.
func OpenBrowser(ctx context.Context, redirectURL *url.URL) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.CommandContext(ctx, "open", redirectURL.String())
	case "windows":
		cmd = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", redirectURL.String())
	default:
		cmd = exec.CommandContext(ctx, "xdg-open", redirectURL.String())
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("trino: opening a browser: %w", err)
	}
	return nil
}

type memoryTokenCache struct {
	mu    sync.Mutex
	token string
}

func (m *memoryTokenCache) Token() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token
}

func (m *memoryTokenCache) SetToken(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.token = token
}

// externalAuthenticator obtains tokens for the connections of one Connector.
type externalAuthenticator struct {
	// server is the origin of Config.ServerURI; token server URLs must match it.
	server   string
	redirect RedirectHandler
	cache    TokenCache
	timeout  time.Duration
	// lock lets one connection obtain a token while the others wait for it.
	lock chan struct{}
}

func newExternalAuthenticator(conf *Config) *externalAuthenticator {
	if !conf.ExternalAuthentication {
		return nil
	}
	a := &externalAuthenticator{
		redirect: conf.RedirectHandler,
		cache:    conf.TokenCache,
		timeout:  defaultExternalAuthenticationTimeout,
		lock:     make(chan struct{}, 1),
	}
	if a.redirect == nil {
		a.redirect = OpenBrowser
	}
	if a.cache == nil {
		a.cache = &memoryTokenCache{}
	}
	if conf.ExternalAuthenticationTimeout != nil {
		a.timeout = *conf.ExternalAuthenticationTimeout
	}
	if serverURL, err := url.Parse(conf.ServerURI); err == nil {
		a.server = origin(serverURL)
	}
	return a
}

// origin returns the scheme, host and port of u, with the default port made
// explicit.
func origin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[strings.ToLower(u.Scheme)]
	}
	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

// checkOrigin rejects a token server URL on another host than the server, so
// that a coordinator cannot make the driver send requests elsewhere.
func (a *externalAuthenticator) checkOrigin(name string, u *url.URL) error {
	if got := origin(u); got != a.server {
		return fmt.Errorf("trino: %s is on %s, not on the server %s; behind a proxy, set http-server.process-forwarded=true on the coordinator", name, got, a.server)
	}
	return nil
}

// maxCachedTokenReuses is how many cached tokens a request tries after a
// rejection before it logs in itself.
const maxCachedTokenReuses = 2

// authenticate returns a token to use instead of the rejected one. With
// reuseCache it returns a token another connection cached in the meantime.
// loggedIn reports whether it obtained a new token from the token server.
func (a *externalAuthenticator) authenticate(ctx context.Context, client *http.Client, challenge *externalAuthChallenge, rejected string, reuseCache bool) (token string, loggedIn bool, err error) {
	if err := a.checkOrigin("x_token_server", challenge.tokenURL); err != nil {
		return "", false, err
	}
	select {
	case a.lock <- struct{}{}:
	case <-ctx.Done():
		return "", false, ctx.Err()
	}
	defer func() { <-a.lock }()

	if token := a.cache.Token(); reuseCache && token != "" && token != rejected {
		return token, false, nil
	}
	a.cache.SetToken("")

	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	if challenge.redirectURL != nil {
		if err := a.redirect(ctx, challenge.redirectURL); err != nil {
			return "", false, fmt.Errorf("trino: external authentication redirect: %w", err)
		}
	}
	token, err = a.pollToken(ctx, client, challenge.tokenURL)
	if err != nil {
		return "", false, err
	}
	a.cache.SetToken(token)
	return token, true, nil
}

type externalAuthChallenge struct {
	tokenURL    *url.URL
	redirectURL *url.URL
}

// parseExternalAuthChallenge reads the challenge Trino sends when external
// authentication is enabled: Bearer x_redirect_server="...", x_token_server="...".
func parseExternalAuthChallenge(header http.Header) (*externalAuthChallenge, error) {
	for _, value := range header.Values("WWW-Authenticate") {
		for _, c := range parseChallenges(value) {
			if !strings.EqualFold(c.scheme, "Bearer") || c.params["x_token_server"] == "" {
				continue
			}
			challenge := &externalAuthChallenge{}
			var err error
			if challenge.tokenURL, err = parseChallengeURL("x_token_server", c.params["x_token_server"]); err != nil {
				return nil, err
			}
			if redirect := c.params["x_redirect_server"]; redirect != "" {
				if challenge.redirectURL, err = parseChallengeURL("x_redirect_server", redirect); err != nil {
					return nil, err
				}
			}
			return challenge, nil
		}
	}
	return nil, nil
}

type authChallenge struct {
	scheme string
	// params has the auth-param names in lower case.
	params map[string]string
}

// parseChallenges splits a WWW-Authenticate field value into its challenges,
// following RFC 9110 section 11.6.1: one value can carry several challenges,
// parameter names are case-insensitive and quoted values can contain commas.
func parseChallenges(value string) []authChallenge {
	var challenges []authChallenge
	p := challengeParser{s: value}
	for p.i < len(p.s) {
		p.skip(" \t,")
		scheme := p.token()
		if scheme == "" {
			// a stray '=' or '"', as in a token68 this driver does not use
			p.i++
			continue
		}
		c := authChallenge{scheme: scheme, params: make(map[string]string)}
		for {
			start := p.i
			p.skip(" \t,")
			name := p.token()
			p.skip(" \t")
			if name == "" || !p.consume('=') {
				// a name without '=' starts the next challenge
				p.i = start
				break
			}
			p.skip(" \t")
			c.params[strings.ToLower(name)] = p.value()
		}
		challenges = append(challenges, c)
	}
	return challenges
}

type challengeParser struct {
	s string
	i int
}

func (p *challengeParser) skip(chars string) {
	for p.i < len(p.s) && strings.IndexByte(chars, p.s[p.i]) >= 0 {
		p.i++
	}
}

func (p *challengeParser) consume(b byte) bool {
	if p.i < len(p.s) && p.s[p.i] == b {
		p.i++
		return true
	}
	return false
}

func (p *challengeParser) token() string {
	start := p.i
	for p.i < len(p.s) && strings.IndexByte(" \t,=\"", p.s[p.i]) < 0 {
		p.i++
	}
	return p.s[start:p.i]
}

// value reads a token or a quoted string, undoing its backslash escapes.
func (p *challengeParser) value() string {
	if !p.consume('"') {
		return p.token()
	}
	var b strings.Builder
	for p.i < len(p.s) && p.s[p.i] != '"' {
		if p.s[p.i] == '\\' && p.i+1 < len(p.s) {
			p.i++
		}
		b.WriteByte(p.s[p.i])
		p.i++
	}
	p.consume('"')
	return b.String()
}

// parseChallengeURL accepts only absolute http and https URLs, since the
// redirect URL is handed to the system's URL opener.
func parseChallengeURL(name, value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("trino: invalid %s: %w", name, err)
	}
	if (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("trino: invalid %s: not an absolute http or https URL", name)
	}
	return u, nil
}

type tokenPollResponse struct {
	Token   string
	NextURI string
	Error   string
}

// pollToken follows the token server until it returns the token, retrying
// unavailable responses and network errors until ctx is done.
func (a *externalAuthenticator) pollToken(ctx context.Context, client *http.Client, uri *url.URL) (string, error) {
	const initialDelay, maxDelay = 100 * time.Millisecond, 500 * time.Millisecond
	delay := initialDelay
	for {
		poll, err := getTokenPoll(ctx, client, uri.String())
		var retryable *retryableError
		if errors.As(err, &retryable) {
			if err := sleep(ctx, delay); err != nil {
				return "", fmt.Errorf("trino: external authentication: %w, last error: %w", err, retryable.err)
			}
			delay = min(delay*2, maxDelay)
			continue
		}
		if err != nil {
			return "", err
		}
		switch {
		case poll.Token != "":
			// Tell the server the token arrived; a failure only delays its cleanup.
			if req, err := http.NewRequestWithContext(ctx, http.MethodDelete, uri.String(), nil); err == nil {
				if resp, err := client.Do(req); err == nil {
					resp.Body.Close()
				}
			}
			return poll.Token, nil
		case poll.Error != "":
			return "", fmt.Errorf("trino: external authentication failed: %s", poll.Error)
		case poll.NextURI != "":
			next, err := parseChallengeURL("token server nextUri", poll.NextURI)
			if err != nil {
				return "", err
			}
			if err := a.checkOrigin("token server nextUri", next); err != nil {
				return "", err
			}
			// Trino holds each poll until the token arrives or 10 seconds
			// pass; the pause keeps a server that answers at once from
			// being polled in a tight loop.
			if err := sleep(ctx, delay); err != nil {
				return "", fmt.Errorf("trino: external authentication: %w", err)
			}
			uri = next
		default:
			return "", errors.New("trino: external authentication failed: empty token server response")
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }

// getTokenPoll reads one token server response. Its errors leave out the URL,
// which identifies the login, and the response body.
func getTokenPoll(ctx context.Context, client *http.Client, uri string) (*tokenPollResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, errors.New("trino: invalid token server URL")
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("trino: external authentication: %w", ctx.Err())
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = fmt.Errorf("%s token server: %w", urlErr.Op, urlErr.Err)
		}
		return nil, &retryableError{err}
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		if mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mediaType != "application/json" {
			return nil, fmt.Errorf("trino: token server returned Content-Type %q, not application/json", resp.Header.Get("Content-Type"))
		}
		poll, err := decodeTokenPoll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("trino: decoding token server response: %w", err)
		}
		return poll, nil
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return nil, &retryableError{fmt.Errorf("token server returned %s", resp.Status)}
	default:
		return nil, fmt.Errorf("trino: token server returned %s", resp.Status)
	}
}

// decodeTokenPoll matches field names exactly, as the Java client does;
// encoding/json would also accept "Token" or "TOKEN".
func decodeTokenPoll(r io.Reader) (*tokenPollResponse, error) {
	var fields map[string]json.RawMessage
	if err := json.NewDecoder(r).Decode(&fields); err != nil {
		return nil, err
	}
	var poll tokenPollResponse
	for name, dst := range map[string]*string{"token": &poll.Token, "nextUri": &poll.NextURI, "error": &poll.Error} {
		if raw, ok := fields[name]; ok {
			if err := json.Unmarshal(raw, dst); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return &poll, nil
}

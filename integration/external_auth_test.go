package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestIntegrationExternalAuthentication(t *testing.T) {
	integrationDSN(t)
	if oauth2Addresses == nil {
		t.Skip("Skipping external authentication test: it needs the Dex container it starts itself, so it runs only with Trino 477 or later and without -trino_server_dsn")
	}
	tlsConfig, err := getTLSConfig(secretsDir)
	require.NoError(t, err)
	var dialer net.Dialer
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if published, ok := oauth2Addresses[address]; ok {
				address = published
			}
			return dialer.DialContext(ctx, network, address)
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	browser := &http.Client{Transport: transport, Jar: jar}

	var redirects atomic.Int32
	connector, err := trino.NewConnector(&trino.Config{
		ServerURI:              "https://trino:8443",
		ExternalAuthentication: true,
		HTTPClient:             &http.Client{Transport: transport},
		RedirectHandler: func(ctx context.Context, redirectURL *url.URL) error {
			redirects.Add(1)
			return logInToDex(ctx, browser, redirectURL)
		},
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	ctx := context.Background()
	first, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, first.Close()) })
	var user string
	require.NoError(t, first.QueryRowContext(ctx, "SELECT current_user").Scan(&user))
	assert.Equal(t, "test", user)

	second, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	require.NoError(t, second.QueryRowContext(ctx, "SELECT current_user").Scan(&user))
	assert.Equal(t, "test", user)
	assert.EqualValues(t, 1, redirects.Load(), "the second connection reuses the token")
}

// logInToDex does what a user does in the browser: follows the redirect to
// the Dex login form and submits it, which sends Dex back to Trino's callback.
func logInToDex(ctx context.Context, browser *http.Client, redirectURL *url.URL) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, redirectURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := browser.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	form := url.Values{"login": {"test@example.com"}, "password": {"password"}}
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, resp.Request.URL.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err = browser.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || resp.Request.URL.Host != "trino:8443" {
		return fmt.Errorf("login ended at %s with %s: %s", resp.Request.URL, resp.Status, body)
	}
	return nil
}

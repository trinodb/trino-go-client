package integration

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

const dexTokenEndpoint = "https://" + DockerDexName + ":5554/dex/token"

func TestIntegrationClientCredentials(t *testing.T) {
	integrationDSN(t)
	if oauth2Addresses == nil {
		t.Skip("Skipping client credentials test: it needs the Dex container it starts itself, so it runs only with Trino 477 or later and without -trino_server_dsn")
	}
	tests := []struct {
		name string
		conf trino.Config
		// minVersion is the first Trino release that advertises the token
		// endpoint the test relies on.
		minVersion int
		wantUser   string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "token endpoint advertised by the server",
			minVersion: 484,
			wantUser:   "go-client",
		},
		{
			name:     "configured token endpoint",
			conf:     trino.Config{OAuth2TokenEndpoint: dexTokenEndpoint},
			wantUser: "go-client",
		},
		{
			name:       "configured token endpoint that the server does not advertise",
			conf:       trino.Config{OAuth2TokenEndpoint: "https://" + DockerDexName + ":5554/dex/other"},
			minVersion: 484,
			wantErr:    "does not match the configured oauth2TokenEndpoint",
		},
		{
			// Trino takes the user from the name claim, which Dex adds only
			// for the profile scope, so the token is rejected.
			name:       "configured scope",
			conf:       trino.Config{OAuth2TokenEndpoint: dexTokenEndpoint, OAuth2Scope: "openid"},
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.minVersion != 0 {
				requireServerVersion(t, tt.minVersion)
			}
			conf := tt.conf
			conf.ServerURI = "https://trino:8443"
			conf.OAuth2ClientID = "trino-go-client"
			conf.OAuth2ClientSecret = "go-client-secret"
			conf.HTTPClient = &http.Client{Transport: newOAuth2Transport(t)}
			connector, err := trino.NewConnector(&conf)
			require.NoError(t, err)
			db := sql.OpenDB(connector)
			t.Cleanup(func() { assert.NoError(t, db.Close()) })

			var user string
			err = db.QueryRowContext(context.Background(), "SELECT current_user").Scan(&user)
			switch {
			case tt.wantErr != "":
				assert.ErrorContains(t, err, tt.wantErr)
			case tt.wantStatus != 0:
				var qf *trino.ErrQueryFailed
				require.ErrorAs(t, err, &qf)
				assert.Equal(t, tt.wantStatus, qf.StatusCode)
				assert.NotContains(t, err.Error(), "failed to obtain OAuth2 client credentials token", "Dex issued the token, Trino rejected it")
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantUser, user)
			}
		})
	}
}

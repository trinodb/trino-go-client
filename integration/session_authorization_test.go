package integration

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The test switches to the DSN's own user, which needs no impersonation rule;
// the server still sends Set-Authorization-User.
func TestIntegrationSessionAuthorization(t *testing.T) {
	dsn := integrationDSN(t)
	// SET SESSION AUTHORIZATION was added in Trino 426.
	requireServerVersion(t, 426)
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	user := parsed.User.Username()
	require.NotEmpty(t, user, "the integration DSN must embed a user")

	db := integrationOpen(t, dsn)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var current string
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_user").Scan(&current))
	require.Equal(t, user, current, "sanity check before the authorization change")

	_, err = conn.ExecContext(ctx, "SET SESSION AUTHORIZATION "+user)
	require.NoError(t, err)

	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_user").Scan(&current))
	assert.Equal(t, user, current, "current_user after SET SESSION AUTHORIZATION")

	_, err = conn.ExecContext(ctx, "RESET SESSION AUTHORIZATION")
	require.NoError(t, err)

	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_user").Scan(&current))
	assert.Equal(t, user, current, "current_user after RESET SESSION AUTHORIZATION")
}

// Trino 484 stopped lowercasing the user named in SET SESSION AUTHORIZATION.
// Switching to the connection's own mixed-case user needs no impersonation
// rule, so the switch only succeeds when the name reaches the server verbatim.
func TestIntegrationSessionAuthorizationKeepsUserCase(t *testing.T) {
	requireServerVersion(t, 484)
	parsed, err := url.Parse(integrationDSN(t))
	require.NoError(t, err)
	const user = "MixedCaseUser"
	parsed.User = url.User(user)

	db := integrationOpen(t, parsed.String())
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "SET SESSION AUTHORIZATION "+user)
	require.NoError(t, err, "a lowercased user name would be denied as impersonation")

	var current string
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT current_user").Scan(&current))
	assert.Equal(t, user, current, "current_user after SET SESSION AUTHORIZATION")
}

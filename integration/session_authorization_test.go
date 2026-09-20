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

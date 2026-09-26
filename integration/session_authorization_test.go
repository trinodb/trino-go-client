package integration

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SET SESSION AUTHORIZATION only needs the target user to equal the caller's
// own identity to succeed without an impersonation rule in the server's
// system access control, since it skips the check when the two match. This
// test switches to the DSN's own user and back.
//
// It does not prove the identity actually swapped underneath a different
// name: the fake-coordinator tests already cover that header round trip in
// full, including a different X-Trino-User. What this test proves instead
// is the capability announcement and an error-free round trip against a
// real server, since the coordinator sends Set-Authorization-User even when
// the target user equals the original: SetSessionAuthorizationTask calls
// setSetAuthorizationUser unconditionally, without comparing identities.
func TestIntegrationSessionAuthorization(t *testing.T) {
	dsn := integrationDSN(t)
	// SET SESSION AUTHORIZATION shipped in Trino 426 (release note #16067);
	// older servers reject it as a syntax error before the driver's
	// SESSION_AUTHORIZATION capability announcement is even considered.
	requireServerVersion(t, 426)
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	user := parsed.User.Username()
	require.NotEmpty(t, user, "the integration DSN must embed a user")

	db := integrationOpen(t, dsn)
	ctx := context.Background()

	// SET SESSION AUTHORIZATION is connection state: database/sql calls
	// ResetSession when it hands a pooled connection to the next Query or
	// Exec, which reverts the authorization user, so the test pins one
	// connection for its whole duration.
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

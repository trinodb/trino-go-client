package trino

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientCapabilitiesIncludeSessionAuthorization(t *testing.T) {
	t.Parallel()
	assert.Contains(t, strings.Split(clientCapabilities, commaSeparator), "SESSION_AUTHORIZATION")
}

// userDSN returns the fake's DSN with user; params must start with "?" when
// not empty.
func userDSN(fc *fakeCoordinator, user, params string) string {
	return strings.Replace(fc.url(), "://", "://"+user+"@", 1) + params
}

func newConnWithUser(t testing.TB, fc *fakeCoordinator, user, params string) *Conn {
	t.Helper()
	c, err := newConn(userDSN(fc, user, params))
	require.NoError(t, err)
	return c
}

func TestSetAuthorizationUserHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	c := newConnWithUser(t, fc, "alice", "?roles=catalog%3Arole1")
	ctx := context.Background()

	fc.respond(
		statementPage().
			withHeader(trinoSetAuthorizationUserHeader, "bob").
			withHeader(trinoSetOriginalRolesHeader, "ROLE{admin}").
			withHeader(trinoSetOriginalRolesHeader, "ALL"),
		emptyPage(),
	)
	require.NoError(t, c.execInternal(ctx, "SET SESSION AUTHORIZATION bob"))

	fc.respond(statementPage(), emptyPage())
	require.NoError(t, c.execInternal(ctx, "SELECT current_user"))

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "alice", requests[0].header.Get(trinoUserHeader), "the configured user should be sent before the authorization change")
	assert.Equal(t, "catalog=ROLE{role1}", requests[0].header.Get(trinoRoleHeader), "the configured roles should be sent before the authorization change")

	assert.Equal(t, "bob", requests[2].header.Get(trinoUserHeader), "the authorization user should replace X-Trino-User on the next statement")
	assert.Equal(t, "alice", requests[2].header.Get(trinoOriginalUserHeader), "the configured user should be sent as the original identity")
	assert.ElementsMatch(t, []string{"ROLE{admin}", "ALL"}, requests[2].header.Values(trinoOriginalRolesHeader), "every Set-Original-Roles value should be sent")
	assert.Empty(t, requests[2].header.Values(trinoRoleHeader), "roles belonged to the original identity and should not be sent")
}

func TestResetAuthorizationUserHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	c := newConnWithUser(t, fc, "alice", "?roles=catalog%3Arole1")
	ctx := context.Background()

	fc.respond(
		statementPage().
			withHeader(trinoSetAuthorizationUserHeader, "bob").
			withHeader(trinoSetOriginalRolesHeader, "ALL"),
		emptyPage(),
	)
	require.NoError(t, c.execInternal(ctx, "SET SESSION AUTHORIZATION bob"))

	fc.respond(
		statementPage().withHeader(trinoResetAuthorizationUserHeader, "true"),
		emptyPage(),
	)
	require.NoError(t, c.execInternal(ctx, "RESET SESSION AUTHORIZATION"))
	assert.Empty(t, c.authorizationUser, "a stale authorization user would make a later ResetSession overwrite roles")

	fc.respond(statementPage(), emptyPage())
	require.NoError(t, c.execInternal(ctx, "SELECT current_user"))

	requests := fc.capturedRequests()
	require.Len(t, requests, 6)
	assert.Equal(t, "bob", requests[2].header.Get(trinoUserHeader), "the authorization user should still be in effect when RESET SESSION AUTHORIZATION is sent")

	assert.Equal(t, "alice", requests[4].header.Get(trinoUserHeader), "the configured user should be restored on the statement after RESET SESSION AUTHORIZATION")
	assert.Empty(t, requests[4].header.Values(trinoOriginalUserHeader), "the original identity header should be dropped")
	assert.Empty(t, requests[4].header.Values(trinoOriginalRolesHeader), "the original roles header should be dropped")
	assert.Equal(t, "catalog=ROLE{role1}", requests[4].header.Get(trinoRoleHeader), "the roles in effect before the change should be restored")
}

func TestResetRestoresRolesInEffectBeforeChange(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://alice@localhost?roles=catalog%3Arole1")
	require.NoError(t, err)

	c.applyResponseHeaders(http.Header{trinoSetRoleHeader: []string{"catalog=ROLE%7Brole2%7D"}})
	selected := c.httpHeaderValue(trinoRoleHeader)
	require.NotEqual(t, c.configuredRoles, selected, "sanity check: SET ROLE changed the live roles")

	c.applyResponseHeaders(http.Header{trinoSetAuthorizationUserHeader: []string{"bob"}})
	c.applyResponseHeaders(http.Header{trinoSetAuthorizationUserHeader: []string{"carol"}})
	assert.Empty(t, c.httpHeaderValues(trinoRoleHeader))

	c.applyResponseHeaders(http.Header{trinoResetAuthorizationUserHeader: []string{"true"}})
	assert.Equal(t, selected, c.httpHeaderValue(trinoRoleHeader), "the role selected before the first change should come back, not the DSN's")
	assert.Equal(t, "alice", c.httpHeaderValue(trinoUserHeader))
}

func TestResetWithoutAuthorizationChangeKeepsRoles(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://alice@localhost?roles=catalog%3Arole1")
	require.NoError(t, err)

	c.applyResponseHeaders(http.Header{trinoSetRoleHeader: []string{"catalog=ROLE%7Brole2%7D"}})
	selected := c.httpHeaderValue(trinoRoleHeader)

	c.applyResponseHeaders(http.Header{trinoResetAuthorizationUserHeader: []string{"true"}})
	assert.Equal(t, selected, c.httpHeaderValue(trinoRoleHeader))
	assert.Equal(t, "alice", c.httpHeaderValue(trinoUserHeader))
}

func TestResetSessionClearsAuthorizationAndRoles(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://alice@localhost?roles=catalog%3Arole1")
	require.NoError(t, err)

	c.applyResponseHeaders(http.Header{
		trinoSetAuthorizationUserHeader: []string{"bob"},
		trinoSetOriginalRolesHeader:     []string{"ALL"},
	})
	c.setHTTPHeader(trinoRoleHeader, "hive=ROLE{admin}")

	require.NoError(t, c.ResetSession(context.Background()))

	assert.Equal(t, "alice", c.httpHeaderValue(trinoUserHeader), "the configured user should be restored")
	assert.Empty(t, c.httpHeaderValues(trinoOriginalUserHeader))
	assert.Empty(t, c.httpHeaderValues(trinoOriginalRolesHeader))
	assert.Equal(t, "catalog=ROLE{role1}", c.httpHeaderValue(trinoRoleHeader))
	assert.Empty(t, c.authorizationUser)

	c.applyResponseHeaders(http.Header{trinoSetRoleHeader: []string{"catalog=ROLE%7Brole2%7D"}})
	require.NoError(t, c.ResetSession(context.Background()))
	assert.Equal(t, "catalog=ROLE{role1}", c.httpHeaderValue(trinoRoleHeader), "a role selected without an authorization change should not survive either")
}

func TestResetSessionRemovesUserHeaderWithoutConfiguredUser(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://localhost")
	require.NoError(t, err)
	require.Empty(t, c.configuredUser, "sanity check: the DSN has no user")

	c.applyResponseHeaders(http.Header{
		trinoSetAuthorizationUserHeader: []string{"bob"},
	})
	require.Equal(t, "bob", c.httpHeaderValue(trinoUserHeader))

	require.NoError(t, c.ResetSession(context.Background()))

	assert.Empty(t, c.httpHeaderValues(trinoUserHeader), "X-Trino-User should be removed, not left set to the authorization user")
	assert.Empty(t, c.httpHeaderValues(trinoOriginalUserHeader))
}

func TestSetOriginalRolesCopiedVerbatim(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://alice@localhost")
	require.NoError(t, err)

	values := []string{"ROLE{a+b}", "ROLE{%0D%0A}", "ROLE{admin}%"}
	c.applyResponseHeaders(http.Header{
		trinoSetAuthorizationUserHeader: []string{"bob"},
		trinoSetOriginalRolesHeader:     values,
	})

	assert.Equal(t, values, c.httpHeaderValues(trinoOriginalRolesHeader))
}

func TestPinnedConnKeepsAuthorizationUser(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db, err := sql.Open("trino", userDSN(fc, "alice", ""))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	fc.respond(statementPage().withHeader(trinoSetAuthorizationUserHeader, "bob"), emptyPage())
	_, err = conn.ExecContext(ctx, "SET SESSION AUTHORIZATION bob")
	require.NoError(t, err)

	fc.respond(statementPage(), emptyPage())
	_, err = conn.ExecContext(ctx, "SELECT current_user")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "bob", requests[2].header.Get(trinoUserHeader))
	assert.Equal(t, "alice", requests[2].header.Get(trinoOriginalUserHeader))
}

func TestPooledExecDoesNotLeakAuthorizationUser(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db, err := sql.Open("trino", userDSN(fc, "alice", ""))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)

	fc.respond(statementPage().withHeader(trinoSetAuthorizationUserHeader, "bob"), emptyPage())
	_, err = db.Exec("SET SESSION AUTHORIZATION bob")
	require.NoError(t, err)

	fc.respond(statementPage(), emptyPage())
	_, err = db.Exec("SELECT current_user")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "alice", requests[2].header.Get(trinoUserHeader))
	assert.Empty(t, requests[2].header.Values(trinoOriginalUserHeader))
}

func failedPage() page {
	return pageOf(&queryResponse{
		ID:    fakeQueryID,
		Error: ErrTrino{ErrorName: "GENERIC_INTERNAL_ERROR", Message: "failed after the header arrived"},
	})
}

func TestFailedStatementRevertsAuthorizationChange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  func(ctx context.Context, conn *sql.Conn) error
	}{
		{
			name: "exec",
			run: func(ctx context.Context, conn *sql.Conn) error {
				_, err := conn.ExecContext(ctx, "SET SESSION AUTHORIZATION bob")
				return err
			},
		},
		{
			name: "query",
			run: func(ctx context.Context, conn *sql.Conn) error {
				rows, err := conn.QueryContext(ctx, "SET SESSION AUTHORIZATION bob")
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
				}
				return rows.Err()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			db, err := sql.Open("trino", userDSN(fc, "alice", "?roles=catalog%3Arole1"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			ctx := context.Background()

			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			fc.respond(
				statementPage().withHeader(trinoSetAuthorizationUserHeader, "bob"),
				resultPage([][]any{{1}}),
				failedPage(),
			)
			require.Error(t, tc.run(ctx, conn))

			fc.respond(statementPage(), emptyPage())
			_, err = conn.ExecContext(ctx, "SELECT current_user")
			require.NoError(t, err)

			requests := fc.capturedRequests()
			last := requests[len(requests)-2]
			assert.Equal(t, "alice", last.header.Get(trinoUserHeader))
			assert.Empty(t, last.header.Values(trinoOriginalUserHeader))
			assert.Equal(t, "catalog=ROLE{role1}", last.header.Get(trinoRoleHeader))
		})
	}
}

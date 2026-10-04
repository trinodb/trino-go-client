package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreshConnectionIsValid(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "")
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Raw(func(driverConn any) error {
		assert.True(t, driverConn.(driver.Validator).IsValid())
		return nil
	}))
}

func TestPoolKeepsHealthyConnection(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "")

	first := pooledDriverConn(t, db, "SELECT 1")
	second := pooledDriverConn(t, db, "SELECT 1")

	assert.Same(t, first, second)
	assert.Equal(t, 1, db.Stats().OpenConnections)
}

// The nextUri names another host, whose service ticket the credential cache
// lacks and the unreachable KDC cannot issue, so the statement reaches the
// server and fails afterwards.
func TestKerberosFailureAfterStatementInvalidatesConnection(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.onStatement(func(w http.ResponseWriter, r *http.Request, query string) {
		nextURI := strings.Replace(fc.url(), "127.0.0.1", "localhost", 1) + "/v1/statement/" + fakeQueryID + "/1"
		assert.NoError(t, json.NewEncoder(w).Encode(&stmtResponse{ID: fakeQueryID, NextURI: nextURI}))
	})
	krb5Files := newKerberosTestFiles(t, "alice", "trino/127.0.0.1")
	db := openKerberos(t, fc, Config{
		KerberosConfigPath:          krb5Files.config,
		KerberosCredentialCachePath: krb5Files.credentialCache,
	})
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	_, err = conn.QueryContext(context.Background(), "SELECT 1")

	require.ErrorContains(t, err, "SPNEGO")
	assert.NotErrorIs(t, err, driver.ErrBadConn, "the server has seen the statement, so it must not be retried")
	require.NoError(t, conn.Raw(func(driverConn any) error {
		trinoConn := driverConn.(*Conn)
		assert.False(t, trinoConn.IsValid())
		assert.ErrorIs(t, trinoConn.ResetSession(context.Background()), driver.ErrBadConn)
		return nil
	}))
	require.NoError(t, conn.Close())
	assert.Equal(t, 0, db.Stats().OpenConnections, "the pool should drop the invalid connection")
}

// The pooled connection loaded a credential cache without a ticket for the
// server. Once the cache is rewritten, as kinit does, the statement is
// retried on a new connection that reloads it.
func TestKerberosFailureBeforeStatementRetriesOnNewConnection(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	krb5Files := newKerberosTestFiles(t, "alice")
	db := openKerberos(t, fc, Config{
		KerberosConfigPath:          krb5Files.config,
		KerberosCredentialCachePath: krb5Files.credentialCache,
	})
	stale, err := db.Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, stale.Close())
	writeCredentialCache(t, krb5Files.credentialCache, "alice", time.Now().Add(time.Hour), "krbtgt/"+testRealm, "trino/127.0.0.1")

	rows, err := db.Query("SELECT 1")

	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Len(t, fc.capturedRequests(), 2, "the stale connection should not have sent the statement")
	assert.Equal(t, 1, db.Stats().OpenConnections)
}

func TestKerberosFailureBeforeStatementKeepsCause(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	krb5Files := newKerberosTestFiles(t, "alice")
	db := openKerberos(t, fc, Config{
		KerberosConfigPath:          krb5Files.config,
		KerberosCredentialCachePath: krb5Files.credentialCache,
	})

	_, err := db.Query("SELECT 1")

	require.ErrorIs(t, err, driver.ErrBadConn)
	assert.ErrorContains(t, err, "SPNEGO")
	assert.Empty(t, fc.capturedRequests())
	assert.Equal(t, 0, db.Stats().OpenConnections)
}

// pooledDriverConn runs query on a connection from the pool and returns its
// driver connection; the connection goes back to the pool afterwards.
func pooledDriverConn(t *testing.T, db *sql.DB, query string) *Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	rows, err := conn.QueryContext(context.Background(), query)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	var driverConn *Conn
	require.NoError(t, conn.Raw(func(dc any) error {
		driverConn = dc.(*Conn)
		return nil
	}))
	return driverConn
}

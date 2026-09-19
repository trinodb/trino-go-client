package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationTransactionCommit(t *testing.T) {
	db := integrationOpen(t)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	var count int
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM system.runtime.nodes").Scan(&count))
	assert.Positive(t, count)

	require.NoError(t, tx.Commit())
}

func TestIntegrationTransactionRollback(t *testing.T) {
	db := integrationOpen(t)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	var count int
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM system.runtime.nodes").Scan(&count))

	require.NoError(t, tx.Rollback())
}

func TestIntegrationTransactionIsolationLevels(t *testing.T) {
	db := integrationOpen(t)

	for _, level := range []sql.IsolationLevel{
		sql.LevelDefault,
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	} {
		t.Run(level.String(), func(t *testing.T) {
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: level})
			require.NoError(t, err)

			var one int
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT 1").Scan(&one))
			assert.Equal(t, 1, one)

			require.NoError(t, tx.Commit())
		})
	}
}

// Rejecting the write also proves the transaction is tracked end to end: the
// server can only know the statement is read-only because the client echoed
// back the ID of the transaction it opened with READ ONLY.
func TestIntegrationTransactionReadOnly(t *testing.T) {
	db := integrationOpen(t)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })

	var one int
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)

	_, err = tx.ExecContext(ctx, "CREATE TABLE memory.default.read_only_check (id integer)")
	assert.ErrorContains(t, err, "Cannot execute write in a read-only transaction")
}

// Raw transaction control cannot work through a pool, because database/sql is
// free to run each statement on a different connection and only the one that
// opened the transaction carries its ID. The server rejects the attempt rather
// than letting a statement silently run outside the transaction (#181).
//
// What earns the rejection is the absence of the transaction header, which the
// driver sends only between BeginTx and Commit or Rollback: the same statement
// is accepted when the header is present, even as NONE. The rejection is
// therefore load-bearing, and this test guards it.
func TestIntegrationRawTransactionStatementsRejected(t *testing.T) {
	db := integrationOpen(t)

	cases := []struct {
		query   string
		wantErr string
	}{
		{query: "START TRANSACTION", wantErr: `trino: query failed (200 OK): "USER_ERROR: Client does not support transactions"`},
		{query: "COMMIT", wantErr: `trino: query failed (200 OK): "USER_ERROR: No transaction in progress"`},
		{query: "ROLLBACK", wantErr: `trino: query failed (200 OK): "USER_ERROR: No transaction in progress"`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			_, err := db.Exec(tc.query)

			assert.EqualError(t, err, tc.wantErr)
		})
	}
}

// Finishing a transaction must clear the header rather than leave it at NONE,
// which is the value that makes the server accept START TRANSACTION. A
// recycled connection that kept it would let raw transaction control through
// the pool start working, silently, with statements landing outside the
// transaction they belong to.
func TestIntegrationRawTransactionStatementsRejectedAfterTransaction(t *testing.T) {
	db := integrationOpen(t)
	// reuse the very connection the transaction ran on
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = db.Exec("START TRANSACTION")

	assert.EqualError(t, err, `trino: query failed (200 OK): "USER_ERROR: Client does not support transactions"`)
}

// A connection released by a finished transaction must go back to autocommit,
// otherwise later queries would reference a transaction that no longer exists.
func TestIntegrationTransactionReleasesConnection(t *testing.T) {
	db := integrationOpen(t)
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)
}

const hiveTransactionSchema = "hive.trino_go_client_transactions"

// hiveTransactionDB opens a connection that can create and write Hive tables.
// The memory connector only supports writes in autocommit mode, so Hive is the
// only catalog configured here that can show what a transaction does to writes.
// The admin role it needs is also what makes the server refuse ROLLBACK on an
// aborted transaction, which one of these tests covers.
func hiveTransactionDB(t *testing.T) *sql.DB {
	t.Helper()
	requireServerVersion(t, 458)
	db := integrationOpen(t, integrationDSN(t)+"?roles=hive:admin")

	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + hiveTransactionSchema)
	require.NoError(t, err, "Failed creating the schema")
	return db
}

func hiveTransactionTable(t *testing.T, db *sql.DB) string {
	t.Helper()
	table := uniqueTable(t, db, hiveTransactionSchema)
	_, err := db.Exec("CREATE TABLE " + table + " (id integer)")
	require.NoError(t, err, "Failed creating the table")
	return table
}

func hiveTransactionRowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
	return count
}

// Rolling back must discard a write that already succeeded. Committing the same
// insert afterwards is the control: an empty table on its own would also be
// explained by the write never happening, so the second half proves the write
// does persist when it is not rolled back.
func TestIntegrationTransactionRollbackDiscardsWrite(t *testing.T) {
	ctx := context.Background()
	db := hiveTransactionDB(t)
	table := hiveTransactionTable(t, db)

	rolledBack, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = rolledBack.ExecContext(ctx, "INSERT INTO "+table+" VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, rolledBack.Rollback())

	assert.Equal(t, 0, hiveTransactionRowCount(t, db, table))

	committed, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = committed.ExecContext(ctx, "INSERT INTO "+table+" VALUES (2)")
	require.NoError(t, err)
	require.NoError(t, committed.Commit())

	assert.Equal(t, 1, hiveTransactionRowCount(t, db, table),
		"the same insert persists when committed, so the rollback is what discarded the first row")
}

// Writes made in a transaction stay invisible to other connections until the
// commit, and then all of them appear. Two tables are used because Hive
// refuses a second insert into the same unpartitioned table in one transaction.
func TestIntegrationTransactionCommitAppliesAllWrites(t *testing.T) {
	ctx := context.Background()
	db := hiveTransactionDB(t)
	orders := hiveTransactionTable(t, db)
	audit := hiveTransactionTable(t, db)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+orders+" VALUES (1)")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+audit+" VALUES (1)")
	require.NoError(t, err)

	assert.Equal(t, 0, hiveTransactionRowCount(t, db, orders), "the write must not be visible before the commit")
	assert.Equal(t, 0, hiveTransactionRowCount(t, db, audit), "the write must not be visible before the commit")

	require.NoError(t, tx.Commit())

	assert.Equal(t, 1, hiveTransactionRowCount(t, db, orders))
	assert.Equal(t, 1, hiveTransactionRowCount(t, db, audit))
}

// The server aborts the transaction itself when a statement in it fails, and
// then refuses ROLLBACK because this session carries a catalog-scoped role.
// Rollback still has to report success, and the earlier write must be gone.
func TestIntegrationTransactionRollbackAfterFailedStatement(t *testing.T) {
	ctx := context.Background()
	db := hiveTransactionDB(t)
	orders := hiveTransactionTable(t, db)
	audit := hiveTransactionTable(t, db)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+orders+" VALUES (1)")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+audit+" VALUES ('not an integer')")
	require.Error(t, err)

	require.NoError(t, tx.Rollback())

	assert.Equal(t, 0, hiveTransactionRowCount(t, db, orders))
	assert.Equal(t, 0, hiveTransactionRowCount(t, db, audit))
}

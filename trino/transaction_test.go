package trino

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postedStatement is a statement the driver sent, together with the
// transaction the client claimed it belonged to.
type postedStatement struct {
	query         string
	transactionID string
}

// postedStatements reads the statements out of the fake's request log.
func postedStatements(fc *fakeCoordinator) []postedStatement {
	var posted []postedStatement
	for _, request := range fc.capturedRequests() {
		if request.method != http.MethodPost || request.path != "/v1/statement" {
			continue
		}
		posted = append(posted, postedStatement{
			query:         string(request.body),
			transactionID: request.header.Get(trinoTransactionHeader),
		})
	}
	return posted
}

// newTransactionCoordinator implements enough of Trino's transaction protocol
// to exercise the client: it opens a transaction only for a client that
// announces support by sending the transaction header, and hands back an ID
// the client is expected to echo on later statements. A statement containing
// failOn fails and, like the server, aborts the transaction it belonged to.
// Any other statement is answered with the pages set by respond, if there are
// any.
func newTransactionCoordinator(t testing.TB, failOn string) *fakeCoordinator {
	t.Helper()
	fc := newFakeCoordinator(t)
	var (
		mu      sync.Mutex
		opened  int
		aborted = map[string]bool{}
	)
	fc.onStatement(func(w http.ResponseWriter, r *http.Request, query string) {
		transactionID := r.Header.Get(trinoTransactionHeader)
		inTransaction := transactionID != "" && transactionID != noTransactionID

		mu.Lock()
		wasAborted := aborted[transactionID]
		failing := failOn != "" && strings.Contains(query, failOn)
		if failing && inTransaction {
			aborted[transactionID] = true
		}
		mu.Unlock()

		switch {
		// Once a statement in a transaction has failed, Trino aborts the
		// transaction and refuses everything else in it, ROLLBACK included.
		case wasAborted:
			writeQueryError(fc, w, trinoErrorTransactionAlreadyAborted,
				"Current transaction is aborted, commands ignored until end of transaction block")
		case failing:
			writeQueryError(fc, w, "TYPE_MISMATCH", "Insert query has mismatched column types")
		case strings.HasPrefix(query, "START TRANSACTION"):
			if transactionID == "" {
				writeQueryError(fc, w, "INCOMPATIBLE_CLIENT", "Client does not support transactions")
				return
			}
			mu.Lock()
			opened++
			started := fmt.Sprintf("txn-%d", opened)
			mu.Unlock()
			w.Header().Set(trinoStartedTransactionHeader, started)
			writeQueryAccepted(fc, w)
		case query == "COMMIT", query == "ROLLBACK":
			if !inTransaction {
				writeQueryError(fc, w, trinoErrorNotInTransaction, "No transaction in progress")
				return
			}
			w.Header().Set(trinoClearTransactionHeader, "true")
			writeQueryAccepted(fc, w)
		case fc.hasPages():
			fc.servePage(w, r, 0)
		default:
			writeQueryAccepted(fc, w)
		}
	})
	return fc
}

func writeQueryAccepted(fc *fakeCoordinator, w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	fc.writeJSON(w, &stmtResponse{ID: fakeQueryID})
}

func writeQueryError(fc *fakeCoordinator, w http.ResponseWriter, name, message string) {
	w.WriteHeader(http.StatusOK)
	fc.writeJSON(w, &stmtResponse{
		ID: fakeQueryID,
		Error: ErrTrino{
			ErrorName: name,
			ErrorType: "USER_ERROR",
			Message:   message,
		},
	})
}

func TestTransactionCommit(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")
	db := fc.open(t, "")

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO orders VALUES (1)")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO order_audit VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	assert.Equal(t, []postedStatement{
		{query: "START TRANSACTION", transactionID: noTransactionID},
		{query: "INSERT INTO orders VALUES (1)", transactionID: "txn-1"},
		{query: "INSERT INTO order_audit VALUES (1)", transactionID: "txn-1"},
		{query: "COMMIT", transactionID: "txn-1"},
	}, postedStatements(fc))
}

// Trino aborts a transaction as soon as a statement in it fails, and then
// refuses ROLLBACK. Rolling back is still what the caller asked for, so it must
// not surface an error, and the connection must stay usable.
func TestTransactionRollbackAfterFailedStatement(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "INSERT")
	db := fc.open(t, "")
	db.SetMaxOpenConns(1)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO orders VALUES (1)")
	require.Error(t, err, "the statement itself must still report its failure")

	require.NoError(t, tx.Rollback())

	_, err = db.Exec("SELECT 1")
	require.NoError(t, err, "the connection must be reusable after the rollback")

	posted := postedStatements(fc)
	require.Len(t, posted, 4)
	assert.Equal(t, postedStatement{query: "ROLLBACK", transactionID: "txn-1"}, posted[2])
	assert.Equal(t, postedStatement{query: "SELECT 1", transactionID: ""}, posted[3])
}

// Commit must not hide an aborted transaction: the caller has to learn that the
// writes did not land.
func TestTransactionCommitAfterFailedStatementFails(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "INSERT")
	db := fc.open(t, "")

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO orders VALUES (1)")
	require.Error(t, err)

	assert.ErrorContains(t, tx.Commit(), "Current transaction is aborted")
}

// A connection returning to the pool must not carry the finished transaction,
// or every later query on it would fail against a transaction that is gone.
func TestTransactionClearedAfterCommit(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")
	db := fc.open(t, "")
	db.SetMaxOpenConns(1)

	tx, err := db.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	posted := postedStatements(fc)
	require.Len(t, posted, 3)
	assert.Equal(t, postedStatement{query: "SELECT 1", transactionID: ""}, posted[2])
}

// Closing rows before the last page cancels the query, and Trino aborts the
// transaction a cancelled query belonged to, so a QueryRow over a result that
// spans several pages leaves the transaction unable to commit. The driver
// cancels like the JDBC client does; the README tells users to read to the end.
func TestTransactionQueryRowOverSeveralPagesCancelsQuery(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")
	fc.respond(
		statementPage(),
		resultPage([][]any{{1}}),
		resultPage([][]any{{2}}),
		emptyPage(),
	)
	db := fc.open(t, "")

	tx, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	var value int
	require.NoError(t, tx.QueryRow("SELECT orderkey FROM lineitem").Scan(&value))
	assert.Equal(t, 1, value)

	var cancelled []capturedRequest
	for _, request := range fc.capturedRequests() {
		if request.method == http.MethodDelete && request.path == "/v1/query/"+fakeQueryID {
			cancelled = append(cancelled, request)
		}
	}
	require.Len(t, cancelled, 1, "closing the row before the last page must cancel the query")
	assert.Equal(t, "txn-1", cancelled[0].header.Get(trinoTransactionHeader))
}

type transactionContextKey struct{}

// statementContextTransport records, for every statement it sends, the
// transactionContextKey value of the request's context.
type statementContextTransport struct {
	mu     sync.Mutex
	values map[string]any
}

func (s *statementContextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost && req.URL.Path == "/v1/statement" {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		query, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.values[string(query)] = req.Context().Value(transactionContextKey{})
		s.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(req)
}

func (s *statementContextTransport) value(query string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[query]
}

// Values on the BeginTx context, such as trace spans, must reach the requests
// that end the transaction, just like they reach the statements inside it.
func TestTransactionFinishUsesBeginTxContextValues(t *testing.T) {
	t.Parallel()
	for _, finish := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(finish, func(t *testing.T) {
			t.Parallel()
			fc := newTransactionCoordinator(t, "")
			transport := &statementContextTransport{values: map[string]any{}}
			connector, err := NewConnector(&Config{ServerURI: fc.url(), HTTPClient: &http.Client{Transport: transport}})
			require.NoError(t, err)
			db := sql.OpenDB(connector)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			ctx := context.WithValue(context.Background(), transactionContextKey{}, "trace-1")
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			if finish == "COMMIT" {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}

			assert.Equal(t, "trace-1", transport.value("START TRANSACTION"))
			assert.Equal(t, "trace-1", transport.value(finish))
		})
	}
}

// database/sql rolls the transaction back when the BeginTx context is
// cancelled, and that ROLLBACK must not inherit the cancellation.
func TestTransactionRolledBackWhenBeginTxContextCancelled(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")
	db := fc.open(t, "")

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	cancel()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		posted := postedStatements(fc)
		assert.Contains(c, posted, postedStatement{query: "ROLLBACK", transactionID: "txn-1"})
	}, 5*time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, tx.Commit(), sql.ErrTxDone)
}

func TestTransactionIsolationLevels(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts sql.TxOptions
		want string
	}{
		{
			name: "default",
			opts: sql.TxOptions{},
			want: "START TRANSACTION",
		},
		{
			name: "read uncommitted",
			opts: sql.TxOptions{Isolation: sql.LevelReadUncommitted},
			want: "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
		},
		{
			name: "read committed",
			opts: sql.TxOptions{Isolation: sql.LevelReadCommitted},
			want: "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
		},
		{
			name: "repeatable read",
			opts: sql.TxOptions{Isolation: sql.LevelRepeatableRead},
			want: "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		},
		{
			name: "serializable",
			opts: sql.TxOptions{Isolation: sql.LevelSerializable},
			want: "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		},
		{
			name: "read only",
			opts: sql.TxOptions{ReadOnly: true},
			want: "START TRANSACTION READ ONLY",
		},
		{
			name: "isolation level and read only",
			opts: sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true},
			want: "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newTransactionCoordinator(t, "")
			db := fc.open(t, "")

			tx, err := db.BeginTx(context.Background(), &tc.opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, tx.Rollback()) })

			posted := postedStatements(fc)
			require.NotEmpty(t, posted)
			assert.Equal(t, tc.want, posted[0].query)
		})
	}
}

func TestTransactionUnsupportedIsolationLevel(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")
	db := fc.open(t, "")

	_, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelLinearizable})
	assert.ErrorContains(t, err, "unsupported transaction isolation level")
	assert.Empty(t, postedStatements(fc), "no statement should reach the server")
}

// A failed START TRANSACTION must leave the connection in autocommit mode.
func TestTransactionStartFailure(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.onStatement(func(w http.ResponseWriter, r *http.Request, query string) {
		writeQueryError(fc, w, "INCOMPATIBLE_CLIENT", "Client does not support transactions")
	})

	c, err := newConn(fc.url())
	require.NoError(t, err)

	_, err = c.Begin()
	assert.ErrorContains(t, err, "Client does not support transactions")
	assert.Empty(t, c.httpHeaderValue(trinoTransactionHeader))
}

func TestTransactionNestedNotSupported(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")

	c, err := newConn(fc.url())
	require.NoError(t, err)

	_, err = c.Begin()
	require.NoError(t, err)

	_, err = c.Begin()
	assert.ErrorIs(t, err, ErrTransactionInProgress)
}

func TestResetSessionClearsTransaction(t *testing.T) {
	t.Parallel()
	fc := newTransactionCoordinator(t, "")

	c, err := newConn(fc.url())
	require.NoError(t, err)

	_, err = c.Begin()
	require.NoError(t, err)
	require.Equal(t, "txn-1", c.transactionID())

	require.NoError(t, c.ResetSession(context.Background()))
	assert.Empty(t, c.transactionID())
}

// Without a transaction open the driver must not announce one to the server;
// the header is what makes a coordinator accept START TRANSACTION, and
// database/sql is free to run each statement on a different pooled connection.
func TestNoTransactionHeaderSentInAutocommit(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	for _, request := range fc.capturedRequests() {
		assert.Empty(t, request.header.Values(trinoTransactionHeader), "%s %s", request.method, request.path)
	}
}

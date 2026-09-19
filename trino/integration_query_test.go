package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integration tests based on python tests:
// https://github.com/trinodb/trino-python-client/tree/master/integration_tests

type nodesRow struct {
	NodeID      string
	HTTPURI     string
	NodeVersion string
	Coordinator bool
	State       string
}

func TestIntegrationSelectQueryIterator(t *testing.T) {
	db := integrationOpen(t)
	rows, err := db.Query("SELECT * FROM system.runtime.nodes")
	require.NoError(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col nodesRow
		err = rows.Scan(
			&col.NodeID,
			&col.HTTPURI,
			&col.NodeVersion,
			&col.Coordinator,
			&col.State,
		)
		require.NoError(t, err)
		assert.Equal(t, "test", col.NodeID, "node_id")
	}
	require.NoError(t, rows.Err())
	assert.GreaterOrEqual(t, count, 1, "no rows returned")
}

func TestIntegrationSelectQueryNoResult(t *testing.T) {
	db := integrationOpen(t)
	row := db.QueryRow("SELECT * FROM system.runtime.nodes where false")
	var col nodesRow
	err := row.Scan(
		&col.NodeID,
		&col.HTTPURI,
		&col.NodeVersion,
		&col.Coordinator,
		&col.State,
	)
	require.Error(t, err, "unexpected query returning data: %+v", col)
}

func TestIntegrationSelectFailedQuery(t *testing.T) {
	db := integrationOpen(t)
	rows, err := db.Query("SELECT * FROM catalog.schema.do_not_exist")
	if err == nil {
		rows.Close()
	}
	require.Error(t, err, "query to invalid catalog succeeded")
	var queryFailed *ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	var trinoErr *ErrTrino
	require.ErrorAs(t, err, &trinoErr)
	expected := ErrTrino{
		Message:   "line 1:15: Catalog 'catalog'",
		SqlState:  "",
		ErrorCode: 44,
		ErrorName: "CATALOG_NOT_FOUND",
		ErrorType: "USER_ERROR",
		ErrorLocation: ErrorLocation{
			LineNumber:   1,
			ColumnNumber: 15,
		},
		FailureInfo: FailureInfo{
			Type:    "io.trino.spi.TrinoException",
			Message: "line 1:15: Catalog 'catalog'",
		},
	}
	assert.True(t, strings.HasPrefix(trinoErr.Message, expected.Message), "expected ErrTrino.Message to start with `%s`, got: %s", expected.Message, trinoErr.Message)
	assert.Equal(t, expected.SqlState, trinoErr.SqlState, "ErrTrino.SqlState")
	assert.Equal(t, expected.ErrorCode, trinoErr.ErrorCode, "ErrTrino.ErrorCode")
	assert.Equal(t, expected.ErrorName, trinoErr.ErrorName, "ErrTrino.ErrorName")
	assert.Equal(t, expected.ErrorType, trinoErr.ErrorType, "ErrTrino.ErrorType")
	assert.Equal(t, expected.ErrorLocation, trinoErr.ErrorLocation, "ErrTrino.ErrorLocation")
	assert.Equal(t, expected.FailureInfo.Type, trinoErr.FailureInfo.Type, "ErrTrino.FailureInfo.Type")
	assert.True(t, strings.HasPrefix(trinoErr.FailureInfo.Message, expected.FailureInfo.Message), "expected ErrTrino.FailureInfo.Message to start with `%s`, got: %s", expected.FailureInfo.Message, trinoErr.FailureInfo.Message)
}

type tpchRow struct {
	CustKey    int
	Name       string
	Address    string
	NationKey  int
	Phone      string
	AcctBal    float64
	MktSegment string
	Comment    string
}

func TestIntegrationSelectCancelQuery(t *testing.T) {
	db := integrationOpen(t)
	deadline := time.Now().Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM tpch.sf1.customer")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var col tpchRow
			err = rows.Scan(
				&col.CustKey,
				&col.Name,
				&col.Address,
				&col.NationKey,
				&col.Phone,
				&col.AcctBal,
				&col.MktSegment,
				&col.Comment,
			)
			if err != nil {
				break
			}
		}
		err = rows.Err()
		require.Error(t, err, "unexpected query with deadline succeeded")
	}
	errmsg := err.Error()
	assert.True(t, strings.Contains(errmsg, "cancel") || strings.Contains(errmsg, "deadline"), "unexpected error: %v", err)
}

func TestIntegrationSessionProperties(t *testing.T) {
	dsn := integrationDSN(t)
	dsn += "?session_properties=query_max_run_time%3A10m%3Bquery_priority%3A2"
	db := integrationOpen(t, dsn)
	rows, err := db.Query("SHOW SESSION")
	require.NoError(t, err)
	for rows.Next() {
		col := struct {
			Name        string
			Value       string
			Default     string
			Type        string
			Description string
		}{}
		err = rows.Scan(
			&col.Name,
			&col.Value,
			&col.Default,
			&col.Type,
			&col.Description,
		)
		require.NoError(t, err)
		switch col.Name {
		case "query_max_run_time":
			assert.Equal(t, "10m", col.Value, "query_max_run_time")
		case "query_priority":
			assert.Equal(t, "2", col.Value, "query_priority")
		}
	}
	require.NoError(t, rows.Err())
}

func TestIntegrationNoResults(t *testing.T) {
	db := integrationOpen(t)
	rows, err := db.Query("SELECT 1 LIMIT 0")
	require.NoError(t, err)
	require.False(t, rows.Next(), "Rows returned")
	require.NoError(t, rows.Err())
}

func TestIntegrationQueryParametersSelect(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		args     []interface{}
		wantRows int
		wantErr  string
	}{
		{
			name:     "valid string as varchar",
			query:    "SELECT * FROM system.runtime.nodes WHERE system.runtime.nodes.node_id=?",
			args:     []interface{}{"test"},
			wantRows: 1,
		},
		{
			name:     "valid int as bigint",
			query:    "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:     []interface{}{int(1)},
			wantRows: 1,
		},
		{
			name:    "invalid string as bigint",
			query:   "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:    []interface{}{"1"},
			wantErr: `trino: query failed (200 OK): "USER_ERROR: line 1:46: Cannot apply operator: bigint = varchar(1)"`,
		},
		{
			name:    "valid string as date",
			query:   "SELECT * FROM tpch.sf1.lineitem WHERE shipdate=? LIMIT 2",
			args:    []interface{}{"1995-01-27"},
			wantErr: `trino: query failed (200 OK): "USER_ERROR: line 1:47: Cannot apply operator: date = varchar(10)"`,
		},
	}

	db := integrationOpen(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.query, tc.args...)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tc.wantRows, count, "row count")
		})
	}
}

func TestIntegrationQueryNextAfterClose(t *testing.T) {
	// NOTE: This is testing invalid behaviour. It ensures that we don't
	// panic if we call driverRows.Next after we closed the driverStmt.

	ctx := context.Background()
	conn, err := (&Driver{}).Open(integrationDSN(t))
	require.NoError(t, err, "Failed to open connection")
	defer conn.Close()

	stmt, err := conn.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	require.NoError(t, err, "Failed preparing query")

	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, []driver.NamedValue{})
	require.NoError(t, err, "Failed running query")
	defer rows.Close()

	stmt.Close() // NOTE: the important bit.

	// the direct protocol still returns the buffered row, the spooling
	// protocol has nothing left; neither may fail with anything but EOF
	var result driver.Value
	if err := rows.Next([]driver.Value{result}); err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.ErrorIs(t, rows.Next([]driver.Value{result}), io.EOF)
}

func TestIntegrationExec(t *testing.T) {
	db := integrationOpen(t)

	_, err := db.Query(`SELECT count(*) FROM nation`)
	require.ErrorContains(t, err, "Schema must be specified when session schema is not set")

	result, err := db.Exec("USE tpch.sf100")
	require.NoError(t, err, "Failed executing query")
	require.NotNil(t, result, "Expected exec result to be not nil")

	a, err := result.RowsAffected()
	require.NoError(t, err, "Expected RowsAffected not to return any error")
	assert.Equal(t, int64(0), a, "RowsAffected")
	rows, err := db.Query(`SELECT count(*) FROM nation`)
	require.NoError(t, err, "Failed executing query")
	require.True(t, rows.Next(), "Failed fetching results: %v", rows.Err())
}

func TestIntegrationQueryContext(t *testing.T) {
	tests := []struct {
		name           string
		timeout        time.Duration
		expectedErrMsg string
	}{
		{
			name:           "Context Cancellation",
			timeout:        0,
			expectedErrMsg: "canceled",
		},
		{
			name:           "Context Deadline Exceeded",
			timeout:        time.Second,
			expectedErrMsg: "context deadline exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := "cancel-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
			db := integrationOpen(t, integrationDSN(t)+"?catalog=tpch&schema=sf100&source="+source+"&custom_client="+uncompressedClient)
			var ctx context.Context
			var cancel context.CancelFunc

			if tt.timeout == 0 {
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			errCh := make(chan error, 1)
			done := make(chan struct{})
			longQuery := "SELECT COUNT(*) FROM lineitem"

			go func() {
				// query will complete in ~7s unless cancelled
				rows, err := db.QueryContext(ctx, longQuery)
				if err != nil {
					errCh <- err
					return
				}
				defer rows.Close()

				rows.Next()
				if err = rows.Err(); err != nil {
					errCh <- err
					return
				}
				close(done)
			}()

			queryID := findRunningQuery(t, db, source, longQuery)
			if tt.timeout == 0 {
				cancel()
			}

			select {
			case <-done:
				require.Fail(t, "unexpected query succeeded despite cancellation or deadline")
			case err := <-errCh:
				require.ErrorContains(t, err, tt.expectedErrMsg)
			}
			requireQueryCancelled(t, db, queryID)
		})
	}
}

// Closing a prepared statement must not leave the statement behind on the
// connection: a later EXECUTE by name has nothing to run.
func TestIntegrationPreparedStatementScopedToStatement(t *testing.T) {
	db := integrationOpen(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	stmt, err := conn.PrepareContext(ctx, "SELECT ?")
	require.NoError(t, err)
	var value int
	require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&value))
	assert.Equal(t, 1, value)
	require.NoError(t, stmt.Close())

	err = conn.QueryRowContext(ctx, "EXECUTE "+preparedStatementName+" USING 1").Scan(&value)

	require.ErrorContains(t, err, "Prepared statement not found: "+preparedStatementName)
}

func TestIntegrationLargeQuery(t *testing.T) {
	requireServerVersion(t, 418)
	dsn := integrationDSN(t)
	dsn += "?explicitPrepare=false"
	db := integrationOpen(t, dsn)
	rows, err := db.Query("SELECT ?, '"+strings.Repeat("a", 5000000)+"'", 42)
	require.NoError(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count, "row count")
}

func TestQueryForUsername(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

	rows, err := db.Query("SELECT current_user", sql.Named("X-Trino-User", string("TestUser")))
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	for rows.Next() {
		var user string
		require.NoError(t, rows.Scan(&user), "Failed scanning query result")

		assert.Equal(t, "TestUser", user, "Expected value does not equal result value")
	}
}

type TestQueryProgressCallback struct {
	progressMap map[time.Time]float64
	statusMap   map[time.Time]string
}

func (qpc *TestQueryProgressCallback) Update(qpi QueryProgressInfo) {
	if qpc.progressMap == nil {
		qpc.progressMap = map[time.Time]float64{}
		qpc.statusMap = map[time.Time]string{}
	}
	qpc.progressMap[time.Now()] = float64(qpi.QueryStats.ProgressPercentage)
	qpc.statusMap[time.Now()] = qpi.QueryStats.State
}

func TestQueryProgressWithCallback(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

	callback := &TestQueryProgressCallback{}

	_, err = db.Query("SELECT 2", sql.Named("X-Trino-Progress-Callback", callback))
	assert.EqualError(t, err, ErrInvalidProgressCallbackHeader.Error(), "unexpected error")
}

func TestQueryProgressWithCallbackPeriod(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

	progressMap := make(map[time.Time]float64)
	statusMap := make(map[time.Time]string)
	progressUpdater := &TestQueryProgressCallback{
		progressMap: progressMap,
		statusMap:   statusMap,
	}
	progressUpdaterPeriod, err := time.ParseDuration("1ms")
	require.NoError(t, err)

	rows, err := db.Query("SELECT 2",
		sql.Named("X-Trino-Progress-Callback", progressUpdater),
		sql.Named("X-Trino-Progress-Callback-Period", progressUpdaterPeriod),
	)
	require.NoError(t, err, "Failed executing query")
	assert.NotNil(t, rows)

	for rows.Next() {
		var ts string
		require.NoError(t, rows.Scan(&ts), "Failed scanning query result")

		assert.Equal(t, "2", ts, "Expected value does not equal result value")
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// sort time in order to calculate interval
	assert.NotEmpty(t, progressMap)
	assert.NotEmpty(t, statusMap)
	var keys []time.Time
	for k := range statusMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Before(keys[j])
	})

	for i, k := range keys {
		if i > 0 {
			assert.GreaterOrEqual(t, k.Sub(keys[i-1]), progressUpdaterPeriod)
		}
		assert.GreaterOrEqual(t, progressMap[k], 0.0)
	}
}

func TestSession(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t) + "?custom_client=" + uncompressedClient,
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)

	_, err = db.Exec("SET SESSION join_distribution_type='BROADCAST'")
	require.NoError(t, err, "Failed executing query")

	row := db.QueryRow("SHOW SESSION LIKE 'join_distribution_type'")
	var name string
	var value string
	var defaultValue string
	var typeName string
	var description string
	err = row.Scan(&name, &value, &defaultValue, &typeName, &description)
	require.NoError(t, err, "Failed executing query")

	assert.Equal(t, "BROADCAST", value)

	_, err = db.Exec("RESET SESSION join_distribution_type")
	require.NoError(t, err, "Failed executing query")

	row = db.QueryRow("SHOW SESSION LIKE 'join_distribution_type'")
	err = row.Scan(&name, &value, &defaultValue, &typeName, &description)
	require.NoError(t, err, "Failed executing query")

	assert.Equal(t, "AUTOMATIC", value)
}

func TestExec(t *testing.T) {
	c := &Config{
		ServerURI:         integrationDSN(t),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	db := integrationOpen(t, dsn)
	table := uniqueTable(t, db, "memory.default")

	_, err = db.Exec("CREATE TABLE " + table + " (id INTEGER, name VARCHAR, optional VARCHAR)")
	require.NoError(t, err, "Failed executing CREATE TABLE query")

	result, err := db.Exec("INSERT INTO "+table+" (id, name, optional) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		123, "abc", nil,
		456, "def", "present",
		789, "ghi", nil)
	require.NoError(t, err, "Failed executing INSERT query")
	_, err = result.LastInsertId()
	assert.ErrorIs(t, err, ErrOperationNotSupported)
	numRows, err := result.RowsAffected()
	require.NoError(t, err, "Failed checking rows affected")
	assert.Equal(t, int64(3), numRows)

	rows, err := db.Query("SELECT * FROM " + table)
	require.NoError(t, err, "Failed executing SELECT query")

	expectedIds := []int{123, 456, 789}
	expectedNames := []string{"abc", "def", "ghi"}
	expectedOptionals := []sql.NullString{
		sql.NullString{Valid: false},
		sql.NullString{String: "present", Valid: true},
		sql.NullString{Valid: false},
	}
	actualIds := []int{}
	actualNames := []string{}
	actualOptionals := []sql.NullString{}
	for rows.Next() {
		var id int
		var name string
		var optional sql.NullString
		require.NoError(t, rows.Scan(&id, &name, &optional), "Failed scanning query result")
		actualIds = append(actualIds, id)
		actualNames = append(actualNames, name)
		actualOptionals = append(actualOptionals, optional)

	}
	assert.Equal(t, expectedIds, actualIds)
	assert.Equal(t, expectedNames, actualNames)
	assert.Equal(t, expectedOptionals, actualOptionals)
}

// uniqueTable returns a table name no other run uses, and drops the table
// when the test ends, so a failed run does not break the next one.
func uniqueTable(t testing.TB, db *sql.DB, schema string) string {
	t.Helper()
	table := fmt.Sprintf("%s.test_%d", schema, time.Now().UnixNano())
	t.Cleanup(func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + table)
		require.NoError(t, err, "Failed dropping %s", table)
	})
	return table
}

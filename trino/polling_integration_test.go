package trino

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Polling client integration tests

func TestIntegrationPollingSelectQueryNoResult(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := pollingConn.StartQuery(ctx, "SELECT * FROM system.runtime.nodes WHERE false")
	require.NoError(t, err)

	results, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Empty(t, results.rows)
}

func TestIntegrationPollingSelectFailedQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := pollingConn.StartQuery(ctx, "SELECT * FROM catalog.schema.do_not_exist")

	// Poll until we get an error or query finishes
	for err == nil && result != nil && !result.Finished {
		result, err = pollingConn.PollQuery(ctx, result.NextURI)
	}

	require.Error(t, err, "Query to invalid catalog should fail")

	queryFailed, ok := err.(*ErrQueryFailed)
	require.True(t, ok, "Expected ErrQueryFailed, got: %T", err)

	trinoErr, ok := errors.Unwrap(queryFailed).(*ErrTrino)
	require.True(t, ok, "Expected ErrTrino, got: %T", errors.Unwrap(queryFailed))
	assert.Equal(t, "CATALOG_NOT_FOUND", trinoErr.ErrorName)
}

func TestIntegrationPollingSelectTpch1000(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := pollingConn.StartQuery(ctx, "SELECT * FROM tpch.sf1.customer LIMIT 1000")
	require.NoError(t, err)
	assert.NotEmpty(t, result.QueryID)

	results, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Len(t, results.rows, 1000)
	assert.Len(t, results.colTypeNames, 8)

	expectedColumns := []string{"custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"}
	assert.Equal(t, expectedColumns, results.colNames)

	expectedColumnTypes := []*PollingColumnType{
		{name: "custkey", databaseType: "BIGINT"},
		{name: "name", databaseType: "VARCHAR", hasLength: true, length: 25},
		{name: "address", databaseType: "VARCHAR", hasLength: true, length: 40},
		{name: "nationkey", databaseType: "BIGINT"},
		{name: "phone", databaseType: "VARCHAR", hasLength: true, length: 15},
		{name: "acctbal", databaseType: "DOUBLE"},
		{name: "mktsegment", databaseType: "VARCHAR", hasLength: true, length: 10},
		{name: "comment", databaseType: "VARCHAR", hasLength: true, length: 117},
	}

	// compare everything but the scanType, which is a pointer to a reflect.Type
	for i := range expectedColumnTypes {
		actualType := results.colTypes[i]
		actualType.scanType = nil
		assert.Equal(t, expectedColumnTypes[i], actualType)
	}
}

func TestIntegrationPollingCancelQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	// Use triple CROSS JOIN to create a very large result set that takes time to execute
	longQuery := "SELECT COUNT(*) FROM tpch.sf1.customer c1 CROSS JOIN tpch.sf1.customer c2 CROSS JOIN tpch.sf1.customer c3"

	result, err := pollingConn.StartQuery(ctx, longQuery)
	require.NoError(t, err)
	assert.NotEmpty(t, result.QueryID)
	assert.NotEmpty(t, result.NextURI)

	t.Logf("Query %s started, cancelling immediately", result.QueryID)

	err = pollingConn.CancelQuery(ctx, result.NextURI)
	require.NoError(t, err)

	t.Logf("Cancel request sent successfully for query %s", result.QueryID)

	// Try to poll the cancelled query - it should either return with an error or show as finished
	pollResult, pollErr := pollingConn.PollQuery(ctx, result.NextURI)
	if pollErr != nil {
		t.Logf("Polling after cancel returned error (expected): %v", pollErr)
		return
	}

	assert.True(t, pollResult.Finished, "Expected query to be finished after cancellation")
	t.Logf("Query %s successfully cancelled", result.QueryID)
}

func TestIntegrationPollingCancelQueryAfterCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := pollingConn.StartQuery(ctx, "SELECT 1")
	require.NoError(t, err)

	nextURI := result.NextURI

	for !result.Finished {
		result, err = pollingConn.PollQuery(ctx, result.NextURI)
		require.NoError(t, err)
		if result.NextURI != "" {
			nextURI = result.NextURI
		}
	}

	err = pollingConn.CancelQuery(ctx, nextURI)
	assert.NoError(t, err, "Cancelling completed query should not error")
}

func TestIntegrationPollingQueryWithNoResults(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{"USE statement", "USE tpch.sf1"},
		{"SELECT with false condition", "SELECT 1 WHERE false"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := pollingConn.StartQuery(ctx, tc.query)
			require.NoError(t, err)

			results, err := collectAllResults(ctx, pollingConn, result)
			require.NoError(t, err)
			assert.Empty(t, results.rows, "Expected no data rows for query: %s", tc.query)
		})
	}
}

func TestIntegrationPollingQueryWithParameters(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()

	testCases := []struct {
		name     string
		query    string
		param    interface{}
		expected [][]interface{}
	}{
		{
			name:     "system table filter",
			query:    "SELECT node_id FROM system.runtime.nodes WHERE node_id = ?",
			param:    "test",
			expected: [][]interface{}{{"test"}},
		},
		{
			name:     "sequence with numeric parameter",
			query:    "SELECT * FROM UNNEST(sequence(1, 10)) AS t(n) WHERE n = ?",
			param:    2,
			expected: [][]interface{}{{int64(2)}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := pollingConn.StartQuery(
				ctx,
				tc.query,
				tc.param,
			)
			require.NoError(t, err)

			results, err := collectAllResults(ctx, pollingConn, result)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, results.rows)
		})
	}
}

func TestIntegrationPollingQueryWithNamedArgs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Test using sql.Named for X-Trino headers
	result, err := pollingConn.StartQuery(
		ctx,
		"SELECT 1",
		sql.Named("X-Trino-User", "test-user"),
		sql.Named("X-Trino-Source", "test-source"),
	)
	require.NoError(t, err)

	results, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Len(t, results.rows, 1)
	assert.Equal(t, [][]interface{}{{int64(1)}}, results.rows)
}

// Tests comparing sync (standard SQL) vs polling client results

func TestIntegrationSyncAndPollingReturnSimilarFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag
	query := `
		SELECT n as col1, CAST(n as varchar) as col2
		FROM UNNEST(sequence(1, 3)) as t(n)
	`

	t.Run("Sync", func(t *testing.T) {
		results := executeSync(t, dsn, query)
		assert.Equal(t, results.colNames, []string{"col1", "col2"})
		assert.Equal(t, results.colTypeNames, []string{"BIGINT", "VARCHAR"})
		assert.Equal(t, results.rows, [][]interface{}{{int64(1), "1"}, {int64(2), "2"}, {int64(3), "3"}})
	})

	t.Run("Polling", func(t *testing.T) {
		results := executePolling(t, dsn, query)
		assert.Equal(t, results.colNames, []string{"col1", "col2"})
		assert.Equal(t, results.colTypeNames, []string{"BIGINT", "VARCHAR"})
		assert.Equal(t, results.rows, [][]interface{}{{int64(1), "1"}, {int64(2), "2"}, {int64(3), "3"}})
	})

	t.Run("Polling with Spooling", func(t *testing.T) {
		if !spoolingProtocolSupported {
			t.Skip("Skipping test when spooling protocol is not supported.")
		}

		// Use a query with >1000 rows to trigger spooling protocol
		spoolingQuery := `
			SELECT n as col1, CAST(n as varchar) as col2
			FROM UNNEST(sequence(1, 1001)) as t(n)
		`

		results := executePolling(t, dsn, spoolingQuery)

		assert.Equal(t, results.protocol, spooled)
		assert.Equal(t, results.colNames, []string{"col1", "col2"})
		assert.Equal(t, results.colTypeNames, []string{"BIGINT", "VARCHAR"})
		assert.Equal(t, 1001, len(results.rows))
		assert.Equal(t, []interface{}{int64(1), "1"}, results.rows[0])
		assert.Equal(t, []interface{}{int64(1001), "1001"}, results.rows[1000])
	})
}

func TestIntegrationCompareSyncAndPollingResults(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	dsn := *integrationServerFlag

	testCases := []struct {
		name  string
		query string
		args  []interface{}
	}{
		{
			name:  "ARRAY of integers and strings",
			query: `SELECT ARRAY[1, 2, 3] AS col1, ARRAY['a', 'b', 'c'] AS col2`,
		},
		{
			name:  "Query with parameters",
			query: `SELECT ? AS col1, ? AS col2`,
			args:  []interface{}{1, "a"},
		},
		{
			name:  "EXPLAIN query",
			query: `EXPLAIN SELECT 1`,
		},
		{
			name:  "Nested ARRAY",
			query: `SELECT ARRAY[ARRAY[ARRAY[1.4]]]`,
		},
		{
			name:  "MAP with nested values",
			query: `SELECT MAP(ARRAY['foo'], ARRAY[MAP(ARRAY['key1'], ARRAY[CAST(1 AS INTEGER)])]) as col`,
		},
		{
			name:  "ROW with mixed types",
			query: `SELECT ROW(1, 5.12, 'foo', false, NULL)`,
		},
		{
			name:  "ROW with DECIMAL and NULL",
			query: `SELECT ROW(CAST(5 AS INTEGER), CAST(1 AS DECIMAL(10, 2)), CAST(NULL AS VARCHAR))`,
		},
		{
			name:  "ARRAY of REAL",
			query: `SELECT ARRAY[CAST(2.3 AS REAL)]`,
		},
		{
			name:  "ARRAY of DOUBLE",
			query: `SELECT ARRAY[CAST(2.3 AS DOUBLE)]`,
		},
		{
			name:  "ARRAY of TINYINT",
			query: `SELECT ARRAY[CAST(2.3 AS TINYINT)]`,
		},
		{
			name:  "ARRAY of INTEGER",
			query: `SELECT ARRAY[CAST(2.3 AS INTEGER)]`,
		},
		{
			name:  "ARRAY of BIGINT",
			query: `SELECT ARRAY[CAST(2.3 AS BIGINT)]`,
		},
		{
			name:  "ARRAY of SMALLINT",
			query: `SELECT ARRAY[CAST(2.3 AS SMALLINT)]`,
		},
		{
			name:  "SELECT from sequence with filter",
			query: `SELECT n FROM UNNEST(sequence(1, 100)) as t(n) WHERE n <= 5`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sync := executeSync(t, dsn, tc.query, tc.args...)
			poll := executePolling(t, dsn, tc.query, tc.args...)

			assert.Equal(t, sync.colNames, poll.colNames, "Column names should match")
			assert.Equal(t, sync.colTypeNames, poll.colTypeNames, "Column types should match")
			assert.Equal(t, sync.rows, poll.rows, "Rows should match")
		})
	}
}

type protocol string

const (
	spooled protocol = "spooled"
	direct  protocol = "direct"
)

type pollResults struct {
	colNames     []string
	colTypeNames []string
	colTypes     []*PollingColumnType
	rows         [][]interface{}
	protocol     protocol
}

type syncResults struct {
	colNames     []string
	colTypeNames []string
	colTypes     []*sql.ColumnType
	rows         [][]interface{}
}

// Execute a sync query using standard SQL interface and return its results
func executeSync(
	t *testing.T,
	dsn string,
	query string,
	args ...interface{},
) *syncResults {
	db, err := sql.Open("trino", dsn)
	require.NoError(t, err)
	defer db.Close()

	sqlRows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer sqlRows.Close()

	results := syncResults{}

	// Get column names
	results.colNames, err = sqlRows.Columns()
	require.NoError(t, err)

	// Get column types
	results.colTypes, err = sqlRows.ColumnTypes()
	require.NoError(t, err)
	for _, ct := range results.colTypes {
		results.colTypeNames = append(results.colTypeNames, ct.DatabaseTypeName())
	}

	// Collect rows
	results.rows = make([][]interface{}, 0)
	for sqlRows.Next() {
		row := make([]interface{}, len(results.colNames))
		dest := make([]interface{}, len(results.colNames))
		for i := range dest {
			dest[i] = &row[i]
		}
		err := sqlRows.Scan(dest...)
		require.NoError(t, err)
		results.rows = append(results.rows, row)
	}
	require.NoError(t, sqlRows.Err())

	return &results
}

// Execute a polling query until completion and return all results
func executePolling(
	t *testing.T,
	dsn string,
	query string,
	args ...interface{},
) *pollResults {
	pollingConn, err := NewPollingConn(dsn, nil)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := pollingConn.StartQuery(ctx, query, args...)
	require.NoError(t, err)

	results, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)

	return results
}

// collectAllResults is a helper that polls until the query finishes and collects all results.
func collectAllResults(ctx context.Context, pc *PollingConn, result *PollingResult) (*pollResults, error) {
	var err error

	results := pollResults{}

	for !result.Finished {
		if result.Rows != nil {
			// Capture metadata from first result with rows
			if len(results.colNames) == 0 {
				results.colNames, _ = result.Rows.Columns()

				// Convert column types to string names
				results.colTypes, _ = result.Rows.ColumnTypes()
				for _, ct := range results.colTypes {
					results.colTypeNames = append(results.colTypeNames, ct.DatabaseTypeName())
				}

				if _, ok := result.Rows.(*spoolingPollingRows); ok {
					results.protocol = spooled
				} else {
					results.protocol = direct
				}
			}

			rows, err := scanAllRows(result.Rows)
			if err != nil {
				return nil, err
			}
			results.rows = append(results.rows, rows...)
		}

		result, err = pc.PollQuery(ctx, result.NextURI)
		if err != nil {
			return nil, err
		}
	}

	return &results, nil
}

// scanAllRows scans all rows from a PollingRows into [][]interface{}
func scanAllRows(ri PollingRows) ([][]interface{}, error) {
	cols, _ := ri.Columns()
	var rows [][]interface{}

	for ri.Next() {
		row := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range dest {
			dest[i] = &row[i]
		}
		if err := ri.Scan(dest...); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, ri.Err()
}

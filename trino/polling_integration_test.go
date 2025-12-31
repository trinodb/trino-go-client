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

	rows, _, _, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Empty(t, rows)
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

	rows, columns, columnTypes, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Len(t, rows, 1000)
	assert.Len(t, columnTypes, 8)

	expectedColumns := []string{"custkey", "name", "address", "nationkey", "phone", "acctbal", "mktsegment", "comment"}
	assert.Equal(t, expectedColumns, columns)

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
		actualType := columnTypes[i]
		actualType.scanType = nil
		assert.Equal(t, expectedColumnTypes[i], actualType)
	}
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

			rows, _, _, err := collectAllResults(ctx, pollingConn, result)
			require.NoError(t, err)
			assert.Empty(t, rows, "Expected no data rows for query: %s", tc.query)
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

			rows, _, _, err := collectAllResults(ctx, pollingConn, result)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, rows)
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

	rows, _, _, err := collectAllResults(ctx, pollingConn, result)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, [][]interface{}{{int64(1)}}, rows)
}

// collectAllResults is a helper that polls until the query finishes and collects all results.
func collectAllResults(ctx context.Context, pc *PollingConn, result *PollingResult) ([][]interface{}, []string, []*PollingColumnType, error) {
	var err error
	var allRows [][]interface{}
	var columns []string
	var columnTypes []*PollingColumnType

	for !result.Finished {
		if result.Rows != nil {
			// Capture metadata from first result with rows
			if len(columns) == 0 {
				columns, _ = result.Rows.Columns()
				columnTypes, _ = result.Rows.ColumnTypes()
			}

			rows, err := scanAllRows(result.Rows)
			if err != nil {
				return nil, nil, nil, err
			}
			allRows = append(allRows, rows...)
		}

		result, err = pc.PollQuery(ctx, result.NextURI)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return allRows, columns, columnTypes, nil
}

// scanAllRows scans all rows from a PollingRows into [][]interface{}
func scanAllRows(pr *PollingRows) ([][]interface{}, error) {
	cols, _ := pr.Columns()
	var rows [][]interface{}

	for pr.Next() {
		row := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range dest {
			dest[i] = &row[i]
		}
		if err := pr.Scan(dest...); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, pr.Err()
}

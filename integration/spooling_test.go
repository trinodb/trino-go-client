package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationSelectTpchCustomer(t *testing.T) {
	db := integrationOpen(t)
	cases := []struct {
		name     string
		limit    int
		encoding string
	}{
		{name: "direct protocol", limit: 1000},
		// 1001 rows exceed protocol.spooling.inlining.max-rows (1000), so the
		// server returns spooled segments; 100 rows stay inline.
		{name: "spooled segments json+zstd", limit: 1001, encoding: "json+zstd"},
		{name: "spooled segments json", limit: 1001, encoding: "json"},
		{name: "spooled segments json+lz4", limit: 1001, encoding: "json+lz4"},
		{name: "inline segments json+zstd", limit: 100, encoding: "json+zstd"},
		{name: "inline segments json+lz4", limit: 100, encoding: "json+lz4"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var args []any
			if tc.encoding != "" {
				if !spoolingProtocolSupported {
					t.Skip("Skipping test when spooling protocol is not supported.")
				}
				args = append(args, sql.Named("encoding", tc.encoding))
			}
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM tpch.sf1.customer LIMIT %d", tc.limit), args...)
			require.NoError(t, err, "Query failed")
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
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
				require.NoError(t, err, "Row scan failed")
			}

			require.NoError(t, rows.Err(), "Rows iteration error")
			assert.Equal(t, tc.limit, count, "row count")
		})
	}
}

func TestSpoolingIntegrationOrderedResults(t *testing.T) {
	if !spoolingProtocolSupported {
		t.Skip("Skipping test when spooling protocol is not supported.")
	}
	db := integrationOpen(t)

	// The container caps spooled segments at 256kB, so this result spans
	// several segments that four workers download concurrently.
	const rowCount = 200_000
	query := `
		SELECT *
		FROM TABLE(sequence(
			start => 1,
			stop => ` + strconv.Itoa(rowCount) + `
		))
		ORDER BY sequential_number
	`

	rows, err := db.Query(query,
		sql.Named("encoding", "json"),
		sql.Named("spooling_worker_count", "4"),
		sql.Named("max_out_of_order_segments", "8"))
	require.NoError(t, err, "Query failed")
	defer rows.Close()

	expected := 1
	var actual int

	for rows.Next() {
		err = rows.Scan(&actual)
		require.NoError(t, err, "Row scan failed")

		if actual != expected {
			require.Failf(t, "Unexpected number", "at position %d: got %d, expected %d", expected, actual, expected)
		}
		expected++
	}

	require.NoError(t, rows.Err(), "Rows iteration error")
	assert.Equal(t, rowCount, expected-1, "row count")
}

// Cancelling a query while its spooled segments are still being produced
// must stop the client and cancel the query on the server.
func TestIntegrationCancelSpooledQuery(t *testing.T) {
	if !spoolingProtocolSupported {
		t.Skip("Skipping test when spooling protocol is not supported.")
	}
	source := "cancel-spooled-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	// The global sort must finish before the first row streams back, which
	// can take longer than the default query_timeout on a busy CI runner, so
	// this DSN gets its own generous one instead of the flag's default.
	db := integrationOpen(t, integrationDSN(t)+"?source="+source+"&query_timeout=1m")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	query := "SELECT * FROM TABLE(sequence(start => 1, stop => 50000000)) ORDER BY sequential_number"
	rows, err := db.QueryContext(ctx, query, sql.Named("encoding", "json"))
	require.NoError(t, err)
	require.True(t, rows.Next(), "no first row: %v", rows.Err())
	queryID := findRunningQuery(t, db, source, query)

	cancel()

	closed := make(chan error, 1)
	go func() { closed <- rows.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "closing the rows of a cancelled spooled query hangs")
	}
	requireQueryCancelled(t, db, queryID)
}

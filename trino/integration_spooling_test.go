package trino

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"

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
				args = append(args, sql.Named(trinoEncoding, tc.encoding))
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
		sql.Named(trinoEncoding, "json"),
		sql.Named(trinoSpoolingWorkerCount, "4"),
		sql.Named(trinoMaxOutOfOrdersSegments, "8"))
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

package trino

import (
	"database/sql"
	"net/http"
	"testing"
	"time"
)

func TestSpoolingWorkersHigherThenAllowedOutOfOrderSegments(t *testing.T) {
	if !spoolingProtocolSupported {
		t.Skip("Skipping test when spooling protocol is not supported.")
	}
	db := integrationOpen(t)
	defer db.Close()

	expectedError := "spooling worker cannot be greater than max out of order segments allowed. spooling workers: 2, allowed out of order segments: 1"
	_, err := db.Query("SELECT 1",
		sql.Named(trinoEncoding, "json"),
		sql.Named(trinoSpoolingWorkerCount, "2"),
		sql.Named(trinoMaxOutOfOrdersSegments, "1"))

	if err == nil || err.Error() != expectedError {
		t.Fatal("unexpected error:", err)
	}
}

func TestIntegrationTypeConversionSpoolingProtocolInlineJsonEncoder(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	var (
		goTime            time.Time
		nullTime          NullTime
		goString          string
		nullString        sql.NullString
		nullStringSlice   NullSliceString
		nullStringSlice2  NullSlice2String
		nullStringSlice3  NullSlice3String
		nullInt64Slice    NullSliceInt64
		nullInt64Slice2   NullSlice2Int64
		nullInt64Slice3   NullSlice3Int64
		nullFloat64Slice  NullSliceFloat64
		nullFloat64Slice2 NullSlice2Float64
		nullFloat64Slice3 NullSlice3Float64
		goMap             map[string]interface{}
		nullMap           NullMap
		goRow             []interface{}
	)
	err = db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY['A', 'B', NULL],
			ARRAY[ARRAY['A'], NULL],
			ARRAY[ARRAY[ARRAY['A'], NULL], NULL],
			ARRAY[1, 2, NULL],
			ARRAY[ARRAY[1, 1, 1], NULL],
			ARRAY[ARRAY[ARRAY[1, 1, 1], NULL], NULL],
			ARRAY[1.0, 2.0, NULL],
			ARRAY[ARRAY[1.1, 1.1, 1.1], NULL],
			ARRAY[ARRAY[ARRAY[1.1, 1.1, 1.1], NULL], NULL],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER))),
			ROW(1, 'a', CAST('2017-07-10 01:02:03.004 UTC' AS TIMESTAMP(6) WITH TIME ZONE), ARRAY['c'])
	`, sql.Named(trinoEncoding, "json")).Scan(
		&goTime,
		&nullTime,
		&goString,
		&nullString,
		&nullStringSlice,
		&nullStringSlice2,
		&nullStringSlice3,
		&nullInt64Slice,
		&nullInt64Slice2,
		&nullInt64Slice3,
		&nullFloat64Slice,
		&nullFloat64Slice2,
		&nullFloat64Slice3,
		&goMap,
		&nullMap,
		&goRow,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationSelectTpchSpoolingSegments(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		encoding string
		expected int
	}{
		// Testing with a LIMIT of 1001 rows.
		// Since we exceed the `protocol.spooling.inlining.max-rows` threshold (1000),
		// this query trigger spooling protocol with spooled segments.
		{
			name:     "Spooled Segment JSON+ZSTD Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json+zstd",
			expected: 1001,
		},
		{
			name:     "Spooled Segment JSON Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json",
			expected: 1001,
		},
		{
			name:     "Spooled Segment JSON+LZ4 Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json+lz4",
			expected: 1001,
		},
		// Testing with a LIMIT of 100 rows.
		// This should remain inline as it is below the `protocol.spooling.inlining.max-rows` (1000) and bellow `protocol.spooling.inlining.max-size` 128kb
		{
			name:     "Inline Segment JSON+ZSTD Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 100",
			encoding: "json+zstd",
			expected: 100,
		},
		{
			name:     "Inline Segment JSON+LZ4 Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 100",
			encoding: "json+lz4",
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := integrationOpen(t)
			defer db.Close()

			rows, err := db.Query(tt.query, sql.Named(trinoEncoding, tt.encoding))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
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
				if err != nil {
					t.Fatalf("Row scan failed: %v", err)
				}
			}

			if rows.Err() != nil {
				t.Fatalf("Rows iteration error: %v", rows.Err())
			}

			if count != tt.expected {
				t.Fatalf("Expected %d rows, got %d", tt.expected, count)
			}
		})
	}
}

func TestSpoolingIntegrationOrderedResults(t *testing.T) {
	if !spoolingProtocolSupported {
		t.Skip("Skipping test when spooling protocol is not supported.")
	}
	db := integrationOpen(t)
	defer db.Close()

	query := `
		SELECT *
		FROM TABLE(sequence(
			start => 1,
			stop => 5000000
		))
		ORDER BY sequential_number
	`

	rows, err := db.Query(query, sql.Named(trinoEncoding, "json"))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	expected := 1
	var actual int

	for rows.Next() {
		err = rows.Scan(&actual)
		if err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}

		if actual != expected {
			t.Fatalf("Unexpected number at position %d: got %d, expected %d", expected, actual, expected)
		}
		expected++
	}

	if rows.Err() != nil {
		t.Fatalf("Rows iteration error: %v", rows.Err())
	}

	if expected != 5_000_001 {
		t.Fatalf("Expected 5,000,000 rows, got %d", expected-1)
	}
}

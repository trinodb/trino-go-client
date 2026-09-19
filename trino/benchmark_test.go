package trino

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkQuery(b *testing.B) {
	c := &Config{
		ServerURI:         integrationDSN(b),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(b, err)

	db := integrationOpen(b, dsn)

	q := `SELECT * FROM tpch.sf1.orders LIMIT 10000000`
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(q)
		require.NoError(b, err)
		for rows.Next() {
		}
		rows.Close()
	}
}

// BenchmarkSpoolingProtocolSpooledSegmentlJsonZstdDecoderQuery benchmarks the performance of querying a large dataset
// from Trino with JSON encoding and Zstd compression, testing the spooling mechanism. The query retrieves a result set
// of 10 million rows, exceeding the default inline row limit of 1000 (defined by `protocol.spooling.inlining.max-rows`),
// triggering the spooling mechanism to handle the large data efficiently.
//
// **Session properties & headers:**
// - **`encoding: json+zstd`**: Specifies JSON encoding with Zstd compression for the query result.
// - **`protocol.spooling.inlining.max-rows`**: Default is 1000, determining when spooling is triggered to manage large result sets.
func BenchmarkSpoolingProtocolSpooledSegmentlJsonZstdDecoderQuery(b *testing.B) {
	c := &Config{
		ServerURI:         integrationDSN(b),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(b, err)

	db := integrationOpen(b, dsn)

	q := `SELECT * FROM tpch.sf1.orders LIMIT 10000000`
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(q, sql.Named(trinoEncoding, "json+zstd"))
		require.NoError(b, err)
		for rows.Next() {
		}
		rows.Close()
	}
}

// BenchmarkSpoolingProtocolSpooledSegmentJsonLz4DecoderQuery benchmarks the performance of querying a large dataset
// from Trino with JSON encoding and LZ4 compression, testing the spooling mechanism. The query retrieves a result set
// of 10 million rows, exceeding the default inline row limit of 1000 (defined by `protocol.spooling.inlining.max-rows`),
// triggering the spooling mechanism to handle the large data efficiently.
//
// **Session properties & headers:**
// - **`encoding: json+lz4`**: Specifies JSON encoding with LZ4 compression for the query result.
// - **`protocol.spooling.inlining.max-rows`**: Default is 1000, determining when spooling is triggered to manage large result sets.
func BenchmarkSpoolingProtocolSpooledSegmentJsonLz4DecoderQuery(b *testing.B) {
	c := &Config{
		ServerURI:         integrationDSN(b),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(b, err)

	db := integrationOpen(b, dsn)

	q := `SELECT * FROM tpch.sf1.orders LIMIT 10000000`
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(q, sql.Named(trinoEncoding, "json+lz4"))
		require.NoError(b, err)
		for rows.Next() {
		}
		rows.Close()
	}
}

// BenchmarkSpoolingProtocolSpooledSegmentJsonDecoderQuery benchmarks the performance of querying a large dataset
// from Trino with JSON encoding (without compression), testing the spooling mechanism. The query retrieves a result set
// of 10 million rows, exceeding the default inline row limit of 1000 (defined by `protocol.spooling.inlining.max-rows`),
// triggering the spooling mechanism to handle the large data efficiently.
//
// **Session properties & headers:**
// - **`encoding: json`**: Specifies JSON encoding without compression for the query result.
// - **`protocol.spooling.inlining.max-rows`**: Default is 1000, determining when spooling is triggered to manage large result sets
func BenchmarkSpoolingProtocolSpooledSegmentJsonDecoderQuery(b *testing.B) {
	c := &Config{
		ServerURI:         integrationDSN(b),
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(b, err)

	db := integrationOpen(b, dsn)

	q := `SELECT * FROM tpch.sf1.orders LIMIT 10000000`
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(q, sql.Named(trinoEncoding, "json"))
		require.NoError(b, err)
		for rows.Next() {
		}
		rows.Close()
	}
}

package trino

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSNCatalogAndSchemaFromPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		catalog string
		schema  string
	}{
		{name: "no path", dsn: "http://user@localhost:8080"},
		{name: "lone slash", dsn: "http://user@localhost:8080/"},
		{name: "catalog", dsn: "http://user@localhost:8080/tpch", catalog: "tpch"},
		{name: "catalog with trailing slash", dsn: "http://user@localhost:8080/tpch/", catalog: "tpch"},
		{name: "catalog and schema", dsn: "http://user@localhost:8080/tpch/sf1", catalog: "tpch", schema: "sf1"},
		{name: "catalog and schema with trailing slash", dsn: "http://user@localhost:8080/tpch/sf1/", catalog: "tpch", schema: "sf1"},
		{name: "escaped names", dsn: "http://user@localhost:8080/my%20catalog/my%20schema", catalog: "my catalog", schema: "my schema"},
		{name: "parameters only", dsn: "http://user@localhost:8080/?catalog=tpch&schema=sf1", catalog: "tpch", schema: "sf1"},
		{name: "catalog in path, schema parameter", dsn: "http://user@localhost:8080/tpch?schema=sf1", catalog: "tpch", schema: "sf1"},
		{name: "schema parameter without catalog", dsn: "http://user@localhost:8080?schema=sf1", schema: "sf1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseDSN(tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, "http://user@localhost:8080", config.ServerURI, "the path is not part of the server URI")
			assert.Equal(t, tt.catalog, config.Catalog)
			assert.Equal(t, tt.schema, config.Schema)

			formatted, err := config.FormatDSN()
			require.NoError(t, err)
			reparsed, err := ParseDSN(formatted)
			require.NoError(t, err)
			assert.Equal(t, config, reparsed)
		})
	}
}

func TestParseDSNRejectsInvalidPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{
			name:    "extra segments",
			dsn:     "http://user@localhost:8080/tpch/sf1/nation",
			wantErr: `trino: invalid path segments in DSN, expected /catalog or /catalog/schema: "/tpch/sf1/nation"`,
		},
		{
			name:    "double slash",
			dsn:     "http://user@localhost:8080//",
			wantErr: `trino: catalog name is empty in DSN path "//"`,
		},
		{
			name:    "schema without catalog",
			dsn:     "http://user@localhost:8080//sf1",
			wantErr: `trino: catalog name is empty in DSN path "//sf1"`,
		},
		{
			name:    "empty schema",
			dsn:     "http://user@localhost:8080/tpch//",
			wantErr: `trino: schema name is empty in DSN path "/tpch//"`,
		},
		{
			name:    "catalog in path and parameter",
			dsn:     "http://user@localhost:8080/tpch?catalog=tpch",
			wantErr: "trino: catalog is set both in the DSN path and the catalog parameter",
		},
		{
			name:    "empty catalog parameter with catalog in path",
			dsn:     "http://user@localhost:8080/tpch?catalog=",
			wantErr: "trino: catalog is set both in the DSN path and the catalog parameter",
		},
		{
			name:    "schema in path and parameter",
			dsn:     "http://user@localhost:8080/tpch/sf1?schema=sf1",
			wantErr: "trino: schema is set both in the DSN path and the schema parameter",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDSN(tt.dsn)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestDSNPathSetsCatalogAndSchemaHeaders(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage())
	db := fc.open(t, "/tpch/sf1")

	_, err := db.Exec("SELECT 1")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 1)
	assert.Equal(t, "/v1/statement", requests[0].path, "the DSN path is not a server prefix")
	assert.Equal(t, "tpch", requests[0].header.Get(trinoCatalogHeader))
	assert.Equal(t, "sf1", requests[0].header.Get(trinoSchemaHeader))
}

func TestServerURIWithPathRejected(t *testing.T) {
	t.Parallel()
	const wantErr = `trino: client configuration error, ServerURI must not have a path, got "/tpch/sf1"; set Config.Catalog and Config.Schema instead`

	t.Run("NewConnector", func(t *testing.T) {
		_, err := NewConnector(&Config{ServerURI: "http://user@localhost:8080/tpch/sf1"})
		assert.EqualError(t, err, wantErr)
	})
	t.Run("FormatDSN", func(t *testing.T) {
		config := &Config{ServerURI: "http://user@localhost:8080/tpch/sf1"}
		_, err := config.FormatDSN()
		assert.EqualError(t, err, wantErr)
	})
	t.Run("lone slash", func(t *testing.T) {
		_, err := NewConnector(&Config{ServerURI: "http://user@localhost:8080/"})
		assert.NoError(t, err)
	})
}

func TestDSNPathRoundTrip(t *testing.T) {
	t.Parallel()
	config, err := ParseDSN("http://user@localhost:8080/tpch/sf1")
	require.NoError(t, err)

	formatted, err := config.FormatDSN()
	require.NoError(t, err)
	assert.Equal(t, "http://user@localhost:8080?catalog=tpch&schema=sf1&source=trino-go-client", formatted, "the catalog and schema are written as parameters")

	reparsed, err := ParseDSN(formatted)
	require.NoError(t, err)
	assert.Equal(t, config, reparsed)
}

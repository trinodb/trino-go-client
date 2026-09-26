package integration

import (
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestIntegrationConnector(t *testing.T) {
	conf, err := trino.ParseDSN(integrationDSN(t))
	require.NoError(t, err)
	conf.HTTPClient = &http.Client{}
	connector, err := trino.NewConnector(conf)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	var n int
	require.NoError(t, db.QueryRow("SELECT 1").Scan(&n))
	assert.Equal(t, 1, n)
}

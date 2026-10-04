package integration

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestIntegrationServerInfo(t *testing.T) {
	db := integrationOpen(t)
	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var info trino.ServerInfo
	require.NoError(t, conn.Raw(func(driverConn any) error {
		info, err = driverConn.(*trino.Conn).ServerInfo(ctx)
		return err
	}))

	version, err := strconv.Atoi(versionPrefix.FindString(info.NodeVersion))
	require.NoError(t, err, "node version %q", info.NodeVersion)
	assert.Positive(t, version)
	assert.Equal(t, serverVersion, version)
	assert.True(t, info.Coordinator)
	assert.False(t, info.Starting)
	assert.NotEmpty(t, info.Environment)
	assert.Positive(t, info.Uptime)
}

// Without a user, the server cannot identify the caller, and only the
// credentials check rejects the connection.
func TestIntegrationPingFailsWithoutUser(t *testing.T) {
	dsn := integrationDSN(t)
	// HEAD /v1/statement was added in Trino 469.
	requireServerVersion(t, 469)
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	parsed.User = nil
	db := integrationOpen(t, parsed.String())

	err = db.PingContext(context.Background())

	var queryFailed *trino.ErrQueryFailed
	require.ErrorAs(t, err, &queryFailed)
	assert.Equal(t, http.StatusUnauthorized, queryFailed.StatusCode)
}

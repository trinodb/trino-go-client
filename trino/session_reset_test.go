package trino

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetSessionRestoresConfiguredSessionState(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://localhost/?catalog=hive&schema=sales&session_properties=query_priority%3A1")
	require.NoError(t, err)

	c.applyResponseHeaders(http.Header{
		trinoSetCatalogHeader:   []string{"memory"},
		trinoSetSchemaHeader:    []string{"default"},
		trinoSetPathHeader:      []string{"memory.default"},
		trinoSetSessionHeader:   []string{"query_priority=5", "time_zone_id=Asia%2FTokyo"},
		trinoAddedPrepareHeader: []string{"stmt1=SELECT+1"},
	})
	require.Equal(t, "memory", c.httpHeaderValue(trinoCatalogHeader))
	require.Equal(t, "Asia/Tokyo", c.location().String())

	require.NoError(t, c.ResetSession(context.Background()))

	assert.Equal(t, "hive", c.httpHeaderValue(trinoCatalogHeader))
	assert.Equal(t, "sales", c.httpHeaderValue(trinoSchemaHeader))
	assert.Empty(t, c.httpHeaderValues(trinoPathHeader), "no path was configured")
	assert.Equal(t, []string{"query_priority=1"}, c.httpHeaderValues(trinoSessionHeader))
	assert.Empty(t, c.httpHeaderValues(preparedStatementHeader))
	assert.Equal(t, c.timeZone, c.location(), "SET TIME ZONE should not survive the reset")
}

func TestResetSessionRemovesStateAbsentFromDSN(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://localhost")
	require.NoError(t, err)

	c.applyResponseHeaders(http.Header{
		trinoSetCatalogHeader: []string{"memory"},
		trinoSetSchemaHeader:  []string{"default"},
		trinoSetSessionHeader: []string{"query_priority=5"},
	})

	require.NoError(t, c.ResetSession(context.Background()))

	for _, name := range sessionStateHeaders {
		assert.Empty(t, c.httpHeaderValues(name), name)
	}
}

// The configured values must be copied, not shared, or a SET SESSION after a
// reset would change what the next reset restores.
func TestResetSessionKeepsConfiguredValuesIntact(t *testing.T) {
	t.Parallel()
	c, err := newConn("http://localhost/?session_properties=query_priority%3A1")
	require.NoError(t, err)

	require.NoError(t, c.ResetSession(context.Background()))
	c.applyResponseHeaders(http.Header{trinoSetSessionHeader: []string{"query_max_run_time=10m"}})
	require.NoError(t, c.ResetSession(context.Background()))

	assert.Equal(t, []string{"query_priority=1"}, c.httpHeaderValues(trinoSessionHeader))
}

func TestPooledConnectionDoesNotCarryUseToNextCaller(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage().withHeader(trinoSetCatalogHeader, "memory").withHeader(trinoSetSchemaHeader, "default"))
	db := fc.open(t, "?catalog=hive&schema=sales")
	db.SetMaxOpenConns(1)

	_, err := db.Exec("USE memory.default")
	require.NoError(t, err)
	_, err = db.Exec("SELECT 1")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, "hive", requests[1].header.Get(trinoCatalogHeader), "the next caller should get the DSN catalog")
	assert.Equal(t, "sales", requests[1].header.Get(trinoSchemaHeader))
}

func TestPinnedConnectionKeepsSessionState(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage().withHeader(trinoSetCatalogHeader, "memory").withHeader(trinoSetSchemaHeader, "default"))
	db := fc.open(t, "?catalog=hive&schema=sales")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "USE memory.default")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	requests := fc.capturedRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, "memory", requests[1].header.Get(trinoCatalogHeader), "a pinned connection keeps what USE set")
	assert.Equal(t, "default", requests[1].header.Get(trinoSchemaHeader))
}

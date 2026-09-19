package trino

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryCancellation(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: ErrTrino{
				ErrorName: "USER_CANCELLED",
			},
		})
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.EqualError(t, err, ErrQueryCancelled.Error(), "unexpected error")
}

// This test ensures that the fetch method is not generating stack overflow errors.
// === RUN   TestFetchNoStackOverflow
// runtime: goroutine stack exceeds 1000000000-byte limit
// runtime: sp=0x14037b00390 stack=[0x14037b00000, 0x14057b00000]
// fatal error: stack overflow
func TestFetchNoStackOverflow(t *testing.T) {
	previousSetting := debug.SetMaxStack(50 * 1024)
	t.Cleanup(func() { debug.SetMaxStack(previousSetting) })
	var count atomic.Int32
	var nextPage bytes.Buffer
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count.Add(1) <= 51 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(nextPage.Bytes())
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(&stmtResponse{
			Error: ErrTrino{
				ErrorName: "TEST",
			},
		})
	}))
	t.Cleanup(ts.Close)
	require.NoError(t, json.NewEncoder(&nextPage).Encode(&stmtResponse{
		ID:      "fake-query",
		NextURI: ts.URL + "/v1/statement/20210817_140827_00000_arvdv/1",
	}))

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	var queryFailed *ErrQueryFailed
	assert.ErrorAs(t, err, &queryFailed)
}

func TestProtocolErrorHandling(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		data    interface{}
		wantErr string
	}{
		{
			name:    "direct protocol invalid row type",
			data:    []interface{}{123},
			wantErr: "unexpected data type for row at index 0: expected []interface{}, got json.Number",
		},
		{
			name:    "spooling protocol missing encoding",
			data:    map[string]interface{}{"segments": []interface{}{}},
			wantErr: "invalid or missing 'encoding' field on spooling protocol, expected string",
		},
		{
			name:    "spooling protocol invalid segments type",
			data:    map[string]interface{}{"encoding": "json", "segments": "invalid"},
			wantErr: "nvalid or missing 'segments' field on spooling protocol, expected []interface{}",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage(tc.data))
			db := fc.open(t, "")

			_, err := db.Query("SELECT 1")
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestSetRoleHeader(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	db := fc.open(t, "?roles=catalog%3Auser")

	fc.respond(
		statementPage().withHeader(trinoSetRoleHeader, "hive=ROLE%7Badmin%7D"),
		resultPage([][]any{{1}}),
	)
	rows, err := db.Query("SET ROLE admin IN hive")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	fc.respond(
		statementPage().
			withHeader(trinoSetRoleHeader, "iceberg=ROLE%7Bwriter%7D").
			withHeader(trinoSetRoleHeader, "catalog=NONE"),
		resultPage([][]any{{1}}),
	)
	rows, err = db.Query("SET ROLE writer IN iceberg")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	requests := fc.capturedRequests()
	require.Len(t, requests, 4)
	assert.Equal(t, "catalog=ROLE{user}", requests[0].header.Get(trinoRoleHeader), "initial role from DSN should be sent in first request")
	assert.Equal(t, "catalog=ROLE{user},hive=ROLE%7Badmin%7D", requests[1].header.Get(trinoRoleHeader), "server-set role should be added to the DSN role")
	assert.Equal(t, "catalog=ROLE{user},hive=ROLE%7Badmin%7D", requests[2].header.Get(trinoRoleHeader), "roles should carry over to the next statement")
	assert.Equal(t, "catalog=NONE,hive=ROLE%7Badmin%7D,iceberg=ROLE%7Bwriter%7D", requests[3].header.Get(trinoRoleHeader), "every Set-Role value should be applied and roles of other catalogs kept")
}

func TestExtraCredentialsSentOnlyWithTheStatement(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}), emptyPage())
	db := fc.open(t, "?extra_credentials=token%3Asecret%3Bother%3Avalue")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.Len(t, requests, 3)
	assert.ElementsMatch(t, []string{"token=secret", "other=value"}, requests[0].header.Values(trinoExtraCredentialHeader), "credentials sent with the statement")
	for _, request := range requests[1:] {
		assert.Empty(t, request.header.Values(trinoExtraCredentialHeader), "credentials sent with %s %s", request.method, request.path)
	}
}

func TestUnsupportedHeader(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(trinoSetPathHeader, "foo.bar")
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(ts.Close)

	db, err := sql.Open("trino", ts.URL)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Query("SELECT 1")
	assert.EqualError(t, err, ErrUnsupportedHeader.Error(), "unexpected error")
}

func TestUnsupportedTransaction(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("trino", "http://localhost:9")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	_, err = db.Begin()
	require.ErrorIs(t, err, ErrOperationNotSupported)
}

func TestResponseHeadersUpdateFollowingRequests(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().
			withHeader(trinoSetSessionHeader, "query_max_run_time=10m").
			withHeader(trinoSetSessionHeader, "query_priority=1").
			withHeader(trinoSetSessionHeader, "join_distribution_type=BROADCAST").
			withHeader(trinoSetCatalogHeader, "memory").
			withHeader(trinoSetSchemaHeader, "default").
			withHeader(trinoAddedPrepareHeader, "stmt1=SELECT 1").
			withHeader(trinoAddedPrepareHeader, "stmt2=SELECT 2"),
		resultPage([][]any{{1}}).
			withHeader(trinoSetSessionHeader, "query_max_run_time=20m").
			withHeader(trinoClearSessionHeader, "query_priority").
			withHeader(trinoClearSessionHeader, "join_distribution_type").
			withHeader(trinoDeallocatedPrepareHeader, "stmt1").
			withHeader(trinoDeallocatedPrepareHeader, "stmt2"),
		emptyPage(),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	collectInts(t, rows)
	require.NoError(t, rows.Err())

	requests := fc.capturedRequests()
	require.Len(t, requests, 3)
	afterFirstPage := requests[1].header
	assert.Equal(t, []string{"query_max_run_time=10m", "query_priority=1", "join_distribution_type=BROADCAST"}, afterFirstPage.Values(trinoSessionHeader), "every session property set by the first page")
	assert.Equal(t, "memory", afterFirstPage.Get(trinoCatalogHeader), "catalog set by the first page")
	assert.Equal(t, "default", afterFirstPage.Get(trinoSchemaHeader), "schema set by the first page")
	assert.Equal(t, []string{"stmt1=SELECT 1", "stmt2=SELECT 2"}, afterFirstPage.Values(preparedStatementHeader), "every statement prepared by the first page")
	afterSecondPage := requests[2].header
	assert.Equal(t, []string{"query_max_run_time=20m"}, afterSecondPage.Values(trinoSessionHeader), "property replaced and the others cleared by the second page")
	assert.Empty(t, afterSecondPage.Values(preparedStatementHeader), "every statement deallocated by the second page")
	assert.Equal(t, "memory", afterSecondPage.Get(trinoCatalogHeader), "catalog kept by the second page")
}

func TestRowsCloseCancelsUnfinishedQuery(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		resultPage([][]any{{1}}),
		resultPage([][]any{{2}}),
		resultPage([][]any{{3}}),
		emptyPage(),
	)
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Close())

	var cancelled bool
	for _, request := range fc.capturedRequests() {
		if request.method == http.MethodDelete && request.path == "/v1/query/"+fakeQueryID {
			cancelled = true
		}
	}
	assert.True(t, cancelled, "closing the rows before the last page must cancel the query")
}

func TestNextReturnsContextError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	firstRowRead := make(chan struct{})
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		resultPage([][]any{{1}}),
		resultPage([][]any{{2}}),
		emptyPage(),
	)
	fc.onPage(func(index int, r *http.Request) {
		if index != 2 {
			return
		}
		// cancel the query while its next page is being fetched
		<-firstRowRead
		cancel()
		<-r.Context().Done()
	})
	db := fc.open(t, "")

	rows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	close(firstRowRead)

	assert.False(t, rows.Next())
	assert.ErrorIs(t, rows.Err(), context.Canceled)
}

// Without transaction support the driver must not announce any to the
// server; the header is what makes a coordinator accept START TRANSACTION.
func TestNoTransactionHeaderSent(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	for _, request := range fc.capturedRequests() {
		assert.Empty(t, request.header.Values("X-Trino-Transaction-Id"), "%s %s", request.method, request.path)
	}
}

package trino

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedCoordinator answers each statement POST with the next query from
// queries and serves that query's pages at /<id>/<n>, so a test controls the
// timing of every page of every execution.
type scriptedCoordinator struct {
	t       *testing.T
	server  *httptest.Server
	queries []scriptedQuery
	posts   atomic.Int32

	mu       sync.Mutex
	requests []capturedRequest
}

type scriptedQuery struct {
	id    string
	pages []scriptedPage
}

type scriptedPage struct {
	// wait, when set, blocks the page until it is closed or the request ends.
	wait   chan struct{}
	header http.Header
	body   func(baseURL string) *queryResponse
}

func newScriptedCoordinator(t *testing.T, queries ...scriptedQuery) *scriptedCoordinator {
	sc := &scriptedCoordinator{t: t, queries: queries}
	sc.server = httptest.NewServer(http.HandlerFunc(sc.serve))
	t.Cleanup(sc.server.Close)
	return sc
}

func (sc *scriptedCoordinator) serve(w http.ResponseWriter, r *http.Request) {
	sc.mu.Lock()
	sc.requests = append(sc.requests, capturedRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone()})
	sc.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
		n := int(sc.posts.Add(1)) - 1
		if n >= len(sc.queries) {
			http.Error(w, "unexpected statement", http.StatusBadRequest)
			return
		}
		q := sc.queries[n]
		_ = json.MarshalWrite(w, &stmtResponse{ID: q.id, NextURI: fmt.Sprintf("%s/%s/0", sc.server.URL, q.id), Stats: stmtStats{State: "QUEUED"}})
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet:
		var id string
		var index int
		if _, err := fmt.Sscanf(strings.ReplaceAll(strings.TrimPrefix(r.URL.Path, "/"), "/", " "), "%s %d", &id, &index); err != nil {
			http.NotFound(w, r)
			return
		}
		for _, q := range sc.queries {
			if q.id != id || index >= len(q.pages) {
				continue
			}
			p := q.pages[index]
			if p.wait != nil {
				select {
				case <-p.wait:
				case <-r.Context().Done():
					return
				}
			}
			for k, v := range p.header {
				w.Header()[k] = v
			}
			resp := p.body(sc.server.URL)
			resp.ID = id
			if index+1 < len(q.pages) {
				resp.NextURI = fmt.Sprintf("%s/%s/%d", sc.server.URL, id, index+1)
			}
			_ = json.MarshalWrite(w, resp)
			return
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (sc *scriptedCoordinator) capturedRequests() []capturedRequest {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return append([]capturedRequest(nil), sc.requests...)
}

func (sc *scriptedCoordinator) open(t *testing.T, params string) *sql.DB {
	t.Helper()
	db, err := sql.Open("trino", strings.Replace(sc.server.URL, "http://", "http://bob@", 1)+params)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

func intRows(values ...int) func(string) *queryResponse {
	return func(string) *queryResponse {
		data := make([][]int, len(values))
		for i, v := range values {
			data[i] = []int{v}
		}
		return &queryResponse{Columns: []queryColumn{integerColumn("v")}, Data: jsonData(data)}
	}
}

func spooledRows(segments ...map[string]any) func(string) *queryResponse {
	return func(baseURL string) *queryResponse {
		resolved := make([]map[string]any, 0, len(segments))
		for _, segment := range segments {
			resolved = append(resolved, resolveSegmentURIs(segment, baseURL))
		}
		return &queryResponse{
			Columns: []queryColumn{integerColumn("v")},
			Data:    jsonData(map[string]any{"encoding": "json", "segments": resolved}),
		}
	}
}

func statusOnly(string) *queryResponse {
	return &queryResponse{}
}

func noRows(string) *queryResponse {
	return &queryResponse{Columns: []queryColumn{integerColumn("v")}}
}

func TestPreparedStatementDoesNotReturnEarlierExecutionPages(t *testing.T) {
	t.Parallel()
	slowLastPage := make(chan struct{})
	t.Cleanup(func() { close(slowLastPage) })
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{
			{body: intRows(1)},
			{body: intRows(2)},
			{body: intRows(999), wait: slowLastPage},
		}},
		scriptedQuery{id: "q2", pages: []scriptedPage{
			{body: intRows(7)},
		}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	var first int
	require.NoError(t, stmt.QueryRowContext(ctx).Scan(&first))
	assert.Equal(t, 1, first)

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{7}, collectInts(t, rows))
	require.NoError(t, rows.Err())
}

func TestPreparedStatementDoesNotReturnEarlierSpooledRows(t *testing.T) {
	t.Parallel()
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{
			{body: spooledRows(reuseInlineRows(t, 0, 111), reuseInlineRows(t, 1, 112), reuseInlineRows(t, 2, 113))},
		}},
		scriptedQuery{id: "q2", pages: []scriptedPage{
			{body: noRows},
		}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.True(t, rows.Next())
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, rows.Close())

	rows, err = stmt.QueryContext(ctx)
	require.NoError(t, err)
	assert.Empty(t, collectInts(t, rows), "the second execution has no rows of its own")
	require.NoError(t, rows.Err())
}

func TestPreparedStatementReexecutedWhileSpooledDownloadRuns(t *testing.T) {
	t.Parallel()
	downloadStarted := make(chan struct{})
	var once sync.Once
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{
			{body: spooledRows(spooledSegment("a", map[string]any{"rowOffset": 0, "segmentSize": 7}))},
		}},
		scriptedQuery{id: "q2", pages: []scriptedPage{
			{body: spooledRows(reuseInlineRows(t, 0, 222))},
		}},
	)
	handler := sc.server.Config.Handler
	sc.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/spooled/download/a":
			once.Do(func() { close(downloadStarted) })
			<-r.Context().Done()
		case "/v1/spooled/ack/a":
		default:
			handler.ServeHTTP(w, r)
		}
	})
	db := sc.open(t, "")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	<-downloadStarted
	require.NoError(t, rows.Close())

	rows, err = stmt.QueryContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{222}, collectInts(t, rows))
	require.NoError(t, rows.Err())
}

func TestExecReadsEverySpooledPage(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage(),
		spooledPage("json", reuseInlineRows(t, 0, 1000, 10001)),
		spooledPage("json", reuseInlineRows(t, 2, 1000, 10001)),
		emptyPage(),
	)
	db := fc.open(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "INSERT 1")
	require.NoError(t, err)
}

func TestPreparedStatementNamedArgumentsApplyToOneExecution(t *testing.T) {
	t.Parallel()
	page := func(string) *queryResponse {
		return &queryResponse{Columns: []queryColumn{timestampColumn("ts")}, Data: jsonData([][]string{{"2024-01-01 12:00:00.000"}})}
	}
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{{body: statusOnly}, {body: page}}},
		scriptedQuery{id: "q2", pages: []scriptedPage{{body: statusOnly}, {body: page}}},
	)
	db := sc.open(t, "?timezone=UTC")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT ts")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	var ts time.Time
	require.NoError(t, stmt.QueryRowContext(ctx, sql.Named(trinoUserHeader, "alice"), sql.Named(trinoTimeZoneHeader, "Asia/Tokyo")).Scan(&ts))
	require.NoError(t, stmt.QueryRowContext(ctx).Scan(&ts))

	assert.Equal(t, "UTC", ts.Location().String(), "the second execution decodes in the connection's zone")
	for _, r := range sc.capturedRequests() {
		if strings.HasPrefix(r.path, "/q2/") {
			assert.Equal(t, "bob", r.header.Get(trinoUserHeader), "request %s %s", r.method, r.path)
		}
	}
}

func TestClosedQueryDoesNotChangeTheNextCallersSession(t *testing.T) {
	t.Parallel()
	lateAuthorization := make(chan struct{})
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{
			{body: intRows(1)},
			{body: intRows(2)},
			{body: intRows(3), wait: lateAuthorization, header: http.Header{trinoSetAuthorizationUserHeader: {"admin"}}},
		}},
		scriptedQuery{id: "q2", pages: []scriptedPage{{body: intRows(10)}}},
		scriptedQuery{id: "q3", pages: []scriptedPage{{body: intRows(11)}}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	var v int
	require.NoError(t, stmt.QueryRowContext(ctx).Scan(&v))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 10").Scan(&v))
	close(lateAuthorization)
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 11").Scan(&v))

	for _, r := range sc.capturedRequests() {
		if r.method == http.MethodPost {
			assert.Equal(t, "bob", r.header.Get(trinoUserHeader))
			assert.Empty(t, r.header.Get(trinoOriginalUserHeader))
		}
	}
}

// A query_timeout that ends while a spooled query still polls its next page
// fails the rows with the deadline.
func TestQueryTimeoutWhileSpooledQueryPolls(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	sc := newScriptedCoordinator(t, scriptedQuery{id: "q1", pages: []scriptedPage{
		{body: spooledRows(reuseInlineRows(t, 0, 1, 2))},
		{wait: block, body: statusOnly},
	}})
	db := sc.open(t, "?query_timeout=300ms")

	rows, err := db.Query("SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { rows.Close() })
	assert.Equal(t, []int{1, 2}, collectInts(t, rows))
	assert.ErrorIs(t, rows.Err(), context.DeadlineExceeded)
}

// Closing rows that were read to the end stops the query's heartbeat,
// although the prepared statement stays open and the connection is back in
// the pool.
func TestHeartbeatStopsWhenExhaustedRowsAreClosed(t *testing.T) {
	t.Parallel()
	sc := newScriptedCoordinator(t, scriptedQuery{id: "q1", pages: []scriptedPage{
		{body: spooledRows(reuseInlineRows(t, 0, 1, 2))},
		{body: statusOnly},
	}})
	db := sc.open(t, "?heartbeat_interval=20ms")
	stmt, err := db.Prepare("SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	rows, err := stmt.Query()
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	heads := func() int {
		n := 0
		for _, r := range sc.capturedRequests() {
			if r.method == http.MethodHead {
				n++
			}
		}
		return n
	}
	atClose := heads()
	time.Sleep(10 * 20 * time.Millisecond)
	assert.Equal(t, atClose, heads(), "no heartbeat may follow closing the rows")
}

// A prepared statement that fails while decoding a page must stop its
// polling before the connection returns to the pool; the next page's
// headers would otherwise change the session of the next caller.
func TestFailedStatementDoesNotChangeTheNextCallersSession(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	undecodable := func(string) *queryResponse {
		return &queryResponse{Columns: []queryColumn{integerColumn("v")}, Data: jsontext.Value("5")}
	}
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{
			{body: undecodable},
			{body: statusOnly, wait: release, header: http.Header{trinoSetAuthorizationUserHeader: {"mallory"}}},
		}},
		scriptedQuery{id: "q2", pages: []scriptedPage{{body: intRows(2)}}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	_, err = stmt.ExecContext(ctx)
	require.ErrorContains(t, err, "unexpected data")

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	// Let a page still in flight arrive after the next caller holds the
	// connection.
	close(release)
	time.Sleep(100 * time.Millisecond)
	var v int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT 2").Scan(&v))

	for _, r := range sc.capturedRequests() {
		if r.method == http.MethodPost {
			assert.Equal(t, "bob", r.header.Get(trinoUserHeader))
		}
	}
}

// reuseInlineRows returns an inline spooled segment of one-column rows that
// starts at rowOffset.
func reuseInlineRows(t testing.TB, rowOffset int, values ...int) map[string]any {
	t.Helper()
	rows := make([][]int, len(values))
	for i, v := range values {
		rows[i] = []int{v}
	}
	data, err := json.Marshal(rows)
	require.NoError(t, err)
	return inlineSegment(base64.StdEncoding.EncodeToString(data), map[string]any{
		"rowOffset": rowOffset, "segmentSize": len(data), "rowsCount": len(values),
	})
}

// A Conn- or Tx-bound prepared statement may run again while rows of an
// earlier execution are open. Those rows must not read the newer execution's
// pages, and closing them must not stop it.
func TestRowsOfAnEarlierExecutionDoNotReadTheNextOne(t *testing.T) {
	t.Parallel()
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{{body: intRows(1)}, {body: intRows(2)}, {body: intRows(3)}}},
		scriptedQuery{id: "q2", pages: []scriptedPage{{body: intRows(100)}, {body: intRows(200)}, {body: intRows(300)}}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	defer stmt.Close()

	first, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.True(t, first.Next())
	second, err := stmt.QueryContext(ctx)
	require.NoError(t, err)

	assert.False(t, first.Next())
	assert.ErrorIs(t, first.Err(), errStatementExecutedAgain)
	require.NoError(t, first.Close())
	assert.Equal(t, []int{100, 200, 300}, collectInts(t, second))
	require.NoError(t, second.Err())
}

// Closing rows of an earlier execution still cancels their query, as the
// user it ran as.
func TestRowsOfAnEarlierExecutionCancelTheirQuery(t *testing.T) {
	t.Parallel()
	sc := newScriptedCoordinator(t,
		scriptedQuery{id: "q1", pages: []scriptedPage{{body: intRows(1)}, {body: intRows(2)}, {body: intRows(3)}}},
		scriptedQuery{id: "q2", pages: []scriptedPage{{body: intRows(100)}}},
	)
	db := sc.open(t, "")
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "SELECT v")
	require.NoError(t, err)
	defer stmt.Close()

	first, err := stmt.QueryContext(ctx, sql.Named(trinoUserHeader, "alice"))
	require.NoError(t, err)
	require.True(t, first.Next())
	second, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.NoError(t, first.Close())
	assert.Equal(t, []int{100}, collectInts(t, second))

	var deletes []string
	for _, r := range sc.capturedRequests() {
		if r.method == http.MethodDelete {
			deletes = append(deletes, r.path+" as "+r.header.Get(trinoUserHeader))
		}
	}
	assert.Contains(t, deletes, "/v1/query/q1 as alice")
}

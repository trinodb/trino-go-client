package trino

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowProgressUpdater takes longer in each callback than the fake
// coordinator takes to serve the next page, so every update arrives while
// the previous one is still being delivered.
type slowProgressUpdater struct {
	mu     sync.Mutex
	states []string
}

func (u *slowProgressUpdater) Update(info QueryProgressInfo) {
	time.Sleep(50 * time.Millisecond)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.states = append(u.states, info.QueryStats.State)
}

func (u *slowProgressUpdater) reportedStates() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.states
}

// A callback that is slower than the pages arrive must still see every
// change of the query state, in particular the final one, which is reported
// only once.
func TestProgressCallbackSlowerThanPagesReportsEveryStateChange(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(
		statementPage().withState("QUEUED"),
		resultPage([][]any{{1}}).withState("RUNNING"),
		resultPage([][]any{{2}}).withState("RUNNING"),
		resultPage([][]any{{3}}).withState("FINISHING"),
		finalPage().withState("FINISHED"),
	)
	updater := &slowProgressUpdater{}

	db := fc.open(t, "")
	rows, err := db.Query("SELECT 1",
		sql.Named(trinoProgressCallbackParam, updater),
		sql.Named(trinoProgressCallbackPeriodParam, time.Minute))
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	assert.Equal(t, []string{"QUEUED", "RUNNING", "FINISHING", "FINISHED"}, updater.reportedStates())
}

// The progress callback belongs to the query it was passed with. A later query
// run without it must not report to it, whether it gets the same pooled
// connection or runs on the same pinned connection.
func TestProgressCallbackReportsOnlyTheQueryItWasPassedWith(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name    string
		querier func(t *testing.T, db *sql.DB) func(args ...any) (*sql.Rows, error)
	}{
		{"pooled connection", func(t *testing.T, db *sql.DB) func(args ...any) (*sql.Rows, error) {
			db.SetMaxOpenConns(1)
			return func(args ...any) (*sql.Rows, error) {
				return db.QueryContext(ctx, "SELECT 1", args...)
			}
		}},
		{"pinned connection", func(t *testing.T, db *sql.DB) func(args ...any) (*sql.Rows, error) {
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			return func(args ...any) (*sql.Rows, error) {
				return conn.QueryContext(ctx, "SELECT 1", args...)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			query := tc.querier(t, fc.open(t, ""))
			updater := &recordingProgressUpdater{}

			fc.respond(statementPage().withState("FIRST"), resultPage([][]any{{1}}).withState("FIRST"))
			rows, err := query(
				sql.Named(trinoProgressCallbackParam, updater),
				sql.Named(trinoProgressCallbackPeriodParam, time.Millisecond))
			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
			require.NoError(t, rows.Close())

			fc.respond(statementPage().withState("SECOND"), resultPage([][]any{{2}}).withState("SECOND"))
			rows, err = query()
			require.NoError(t, err)
			assert.Equal(t, []int{2}, collectInts(t, rows))
			require.NoError(t, rows.Close())

			assert.Contains(t, updater.reportedStates(), "FIRST")
			assert.NotContains(t, updater.reportedStates(), "SECOND", "a query without the callback must not report to it")
		})
	}
}

func TestProgressCallbackArgumentsOfWrongTypeReturnError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		args    []any
		wantErr string
	}{
		{"callback", []any{sql.Named(trinoProgressCallbackParam, "not an updater"), sql.Named(trinoProgressCallbackPeriodParam, time.Second)}, "must be a ProgressUpdater, got string"},
		{"period", []any{sql.Named(trinoProgressCallbackParam, &recordingProgressUpdater{}), sql.Named(trinoProgressCallbackPeriodParam, "1s")}, "must be a time.Duration, got string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(t)
			fc.respond(statementPage(), resultPage([][]any{{1}}))

			_, err := fc.open(t, "").Query("SELECT 1", tc.args...)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// Closing the rows stops the progress callback while the spooling processor
// may still be about to report the page it read. That report must be dropped,
// not panic on a closed channel.
func TestProgressCallbackCloseWhileSpoolingProcessorReports(t *testing.T) {
	t.Parallel()
	segment := inlineSegment("W1sxMDAwXSwgWzEwMDAxXV0=", map[string]any{"segmentSize": 17, "rowOffset": 0})
	// The send of each report races the stop, so a few queries are needed to
	// hit it.
	for range 10 {
		fc := newFakeCoordinator(t)
		fc.respond(statementPage(), spooledPage("json", segment), resultPage([][]any{{1}}), finalPage())
		db := fc.open(t, "")

		rows, err := db.Query("SELECT 1",
			sql.Named(trinoProgressCallbackParam, &recordingProgressUpdater{}),
			sql.Named(trinoProgressCallbackPeriodParam, time.Nanosecond))
		require.NoError(t, err)
		// The second page fails the query, so Next may return that
		// error instead of the first row; only the Close matters here.
		rows.Next()
		require.NoError(t, rows.Close())
	}
}

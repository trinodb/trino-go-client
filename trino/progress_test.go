package trino

import (
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

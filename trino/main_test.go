package trino

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the run if a goroutine started by a test or by the driver is
// still alive once every test has finished, such as a spooling worker or a
// heartbeat that Close did not stop.
//
// The goroutineleak profile in runtime/pprof is not enough for this: it only
// reports goroutines blocked on something no live goroutine can reach, and
// the heartbeat goroutine waits on a ticker, which the runtime counts as a
// way to be woken, so it was not reported when its shutdown was removed.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

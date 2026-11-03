package integration

import (
	"testing"
	"time"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/pipeline"
)

// Stopping a worker must end its stream at once, even while it waits for a
// message, so the controller can put its unstarted jobs back right away.
// (Cancelling the context alone left Receive blocked until the controller
// ended the stream.)
func TestClientStopsPromptly(t *testing.T) {
	e := newEnv(t)
	w := e.fakeWorker("w", workerOpts{behave: func(*opificav1.Job) outcome { return outcome{silent: true} }})
	e.createRun("on: push\njobs:\n  build:\n    runs-on: linux\n    steps: [{ run: make }]\n", pipeline.Event{}, "")
	eventually(t, "job", func() bool { return len(w.jobs()) == 1 })

	start := time.Now()
	w.cancel()
	err := <-w.done
	w.done <- err
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("Run took %v to return after cancel", d)
	}
}

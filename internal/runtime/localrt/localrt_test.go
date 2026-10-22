package localrt

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/alper/hubwagen/internal/taskexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	mu      sync.Mutex
	reports []core.Report
}

func (r *recorder) Report(rep core.Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, rep)
}

// statuses returns "attempt:status" in report order.
func (r *recorder) statuses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, rep := range r.reports {
		out = append(out, rep.AttemptID+":"+rep.Status)
	}
	return out
}

func (r *recorder) final(attemptID string) (core.Report, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rep := range r.reports {
		if rep.AttemptID == attemptID && rep.Status != state.AttemptRunning {
			return rep, true
		}
	}
	return core.Report{}, false
}

type fixture struct {
	t   *testing.T
	rec *recorder
	rt  *Runtime
	l   layout.Layout
}

func newFixture(t *testing.T, slots int) *fixture {
	t.Helper()
	b, err := blob.Open(blob.FileURI(t.TempDir()))
	require.NoError(t, err)
	rec := &recorder{}
	rt := New(rec, Config{Slots: slots, Pools: []string{"local", "py"}, Exec: taskexec.Options{
		Bucket: b, BaseEnv: []string{"PATH=" + os.Getenv("PATH")}, TerminationGrace: 200 * time.Millisecond, FlushInterval: 50 * time.Millisecond,
	}})
	t.Cleanup(rt.Close)
	return &fixture{t: t, rec: rec, rt: rt, l: layout.New(b.Base())}
}

func (f *fixture) assignment(id, pool, script string) core.Assignment {
	return core.Assignment{
		AttemptID: id, RunID: "r", TaskID: id, Attempt: 1, Tenant: "t", Pipeline: "p", RunKey: "k", Generation: 1,
		Runtime: spec.Runtime{Kind: "agent", Pool: pool}, Command: []string{"/bin/sh", "-c", script},
		Timeout: 10 * time.Second, LogsURI: f.l.Logs("t", "p", "r", id, 1), ResultURI: f.l.Result("t", "r", id, 1),
		CommitURI: f.l.TaskCommit("t", "p", id, "k", 1),
	}
}

func (f *fixture) wait(id string) core.Report {
	f.t.Helper()
	var rep core.Report
	require.Eventually(f.t, func() bool {
		var ok bool
		rep, ok = f.rec.final(id)
		return ok
	}, 10*time.Second, 10*time.Millisecond, "no final report for %s", id)
	return rep
}

func TestRunsAndReports(t *testing.T) {
	f := newFixture(t, 2)
	f.rt.Dispatch(f.assignment("a1", "local", "echo hi"))
	rep := f.wait("a1")
	assert.Equal(t, state.AttemptSucceeded, rep.Status)
	assert.Equal(t, "local", rep.AgentID)
	assert.Equal(t, []string{"a1:running", "a1:succeeded"}, f.rec.statuses())
}

func TestSlotsLimitConcurrency(t *testing.T) {
	f := newFixture(t, 1)
	f.rt.Dispatch(f.assignment("a1", "local", "sleep 0.3"))
	f.rt.Dispatch(f.assignment("a2", "py", "true"))
	f.wait("a2")
	assert.Equal(t, []string{"a1:running", "a1:succeeded", "a2:running", "a2:succeeded"}, f.rec.statuses(),
		"with one slot, a2 starts only after a1 finished")
}

func TestUnknownPoolFailsImmediately(t *testing.T) {
	f := newFixture(t, 1)
	f.rt.Dispatch(f.assignment("a1", "gpu", "true"))
	rep := f.wait("a1")
	assert.Equal(t, state.AttemptFailed, rep.Status)
	assert.Equal(t, "no_runtime", rep.Reason)
	assert.Contains(t, rep.Message, `pool "gpu"`)
}

func TestCancelQueuedAndRunning(t *testing.T) {
	f := newFixture(t, 1)
	f.rt.Dispatch(f.assignment("running", "local", "sleep 30"))
	f.rt.Dispatch(f.assignment("queued", "local", "echo should-not-run"))
	require.Eventually(t, func() bool { return len(f.rec.statuses()) == 1 }, 5*time.Second, 10*time.Millisecond)

	f.rt.Cancel("queued", "run cancelled")
	assert.Equal(t, state.AttemptCancelled, f.wait("queued").Status)
	f.rt.Cancel("running", "run cancelled")
	assert.Equal(t, state.AttemptCancelled, f.wait("running").Status)
	assert.NotContains(t, f.rec.statuses(), "queued:running")

	f.rt.Cancel("unknown", "nothing happens")
}

func TestCloseStopsRunningAttempts(t *testing.T) {
	f := newFixture(t, 1)
	f.rt.Dispatch(f.assignment("a1", "local", "sleep 30"))
	require.Eventually(t, func() bool { return len(f.rec.statuses()) == 1 }, 5*time.Second, 10*time.Millisecond)
	start := time.Now()
	f.rt.Close()
	assert.Less(t, time.Since(start), 5*time.Second)
	rep, ok := f.rec.final("a1")
	require.True(t, ok)
	assert.Equal(t, state.AttemptCancelled, rep.Status)

	f.rt.Dispatch(f.assignment("late", "local", "true")) // ignored after Close
	_, ok = f.rec.final("late")
	assert.False(t, ok)
}

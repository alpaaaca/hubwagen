package core

import (
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withHeartbeat(d time.Duration) option { return func(c *Config) { c.HeartbeatTimeout = d } }

func TestStepLeaseExpiresWithoutHeartbeats(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	cmd := stepCmd("extract")
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a := h.startStep(cmd)

	// Heartbeats keep it alive past the timeout.
	for range 3 {
		h.advance(50 * time.Second)
		stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
		require.NoError(t, err)
		assert.Empty(t, stop)
	}
	assert.Equal(t, state.StepDispatched, h.step(id).Status)

	// The worker goes quiet: lost, and retried.
	h.advance(time.Minute)
	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptLost, atts[0].Status)
	assert.Equal(t, "lost", atts[0].Reason)
	assert.Equal(t, `no heartbeat from worker "w1" within 1m0s`, atts[0].Message)

	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID}, stop, "the late worker is told to stop")
	h.advance(time.Second)
	assert.Equal(t, "extract", h.poll(TaskStep).Name)
}

func TestHeartbeatTellsWorkersToStopCancelledSteps(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startStep(stepCmd("extract"))
	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID, "unknown"})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID, "unknown"}, stop)
}

func TestRunTaskLeaseExpiryReplacesTheTask(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id := h.startRun("x")
	old := h.poll(TaskRun)

	h.advance(time.Minute)
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, state.RunTaskLost, tasks[0].Status)
	fresh := h.poll(TaskRun)
	assert.NotEqual(t, old.ID, fresh.ID)
	assert.ErrorIs(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: old.ID}), ErrNotFound)
}

func TestCompletedWorkHasNoLease(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startStep(stepCmd("extract"))
	h.succeedStep(a)
	h.poll(TaskRun) // the replay after the result, left running briefly
	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID}, stop)
	h.advance(10 * time.Minute)
	assert.Equal(t, state.StepCompleted, h.step(id).Status)
}

func TestRestoredAttemptGetsAFreshLease(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startStep(stepCmd("extract"))
	h.advance(50 * time.Second)
	h.restart(withHeartbeat(time.Minute))

	h.advance(50 * time.Second)
	assert.Equal(t, state.StepDispatched, h.step(id).Status, "the lease started over")
	h.succeedStep(a)
	assert.Equal(t, state.StepCompleted, h.step(id).Status)
}

func TestNewBuildResumesStuckRuns(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a")), nil)
	h.succeedStep(h.poll(TaskStep))
	h.replay(seqs(stepCmd("b")), nil)
	require.Equal(t, state.RunStuck, h.run(id).Status)
	h.pollNothing()

	h.build("b2", map[string]string{"a": "v2"})
	h.replay(seqs(stepCmd("a")), &RunOutcome{Status: state.RunCompleted})
	assert.Equal(t, state.RunCompleted, h.run(id).Status)

	h.build("b2", map[string]string{"a": "v2"}) // the same build again: nothing to resume
	h.pollNothing()
}

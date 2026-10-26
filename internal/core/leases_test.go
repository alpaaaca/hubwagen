package core

import (
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withHeartbeat(d time.Duration) option { return func(c *Config) { c.HeartbeatTimeout = d } }

func TestLiftLeaseExpiresWithoutHeartbeats(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	cmd := lift("extract")
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a := h.startLift(cmd)

	// Heartbeats keep it alive past the timeout.
	for range 3 {
		h.advance(50 * time.Second)
		stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
		require.NoError(t, err)
		assert.Empty(t, stop)
	}
	assert.Equal(t, state.LiftDispatched, h.lift(id).Status)

	// The worker goes quiet: lost, and retried.
	h.advance(time.Minute)
	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptLost, atts[0].Status)
	assert.Equal(t, "lost", atts[0].Reason)
	assert.Equal(t, `no heartbeat from worker "w1" within 1m0s`, atts[0].Message)

	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID}, stop, "the late worker is told to stop")
	h.advance(time.Second)
	assert.Equal(t, "extract", h.poll(TaskLift).Name)
}

func TestHeartbeatTellsWorkersToStopCancelledLifts(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startLift(lift("extract"))
	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID, "unknown"})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID, "unknown"}, stop)
}

func TestHaulTaskLeaseExpiryReplacesTheTask(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id := h.startHaul("x")
	old := h.poll(TaskHaul)

	h.advance(time.Minute)
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, state.HaulTaskLost, tasks[0].Status)
	fresh := h.poll(TaskHaul)
	assert.NotEqual(t, old.ID, fresh.ID)
	assert.ErrorIs(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: old.ID}), ErrNotFound)
}

func TestCompletedWorkHasNoLease(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startLift(lift("extract"))
	h.succeedLift(a)
	h.poll(TaskHaul) // the replay after the result, left running briefly
	stop, err := h.core.Heartbeat(h.ctx, "w1", []string{a.ID})
	require.NoError(t, err)
	assert.Equal(t, []string{a.ID}, stop)
	h.advance(10 * time.Minute)
	assert.Equal(t, state.LiftCompleted, h.lift(id).Status)
}

func TestRestoredAttemptGetsAFreshLease(t *testing.T) {
	h := newHarness(t, withHeartbeat(time.Minute))
	id, a := h.startLift(lift("extract"))
	h.advance(50 * time.Second)
	h.restart(withHeartbeat(time.Minute))

	h.advance(50 * time.Second)
	assert.Equal(t, state.LiftDispatched, h.lift(id).Status, "the lease started over")
	h.succeedLift(a)
	assert.Equal(t, state.LiftCompleted, h.lift(id).Status)
}

func TestNewBuildResumesStuckHauls(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a")), nil)
	h.succeedLift(h.poll(TaskLift))
	h.replay(seqs(lift("b")), nil)
	require.Equal(t, state.HaulStuck, h.haul(id).Status)
	h.pollNothing()

	h.build("b2", map[string]string{"a": "v2"})
	h.replay(seqs(lift("a")), &HaulOutcome{Status: state.HaulCompleted})
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)

	h.build("b2", map[string]string{"a": "v2"}) // the same build again: nothing to resume
	h.pollNothing()
}

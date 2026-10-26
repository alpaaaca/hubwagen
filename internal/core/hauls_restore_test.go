package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartRequeuesQueuedHaulTask(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)

	h.restart()
	got := h.poll(TaskHaul)
	assert.Equal(t, tasks[0].ID, got.ID, "the same haul task, still queued")
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: got.ID, Outcome: &HaulOutcome{Status: state.HaulCompleted}}))
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
}

func TestRestartReplacesDispatchedHaulTask(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	old := h.poll(TaskHaul)

	h.restart()
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, state.HaulTaskLost, tasks[0].Status)
	assert.Equal(t, "leader_restart", tasks[0].Reason)
	assert.Equal(t, 2, tasks[1].Number)

	assert.ErrorIs(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: old.ID}), ErrNotFound)
	fresh := h.poll(TaskHaul)
	assert.Equal(t, tasks[1].ID, fresh.ID)

	evs, err := h.store.EventsSince(h.ctx, 0, "", 0)
	require.NoError(t, err)
	var term []byte
	for _, e := range evs {
		if e.Type == state.EventLeaderTermStarted && e.Term == 2 {
			term = e.Payload
		}
	}
	assert.JSONEq(t, `{"term":2,"active_hauls":1,"lost_haul_tasks":1}`, string(term))
}

func TestDispatchedLiftSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Timeout = 10 * time.Minute
	id, a := h.startLift(cmd)
	h.advance(4 * time.Minute)

	h.restart()
	h.pollNothing() // no second attempt: the worker is still running the first

	// The worker reports to the new leader.
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":1}`)}))
	l := h.lift(id)
	assert.Equal(t, state.LiftCompleted, l.Status)
	assert.Equal(t, 1, l.Attempts)
	assert.Equal(t, TaskHaul, h.poll().Kind)
}

func TestRestoredLiftKeepsWhatIsLeftOfItsTimeout(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Timeout = 10 * time.Minute
	id, _ := h.startLift(cmd)
	h.advance(4 * time.Minute)
	h.restart()

	h.advance(5 * time.Minute)
	assert.Equal(t, state.LiftDispatched, h.lift(id).Status)
	h.advance(time.Minute)
	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status)
	assert.Equal(t, "timeout", l.Reason)
}

func TestRestartRequeuesUndispatchedLift(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a")), nil)

	h.restart()
	a := h.poll(TaskLift)
	assert.Equal(t, "a", a.Name)
	h.succeedLift(a)
	assert.Equal(t, state.LiftCompleted, h.lift(id).Status)
}

func TestRestoredDockLimitCountsRunningLifts(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Docks = map[string]DockConfig{DefaultDock: {MaxRunningLifts: 1}} })
	h.startHaul("x")
	h.replay(seqs(lift("a"), lift("b")), nil)
	a := h.poll(TaskLift)

	h.restart(func(c *Config) { c.Docks = map[string]DockConfig{DefaultDock: {MaxRunningLifts: 1}} })
	h.pollNothing() // a still holds the only slot
	h.succeedLift(a)
	assert.Equal(t, "b", h.poll(TaskLift).Name)
}

func TestRestoredRetryWaitAndTimer(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	retried := lift("a")
	retried.Retry = &RetryPolicy{Limit: 1, Initial: 10 * time.Minute}
	h.replay(seqs(retried, sleep(time.Hour)), nil)
	h.failLift(h.poll(TaskLift), "error")
	h.advance(5 * time.Minute)

	h.restart()
	h.advance(4 * time.Minute)
	h.pollNothing()
	h.advance(time.Minute) // 10 minutes after the failure
	a2 := h.poll(TaskLift)
	h.succeedLift(a2)
	h.poll(TaskHaul) // replay after the lift's result; leave it running

	h.advance(49 * time.Minute)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerPending, tms[0].Status)
	h.advance(time.Minute) // an hour after the sleep began
	tms, err = h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerFired, tms[0].Status)
}

func TestOverdueTimerFiresOnRestart(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(sleep(time.Hour)), nil)
	require.NoError(t, h.stop())
	h.clock.Advance(2 * time.Hour) // the leader was down when it was due

	h.restart()
	h.advance(0)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerFired, tms[0].Status)
	assert.Equal(t, TaskHaul, h.poll().Kind)
}

func TestRestoredHaulIsStillCheckedAgainstItsLogbook(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a"), lift("b")), nil)
	h.succeedLift(h.poll(TaskLift))
	h.restart()

	h.replay(seqs(lift("a"), lift("z")), nil)
	assert.Equal(t, state.HaulStuck, h.haul(id).Status)

	h.restart()
	assert.Equal(t, state.HaulStuck, h.haul(id).Status, "still stuck")
	_, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskHaul}})
	require.NoError(t, err)
	assert.False(t, ok, "no replay until ResumeHaul")
	require.NoError(t, h.core.ResumeHaul(h.ctx, id))
	h.replay(seqs(lift("a"), lift("b")), nil)
	assert.Equal(t, state.HaulRunning, h.haul(id).Status)
}

func TestRestoredHaulCanBeCancelled(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	retried := lift("a")
	retried.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	h.replay(seqs(retried, lift("b"), sleep(time.Hour)), nil)
	h.failLift(h.poll(TaskLift), "error")
	b := h.poll(TaskLift)

	h.restart()
	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	h.advance(2 * time.Hour)
	h.pollNothing()
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: b.ID, Status: state.AttemptSucceeded}), ErrNotFound)
	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	for _, l := range lifts {
		assert.Equal(t, state.LiftCancelled, l.Status, l.Name)
	}
}

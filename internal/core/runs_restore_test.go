package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartRequeuesQueuedRunTask(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)

	h.restart()
	got := h.poll(TaskRun)
	assert.Equal(t, tasks[0].ID, got.ID, "the same run task, still queued")
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: got.ID, Outcome: &RunOutcome{Status: state.RunCompleted}}))
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
}

func TestRestartReplacesDispatchedRunTask(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	old := h.poll(TaskRun)

	h.restart()
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, state.RunTaskLost, tasks[0].Status)
	assert.Equal(t, "leader_restart", tasks[0].Reason)
	assert.Equal(t, 2, tasks[1].Number)

	assert.ErrorIs(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: old.ID}), ErrNotFound)
	fresh := h.poll(TaskRun)
	assert.Equal(t, tasks[1].ID, fresh.ID)

	evs, err := h.store.EventsSince(h.ctx, 0, "", 0)
	require.NoError(t, err)
	var term []byte
	for _, e := range evs {
		if e.Type == state.EventLeaderTermStarted && e.Term == 2 {
			term = e.Payload
		}
	}
	assert.JSONEq(t, `{"term":2,"active_runs":1,"lost_run_tasks":1}`, string(term))
}

func TestDispatchedStepSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Timeout = 10 * time.Minute
	id, a := h.startStep(cmd)
	h.advance(4 * time.Minute)

	h.restart()
	h.pollNothing() // no second attempt: the worker is still running the first

	// The worker reports to the new leader.
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":1}`)}))
	l := h.step(id)
	assert.Equal(t, state.StepCompleted, l.Status)
	assert.Equal(t, 1, l.Attempts)
	assert.Equal(t, TaskRun, h.poll().Kind)
}

func TestRestoredStepKeepsWhatIsLeftOfItsTimeout(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Timeout = 10 * time.Minute
	id, _ := h.startStep(cmd)
	h.advance(4 * time.Minute)
	h.restart()

	h.advance(5 * time.Minute)
	assert.Equal(t, state.StepDispatched, h.step(id).Status)
	h.advance(time.Minute)
	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status)
	assert.Equal(t, "timeout", l.Reason)
}

func TestRestartRequeuesUndispatchedStep(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a")), nil)

	h.restart()
	a := h.poll(TaskStep)
	assert.Equal(t, "a", a.Name)
	h.succeedStep(a)
	assert.Equal(t, state.StepCompleted, h.step(id).Status)
}

func TestRestoredQueueLimitCountsRunningSteps(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Queues = map[string]QueueConfig{DefaultQueue: {MaxRunningSteps: 1}} })
	h.startRun("x")
	h.replay(seqs(stepCmd("a"), stepCmd("b")), nil)
	a := h.poll(TaskStep)

	h.restart(func(c *Config) { c.Queues = map[string]QueueConfig{DefaultQueue: {MaxRunningSteps: 1}} })
	h.pollNothing() // a still holds the only slot
	h.succeedStep(a)
	assert.Equal(t, "b", h.poll(TaskStep).Name)
}

func TestRestoredRetryWaitAndTimer(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	retried := stepCmd("a")
	retried.Retry = &RetryPolicy{Limit: 1, Initial: 10 * time.Minute}
	h.replay(seqs(retried, sleep(time.Hour)), nil)
	h.failStep(h.poll(TaskStep), "error")
	h.advance(5 * time.Minute)

	h.restart()
	h.advance(4 * time.Minute)
	h.pollNothing()
	h.advance(time.Minute) // 10 minutes after the failure
	a2 := h.poll(TaskStep)
	h.succeedStep(a2)
	h.poll(TaskRun) // replay after the step's result; leave it running

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
	id := h.startRun("x")
	h.replay(seqs(sleep(time.Hour)), nil)
	require.NoError(t, h.stop())
	h.clock.Advance(2 * time.Hour) // the leader was down when it was due

	h.restart()
	h.advance(0)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerFired, tms[0].Status)
	assert.Equal(t, TaskRun, h.poll().Kind)
}

func TestRestoredRunIsStillCheckedAgainstItsEventLog(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a"), stepCmd("b")), nil)
	h.succeedStep(h.poll(TaskStep))
	h.restart()

	h.replay(seqs(stepCmd("a"), stepCmd("z")), nil)
	assert.Equal(t, state.RunStuck, h.run(id).Status)

	h.restart()
	assert.Equal(t, state.RunStuck, h.run(id).Status, "still stuck")
	_, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}, Kinds: []TaskKind{TaskRun}})
	require.NoError(t, err)
	assert.False(t, ok, "no replay until ResumeRun")
	require.NoError(t, h.core.ResumeRun(h.ctx, id))
	h.replay(seqs(stepCmd("a"), stepCmd("b")), nil)
	assert.Equal(t, state.RunRunning, h.run(id).Status)
}

func TestRestoredRunCanBeCancelled(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	retried := stepCmd("a")
	retried.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	h.replay(seqs(retried, stepCmd("b"), sleep(time.Hour)), nil)
	h.failStep(h.poll(TaskStep), "error")
	b := h.poll(TaskStep)

	h.restart()
	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	h.advance(2 * time.Hour)
	h.pollNothing()
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: b.ID, Status: state.AttemptSucceeded}), ErrNotFound)
	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	for _, l := range steps {
		assert.Equal(t, state.StepCancelled, l.Status, l.Name)
	}
}

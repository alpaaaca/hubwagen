package core

import (
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startStep starts a run whose first replay schedules cmd, and takes the
// step's first attempt.
func (h *harness) startStep(cmd Command) (runID string, attempt Task) {
	h.t.Helper()
	runID = h.startRun("x")
	t1 := h.poll(TaskRun)
	require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: seqs(cmd)}))
	return runID, h.poll(TaskStep)
}

func (h *harness) step(runID string) state.Step {
	h.t.Helper()
	ls, err := h.store.ListSteps(h.ctx, runID)
	require.NoError(h.t, err)
	require.Len(h.t, ls, 1)
	return ls[0]
}

func (h *harness) failStep(a Task, reason string) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: reason, Message: "boom"}))
}

func TestStepRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Retry = &RetryPolicy{Limit: 2, Initial: time.Minute}
	id, a1 := h.startStep(cmd)

	h.failStep(a1, "error")
	l := h.step(id)
	assert.Equal(t, state.StepRetryWait, l.Status)
	assert.Equal(t, t0.Add(time.Minute), *l.RetryAt)
	h.pollNothing() // neither a retry nor a run task yet: the run doesn't see failed attempts

	h.advance(time.Minute)
	a2 := h.poll(TaskStep)
	assert.NotEqual(t, a1.ID, a2.ID)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a2.ID, Status: state.AttemptSucceeded}))

	l = h.step(id)
	assert.Equal(t, state.StepCompleted, l.Status)
	assert.Equal(t, 2, l.Attempts)
	assert.Nil(t, l.RetryAt)
	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, atts, 2)
	assert.Equal(t, []string{state.AttemptFailed, state.AttemptSucceeded}, []string{atts[0].Status, atts[1].Status})
	assert.Contains(t, h.runEventTypes(id), state.EventStepRetryScheduled)
	assert.Equal(t, TaskRun, h.poll().Kind, "the run replays with the step's result")
}

func TestStepExponentialBackoffThenFailure(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Retry = &RetryPolicy{Limit: 3, Backoff: "exponential", Initial: time.Minute, Max: 3 * time.Minute}
	id, a := h.startStep(cmd)

	for _, d := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute} { // 4m capped at 3m
		h.failStep(a, "error")
		assert.Equal(t, h.clock.Now().Add(d), *h.step(id).RetryAt)
		h.advance(d)
		a = h.poll(TaskStep)
	}
	h.failStep(a, "error")

	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status, "fourth attempt was the last")
	assert.Equal(t, 4, l.Attempts)
	assert.Equal(t, TaskRun, h.poll().Kind, "the run sees the failure")
}

func TestStepTimesOut(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("slow")
	cmd.Timeout = 10 * time.Minute
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	id, a1 := h.startStep(cmd)

	h.advance(10 * time.Minute)
	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptFailed, atts[0].Status)
	assert.Equal(t, "timeout", atts[0].Reason)
	assert.Equal(t, "no result within 10m0s", atts[0].Message)
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a1.ID, Status: state.AttemptSucceeded}), ErrNotFound,
		"a result after the timeout is ignored")

	h.advance(time.Minute)
	a2 := h.poll(TaskStep)
	h.advance(5 * time.Minute)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a2.ID, Status: state.AttemptFailed, Reason: "error"}))
	h.advance(time.Hour) // the second attempt's timeout was stopped by its result
	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status)
	assert.Equal(t, "error", l.Reason)
	atts, err = h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, "error", atts[1].Reason)
}

func TestStepTimeoutWithoutRetriesFailsTheStep(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("slow")
	cmd.Timeout = time.Minute
	id, _ := h.startStep(cmd)
	h.advance(time.Minute)
	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status)
	assert.Equal(t, "timeout", l.Reason)
}

func TestStepNoRetry(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Retry = &RetryPolicy{Limit: 3, Initial: time.Minute}
	id, a := h.startStep(cmd)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: "bad_args", NoRetry: true}))
	assert.Equal(t, state.StepFailed, h.step(id).Status)
}

func TestCancelRunDuringStepRetryWait(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Retry = &RetryPolicy{Limit: 2, Initial: time.Minute}
	id, a := h.startStep(cmd)
	h.failStep(a, "error")

	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	l := h.step(id)
	assert.Equal(t, state.StepCancelled, l.Status)
	assert.Nil(t, l.RetryAt)

	h.advance(time.Minute)
	h.pollNothing()
	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Len(t, atts, 1, "no attempt after the cancel")
}

func TestCancelRunStopsStepTimeout(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("slow")
	cmd.Timeout = time.Minute
	id, _ := h.startStep(cmd)
	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	h.advance(time.Minute)

	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptCancelled, atts[0].Status)
	assert.Equal(t, "cancelled", atts[0].Reason)
}

func TestStepPolicyIsStored(t *testing.T) {
	h := newHarness(t)
	cmd := stepCmd("extract")
	cmd.Timeout = 30 * time.Minute
	cmd.Retry = &RetryPolicy{Limit: 2, Backoff: "exponential", Initial: time.Second, Max: time.Minute}
	id, _ := h.startStep(cmd)
	l := h.step(id)
	assert.Equal(t, 30*time.Minute, l.Timeout)
	assert.JSONEq(t, `{"limit":2,"backoff":"exponential","initial":1000000000,"max":60000000000}`, string(l.Retry))
}

func TestRetryPolicy(t *testing.T) {
	fixed := RetryPolicy{Limit: 5, Initial: time.Minute}
	assert.Equal(t, time.Minute, fixed.delay(1))
	assert.Equal(t, time.Minute, fixed.delay(4))
	exp := RetryPolicy{Limit: 5, Backoff: "exponential", Initial: time.Second, Max: 10 * time.Second}
	var got []time.Duration
	for n := 1; n <= 5; n++ {
		got = append(got, exp.delay(n))
	}
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}, got)

	for name, r := range map[string]RetryPolicy{
		"negative limit": {Limit: -1},
		"unknown":        {Backoff: "linear"},
		"negative delay": {Initial: -time.Second},
		"max below":      {Initial: time.Minute, Max: time.Second},
	} {
		cmd := stepCmd("x")
		cmd.Retry = &r
		assert.Error(t, validateRunTaskResult(RunTaskResult{Commands: seqs(cmd)}), name)
	}
	neg := stepCmd("x")
	neg.Timeout = -time.Second
	assert.Error(t, validateRunTaskResult(RunTaskResult{Commands: seqs(neg)}))
}

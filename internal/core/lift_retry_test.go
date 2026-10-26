package core

import (
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startLift starts a haul whose first replay schedules cmd, and takes the
// lift's first attempt.
func (h *harness) startLift(cmd Command) (haulID string, attempt Task) {
	h.t.Helper()
	haulID = h.startHaul("x")
	t1 := h.poll(TaskHaul)
	require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: seqs(cmd)}))
	return haulID, h.poll(TaskLift)
}

func (h *harness) lift(haulID string) state.Lift {
	h.t.Helper()
	ls, err := h.store.ListLifts(h.ctx, haulID)
	require.NoError(h.t, err)
	require.Len(h.t, ls, 1)
	return ls[0]
}

func (h *harness) failLift(a Task, reason string) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: reason, Message: "boom"}))
}

func TestLiftRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Retry = &RetryPolicy{Limit: 2, Initial: time.Minute}
	id, a1 := h.startLift(cmd)

	h.failLift(a1, "error")
	l := h.lift(id)
	assert.Equal(t, state.LiftRetryWait, l.Status)
	assert.Equal(t, t0.Add(time.Minute), *l.RetryAt)
	h.pollNothing() // neither a retry nor a haul task yet: the haul doesn't see failed attempts

	h.advance(time.Minute)
	a2 := h.poll(TaskLift)
	assert.NotEqual(t, a1.ID, a2.ID)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a2.ID, Status: state.AttemptSucceeded}))

	l = h.lift(id)
	assert.Equal(t, state.LiftCompleted, l.Status)
	assert.Equal(t, 2, l.Attempts)
	assert.Nil(t, l.RetryAt)
	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, atts, 2)
	assert.Equal(t, []string{state.AttemptFailed, state.AttemptSucceeded}, []string{atts[0].Status, atts[1].Status})
	assert.Contains(t, h.haulEventTypes(id), state.EventLiftRetryScheduled)
	assert.Equal(t, TaskHaul, h.poll().Kind, "the haul replays with the lift's result")
}

func TestLiftExponentialBackoffThenFailure(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Retry = &RetryPolicy{Limit: 3, Backoff: "exponential", Initial: time.Minute, Max: 3 * time.Minute}
	id, a := h.startLift(cmd)

	for _, d := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute} { // 4m capped at 3m
		h.failLift(a, "error")
		assert.Equal(t, h.clock.Now().Add(d), *h.lift(id).RetryAt)
		h.advance(d)
		a = h.poll(TaskLift)
	}
	h.failLift(a, "error")

	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status, "fourth attempt was the last")
	assert.Equal(t, 4, l.Attempts)
	assert.Equal(t, TaskHaul, h.poll().Kind, "the haul sees the failure")
}

func TestLiftTimesOut(t *testing.T) {
	h := newHarness(t)
	cmd := lift("slow")
	cmd.Timeout = 10 * time.Minute
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	id, a1 := h.startLift(cmd)

	h.advance(10 * time.Minute)
	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptFailed, atts[0].Status)
	assert.Equal(t, "timeout", atts[0].Reason)
	assert.Equal(t, "no result within 10m0s", atts[0].Message)
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a1.ID, Status: state.AttemptSucceeded}), ErrNotFound,
		"a result after the timeout is ignored")

	h.advance(time.Minute)
	a2 := h.poll(TaskLift)
	h.advance(5 * time.Minute)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a2.ID, Status: state.AttemptFailed, Reason: "error"}))
	h.advance(time.Hour) // the second attempt's timeout was stopped by its result
	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status)
	assert.Equal(t, "error", l.Reason)
	atts, err = h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, "error", atts[1].Reason)
}

func TestLiftTimeoutWithoutRetriesFailsTheLift(t *testing.T) {
	h := newHarness(t)
	cmd := lift("slow")
	cmd.Timeout = time.Minute
	id, _ := h.startLift(cmd)
	h.advance(time.Minute)
	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status)
	assert.Equal(t, "timeout", l.Reason)
}

func TestLiftNoRetry(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Retry = &RetryPolicy{Limit: 3, Initial: time.Minute}
	id, a := h.startLift(cmd)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: "bad_args", NoRetry: true}))
	assert.Equal(t, state.LiftFailed, h.lift(id).Status)
}

func TestCancelHaulDuringLiftRetryWait(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Retry = &RetryPolicy{Limit: 2, Initial: time.Minute}
	id, a := h.startLift(cmd)
	h.failLift(a, "error")

	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	l := h.lift(id)
	assert.Equal(t, state.LiftCancelled, l.Status)
	assert.Nil(t, l.RetryAt)

	h.advance(time.Minute)
	h.pollNothing()
	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Len(t, atts, 1, "no attempt after the cancel")
}

func TestCancelHaulStopsLiftTimeout(t *testing.T) {
	h := newHarness(t)
	cmd := lift("slow")
	cmd.Timeout = time.Minute
	id, _ := h.startLift(cmd)
	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	h.advance(time.Minute)

	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, state.AttemptCancelled, atts[0].Status)
	assert.Equal(t, "cancelled", atts[0].Reason)
}

func TestLiftPolicyIsStored(t *testing.T) {
	h := newHarness(t)
	cmd := lift("extract")
	cmd.Timeout = 30 * time.Minute
	cmd.Retry = &RetryPolicy{Limit: 2, Backoff: "exponential", Initial: time.Second, Max: time.Minute}
	id, _ := h.startLift(cmd)
	l := h.lift(id)
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
		cmd := lift("x")
		cmd.Retry = &r
		assert.Error(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(cmd)}), name)
	}
	neg := lift("x")
	neg.Timeout = -time.Second
	assert.Error(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(neg)}))
}

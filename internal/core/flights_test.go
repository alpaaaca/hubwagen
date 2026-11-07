package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/moksha/internal/blob"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoRun starts a run of tenant whose first replay schedules cmd under
// build b1, which knows extract at v1.
func (h *harness) memoRun(tenant string, cmd Command) string {
	h.t.Helper()
	res, err := h.core.StartRun(h.ctx, StartRunRequest{Tenant: tenant, Name: "x", Reuse: h.reuse})
	require.NoError(h.t, err)
	t := h.poll(TaskRun)
	require.Equal(h.t, res.RunID, t.RunID)
	require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t.ID, BuildID: "b1", Commands: seqs(cmd)}))
	return res.RunID
}

func memoExtract() Command {
	cmd := output("extract", "raw", "")
	cmd.Memo = "fingerprint"
	return cmd
}

func (h *harness) succeedWithOutput(b blob.Bucket, a Task) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":1}`), Output: writeOutput(h.t, b, a, map[string]string{"a.csv": "x"})}))
}

func waitingOn(t *testing.T, h *harness, runID string) stepWaitingPayload {
	t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(t, err)
	var p stepWaitingPayload
	found := false
	for _, e := range evs {
		if e.Type == state.EventStepWaiting {
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			found = true
		}
	}
	require.True(t, found, "run %s never waited", runID)
	return p
}

func newFlightHarness(t *testing.T) (*harness, blob.Bucket) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	h.build("b1", map[string]string{"extract": "v1"})
	return h, b
}

func TestConcurrentRunsRunAFingerprintOnce(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())

	a := h.poll(TaskStep)
	assert.Equal(t, first, a.RunID)
	h.pollNothing() // the second run's step waits instead of running
	assert.Equal(t, state.StepWaiting, h.step(second).Status)
	w := waitingOn(t, h, second)
	assert.Equal(t, first, w.Run)
	assert.Equal(t, 1, w.OnSeq)
	assert.Equal(t, h.step(first).Fingerprint, w.Fingerprint)

	h.succeedWithOutput(b, a)
	h.waitStep(first, state.StepCompleted)
	got := h.waitStep(second, state.StepCompleted)
	assert.Equal(t, 0, got.Attempts, "it never ran")
	assert.JSONEq(t, `{"rows":1}`, string(got.Result))

	built := h.dataVersions(first)
	require.Len(t, built, 1)
	assert.Empty(t, h.dataVersions(second))
	assert.Contains(t, h.runEventTypes(second), state.EventStepReused)

	// Both runs continue, with the same data version.
	var runs []string
	for range 2 {
		task := h.poll(TaskRun)
		runs = append(runs, task.RunID)
		var finished stepFinishedPayload
		for _, e := range task.EventLog {
			if e.Type == state.EventStepFinished {
				require.NoError(t, json.Unmarshal(e.Payload, &finished))
			}
		}
		assert.Equal(t, built[0].ID, finished.DataVersion)
	}
	assert.ElementsMatch(t, []string{first, second}, runs)
}

func TestWaiterRunsWhenTheOwnerFails(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())
	third := h.memoRun("t", memoExtract())
	a := h.poll(TaskStep)
	h.failStep(a, "error")

	assert.Equal(t, state.StepFailed, h.step(first).Status)
	next := h.poll(TaskStep)
	assert.Equal(t, second, next.RunID, "the first waiter runs itself")
	assert.Contains(t, h.step(second).RunReason, "failed")
	assert.Equal(t, state.StepWaiting, h.step(third).Status)
	assert.Equal(t, second, waitingOn(t, h, third).Run, "the rest now wait on it")

	h.succeedWithOutput(b, next)
	h.waitStep(third, state.StepCompleted)
	assert.Equal(t, h.dataVersions(second)[0].ID, func() string {
		evs, err := h.store.RunEvents(h.ctx, third)
		require.NoError(t, err)
		var p stepReusedPayload
		for _, e := range evs {
			if e.Type == state.EventStepReused {
				require.NoError(t, json.Unmarshal(e.Payload, &p))
			}
		}
		return p.DataVersion
	}())
}

func TestWaitersWaitThroughTheOwnersRetries(t *testing.T) {
	h, b := newFlightHarness(t)
	cmd := memoExtract()
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	first := h.memoRun("t", cmd)
	second := h.memoRun("t", memoExtract())
	h.failStep(h.poll(TaskStep), "error")
	assert.Equal(t, state.StepRetryWait, h.step(first).Status)
	assert.Equal(t, state.StepWaiting, h.step(second).Status)

	h.advance(time.Minute)
	retry := h.poll(TaskStep)
	assert.Equal(t, first, retry.RunID)
	h.succeedWithOutput(b, retry)
	h.waitStep(second, state.StepCompleted)
}

func TestCancelledWaiterLeavesTheFlight(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())
	a := h.poll(TaskStep)

	require.NoError(t, h.core.CancelRun(h.ctx, second, "x"))
	assert.Equal(t, state.StepCancelled, h.step(second).Status)
	h.succeedWithOutput(b, a)
	h.waitStep(first, state.StepCompleted)
	assert.Equal(t, state.StepCancelled, h.step(second).Status, "not reused after its cancel")
	assert.NotContains(t, h.runEventTypes(second), state.EventStepReused)
}

func TestCancelledOwnerHandsOver(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())
	h.poll(TaskStep)

	require.NoError(t, h.core.CancelRun(h.ctx, first, "x"))
	next := h.poll(TaskStep)
	assert.Equal(t, second, next.RunID)
	h.succeedWithOutput(b, next)
	h.waitStep(second, state.StepCompleted)
}

func TestTenantsDontShareFlights(t *testing.T) {
	h, _ := newFlightHarness(t)
	h.memoRun("t", memoExtract())
	h.memoRun("u", memoExtract())
	a, b := h.poll(TaskStep), h.poll(TaskStep)
	assert.NotEqual(t, a.RunID, b.RunID, "both run")
}

func TestFlightsSurviveRestart(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())
	a := h.poll(TaskStep)

	h.restart(withBucket(b, nil))
	h.pollNothing() // the owner's attempt is still out; the waiter still waits
	assert.Equal(t, state.StepWaiting, h.step(second).Status)

	h.succeedWithOutput(b, a) // the worker reports to the new leader
	h.waitStep(first, state.StepCompleted)
	h.waitStep(second, state.StepCompleted)
}

func TestWaiterWhoseOwnerIsGoneRunsAfterRestart(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	second := h.memoRun("t", memoExtract())
	h.poll(TaskStep)
	// The owner finishes in the store but its flight never landed (as if
	// the leader died in between): simulate by closing the owner's step.
	l := h.step(first)
	l.Status = state.StepFailed
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutStep(l) })
	require.NoError(t, err)

	h.restart(withBucket(b, nil))
	next := h.poll(TaskStep)
	assert.Equal(t, second, next.RunID)
}

// A run that reuses nothing doesn't wait for another run's step with the
// same fingerprint: it runs its own, and gets its own result.
func TestARunThatReusesNothingDoesntWaitForAFlight(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoRun("t", memoExtract())
	h.reuse = state.ReuseNone
	second := h.memoRun("t", memoExtract())

	a1, a2 := h.poll(TaskStep), h.poll(TaskStep)
	assert.Equal(t, []string{first, second}, []string{a1.RunID, a2.RunID}, "both run")
	h.succeedWithOutput(b, a1)
	h.succeedWithOutput(b, a2)
	assert.False(t, h.waitStep(second, state.StepCompleted).Reused)
	assert.False(t, h.waitStep(first, state.StepCompleted).Reused)
}

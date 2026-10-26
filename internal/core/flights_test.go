package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoHaul starts a haul of tenant whose first replay schedules cmd under
// build b1, which knows extract at v1.
func (h *harness) memoHaul(tenant string, cmd Command) string {
	h.t.Helper()
	res, err := h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: tenant, Name: "x"})
	require.NoError(h.t, err)
	t := h.poll(TaskHaul)
	require.Equal(h.t, res.HaulID, t.HaulID)
	require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t.ID, BuildID: "b1", Commands: seqs(cmd)}))
	return res.HaulID
}

func memoExtract() Command {
	cmd := output("extract", "raw", "")
	cmd.Memo = "fingerprint"
	return cmd
}

func (h *harness) succeedWithOutput(b blob.Bucket, a Task) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":1}`), Output: writeOutput(h.t, b, a, map[string]string{"a.csv": "x"})}))
}

func waitingOn(t *testing.T, h *harness, haulID string) liftWaitingPayload {
	t.Helper()
	evs, err := h.store.HaulEvents(h.ctx, haulID)
	require.NoError(t, err)
	var p liftWaitingPayload
	found := false
	for _, e := range evs {
		if e.Type == state.EventLiftWaiting {
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			found = true
		}
	}
	require.True(t, found, "haul %s never waited", haulID)
	return p
}

func newFlightHarness(t *testing.T) (*harness, blob.Bucket) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	h.build("b1", map[string]string{"extract": "v1"})
	return h, b
}

func TestConcurrentHaulsRunAFingerprintOnce(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())

	a := h.poll(TaskLift)
	assert.Equal(t, first, a.HaulID)
	h.pollNothing() // the second haul's lift waits instead of running
	assert.Equal(t, state.LiftWaiting, h.lift(second).Status)
	w := waitingOn(t, h, second)
	assert.Equal(t, first, w.Haul)
	assert.Equal(t, 1, w.OnSeq)
	assert.Equal(t, h.lift(first).Fingerprint, w.Fingerprint)

	h.succeedWithOutput(b, a)
	h.waitLift(first, state.LiftCompleted)
	got := h.waitLift(second, state.LiftCompleted)
	assert.Equal(t, 0, got.Attempts, "it never ran")
	assert.JSONEq(t, `{"rows":1}`, string(got.Result))

	built := h.pallets(first)
	require.Len(t, built, 1)
	assert.Empty(t, h.pallets(second))
	assert.Contains(t, h.haulEventTypes(second), state.EventLiftReused)

	// Both hauls continue, with the same pallet.
	var hauls []string
	for range 2 {
		task := h.poll(TaskHaul)
		hauls = append(hauls, task.HaulID)
		var finished liftFinishedPayload
		for _, e := range task.Logbook {
			if e.Type == state.EventLiftFinished {
				require.NoError(t, json.Unmarshal(e.Payload, &finished))
			}
		}
		assert.Equal(t, built[0].ID, finished.Pallet)
	}
	assert.ElementsMatch(t, []string{first, second}, hauls)
}

func TestWaiterRunsWhenTheOwnerFails(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())
	third := h.memoHaul("t", memoExtract())
	a := h.poll(TaskLift)
	h.failLift(a, "error")

	assert.Equal(t, state.LiftFailed, h.lift(first).Status)
	next := h.poll(TaskLift)
	assert.Equal(t, second, next.HaulID, "the first waiter runs itself")
	assert.Contains(t, h.lift(second).RunReason, "failed")
	assert.Equal(t, state.LiftWaiting, h.lift(third).Status)
	assert.Equal(t, second, waitingOn(t, h, third).Haul, "the rest now wait on it")

	h.succeedWithOutput(b, next)
	h.waitLift(third, state.LiftCompleted)
	assert.Equal(t, h.pallets(second)[0].ID, func() string {
		evs, err := h.store.HaulEvents(h.ctx, third)
		require.NoError(t, err)
		var p liftReusedPayload
		for _, e := range evs {
			if e.Type == state.EventLiftReused {
				require.NoError(t, json.Unmarshal(e.Payload, &p))
			}
		}
		return p.Pallet
	}())
}

func TestWaitersWaitThroughTheOwnersRetries(t *testing.T) {
	h, b := newFlightHarness(t)
	cmd := memoExtract()
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	first := h.memoHaul("t", cmd)
	second := h.memoHaul("t", memoExtract())
	h.failLift(h.poll(TaskLift), "error")
	assert.Equal(t, state.LiftRetryWait, h.lift(first).Status)
	assert.Equal(t, state.LiftWaiting, h.lift(second).Status)

	h.advance(time.Minute)
	retry := h.poll(TaskLift)
	assert.Equal(t, first, retry.HaulID)
	h.succeedWithOutput(b, retry)
	h.waitLift(second, state.LiftCompleted)
}

func TestCancelledWaiterLeavesTheFlight(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())
	a := h.poll(TaskLift)

	require.NoError(t, h.core.CancelHaul(h.ctx, second, "x"))
	assert.Equal(t, state.LiftCancelled, h.lift(second).Status)
	h.succeedWithOutput(b, a)
	h.waitLift(first, state.LiftCompleted)
	assert.Equal(t, state.LiftCancelled, h.lift(second).Status, "not reused after its cancel")
	assert.NotContains(t, h.haulEventTypes(second), state.EventLiftReused)
}

func TestCancelledOwnerHandsOver(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())
	h.poll(TaskLift)

	require.NoError(t, h.core.CancelHaul(h.ctx, first, "x"))
	next := h.poll(TaskLift)
	assert.Equal(t, second, next.HaulID)
	h.succeedWithOutput(b, next)
	h.waitLift(second, state.LiftCompleted)
}

func TestTenantsDontShareFlights(t *testing.T) {
	h, _ := newFlightHarness(t)
	h.memoHaul("t", memoExtract())
	h.memoHaul("u", memoExtract())
	a, b := h.poll(TaskLift), h.poll(TaskLift)
	assert.NotEqual(t, a.HaulID, b.HaulID, "both run")
}

func TestFlightsSurviveRestart(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())
	a := h.poll(TaskLift)

	h.restart(withBucket(b, nil))
	h.pollNothing() // the owner's attempt is still out; the waiter still waits
	assert.Equal(t, state.LiftWaiting, h.lift(second).Status)

	h.succeedWithOutput(b, a) // the worker reports to the new leader
	h.waitLift(first, state.LiftCompleted)
	h.waitLift(second, state.LiftCompleted)
}

func TestWaiterWhoseOwnerIsGoneRunsAfterRestart(t *testing.T) {
	h, b := newFlightHarness(t)
	first := h.memoHaul("t", memoExtract())
	second := h.memoHaul("t", memoExtract())
	h.poll(TaskLift)
	// The owner finishes in the store but its flight never landed (as if
	// the leader died in between): simulate by closing the owner's lift.
	l := h.lift(first)
	l.Status = state.LiftFailed
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutLift(l) })
	require.NoError(t, err)

	h.restart(withBucket(b, nil))
	next := h.poll(TaskLift)
	assert.Equal(t, second, next.HaulID)
}

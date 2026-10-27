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

// Lifts without an output are memoized by their result.

func memoResult(name string) step {
	cmd := lift(name)
	cmd.Memo = "fingerprint"
	return step{cmd: cmd}
}

func reusedEvent(t *testing.T, h *harness, haulID string) liftReusedPayload {
	t.Helper()
	evs, err := h.store.HaulEvents(h.ctx, haulID)
	require.NoError(t, err)
	for _, e := range evs {
		if e.Type == state.EventLiftReused {
			var p liftReusedPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			return p
		}
	}
	t.Fatalf("haul %s reused nothing", haulID)
	return liftReusedPayload{}
}

func TestMemoizedResultIsReused(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	steps := []step{memoResult("rates")}
	h.results = map[string]string{"rates": `{"eur": 1.08}`}
	first, ran := h.runHaul(nil, "b1", steps, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "no previous run", h.reasons(first)["rates"])

	h.results = map[string]string{"rates": `{"eur": 2}`} // what it would return if it ran
	second, ran := h.runHaul(nil, "b1", steps, nil)
	assert.Empty(t, ran)
	l := h.lift(second)
	assert.True(t, l.Reused)
	assert.Equal(t, 0, l.Attempts)
	assert.JSONEq(t, `{"eur": 1.08}`, string(l.Result), "the recorded result")
	assert.Equal(t, h.lift(first).Fingerprint, l.Fingerprint)
	assert.Equal(t, liftReusedPayload{Seq: 1, Haul: first, OnSeq: 1, Fingerprint: l.Fingerprint}, reusedEvent(t, h, second))
	assert.Empty(t, h.pallets(first), "no pallet: the result is in the store")

	notMemo := steps[0]
	notMemo.cmd.Memo = ""
	_, ran = h.runHaul(nil, "b1", []step{notMemo}, nil)
	assert.Equal(t, []string{"rates"}, ran, "without memo it runs every time")
}

func TestResultRerunReasons(t *testing.T) {
	h := newHarness(t)
	st := memoResult("rates")
	st.cmd.Args = json.RawMessage(`{"day":"2026-10-24"}`)
	h.build("b1", map[string]string{"rates": "v1"})
	h.runHaul(nil, "b1", []step{st}, nil)

	other := st
	other.cmd.Args = json.RawMessage(`{"day":"2026-10-25"}`)
	id, ran := h.runHaul(nil, "b1", []step{other}, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "no previous run", h.reasons(id)["rates"], "no earlier run with these arguments")

	h.build("b2", map[string]string{"rates": "v2"})
	id, ran = h.runHaul(nil, "b2", []step{st}, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "code changed (v1 → v2)", h.reasons(id)["rates"])
}

func TestResultTTLCountsFromTheRun(t *testing.T) {
	h := newHarness(t)
	st := memoResult("fetch")
	st.cmd.Memo = "ttl:1h"
	h.build("b1", map[string]string{"fetch": "v1"})
	h.runHaul(nil, "b1", []step{st}, nil)

	h.advance(40 * time.Minute)
	_, ran := h.runHaul(nil, "b1", []step{st}, nil)
	assert.Empty(t, ran, "younger than the TTL")

	h.advance(40 * time.Minute) // 80 minutes after the run, 40 after the reuse
	id, ran := h.runHaul(nil, "b1", []step{st}, nil)
	assert.Equal(t, []string{"fetch"}, ran, "a reuse doesn't renew the result")
	assert.Equal(t, "expired (older than 1h0m0s)", h.reasons(id)["fetch"])
}

func TestFailedLiftResultIsNotReused(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	first := h.memoHaul("t", memoResult("rates").cmd)
	a := h.poll(TaskLift)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: "boom"}))
	h.waitLift(first, state.LiftFailed)

	_, ran := h.runHaul(nil, "b1", []step{memoResult("rates")}, nil)
	assert.Equal(t, []string{"rates"}, ran)
}

func TestConcurrentHaulsRunAResultOnce(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"extract": "v1"})
	cmd := lift("extract")
	cmd.Memo = "fingerprint"
	first := h.memoHaul("t", cmd)
	second := h.memoHaul("t", cmd)

	a := h.poll(TaskLift)
	assert.Equal(t, first, a.HaulID)
	h.pollNothing()
	assert.Equal(t, first, waitingOn(t, h, second).Haul)

	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":3}`)}))
	h.waitLift(first, state.LiftCompleted)
	got := h.waitLift(second, state.LiftCompleted)
	assert.Equal(t, 0, got.Attempts, "it never ran")
	assert.True(t, got.Reused)
	assert.JSONEq(t, `{"rows":3}`, string(got.Result))
}

func TestMemoizedResultSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	h.runHaul(nil, "b1", []step{memoResult("rates")}, nil)

	h.restart()
	_, ran := h.runHaul(nil, "b1", []step{memoResult("rates")}, nil)
	assert.Empty(t, ran)
}

func TestContentHashCoversTheResult(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	extract := output("extract", "raw", "")
	steps := []step{{cmd: extract}, memoLift("transform", "mart", 0)}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1"})
	content := map[string]string{"extract": "same bytes"}

	h.results = map[string]string{"extract": `{"rows":1}`}
	first, _ := h.runHaul(b, "b1", steps, content)
	_, ran := h.runHaul(b, "b1", steps, content)
	assert.Equal(t, []string{"extract"}, ran, "same files and result: transform is reused")

	h.results = map[string]string{"extract": `{"rows":2}`}
	id, ran := h.runHaul(b, "b1", steps, content)
	assert.Equal(t, []string{"extract", "transform"}, ran, "same files, another result")
	assert.Equal(t, "input changed", h.reasons(id)["transform"])
	hash := func(haulID string) string {
		for _, p := range h.pallets(haulID) {
			if p.Dataset == "raw" {
				return p.ContentHash
			}
		}
		t.Fatalf("haul %s built no raw pallet", haulID)
		return ""
	}
	assert.NotEqual(t, hash(first), hash(id))
}

// frameRun runs one haul whose lift (memoized, no output) builds a pallet
// in its implicit dataset, unless it is reused. It returns the haul and
// whether the lift ran.
func frameRun(t *testing.T, h *harness, b blob.Bucket) (string, bool) {
	t.Helper()
	cmd := lift("frame")
	cmd.Memo = "fingerprint"
	id := h.memoHaul("t", cmd)
	ran := false
	if h.lift(id).Status != state.LiftCompleted {
		task := h.poll(TaskLift)
		require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: task.ID, Status: state.AttemptSucceeded,
			Result: json.RawMessage(`{"$frame":"polars"}`), Output: writeOutput(t, b, task, map[string]string{"data.parquet": "x"})}))
		ran = true
		h.waitLift(id, state.LiftCompleted)
	}
	// Let the haul finish, so the next one's haul task is the first queued.
	ht := h.poll(TaskHaul)
	require.Equal(t, id, ht.HaulID)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: ht.ID, BuildID: "b1", Commands: seqs(cmd),
		Outcome: &HaulOutcome{Status: state.HaulCompleted}}))
	return id, ran
}

func TestMemoizedLiftReusesItsImplicitPallet(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	h.build("b1", map[string]string{"frame": "v1"})
	first, ran := frameRun(t, h, b)
	require.True(t, ran)
	h.waitLift(first, state.LiftCompleted)
	built := h.pallets(first)
	require.Len(t, built, 1)

	second, ran := frameRun(t, h, b)
	assert.False(t, ran)
	l := h.waitLift(second, state.LiftCompleted)
	assert.True(t, l.Reused)
	p := reusedEvent(t, h, second)
	assert.Equal(t, built[0].ID, p.Pallet, "the pallet stands in, so the haul gets a handle")

	// Once that pallet is invalid, the lift runs again.
	inv := built[0]
	inv.State, inv.Reason = state.PalletInvalid, "bad data"
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutPallet(inv) })
	require.NoError(t, err)
	third, ran := frameRun(t, h, b)
	assert.True(t, ran)
	assert.Contains(t, h.reasons(third)["frame"], "can't be read")
}

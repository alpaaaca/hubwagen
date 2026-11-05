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

// Steps without an output are memoized by their result.

func memoResult(name string) step {
	cmd := stepCmd(name)
	cmd.Memo = "fingerprint"
	return step{cmd: cmd}
}

func reusedEvent(t *testing.T, h *harness, runID string) stepReusedPayload {
	t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(t, err)
	for _, e := range evs {
		if e.Type == state.EventStepReused {
			var p stepReusedPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			return p
		}
	}
	t.Fatalf("run %s reused nothing", runID)
	return stepReusedPayload{}
}

func TestMemoizedResultIsReused(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	steps := []step{memoResult("rates")}
	h.results = map[string]string{"rates": `{"eur": 1.08}`}
	first, ran := h.runPipeline(nil, "b1", steps, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "no previous run", h.reasons(first)["rates"])

	h.results = map[string]string{"rates": `{"eur": 2}`} // what it would return if it ran
	second, ran := h.runPipeline(nil, "b1", steps, nil)
	assert.Empty(t, ran)
	l := h.step(second)
	assert.True(t, l.Reused)
	assert.Equal(t, 0, l.Attempts)
	assert.JSONEq(t, `{"eur": 1.08}`, string(l.Result), "the recorded result")
	assert.Equal(t, h.step(first).Fingerprint, l.Fingerprint)
	assert.Equal(t, stepReusedPayload{Seq: 1, Run: first, OnSeq: 1, Fingerprint: l.Fingerprint}, reusedEvent(t, h, second))
	assert.Empty(t, h.dataVersions(first), "no data version: the result is in the store")

	notMemo := steps[0]
	notMemo.cmd.Memo = ""
	_, ran = h.runPipeline(nil, "b1", []step{notMemo}, nil)
	assert.Equal(t, []string{"rates"}, ran, "without memo it runs every time")
}

func TestResultRerunReasons(t *testing.T) {
	h := newHarness(t)
	st := memoResult("rates")
	st.cmd.Args = json.RawMessage(`{"day":"2026-10-24"}`)
	h.build("b1", map[string]string{"rates": "v1"})
	h.runPipeline(nil, "b1", []step{st}, nil)

	other := st
	other.cmd.Args = json.RawMessage(`{"day":"2026-10-25"}`)
	id, ran := h.runPipeline(nil, "b1", []step{other}, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "no previous run", h.reasons(id)["rates"], "no earlier run with these arguments")

	h.build("b2", map[string]string{"rates": "v2"})
	id, ran = h.runPipeline(nil, "b2", []step{st}, nil)
	assert.Equal(t, []string{"rates"}, ran)
	assert.Equal(t, "code changed (v1 → v2)", h.reasons(id)["rates"])
}

func TestResultTTLCountsFromTheRun(t *testing.T) {
	h := newHarness(t)
	st := memoResult("fetch")
	st.cmd.Memo = "ttl:1h"
	h.build("b1", map[string]string{"fetch": "v1"})
	h.runPipeline(nil, "b1", []step{st}, nil)

	h.advance(40 * time.Minute)
	_, ran := h.runPipeline(nil, "b1", []step{st}, nil)
	assert.Empty(t, ran, "younger than the TTL")

	h.advance(40 * time.Minute) // 80 minutes after the run, 40 after the reuse
	id, ran := h.runPipeline(nil, "b1", []step{st}, nil)
	assert.Equal(t, []string{"fetch"}, ran, "a reuse doesn't renew the result")
	assert.Equal(t, "expired (older than 1h0m0s)", h.reasons(id)["fetch"])
}

func TestFailedStepResultIsNotReused(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	first := h.memoRun("t", memoResult("rates").cmd)
	a := h.poll(TaskStep)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptFailed, Reason: "boom"}))
	h.waitStep(first, state.StepFailed)

	_, ran := h.runPipeline(nil, "b1", []step{memoResult("rates")}, nil)
	assert.Equal(t, []string{"rates"}, ran)
}

func TestConcurrentRunsRunAResultOnce(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"extract": "v1"})
	cmd := stepCmd("extract")
	cmd.Memo = "fingerprint"
	first := h.memoRun("t", cmd)
	second := h.memoRun("t", cmd)

	a := h.poll(TaskStep)
	assert.Equal(t, first, a.RunID)
	h.pollNothing()
	assert.Equal(t, first, waitingOn(t, h, second).Run)

	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":3}`)}))
	h.waitStep(first, state.StepCompleted)
	got := h.waitStep(second, state.StepCompleted)
	assert.Equal(t, 0, got.Attempts, "it never ran")
	assert.True(t, got.Reused)
	assert.JSONEq(t, `{"rows":3}`, string(got.Result))
}

func TestMemoizedResultSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"rates": "v1"})
	h.runPipeline(nil, "b1", []step{memoResult("rates")}, nil)

	h.restart()
	_, ran := h.runPipeline(nil, "b1", []step{memoResult("rates")}, nil)
	assert.Empty(t, ran)
}

func TestContentHashCoversTheResult(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	extract := output("extract", "raw", "")
	steps := []step{{cmd: extract}, memoStep("transform", "mart", 0)}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1"})
	content := map[string]string{"extract": "same bytes"}

	h.results = map[string]string{"extract": `{"rows":1}`}
	first, _ := h.runPipeline(b, "b1", steps, content)
	_, ran := h.runPipeline(b, "b1", steps, content)
	assert.Equal(t, []string{"extract"}, ran, "same files and result: transform is reused")

	h.results = map[string]string{"extract": `{"rows":2}`}
	id, ran := h.runPipeline(b, "b1", steps, content)
	assert.Equal(t, []string{"extract", "transform"}, ran, "same files, another result")
	assert.Equal(t, "input changed", h.reasons(id)["transform"])
	hash := func(runID string) string {
		for _, p := range h.dataVersions(runID) {
			if p.Dataset == "raw" {
				return p.ContentHash
			}
		}
		t.Fatalf("run %s built no raw data version", runID)
		return ""
	}
	assert.NotEqual(t, hash(first), hash(id))
}

// frameRun runs one run whose step (memoized, no output) builds a data version
// in its implicit dataset, unless it is reused. It returns the run and
// whether the step ran.
func frameRun(t *testing.T, h *harness, b blob.Bucket) (string, bool) {
	t.Helper()
	cmd := stepCmd("frame")
	cmd.Memo = "fingerprint"
	id := h.memoRun("t", cmd)
	ran := false
	if h.step(id).Status != state.StepCompleted {
		task := h.poll(TaskStep)
		require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: task.ID, Status: state.AttemptSucceeded,
			Result: json.RawMessage(`{"$frame":"polars"}`), Output: writeOutput(t, b, task, map[string]string{"data.parquet": "x"})}))
		ran = true
		h.waitStep(id, state.StepCompleted)
	}
	// Let the run finish, so the next one's run task is the first queued.
	ht := h.poll(TaskRun)
	require.Equal(t, id, ht.RunID)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: ht.ID, BuildID: "b1", Commands: seqs(cmd),
		Outcome: &RunOutcome{Status: state.RunCompleted}}))
	return id, ran
}

func TestMemoizedStepReusesItsImplicitDataVersion(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	h.build("b1", map[string]string{"frame": "v1"})
	first, ran := frameRun(t, h, b)
	require.True(t, ran)
	h.waitStep(first, state.StepCompleted)
	built := h.dataVersions(first)
	require.Len(t, built, 1)

	second, ran := frameRun(t, h, b)
	assert.False(t, ran)
	l := h.waitStep(second, state.StepCompleted)
	assert.True(t, l.Reused)
	p := reusedEvent(t, h, second)
	assert.Equal(t, built[0].ID, p.DataVersion, "the data version stands in, so the run gets a handle")

	// Once that data version is invalid, the step runs again.
	inv := built[0]
	inv.State, inv.Reason = state.DataVersionInvalid, "bad data"
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutDataVersion(inv) })
	require.NoError(t, err)
	third, ran := frameRun(t, h, b)
	assert.True(t, ran)
	assert.Contains(t, h.reasons(third)["frame"], "can't be read")
}

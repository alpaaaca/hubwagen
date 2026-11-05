package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dataVersionOf(t *testing.T, h *harness, runID, dataset string) state.DataVersion {
	t.Helper()
	for _, p := range h.dataVersions(runID) {
		if p.Dataset == dataset {
			return p
		}
	}
	t.Fatalf("run %s built no %s data version", runID, dataset)
	return state.DataVersion{}
}

func TestInvalidationMarksDownstreamAndForcesARerun(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw"), memoStep("transform", "mart", 0), memoStep("report", "reports", 1)}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1", "report": "v1"})
	first, _ := h.runPipeline(b, "b1", steps, nil)
	mart := dataVersionOf(t, h, first, "mart")

	dry, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: mart.ID, Reason: "bad fx rates", DryRun: true})
	require.NoError(t, err)
	assert.Len(t, dry.DataVersions, 2, "mart and the report built from it, not raw")
	assert.Equal(t, mart.ID, dry.DataVersions[0].ID, "the data version itself first")
	require.Len(t, dry.Runs, 1)
	assert.Equal(t, first, dry.Runs[0].ID)
	assert.Equal(t, state.DataVersionCommitted, dataVersionOf(t, h, first, "mart").State, "a dry run marks nothing")

	res, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: mart.ID, Reason: "bad fx rates", By: "ana"})
	require.NoError(t, err)
	assert.Len(t, res.DataVersions, 2)
	got := dataVersionOf(t, h, first, "mart")
	assert.Equal(t, state.DataVersionInvalid, got.State)
	assert.Equal(t, "bad fx rates", got.Reason)
	rep := dataVersionOf(t, h, first, "reports")
	assert.Equal(t, state.DataVersionInvalid, rep.State)
	assert.Equal(t, "built from invalidated version "+mart.ID+": bad fx rates", rep.Reason)
	assert.Equal(t, state.DataVersionCommitted, dataVersionOf(t, h, first, "raw").State, "upstream stays")

	evs, err := h.store.EventsSince(h.ctx, 0, "", 1<<20)
	require.NoError(t, err)
	var marked []dataVersionInvalidatedPayload
	for _, e := range evs {
		if e.Type == state.EventDataVersionInvalidated {
			assert.Empty(t, e.RunID, "not in any run's event log")
			var p dataVersionInvalidatedPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			marked = append(marked, p)
		}
	}
	require.Len(t, marked, 2)
	assert.Equal(t, mart.ID, marked[0].Cause)
	assert.Equal(t, "ana", marked[0].By)

	// The replay reruns exactly what was invalidated.
	second, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Equal(t, []string{"transform", "report"}, ran)
	assert.Equal(t, "reused", h.reasons(second)["extract"])
	assert.Equal(t, "earlier data version invalidated", h.reasons(second)["transform"], "the rerun says why")
	latest, err := h.store.LatestDataVersion(h.ctx, "t", "mart", "")
	require.NoError(t, err)
	assert.Equal(t, dataVersionOf(t, h, second, "mart").ID, latest.ID)

	again, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: mart.ID, Reason: "again"})
	require.NoError(t, err)
	assert.Empty(t, again.DataVersions, "already invalid: nothing to mark")
}

func TestInvalidatingADataVersionBeingPromoted(t *testing.T) {
	b := newBucket(t)
	g := &gate{release: make(chan struct{}), started: make(chan string, 1)}
	h := newHarness(t, withBucket(b, func(ps DataVersionStore) DataVersionStore { g.DataVersionStore = ps; return g }))
	id, a := h.startStep(output("extract", "raw", ""))
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	p := <-g.started

	res, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: p, Reason: "wrong source"})
	require.NoError(t, err)
	require.Len(t, res.DataVersions, 1)
	close(g.release)
	h.waitStep(id, state.StepCompleted)
	got := h.dataVersions(id)[0]
	assert.Equal(t, state.DataVersionInvalid, got.State, "the promote didn't make it committed")
	assert.Equal(t, "wrong source", got.Reason)
	_, err = h.store.LatestDataVersion(h.ctx, "t", "raw", "")
	assert.ErrorIs(t, err, state.ErrNotFound)
}

func TestInvalidatedWhilePromotingSurvivesRestart(t *testing.T) {
	b := newBucket(t)
	g := &gate{release: make(chan struct{}), started: make(chan string, 1)}
	h := newHarness(t, withBucket(b, func(ps DataVersionStore) DataVersionStore { g.DataVersionStore = ps; return g }))
	id, a := h.startStep(output("extract", "raw", ""))
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	p := <-g.started
	_, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: p, Reason: "wrong source"})
	require.NoError(t, err)

	require.NoError(t, h.stop()) // the promote never finished
	h.restart(withBucket(b, nil))
	h.waitStep(id, state.StepCompleted)
	got := h.dataVersions(id)[0]
	assert.Equal(t, state.DataVersionInvalid, got.State)
	assert.Equal(t, "wrong source", got.Reason)
}

func TestInvalidateErrors(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: "nope", Reason: "x"})
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = h.core.InvalidateDataVersion(h.ctx, InvalidateRequest{DataVersionID: "nope"})
	assert.ErrorIs(t, err, ErrInvalid)
}

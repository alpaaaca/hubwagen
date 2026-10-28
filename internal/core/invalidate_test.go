package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func palletOf(t *testing.T, h *harness, haulID, dataset string) state.Pallet {
	t.Helper()
	for _, p := range h.pallets(haulID) {
		if p.Dataset == dataset {
			return p
		}
	}
	t.Fatalf("haul %s built no %s pallet", haulID, dataset)
	return state.Pallet{}
}

func TestInvalidationMarksDownstreamAndForcesARerun(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoLift("extract", "raw"), memoLift("transform", "mart", 0), memoLift("report", "reports", 1)}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1", "report": "v1"})
	first, _ := h.runHaul(b, "b1", steps, nil)
	mart := palletOf(t, h, first, "mart")

	dry, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: mart.ID, Reason: "bad fx rates", DryRun: true})
	require.NoError(t, err)
	assert.Len(t, dry.Pallets, 2, "mart and the report built from it, not raw")
	assert.Equal(t, mart.ID, dry.Pallets[0].ID, "the pallet itself first")
	require.Len(t, dry.Hauls, 1)
	assert.Equal(t, first, dry.Hauls[0].ID)
	assert.Equal(t, state.PalletCommitted, palletOf(t, h, first, "mart").State, "a dry run marks nothing")

	res, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: mart.ID, Reason: "bad fx rates", By: "ana"})
	require.NoError(t, err)
	assert.Len(t, res.Pallets, 2)
	got := palletOf(t, h, first, "mart")
	assert.Equal(t, state.PalletInvalid, got.State)
	assert.Equal(t, "bad fx rates", got.Reason)
	rep := palletOf(t, h, first, "reports")
	assert.Equal(t, state.PalletInvalid, rep.State)
	assert.Equal(t, "built from invalidated pallet "+mart.ID+": bad fx rates", rep.Reason)
	assert.Equal(t, state.PalletCommitted, palletOf(t, h, first, "raw").State, "upstream stays")

	evs, err := h.store.EventsSince(h.ctx, 0, "", 1<<20)
	require.NoError(t, err)
	var marked []palletInvalidatedPayload
	for _, e := range evs {
		if e.Type == state.EventPalletInvalidated {
			assert.Empty(t, e.HaulID, "not in any haul's logbook")
			var p palletInvalidatedPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			marked = append(marked, p)
		}
	}
	require.Len(t, marked, 2)
	assert.Equal(t, mart.ID, marked[0].Cause)
	assert.Equal(t, "ana", marked[0].By)

	// The replay reruns exactly what was invalidated.
	second, ran := h.runHaul(b, "b1", steps, nil)
	assert.Equal(t, []string{"transform", "report"}, ran)
	assert.Equal(t, "reused", h.reasons(second)["extract"])
	assert.Equal(t, "earlier pallet invalidated", h.reasons(second)["transform"], "the rerun says why")
	latest, err := h.store.LatestPallet(h.ctx, "t", "mart", "")
	require.NoError(t, err)
	assert.Equal(t, palletOf(t, h, second, "mart").ID, latest.ID)

	again, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: mart.ID, Reason: "again"})
	require.NoError(t, err)
	assert.Empty(t, again.Pallets, "already invalid: nothing to mark")
}

func TestInvalidatingAPalletBeingPromoted(t *testing.T) {
	b := newBucket(t)
	g := &gate{release: make(chan struct{}), started: make(chan string, 1)}
	h := newHarness(t, withBucket(b, func(ps PalletStore) PalletStore { g.PalletStore = ps; return g }))
	id, a := h.startLift(output("extract", "raw", ""))
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	p := <-g.started

	res, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: p, Reason: "wrong source"})
	require.NoError(t, err)
	require.Len(t, res.Pallets, 1)
	close(g.release)
	h.waitLift(id, state.LiftCompleted)
	got := h.pallets(id)[0]
	assert.Equal(t, state.PalletInvalid, got.State, "the promote didn't make it committed")
	assert.Equal(t, "wrong source", got.Reason)
	_, err = h.store.LatestPallet(h.ctx, "t", "raw", "")
	assert.ErrorIs(t, err, state.ErrNotFound)
}

func TestInvalidatedWhilePromotingSurvivesRestart(t *testing.T) {
	b := newBucket(t)
	g := &gate{release: make(chan struct{}), started: make(chan string, 1)}
	h := newHarness(t, withBucket(b, func(ps PalletStore) PalletStore { g.PalletStore = ps; return g }))
	id, a := h.startLift(output("extract", "raw", ""))
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	p := <-g.started
	_, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: p, Reason: "wrong source"})
	require.NoError(t, err)

	require.NoError(t, h.stop()) // the promote never finished
	h.restart(withBucket(b, nil))
	h.waitLift(id, state.LiftCompleted)
	got := h.pallets(id)[0]
	assert.Equal(t, state.PalletInvalid, got.State)
	assert.Equal(t, "wrong source", got.Reason)
}

func TestInvalidateErrors(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: "nope", Reason: "x"})
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = h.core.InvalidatePallet(h.ctx, InvalidateRequest{PalletID: "nope"})
	assert.ErrorIs(t, err, ErrInvalid)
}

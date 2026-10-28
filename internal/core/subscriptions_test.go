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

// commitPallet runs a haul "x" whose one lift commits a pallet to dataset,
// and returns the pallet's ID. The haul is left waiting for its next replay.
func (h *harness) commitPallet(b blob.Bucket, dataset string) string {
	h.t.Helper()
	id, a := h.startLift(output("extract", dataset, ""))
	require.NoError(h.t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(h.t, b, a, map[string]string{"a.csv": dataset + id})}))
	h.waitLift(id, state.LiftCompleted)
	h.cancelAll(TaskHaul) // its next replay: not needed
	ps := h.pallets(id)
	require.Len(h.t, ps, 1)
	return ps[0].ID
}

// cancelAll takes the queued tasks of kind and cancels their hauls.
func (h *harness) cancelAll(kind TaskKind) {
	h.t.Helper()
	for {
		t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}, Kinds: []TaskKind{kind}})
		require.NoError(h.t, err)
		if !ok {
			return
		}
		require.NoError(h.t, h.core.CancelHaul(h.ctx, t.HaulID, "test"))
	}
}

func (h *harness) subscribe(spec SubscriptionSpec) {
	h.t.Helper()
	h.ids++
	require.NoError(h.t, h.core.RegisterBuild(h.ctx, BuildRequest{
		ID: "sub-" + time.Duration(h.ids).String(), Hauls: []string{"y"}, Subscriptions: []SubscriptionSpec{spec}}))
}

// started returns the SubscriptionStarted payloads so far.
func (h *harness) started() []subscriptionStartedPayload {
	h.t.Helper()
	evs, err := h.store.EventsSince(h.ctx, 0, "", 1<<20)
	require.NoError(h.t, err)
	var out []subscriptionStartedPayload
	for _, e := range evs {
		if e.Type == state.EventSubscriptionStarted {
			var p subscriptionStartedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			out = append(out, p)
		}
	}
	return out
}

func newSubscriptionHarness(t *testing.T) (*harness, blob.Bucket) {
	b := newBucket(t)
	return newHarness(t, withBucket(b, nil)), b
}

func TestSubscriptionDebounces(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Haul: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	p1 := h.commitPallet(b, "raw.orders")
	h.commitPallet(b, "other.data") // not subscribed
	h.advance(30 * time.Second)
	p2 := h.commitPallet(b, "raw.orders")
	h.advance(59 * time.Second)
	assert.Empty(t, h.started(), "quiet for less than a minute")

	h.advance(time.Second)
	got := h.started()
	require.Len(t, got, 1)
	assert.Equal(t, []string{p1, p2}, got[0].Pallets)
	started := h.haul(got[0].HaulID)
	assert.Equal(t, "y", started.Name)
	assert.Equal(t, "subscription", started.Trigger)
	pallets := started.Params["pallets"].([]any)
	assert.Equal(t, map[string]any{"$pallet": p1, "dataset": "raw.orders", "partition": ""}, pallets[0])

	waiting, err := h.store.ListSubscriptionPallets(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, waiting, "the batch is cleared")
}

func TestSubscriptionMaxBatchAndMaxWait(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Haul: "y", Datasets: []string{"raw.orders"}, Debounce: time.Hour, MaxBatch: 2})
	h.commitPallet(b, "raw.orders")
	h.commitPallet(b, "raw.orders")
	h.barrier()
	require.Len(t, h.started(), 1, "full: started at once")
	h.cancelAll(TaskHaul)

	h.subscribe(SubscriptionSpec{Haul: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute, MaxWait: 2 * time.Minute})
	for range 3 {
		h.commitPallet(b, "raw.orders")
		h.advance(50 * time.Second) // never quiet for a minute
	}
	got := h.started()
	require.Len(t, got, 2, "started after max_wait though pallets kept coming")
	assert.Len(t, got[1].Pallets, 3)
}

func TestAHaulDoesntStartItself(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "loop", Hauls: []string{"x"},
		Subscriptions: []SubscriptionSpec{{Haul: "x", Datasets: []string{"raw.orders"}}}}))
	h.commitPallet(b, "raw.orders") // committed by haul x
	h.advance(time.Second)
	assert.Empty(t, h.started())
}

func TestSubscriptionBatchSurvivesRestart(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Haul: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	p1 := h.commitPallet(b, "raw.orders")
	h.restart(withBucket(b, nil))
	assert.Empty(t, h.started())
	h.advance(time.Minute)
	got := h.started()
	require.Len(t, got, 1)
	assert.Equal(t, []string{p1}, got[0].Pallets)
}

func TestNewestBuildDecidesTheSubscription(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Haul: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	h.commitPallet(b, "raw.orders")
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "no-sub", Hauls: []string{"y"}}))
	h.advance(time.Hour)
	assert.Empty(t, h.started(), "removed, with what was waiting")
	subs, err := h.store.ListSubscriptions(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestSubscriptionValidation(t *testing.T) {
	h := newHarness(t)
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "bad", Hauls: []string{"y"}, Subscriptions: []SubscriptionSpec{
		{Haul: "nope", Datasets: []string{"a"}},
		{Haul: "y"},
		{Haul: "y", Datasets: []string{"a"}, Debounce: time.Minute, MaxWait: time.Second},
	}})
	require.ErrorIs(t, err, ErrInvalid)
	for _, want := range []string{`subscriptions[0]: haul "nope" isn't one of the build's hauls`,
		"subscriptions[1] (y): needs dataset names", `subscriptions[2]: haul "y" has two subscriptions`,
		"max_wait ≥ debounce"} {
		assert.ErrorContains(t, err, want)
	}
}

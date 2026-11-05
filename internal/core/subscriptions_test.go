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

// commitDataVersion runs a run "x" whose one step commits a data version to dataset,
// and returns the data version's ID. The run is left waiting for its next replay.
func (h *harness) commitDataVersion(b blob.Bucket, dataset string) string {
	h.t.Helper()
	id, a := h.startStep(output("extract", dataset, ""))
	require.NoError(h.t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(h.t, b, a, map[string]string{"a.csv": dataset + id})}))
	h.waitStep(id, state.StepCompleted)
	h.cancelAll(TaskRun) // its next replay: not needed
	ps := h.dataVersions(id)
	require.Len(h.t, ps, 1)
	return ps[0].ID
}

// cancelAll takes the queued tasks of kind and cancels their runs.
func (h *harness) cancelAll(kind TaskKind) {
	h.t.Helper()
	for {
		t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}, Kinds: []TaskKind{kind}})
		require.NoError(h.t, err)
		if !ok {
			return
		}
		require.NoError(h.t, h.core.CancelRun(h.ctx, t.RunID, "test"))
	}
}

func (h *harness) subscribe(spec SubscriptionSpec) {
	h.t.Helper()
	h.ids++
	require.NoError(h.t, h.core.RegisterBuild(h.ctx, BuildRequest{
		ID: "sub-" + time.Duration(h.ids).String(), Pipelines: []string{"y"}, Subscriptions: []SubscriptionSpec{spec}}))
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
	h.subscribe(SubscriptionSpec{Pipeline: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	p1 := h.commitDataVersion(b, "raw.orders")
	h.commitDataVersion(b, "other.data") // not subscribed
	h.advance(30 * time.Second)
	p2 := h.commitDataVersion(b, "raw.orders")
	h.advance(59 * time.Second)
	assert.Empty(t, h.started(), "quiet for less than a minute")

	h.advance(time.Second)
	got := h.started()
	require.Len(t, got, 1)
	assert.Equal(t, []string{p1, p2}, got[0].DataVersions)
	started := h.run(got[0].RunID)
	assert.Equal(t, "y", started.Name)
	assert.Equal(t, "subscription", started.Trigger)
	dataVersions := started.Params["versions"].([]any)
	assert.Equal(t, map[string]any{"$data_version": p1, "dataset": "raw.orders", "partition": ""}, dataVersions[0])

	waiting, err := h.store.ListSubscriptionDataVersions(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, waiting, "the batch is cleared")
}

func TestSubscriptionMaxBatchAndMaxWait(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Pipeline: "y", Datasets: []string{"raw.orders"}, Debounce: time.Hour, MaxBatch: 2})
	h.commitDataVersion(b, "raw.orders")
	h.commitDataVersion(b, "raw.orders")
	h.barrier()
	require.Len(t, h.started(), 1, "full: started at once")
	h.cancelAll(TaskRun)

	h.subscribe(SubscriptionSpec{Pipeline: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute, MaxWait: 2 * time.Minute})
	for range 3 {
		h.commitDataVersion(b, "raw.orders")
		h.advance(50 * time.Second) // never quiet for a minute
	}
	got := h.started()
	require.Len(t, got, 2, "started after max_wait though data versions kept coming")
	assert.Len(t, got[1].DataVersions, 3)
}

func TestARunDoesntStartItself(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "loop", Pipelines: []string{"x"},
		Subscriptions: []SubscriptionSpec{{Pipeline: "x", Datasets: []string{"raw.orders"}}}}))
	h.commitDataVersion(b, "raw.orders") // committed by run x
	h.advance(time.Second)
	assert.Empty(t, h.started())
}

func TestSubscriptionBatchSurvivesRestart(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Pipeline: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	p1 := h.commitDataVersion(b, "raw.orders")
	h.restart(withBucket(b, nil))
	assert.Empty(t, h.started())
	h.advance(time.Minute)
	got := h.started()
	require.Len(t, got, 1)
	assert.Equal(t, []string{p1}, got[0].DataVersions)
}

func TestNewestBuildDecidesTheSubscription(t *testing.T) {
	h, b := newSubscriptionHarness(t)
	h.subscribe(SubscriptionSpec{Pipeline: "y", Datasets: []string{"raw.orders"}, Debounce: time.Minute})
	h.commitDataVersion(b, "raw.orders")
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "no-sub", Pipelines: []string{"y"}}))
	h.advance(time.Hour)
	assert.Empty(t, h.started(), "removed, with what was waiting")
	subs, err := h.store.ListSubscriptions(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestSubscriptionValidation(t *testing.T) {
	h := newHarness(t)
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "bad", Pipelines: []string{"y"}, Subscriptions: []SubscriptionSpec{
		{Pipeline: "nope", Datasets: []string{"a"}},
		{Pipeline: "y"},
		{Pipeline: "y", Datasets: []string{"a"}, Debounce: time.Minute, MaxWait: time.Second},
	}})
	require.ErrorIs(t, err, ErrInvalid)
	for _, want := range []string{`subscriptions[0]: pipeline "nope" isn't one of the build's pipelines`,
		"subscriptions[1] (y): needs dataset names", `subscriptions[2]: pipeline "y" has two subscriptions`,
		"max_wait ≥ debounce"} {
		assert.ErrorContains(t, err, want)
	}
}

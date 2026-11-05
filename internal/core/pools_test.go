package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (h *harness) demand() map[string]QueueDemand {
	h.t.Helper()
	d, err := h.core.Demand(h.ctx)
	require.NoError(h.t, err)
	return d
}

func TestDemandIsWorkThatWaitsForAWorker(t *testing.T) {
	h := newHarness(t)
	h.startDaily()
	h.queue(TaskStep, "gpu", "l1")
	h.queue(TaskStep, "gpu", "l2")
	assert.Equal(t, map[string]QueueDemand{DefaultQueue: {RunTasks: 1}, "gpu": {Steps: 2}}, h.demand())

	// Workers whose code doesn't define the pipeline: more of them wouldn't help.
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "other", Pipelines: []string{"x"}}))
	h.startWorker("w1", PollRequest{BuildID: "other", Queues: []string{DefaultQueue}, Kinds: []TaskKind{TaskRun}}, nil)
	assert.Equal(t, map[string]QueueDemand{"gpu": {Steps: 2}}, h.demand())
}

func TestARunWaitsForAStartingPool(t *testing.T) {
	h := newHarness(t)
	id := h.startDaily()
	require.NoError(t, h.core.SetPools(h.ctx, []PoolStatus{
		{Name: "gpu", Queues: []string{"gpu"}, Max: 1, Starting: 1},
		{Name: "py", Queues: []string{DefaultQueue}, Max: 3, Workers: []string{"py-1"}, Starting: 1},
	}))
	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitStarting, w.Reason)
	assert.Equal(t, "py", w.Pool)
	assert.Equal(t, map[string]QueueDemand{DefaultQueue: {RunTasks: 1}}, h.demand(), "still demand: the pool counts it")

	pools, err := h.core.Pools(h.ctx)
	require.NoError(t, err)
	assert.Len(t, pools, 2)

	// Once it started, the run waits for what it waited for before.
	require.NoError(t, h.core.SetPools(h.ctx, []PoolStatus{{Name: "py", Queues: []string{DefaultQueue}, Max: 3}}))
	w, _ = h.waiting(id)
	assert.Equal(t, WaitNoWorker, w.Reason)
}

func TestWorkersSayHowMuchTheyRun(t *testing.T) {
	h := newHarness(t)
	h.startDaily()
	h.pollOnce(PollRequest{WorkerID: "w1", BuildID: "test-runs", Queues: []string{DefaultQueue}})
	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	assert.Equal(t, 1, workers[0].Running)
}

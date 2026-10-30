package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (h *harness) demand() map[string]DockDemand {
	h.t.Helper()
	d, err := h.core.Demand(h.ctx)
	require.NoError(h.t, err)
	return d
}

func TestDemandIsWorkThatWaitsForAWorker(t *testing.T) {
	h := newHarness(t)
	h.startDaily()
	h.queue(TaskLift, "gpu", "l1")
	h.queue(TaskLift, "gpu", "l2")
	assert.Equal(t, map[string]DockDemand{DefaultDock: {HaulTasks: 1}, "gpu": {Lifts: 2}}, h.demand())

	// Workers whose code doesn't define the pipeline: more of them wouldn't help.
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "other", Hauls: []string{"x"}}))
	h.startWorker("w1", PollRequest{BuildID: "other", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskHaul}}, nil)
	assert.Equal(t, map[string]DockDemand{"gpu": {Lifts: 2}}, h.demand())
}

func TestARunWaitsForAStartingPool(t *testing.T) {
	h := newHarness(t)
	id := h.startDaily()
	require.NoError(t, h.core.SetPools(h.ctx, []PoolStatus{
		{Name: "gpu", Docks: []string{"gpu"}, Max: 1, Starting: 1},
		{Name: "py", Docks: []string{DefaultDock}, Max: 3, Workers: []string{"py-1"}, Starting: 1},
	}))
	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitStarting, w.Reason)
	assert.Equal(t, "py", w.Pool)
	assert.Equal(t, map[string]DockDemand{DefaultDock: {HaulTasks: 1}}, h.demand(), "still demand: the pool counts it")

	pools, err := h.core.Pools(h.ctx)
	require.NoError(t, err)
	assert.Len(t, pools, 2)

	// Once it started, the run waits for what it waited for before.
	require.NoError(t, h.core.SetPools(h.ctx, []PoolStatus{{Name: "py", Docks: []string{DefaultDock}, Max: 3}}))
	w, _ = h.waiting(id)
	assert.Equal(t, WaitNoWorker, w.Reason)
}

func TestWorkersSayHowMuchTheyRun(t *testing.T) {
	h := newHarness(t)
	h.startDaily()
	h.pollOnce(PollRequest{WorkerID: "w1", BuildID: "test-hauls", Docks: []string{DefaultDock}})
	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	assert.Equal(t, 1, workers[0].Running)
}

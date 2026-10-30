package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (h *harness) waiting(haulID string) (Waiting, bool) {
	h.t.Helper()
	ws, err := h.core.Waiting(h.ctx, []string{haulID})
	require.NoError(h.t, err)
	w, ok := ws[haulID]
	return w, ok
}

func (h *harness) startDaily() string {
	h.t.Helper()
	res, err := h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "daily_revenue"})
	require.NoError(h.t, err)
	return res.HaulID
}

// pollOnce polls without waiting, as a worker between tasks does.
func (h *harness) pollOnce(req PollRequest) {
	h.t.Helper()
	_, _, err := h.core.Poll(h.ctx, req)
	require.NoError(h.t, err)
}

func TestWaitingWithoutWorker(t *testing.T) {
	h := newHarness(t)
	id := h.startDaily()
	h.advance(10 * time.Second)

	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, Waiting{Reason: WaitNoWorker, Kind: TaskHaul, Name: "daily_revenue", Dock: DefaultDock, Since: t0, Queued: 1}, w)

	// A worker that takes the haul task ends the wait.
	wk := h.startWorker("w1", PollRequest{BuildID: "test-hauls", Docks: []string{DefaultDock}}, nil)
	assert.Equal(t, id, wk.next(t).HaulID)
	_, ok = h.waiting(id)
	assert.False(t, ok)
}

func TestWaitingForABuildThatDefinesIt(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "other", Hauls: []string{"x"}}))
	h.startWorker("w1", PollRequest{BuildID: "other", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskHaul}}, nil)
	// One on another dock, and one taking lifts only, don't count.
	h.startWorker("w2", PollRequest{BuildID: "test-hauls", Docks: []string{"gpu"}}, nil)
	h.startWorker("w3", PollRequest{BuildID: "test-hauls", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskLift}}, nil)
	id := h.startDaily()

	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitNoBuild, w.Reason)
	assert.Equal(t, 1, w.Workers)
	assert.Equal(t, []string{"other"}, w.Builds)
}

func TestWaitingForABusyWorker(t *testing.T) {
	h := newHarness(t)
	h.pollOnce(PollRequest{WorkerID: "w1", BuildID: "test-hauls", Docks: []string{DefaultDock}})
	id := h.startDaily()

	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitBusy, w.Reason)
	assert.Equal(t, []string{"test-hauls"}, w.Builds)

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	assert.Equal(t, []Worker{{ID: "w1", BuildID: "test-hauls", Docks: []string{DefaultDock},
		Kinds: []TaskKind{TaskHaul, TaskLift}, LastSeen: t0}}, workers)
}

func TestWorkersExpireUnlessTheyHeartbeat(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.WorkerExpiry = time.Minute })
	live := func() []string {
		t.Helper()
		workers, err := h.core.Workers(h.ctx)
		require.NoError(t, err)
		var ids []string
		for _, w := range workers {
			ids = append(ids, w.ID)
		}
		return ids
	}
	// w1 has nothing to run; w2 takes a haul task.
	h.pollOnce(PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}})
	h.startDaily()
	h.pollOnce(PollRequest{WorkerID: "w2", Docks: []string{DefaultDock}})

	// A worker that neither polls nor runs anything has stopped.
	h.advance(10 * time.Second)
	assert.Equal(t, []string{"w2"}, live())

	// One running a task stays while it heartbeats.
	h.advance(40 * time.Second)
	_, err := h.core.Heartbeat(h.ctx, "w2", nil)
	require.NoError(t, err)
	h.advance(40 * time.Second)
	assert.Equal(t, []string{"w2"}, live())
	h.advance(30 * time.Second)
	assert.Empty(t, live())

	id := h.startDaily()
	w, _ := h.waiting(id)
	assert.Equal(t, WaitNoWorker, w.Reason)
}

func TestAPollingWorkerStaysConnected(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.WorkerExpiry = time.Minute })
	h.startWorker("w1", PollRequest{Docks: []string{"gpu"}, Wait: 24 * time.Hour}, nil)
	h.advance(2 * time.Hour)

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	assert.True(t, workers[0].Polling)
}

func TestWaitingForTheDockLimit(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Docks = map[string]DockConfig{"gpu": {MaxRunningLifts: 1}} })
	wk := h.startWorker("w1", PollRequest{Docks: []string{"gpu"}}, nil)
	h.queue(TaskLift, "gpu", "l1")
	h.queue(TaskLift, "gpu", "l2")
	assert.Equal(t, "l1", wk.next(t).ID)

	w, ok := h.waiting("h-l2")
	require.True(t, ok)
	assert.Equal(t, WaitDockLimit, w.Reason)
	assert.Equal(t, TaskLift, w.Kind)
	assert.Equal(t, "fn", w.Name)
}

func TestWaitingTellsTheHaulTaskFirst(t *testing.T) {
	h := newHarness(t)
	h.do(func(c *Core) {
		c.enqueue(Task{Kind: TaskLift, ID: "l1", Dock: "default", HaulID: "h1", Name: "load", Seq: 2}, nil)
		c.enqueue(Task{Kind: TaskLift, ID: "l0", Dock: "default", HaulID: "h1", Name: "fetch", Seq: 1}, nil)
	})
	w, _ := h.waiting("h1")
	assert.Equal(t, "load", w.Name, "the oldest lift")
	assert.Equal(t, 2, w.Queued)

	h.do(func(c *Core) {
		c.enqueue(Task{Kind: TaskHaul, ID: "ht", Dock: "default", HaulID: "h1", Name: "pipeline"}, nil)
	})
	w, _ = h.waiting("h1")
	assert.Equal(t, TaskHaul, w.Kind)
	assert.Equal(t, 3, w.Queued)
}

func TestAWorkerThatHangsUpIsGone(t *testing.T) {
	h := newHarness(t)
	w := h.startWorker("w1", PollRequest{Docks: []string{DefaultDock}}, nil)
	w.stop()

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, workers)
}

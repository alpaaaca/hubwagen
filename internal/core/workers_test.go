package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (h *harness) waiting(runID string) (Waiting, bool) {
	h.t.Helper()
	ws, err := h.core.Waiting(h.ctx, []string{runID})
	require.NoError(h.t, err)
	w, ok := ws[runID]
	return w, ok
}

func (h *harness) startDaily() string {
	h.t.Helper()
	res, err := h.core.StartRun(h.ctx, StartRunRequest{Tenant: "t", Name: "daily_revenue"})
	require.NoError(h.t, err)
	return res.RunID
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
	assert.Equal(t, Waiting{Reason: WaitNoWorker, Kind: TaskRun, Name: "daily_revenue", Queue: DefaultQueue, Since: t0, Queued: 1}, w)

	// A worker that takes the run task ends the wait.
	wk := h.startWorker("w1", PollRequest{BuildID: "test-runs", Queues: []string{DefaultQueue}}, nil)
	assert.Equal(t, id, wk.next(t).RunID)
	_, ok = h.waiting(id)
	assert.False(t, ok)
}

func TestWaitingForABuildThatDefinesIt(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "other", Pipelines: []string{"x"}}))
	h.startWorker("w1", PollRequest{BuildID: "other", Queues: []string{DefaultQueue}, Kinds: []TaskKind{TaskRun}}, nil)
	// One on another queue, and one taking steps only, don't count.
	h.startWorker("w2", PollRequest{BuildID: "test-runs", Queues: []string{"gpu"}}, nil)
	h.startWorker("w3", PollRequest{BuildID: "test-runs", Queues: []string{DefaultQueue}, Kinds: []TaskKind{TaskStep}}, nil)
	id := h.startDaily()

	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitNoBuild, w.Reason)
	assert.Equal(t, 1, w.Workers)
	assert.Equal(t, []string{"other"}, w.Builds)
}

func TestWaitingForABusyWorker(t *testing.T) {
	h := newHarness(t)
	h.pollOnce(PollRequest{WorkerID: "w1", BuildID: "test-runs", Queues: []string{DefaultQueue}})
	id := h.startDaily()

	w, ok := h.waiting(id)
	require.True(t, ok)
	assert.Equal(t, WaitBusy, w.Reason)
	assert.Equal(t, []string{"test-runs"}, w.Builds)

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	assert.Equal(t, []Worker{{ID: "w1", BuildID: "test-runs", Queues: []string{DefaultQueue},
		Kinds: []TaskKind{TaskRun, TaskStep}, LastSeen: t0}}, workers)
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
	// w1 has nothing to run; w2 takes a run task.
	h.pollOnce(PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}})
	h.startDaily()
	h.pollOnce(PollRequest{WorkerID: "w2", Queues: []string{DefaultQueue}})

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
	h.startWorker("w1", PollRequest{Queues: []string{"gpu"}, Wait: 24 * time.Hour}, nil)
	h.advance(2 * time.Hour)

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	assert.True(t, workers[0].Polling)
}

func TestWaitingForTheQueueLimit(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Queues = map[string]QueueConfig{"gpu": {MaxRunningSteps: 1}} })
	wk := h.startWorker("w1", PollRequest{Queues: []string{"gpu"}}, nil)
	h.queue(TaskStep, "gpu", "l1")
	h.queue(TaskStep, "gpu", "l2")
	assert.Equal(t, "l1", wk.next(t).ID)

	w, ok := h.waiting("h-l2")
	require.True(t, ok)
	assert.Equal(t, WaitQueueLimit, w.Reason)
	assert.Equal(t, TaskStep, w.Kind)
	assert.Equal(t, "fn", w.Name)
}

func TestWaitingTellsTheRunTaskFirst(t *testing.T) {
	h := newHarness(t)
	h.do(func(c *Core) {
		c.enqueue(Task{Kind: TaskStep, ID: "l1", Queue: "default", RunID: "h1", Name: "load", Seq: 2}, nil)
		c.enqueue(Task{Kind: TaskStep, ID: "l0", Queue: "default", RunID: "h1", Name: "fetch", Seq: 1}, nil)
	})
	w, _ := h.waiting("h1")
	assert.Equal(t, "load", w.Name, "the oldest step")
	assert.Equal(t, 2, w.Queued)

	h.do(func(c *Core) {
		c.enqueue(Task{Kind: TaskRun, ID: "ht", Queue: "default", RunID: "h1", Name: "pipeline"}, nil)
	})
	w, _ = h.waiting("h1")
	assert.Equal(t, TaskRun, w.Kind)
	assert.Equal(t, 3, w.Queued)
}

func TestAWorkerThatHangsUpIsGone(t *testing.T) {
	h := newHarness(t)
	w := h.startWorker("w1", PollRequest{Queues: []string{DefaultQueue}}, nil)
	w.stop()

	workers, err := h.core.Workers(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, workers)
}

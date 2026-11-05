package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queue puts a task on a queue. When a worker takes it, the dispatch is
// recorded in the run's event log with the worker's ID.
func (h *harness) queue(kind TaskKind, queue, id string) {
	h.t.Helper()
	h.do(func(c *Core) {
		c.enqueue(Task{Kind: kind, ID: id, Queue: queue, RunID: "h-" + id, Name: "fn"},
			func(c *Core, workerID string) Task {
				typ := state.EventRunTaskDispatched
				if kind == TaskStep {
					typ = state.EventStepAttemptDispatched
				}
				c.emitRun(typ, "t", "h-"+id, id, map[string]string{"worker": workerID})
				return Task{Kind: kind, ID: id, Queue: queue, RunID: "h-" + id, Name: "fn", Args: json.RawMessage(`[1]`)}
			})
	})
}

func ids(tasks ...Task) []string {
	var out []string
	for _, t := range tasks {
		out = append(out, t.ID)
	}
	return out
}

func TestPollDeliversAfterDispatchCommits(t *testing.T) {
	h := newHarness(t)
	h.queue(TaskStep, "default", "l1")

	w := h.startWorker("w1", PollRequest{Queues: []string{"default"}}, nil)
	task := w.next(t)
	assert.Equal(t, Task{Kind: TaskStep, ID: "l1", Queue: "default", RunID: "h-l1", Name: "fn", Args: json.RawMessage(`[1]`)}, task)

	// The worker only got the task once its dispatch was durable.
	evs, err := h.store.RunEvents(h.ctx, "h-l1")
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, state.EventStepAttemptDispatched, evs[0].Type)
	assert.JSONEq(t, `{"worker":"w1"}`, string(evs[0].Payload))
}

func TestLongPollWaitsForWork(t *testing.T) {
	h := newHarness(t)
	w := h.startWorker("w1", PollRequest{Queues: []string{"default"}}, nil)
	w.none(t)

	h.queue(TaskRun, "default", "ht1")
	assert.Equal(t, "ht1", w.next(t).ID)
}

func TestPollWaitExpiresOnCoreClock(t *testing.T) {
	h := newHarness(t)
	timers := h.clock.Pending() // the core's own, such as retention
	done := make(chan pollResult, 1)
	go func() {
		task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"default"}, Wait: 30 * time.Second})
		done <- pollResult{task, ok, err}
	}()
	require.Eventually(t, func() bool { return h.clock.Pending() > timers }, 5*time.Second, time.Millisecond)

	h.advance(29 * time.Second)
	select {
	case <-done:
		t.Fatal("poll returned before its wait elapsed")
	default:
	}
	h.advance(time.Second)
	r := <-done
	assert.NoError(t, r.err)
	assert.False(t, r.ok)

	// The expired poller is gone: new work stays queued for the next poll.
	h.queue(TaskStep, "default", "l1")
	task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"default"}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "l1", task.ID)
}

func TestPollWithoutWaitAnswersAtOnce(t *testing.T) {
	h := newHarness(t)
	_, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"default"}})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRunTasksGoBeforeStepsThenOldestFirst(t *testing.T) {
	h := newHarness(t)
	h.queue(TaskStep, "a", "l1")
	h.queue(TaskStep, "b", "l2")
	h.queue(TaskRun, "b", "ht1")
	h.queue(TaskStep, "a", "l3")
	h.queue(TaskRun, "a", "ht2")

	var got []Task
	for range 5 {
		task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"a", "b"}})
		require.NoError(t, err)
		require.True(t, ok)
		got = append(got, task)
	}
	assert.Equal(t, []string{"ht1", "ht2", "l1", "l2", "l3"}, ids(got...))
}

func TestOldestPollerGoesFirst(t *testing.T) {
	h := newHarness(t)
	w1 := h.startWorker("w1", PollRequest{Queues: []string{"default"}}, nil)
	w2 := h.startWorker("w2", PollRequest{Queues: []string{"default"}}, nil)

	h.queue(TaskStep, "default", "l1")
	assert.Equal(t, "l1", w1.next(t).ID)
	w2.none(t)
	h.queue(TaskStep, "default", "l2")
	got := []string{}
	select {
	case task := <-w1.tasks:
		got = append(got, "w1:"+task.ID)
	case task := <-w2.tasks:
		got = append(got, "w2:"+task.ID)
	case <-time.After(5 * time.Second):
	}
	assert.Equal(t, []string{"w2:l2"}, got, "w1 is busy re-polling after w2, which has been waiting")
}

func TestPollOnlyServesRequestedQueuesAndKinds(t *testing.T) {
	h := newHarness(t)
	stepsOnly := h.startWorker("pods", PollRequest{Queues: []string{"gpu"}, Kinds: []TaskKind{TaskStep}}, nil)
	other := h.startWorker("cpu", PollRequest{Queues: []string{"cpu"}}, nil)

	h.queue(TaskRun, "gpu", "ht1")
	stepsOnly.none(t)
	other.none(t)

	h.queue(TaskStep, "gpu", "l1")
	assert.Equal(t, "l1", stepsOnly.next(t).ID)
	other.none(t)

	// The run task is still queued for a worker that takes run tasks.
	any := h.startWorker("any", PollRequest{Queues: []string{"gpu"}}, nil)
	assert.Equal(t, "ht1", any.next(t).ID)
}

func TestQueueStepLimit(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Queues = map[string]QueueConfig{"gpu": {MaxRunningSteps: 1}} })
	w := h.startWorker("w1", PollRequest{Queues: []string{"gpu"}}, nil)
	h.queue(TaskStep, "gpu", "l1")
	h.queue(TaskStep, "gpu", "l2")
	h.queue(TaskRun, "gpu", "ht1")

	first := w.next(t)
	assert.Equal(t, "l1", first.ID, "queued before the run task, which arrived later")
	assert.Equal(t, "ht1", w.next(t).ID, "run tasks are not limited")
	w.none(t)

	h.do(func(c *Core) { c.releaseStep("gpu") })
	assert.Equal(t, "l2", w.next(t).ID)
}

func TestUnqueue(t *testing.T) {
	h := newHarness(t)
	h.queue(TaskStep, "default", "l1")
	h.queue(TaskStep, "default", "l2")
	var removed, missing bool
	h.do(func(c *Core) {
		removed = c.unqueue("default", "l1")
		missing = c.unqueue("default", "nope") || c.unqueue("other", "l2")
	})
	assert.True(t, removed)
	assert.False(t, missing)

	task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"default"}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "l2", task.ID)
}

func TestPollWithdrawsWhenContextEnds(t *testing.T) {
	h := newHarness(t)
	timers := h.clock.Pending() // the core's own, such as retention
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		_, _, err := h.core.Poll(ctx, PollRequest{WorkerID: "gone", Queues: []string{"default"}, Wait: time.Hour})
		done <- err
	}()
	require.Eventually(t, func() bool { return h.clock.Pending() > timers }, 5*time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)

	// Work queued afterwards goes to a live worker, not the withdrawn one.
	h.queue(TaskStep, "default", "l1")
	w := h.startWorker("w1", PollRequest{Queues: []string{"default"}}, nil)
	assert.Equal(t, "l1", w.next(t).ID)
}

func TestPollersAreAnsweredWhenCoreStops(t *testing.T) {
	h := newHarness(t)
	w := h.startWorker("w1", PollRequest{Queues: []string{"default"}}, nil)
	require.NoError(t, h.stop())
	<-w.done
	assert.ErrorIs(t, w.err, ErrStopped)

	_, _, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{"default"}})
	assert.ErrorIs(t, err, ErrStopped)
}

func TestPollRejectsIncompleteRequests(t *testing.T) {
	h := newHarness(t)
	_, _, err := h.core.Poll(h.ctx, PollRequest{Queues: []string{"default"}})
	assert.ErrorIs(t, err, ErrInvalid)
	_, _, err = h.core.Poll(h.ctx, PollRequest{WorkerID: "w1"})
	assert.ErrorIs(t, err, ErrInvalid)
}

func TestFakeWorkerDrainsADock(t *testing.T) {
	h := newHarness(t)
	got := make(chan string, 10)
	h.startWorker("w1", PollRequest{Queues: []string{"default"}}, func(task Task) { got <- task.ID })
	for _, id := range []string{"l1", "l2", "l3"} {
		h.queue(TaskStep, "default", id)
	}
	var seen []string
	for range 3 {
		select {
		case id := <-got:
			seen = append(seen, id)
		case <-time.After(5 * time.Second):
			t.Fatal("worker stalled")
		}
	}
	assert.Equal(t, []string{"l1", "l2", "l3"}, seen)
}

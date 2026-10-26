package core

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replaySequential plays a haul that runs steps one after the other: it
// issues commands until one has no result in the logbook, completes once
// every step has one, and fails at the first failed lift.
func replaySequential(steps []Command, logbook []state.Event) ([]Command, *HaulOutcome) {
	results := map[int]liftFinishedPayload{}
	for _, e := range logbook {
		switch e.Type {
		case state.EventLiftFinished, state.EventTimerFired:
			var p liftFinishedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				panic(err)
			}
			results[p.Seq] = p
		}
	}
	var cmds []Command
	for i, step := range steps {
		step.Seq = i + 1
		cmds = append(cmds, step)
		r, ok := results[step.Seq]
		if !ok {
			return cmds, nil // blocked
		}
		if r.Status == state.LiftFailed {
			return cmds, &HaulOutcome{Status: state.HaulFailed, Reason: "lift_failed", Message: step.Name + ": " + r.Message}
		}
	}
	return cmds, &HaulOutcome{Status: state.HaulCompleted, Result: json.RawMessage(`{"ok":true}`)}
}

// haulWorker runs haul tasks with replaySequential and lifts with run.
type haulWorker struct {
	*fakeWorker
	mu   sync.Mutex
	runs map[string]int // lift name → attempts run
	errs []error
}

func (h *harness) startHaulWorker(steps []Command, run func(Task) LiftResult) *haulWorker {
	h.t.Helper()
	if run == nil {
		run = func(t Task) LiftResult {
			return LiftResult{Status: state.AttemptSucceeded, Result: json.RawMessage(fmt.Sprintf(`{"lift":%q}`, t.Name))}
		}
	}
	w := &haulWorker{runs: map[string]int{}}
	w.fakeWorker = h.startWorker("w1", PollRequest{Docks: []string{DefaultDock}}, func(t Task) {
		var err error
		switch t.Kind {
		case TaskHaul:
			cmds, outcome := replaySequential(steps, t.Logbook)
			err = h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t.ID, BuildID: "b1", Commands: cmds, Outcome: outcome})
		case TaskLift:
			w.mu.Lock()
			w.runs[t.Name]++
			w.mu.Unlock()
			res := run(t)
			res.AttemptID = t.ID
			err = h.core.CompleteLift(h.ctx, res)
		}
		if err != nil {
			w.mu.Lock()
			w.errs = append(w.errs, err)
			w.mu.Unlock()
		}
	})
	return w
}

func lift(name string) Command { return Command{Kind: CommandLift, Name: name} }

func sleep(d time.Duration) Command { return Command{Kind: CommandSleep, Duration: d} }

func (h *harness) startHaul(name string) string {
	h.t.Helper()
	res, err := h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: name, Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(h.t, err)
	return res.HaulID
}

func (h *harness) haul(id string) state.Haul {
	h.t.Helper()
	v, err := h.store.GetHaul(h.ctx, id)
	require.NoError(h.t, err)
	return v
}

func (h *harness) waitHaul(id, status string) state.Haul {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.haul(id).Status == status }, 5*time.Second, time.Millisecond,
		"haul %s never reached %s", id, status)
	return h.haul(id)
}

func (h *harness) haulEventTypes(id string) []state.EventType {
	h.t.Helper()
	evs, err := h.store.HaulEvents(h.ctx, id)
	require.NoError(h.t, err)
	var out []state.EventType
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

// poll takes the next task at once, failing if there is none.
func (h *harness) poll(kinds ...TaskKind) Task {
	h.t.Helper()
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}, Kinds: kinds})
	require.NoError(h.t, err)
	require.True(h.t, ok, "no task queued")
	return t
}

func (h *harness) pollNothing() {
	h.t.Helper()
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}})
	require.NoError(h.t, err)
	require.False(h.t, ok, "unexpected %s task %s", t.Kind, t.ID)
}

func seqs(cmds ...Command) []Command {
	for i := range cmds {
		cmds[i].Seq = i + 1
	}
	return cmds
}

func TestHaulRunsLiftsAndSleepToCompletion(t *testing.T) {
	h := newHarness(t)
	w := h.startHaulWorker([]Command{lift("extract"), lift("transform"), sleep(15 * time.Minute), lift("report")}, nil)
	id := h.startHaul("daily_revenue")

	// Two lifts run, then the haul sleeps on the core's clock.
	require.Eventually(t, func() bool {
		tms, err := h.store.ListTimers(h.ctx, id)
		require.NoError(t, err)
		return len(tms) == 1
	}, 5*time.Second, time.Millisecond)
	assert.Equal(t, state.HaulRunning, h.haul(id).Status)
	h.advance(15 * time.Minute)

	got := h.waitHaul(id, state.HaulCompleted)
	assert.JSONEq(t, `{"ok":true}`, string(got.Result))
	assert.Equal(t, 4, got.Commands)
	assert.Equal(t, "b1", got.BuildID)
	assert.Empty(t, w.errs)
	assert.Equal(t, map[string]int{"extract": 1, "transform": 1, "report": 1}, w.runs, "each lift ran once")

	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, lifts, 3)
	for _, l := range lifts {
		assert.Equal(t, state.LiftCompleted, l.Status)
		assert.Equal(t, 1, l.Attempts)
		assert.JSONEq(t, fmt.Sprintf(`{"lift":%q}`, l.Name), string(l.Result))
	}
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Len(t, tasks, 5, "one replay at the start and one after each of four results")

	S, Q, D, C := state.EventHaulStarted, state.EventHaulTaskQueued, state.EventHaulTaskDispatched, state.EventHaulTaskCompleted
	L, LD, LAF, LF := state.EventLiftScheduled, state.EventLiftAttemptDispatched, state.EventLiftAttemptFinished, state.EventLiftFinished
	assert.Equal(t, []state.EventType{
		S, Q, D, C, L, LD, LAF, LF, // extract
		Q, D, C, L, LD, LAF, LF, // transform
		Q, D, C, state.EventTimerStarted, state.EventTimerFired, // sleep
		Q, D, C, L, LD, LAF, LF, // report
		Q, D, C, state.EventHaulFinished,
	}, h.haulEventTypes(id))
}

func TestFailedLiftReachesTheHaul(t *testing.T) {
	h := newHarness(t)
	h.startHaulWorker([]Command{lift("extract"), lift("load")}, func(t Task) LiftResult {
		if t.Name == "extract" {
			return LiftResult{Status: state.AttemptFailed, Reason: "error", Message: "source down"}
		}
		return LiftResult{Status: state.AttemptSucceeded}
	})
	id := h.startHaul("daily_revenue")

	got := h.waitHaul(id, state.HaulFailed)
	assert.Equal(t, "lift_failed", got.Reason)
	assert.Equal(t, "extract: source down", got.Message)
	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, lifts, 1, "load was never scheduled")
	assert.Equal(t, state.LiftFailed, lifts[0].Status)
}

func TestResultDuringReplayTriggersAnotherReplay(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("fanout")

	t1 := h.poll(TaskHaul)
	require.Len(t, t1.Logbook, 3, "started, queued, dispatched")
	both := seqs(lift("a"), lift("b"))
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: both}))
	a, b := h.poll(TaskLift), h.poll(TaskLift)
	assert.Equal(t, []string{"a", "b"}, []string{a.Name, b.Name})

	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
	t2 := h.poll(TaskHaul)
	// b finishes while t2's replay runs on a logbook without b's result.
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: b.ID, Status: state.AttemptSucceeded}))
	h.pollNothing()
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t2.ID, Commands: both}))

	t3 := h.poll(TaskHaul)
	finished := 0
	for _, e := range t3.Logbook {
		if e.Type == state.EventLiftFinished {
			finished++
		}
	}
	assert.Equal(t, 2, finished, "t3 replays with b's result")
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t3.ID, Commands: both,
		Outcome: &HaulOutcome{Status: state.HaulCompleted}}))
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
	h.pollNothing()
}

func TestCancelHaulStopsItsWork(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("fanout")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID,
		Commands: seqs(lift("a"), lift("b"), sleep(time.Hour))}))
	a := h.poll(TaskLift) // a is dispatched; b stays queued

	require.NoError(t, h.core.CancelHaul(h.ctx, id, "alper"))
	got := h.haul(id)
	assert.Equal(t, state.HaulCancelled, got.Status)
	assert.Equal(t, "cancelled by alper", got.Message)

	h.pollNothing() // b was withdrawn from the dock
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded}), ErrNotFound)

	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	for _, l := range lifts {
		assert.Equal(t, state.LiftCancelled, l.Status, l.Name)
	}
	atts, err := h.store.ListLiftAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, atts, 1)
	assert.Equal(t, state.AttemptCancelled, atts[0].Status)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerCancelled, tms[0].Status)

	h.advance(time.Hour) // the stopped timer doesn't fire
	assert.NotContains(t, h.haulEventTypes(id), state.EventTimerFired)

	require.NoError(t, h.core.CancelHaul(h.ctx, id, "alper"), "cancelling a finished haul is a no-op")
	assert.ErrorIs(t, h.core.CancelHaul(h.ctx, "nope", "alper"), ErrNotFound)
}

func TestCancelWithQueuedHaulTaskAndLateReplay(t *testing.T) {
	h := newHarness(t)
	queued := h.startHaul("a")
	require.NoError(t, h.core.CancelHaul(h.ctx, queued, "x"))
	h.pollNothing()
	tasks, err := h.store.ListHaulTasks(h.ctx, queued)
	require.NoError(t, err)
	assert.Equal(t, state.HaulTaskFailed, tasks[0].Status)

	// A replay that was already running when its haul was cancelled is
	// accepted, but nothing it asked for happens.
	running := h.startHaul("b")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CancelHaul(h.ctx, running, "x"))
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: seqs(lift("a"))}))
	h.pollNothing()
	lifts, err := h.store.ListLifts(h.ctx, running)
	require.NoError(t, err)
	assert.Empty(t, lifts)
}

func TestHaulOutcomeCancelsUnawaitedLifts(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("fire_and_forget")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: seqs(lift("a")),
		Outcome: &HaulOutcome{Status: state.HaulCompleted}}))
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
	h.pollNothing()
	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.LiftCancelled, lifts[0].Status)
}

func TestLiftOnAnotherDock(t *testing.T) {
	h := newHarness(t)
	h.startHaul("x")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID,
		Commands: seqs(Command{Kind: CommandLift, Name: "train", Dock: "gpu", Args: json.RawMessage(`[1,2]`)})}))
	h.pollNothing()
	task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "g", Docks: []string{"gpu"}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "train", task.Name)
	assert.Equal(t, 1, task.Seq)
	assert.JSONEq(t, `[1,2]`, string(task.Args))
}

func TestInvalidHaulTaskResultMakesHaulStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	t1 := h.poll(TaskHaul)
	err := h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: []Command{{Seq: 2, Kind: CommandLift, Name: "a"}}})
	assert.ErrorIs(t, err, ErrInvalid)
	assert.ErrorContains(t, err, "commands[0]: seq is 2, want 1")

	got := h.haul(id)
	assert.Equal(t, state.HaulStuck, got.Status)
	assert.Equal(t, "invalid_result", got.Reason)
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.HaulTaskFailed, tasks[0].Status)

	for name, res := range map[string]HaulTaskResult{
		"unnamed lift":      {Commands: []Command{{Seq: 1, Kind: CommandLift}}},
		"zero sleep":        {Commands: []Command{{Seq: 1, Kind: CommandSleep}}},
		"unknown kind":      {Commands: []Command{{Seq: 1, Kind: "fly"}}},
		"bad outcome":       {Outcome: &HaulOutcome{Status: "done"}},
		"outcome and error": {Outcome: &HaulOutcome{Status: state.HaulFailed}, Error: &TaskError{Reason: "x"}},
	} {
		assert.Error(t, validateHaulTaskResult(res), name)
	}
}

func TestHaulTaskErrorFailsTheHaul(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID,
		Error: &TaskError{Reason: "import_error", Message: "no module named app"}}))
	got := h.haul(id)
	assert.Equal(t, state.HaulFailed, got.Status)
	assert.Equal(t, "haul_task_failed", got.Reason)
	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "import_error", tasks[0].Reason)
}

func TestHaulRequestErrors(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.StartHaul(h.ctx, StartHaulRequest{Name: "x"})
	assert.ErrorIs(t, err, ErrInvalid)

	assert.ErrorIs(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: "nope"}), ErrNotFound)
	h.startHaul("x")
	t1 := h.poll(TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID, Commands: seqs(lift("a"))}))
	assert.ErrorIs(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t1.ID}), ErrNotFound, "completed twice")

	a := h.poll(TaskLift)
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: "maybe"}), ErrInvalid)
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: "nope", Status: state.AttemptSucceeded}), ErrNotFound)
}

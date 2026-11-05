package core

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replaySequential plays a run that runs steps one after the other: it
// issues commands until one has no result in the event log, completes once
// every step has one, and fails at the first failed step.
func replaySequential(steps []Command, eventLog []state.Event) ([]Command, *RunOutcome) {
	results := map[int]stepFinishedPayload{}
	for _, e := range eventLog {
		switch e.Type {
		case state.EventStepFinished, state.EventTimerFired:
			var p stepFinishedPayload
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
		if r.Status == state.StepFailed {
			return cmds, &RunOutcome{Status: state.RunFailed, Reason: "step_failed", Message: step.Name + ": " + r.Message}
		}
	}
	return cmds, &RunOutcome{Status: state.RunCompleted, Result: json.RawMessage(`{"ok":true}`)}
}

// runWorker executes run tasks with replaySequential and steps with run.
type runWorker struct {
	*fakeWorker
	mu   sync.Mutex
	runs map[string]int // step name → attempts run
	errs []error
}

func (h *harness) startRunWorker(steps []Command, run func(Task) StepResult) *runWorker {
	h.t.Helper()
	if run == nil {
		run = func(t Task) StepResult {
			return StepResult{Status: state.AttemptSucceeded, Result: json.RawMessage(fmt.Sprintf(`{"step":%q}`, t.Name))}
		}
	}
	w := &runWorker{runs: map[string]int{}}
	w.fakeWorker = h.startWorker("w1", PollRequest{Queues: []string{DefaultQueue}}, func(t Task) {
		var err error
		switch t.Kind {
		case TaskRun:
			cmds, outcome := replaySequential(steps, t.EventLog)
			err = h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t.ID, BuildID: "b1", Commands: cmds, Outcome: outcome})
		case TaskStep:
			w.mu.Lock()
			w.runs[t.Name]++
			w.mu.Unlock()
			res := run(t)
			res.AttemptID = t.ID
			err = h.core.CompleteStep(h.ctx, res)
		}
		if err != nil {
			w.mu.Lock()
			w.errs = append(w.errs, err)
			w.mu.Unlock()
		}
	})
	return w
}

func stepCmd(name string) Command { return Command{Kind: CommandStep, Name: name} }

func sleep(d time.Duration) Command { return Command{Kind: CommandSleep, Duration: d} }

func (h *harness) startRun(name string) string {
	h.t.Helper()
	res, err := h.core.StartRun(h.ctx, StartRunRequest{Tenant: "t", Name: name, Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(h.t, err)
	return res.RunID
}

func (h *harness) run(id string) state.Run {
	h.t.Helper()
	v, err := h.store.GetRun(h.ctx, id)
	require.NoError(h.t, err)
	return v
}

func (h *harness) waitRun(id, status string) state.Run {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.run(id).Status == status }, 5*time.Second, time.Millisecond,
		"run %s never reached %s", id, status)
	return h.run(id)
}

func (h *harness) runEventTypes(id string) []state.EventType {
	h.t.Helper()
	evs, err := h.store.RunEvents(h.ctx, id)
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
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}, Kinds: kinds})
	require.NoError(h.t, err)
	require.True(h.t, ok, "no task queued")
	return t
}

func (h *harness) pollNothing() {
	h.t.Helper()
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}})
	require.NoError(h.t, err)
	require.False(h.t, ok, "unexpected %s task %s", t.Kind, t.ID)
}

func seqs(cmds ...Command) []Command {
	for i := range cmds {
		cmds[i].Seq = i + 1
	}
	return cmds
}

func TestRunRunsStepsAndSleepToCompletion(t *testing.T) {
	h := newHarness(t)
	w := h.startRunWorker([]Command{stepCmd("extract"), stepCmd("transform"), sleep(15 * time.Minute), stepCmd("report")}, nil)
	id := h.startRun("daily_revenue")

	// Two steps run, then the run sleeps on the core's clock.
	require.Eventually(t, func() bool {
		tms, err := h.store.ListTimers(h.ctx, id)
		require.NoError(t, err)
		return len(tms) == 1
	}, 5*time.Second, time.Millisecond)
	assert.Equal(t, state.RunRunning, h.run(id).Status)
	h.advance(15 * time.Minute)

	got := h.waitRun(id, state.RunCompleted)
	assert.JSONEq(t, `{"ok":true}`, string(got.Result))
	assert.Equal(t, 4, got.Commands)
	assert.Equal(t, "b1", got.BuildID)
	assert.Empty(t, w.errs)
	assert.Equal(t, map[string]int{"extract": 1, "transform": 1, "report": 1}, w.runs, "each step ran once")

	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, steps, 3)
	for _, l := range steps {
		assert.Equal(t, state.StepCompleted, l.Status)
		assert.Equal(t, 1, l.Attempts)
		assert.JSONEq(t, fmt.Sprintf(`{"step":%q}`, l.Name), string(l.Result))
	}
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Len(t, tasks, 5, "one replay at the start and one after each of four results")

	S, Q, D, C := state.EventRunStarted, state.EventRunTaskQueued, state.EventRunTaskDispatched, state.EventRunTaskCompleted
	L, LD, LAF, LF := state.EventStepScheduled, state.EventStepAttemptDispatched, state.EventStepAttemptFinished, state.EventStepFinished
	assert.Equal(t, []state.EventType{
		S, Q, D, C, L, LD, LAF, LF, // extract
		Q, D, C, L, LD, LAF, LF, // transform
		Q, D, C, state.EventTimerStarted, state.EventTimerFired, // sleep
		Q, D, C, L, LD, LAF, LF, // report
		Q, D, C, state.EventRunFinished,
	}, h.runEventTypes(id))
}

func TestFailedStepReachesTheRun(t *testing.T) {
	h := newHarness(t)
	h.startRunWorker([]Command{stepCmd("extract"), stepCmd("load")}, func(t Task) StepResult {
		if t.Name == "extract" {
			return StepResult{Status: state.AttemptFailed, Reason: "error", Message: "source down"}
		}
		return StepResult{Status: state.AttemptSucceeded}
	})
	id := h.startRun("daily_revenue")

	got := h.waitRun(id, state.RunFailed)
	assert.Equal(t, "step_failed", got.Reason)
	assert.Equal(t, "extract: source down", got.Message)
	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, steps, 1, "load was never scheduled")
	assert.Equal(t, state.StepFailed, steps[0].Status)
}

func TestResultDuringReplayTriggersAnotherReplay(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("fanout")

	t1 := h.poll(TaskRun)
	require.Len(t, t1.EventLog, 3, "started, queued, dispatched")
	both := seqs(stepCmd("a"), stepCmd("b"))
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: both}))
	a, b := h.poll(TaskStep), h.poll(TaskStep)
	assert.Equal(t, []string{"a", "b"}, []string{a.Name, b.Name})

	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
	t2 := h.poll(TaskRun)
	// b finishes while t2's replay runs on an event log without b's result.
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: b.ID, Status: state.AttemptSucceeded}))
	h.pollNothing()
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t2.ID, Commands: both}))

	t3 := h.poll(TaskRun)
	finished := 0
	for _, e := range t3.EventLog {
		if e.Type == state.EventStepFinished {
			finished++
		}
	}
	assert.Equal(t, 2, finished, "t3 replays with b's result")
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t3.ID, Commands: both,
		Outcome: &RunOutcome{Status: state.RunCompleted}}))
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
	h.pollNothing()
}

func TestCancelRunStopsItsWork(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("fanout")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID,
		Commands: seqs(stepCmd("a"), stepCmd("b"), sleep(time.Hour))}))
	a := h.poll(TaskStep) // a is dispatched; b stays queued

	require.NoError(t, h.core.CancelRun(h.ctx, id, "alper"))
	got := h.run(id)
	assert.Equal(t, state.RunCancelled, got.Status)
	assert.Equal(t, "cancelled by alper", got.Message)

	h.pollNothing() // b was withdrawn from the queue
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded}), ErrNotFound)

	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	for _, l := range steps {
		assert.Equal(t, state.StepCancelled, l.Status, l.Name)
	}
	atts, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, atts, 1)
	assert.Equal(t, state.AttemptCancelled, atts[0].Status)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerCancelled, tms[0].Status)

	h.advance(time.Hour) // the stopped timer doesn't fire
	assert.NotContains(t, h.runEventTypes(id), state.EventTimerFired)

	require.NoError(t, h.core.CancelRun(h.ctx, id, "alper"), "cancelling a finished run is a no-op")
	assert.ErrorIs(t, h.core.CancelRun(h.ctx, "nope", "alper"), ErrNotFound)
}

func TestCancelWithQueuedRunTaskAndLateReplay(t *testing.T) {
	h := newHarness(t)
	queued := h.startRun("a")
	require.NoError(t, h.core.CancelRun(h.ctx, queued, "x"))
	h.pollNothing()
	tasks, err := h.store.ListRunTasks(h.ctx, queued)
	require.NoError(t, err)
	assert.Equal(t, state.RunTaskFailed, tasks[0].Status)

	// A replay that was already running when its run was cancelled is
	// accepted, but nothing it asked for happens.
	running := h.startRun("b")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CancelRun(h.ctx, running, "x"))
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: seqs(stepCmd("a"))}))
	h.pollNothing()
	steps, err := h.store.ListSteps(h.ctx, running)
	require.NoError(t, err)
	assert.Empty(t, steps)
}

func TestRunOutcomeCancelsUnawaitedSteps(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("fire_and_forget")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: seqs(stepCmd("a")),
		Outcome: &RunOutcome{Status: state.RunCompleted}}))
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
	h.pollNothing()
	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.StepCancelled, steps[0].Status)
}

func TestStepOnAnotherQueue(t *testing.T) {
	h := newHarness(t)
	h.startRun("x")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID,
		Commands: seqs(Command{Kind: CommandStep, Name: "train", Queue: "gpu", Args: json.RawMessage(`[1,2]`)})}))
	h.pollNothing()
	task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "g", Queues: []string{"gpu"}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "train", task.Name)
	assert.Equal(t, 1, task.Seq)
	assert.JSONEq(t, `[1,2]`, string(task.Args))
}

func TestInvalidRunTaskResultMakesRunStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	t1 := h.poll(TaskRun)
	err := h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: []Command{{Seq: 2, Kind: CommandStep, Name: "a"}}})
	assert.ErrorIs(t, err, ErrInvalid)
	assert.ErrorContains(t, err, "commands[0]: seq is 2, want 1")

	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status)
	assert.Equal(t, "invalid_result", got.Reason)
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.RunTaskFailed, tasks[0].Status)

	for name, res := range map[string]RunTaskResult{
		"unnamed step":      {Commands: []Command{{Seq: 1, Kind: CommandStep}}},
		"zero sleep":        {Commands: []Command{{Seq: 1, Kind: CommandSleep}}},
		"unknown kind":      {Commands: []Command{{Seq: 1, Kind: "fly"}}},
		"bad outcome":       {Outcome: &RunOutcome{Status: "done"}},
		"outcome and error": {Outcome: &RunOutcome{Status: state.RunFailed}, Error: &TaskError{Reason: "x"}},
	} {
		assert.Error(t, validateRunTaskResult(res), name)
	}
}

func TestRunTaskErrorFailsTheRun(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID,
		Error: &TaskError{Reason: "import_error", Message: "no module named app"}}))
	got := h.run(id)
	assert.Equal(t, state.RunFailed, got.Status)
	assert.Equal(t, "run_task_failed", got.Reason)
	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "import_error", tasks[0].Reason)
}

func TestRunRequestErrors(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.StartRun(h.ctx, StartRunRequest{Name: "x"})
	assert.ErrorIs(t, err, ErrInvalid)

	assert.ErrorIs(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: "nope"}), ErrNotFound)
	h.startRun("x")
	t1 := h.poll(TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID, Commands: seqs(stepCmd("a"))}))
	assert.ErrorIs(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t1.ID}), ErrNotFound, "completed twice")

	a := h.poll(TaskStep)
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: "maybe"}), ErrInvalid)
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: "nope", Status: state.AttemptSucceeded}), ErrNotFound)
}

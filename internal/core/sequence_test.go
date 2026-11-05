package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func at(cmd Command, source string) Command {
	cmd.Source = source
	return cmd
}

// replay completes the next run task with cmds.
func (h *harness) replay(cmds []Command, outcome *RunOutcome) Task {
	h.t.Helper()
	t := h.poll(TaskRun)
	require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Outcome: outcome}))
	return t
}

func (h *harness) succeedStep(a Task) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
}

func TestConsistentReplayExtendsTheEventLog(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a")), nil)
	h.succeedStep(h.poll(TaskStep))

	// Arguments, durations and call sites may differ: only kind and target count.
	again := seqs(Command{Kind: CommandStep, Name: "a", Args: json.RawMessage(`[2]`), Source: "new.py:1"}, sleep(time.Minute))
	h.replay(again, nil)
	assert.Equal(t, state.RunRunning, h.run(id).Status)
	assert.Equal(t, 2, h.run(id).Commands)
}

func TestContradictingReplayMakesRunStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(at(stepCmd("extract"), "app.py:10"), at(stepCmd("audit"), "app.py:11")), nil)
	extract, audit := h.poll(TaskStep), h.poll(TaskStep)
	h.succeedStep(extract)

	// The code changed: the second call is now a different step.
	t2 := h.replay(seqs(at(stepCmd("extract"), "app.py:10"), at(stepCmd("transform"), "app.py:12")), nil)

	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status)
	assert.Equal(t, "nondeterminism", got.Reason)
	assert.Equal(t, `command 2: replay issued step "transform" at app.py:12, the event log has step "audit" at app.py:11`, got.Message)
	assert.Equal(t, 2, got.Commands, "nothing from the contradicting replay was applied")

	tasks, err := h.store.ListRunTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.RunTaskFailed, tasks[1].Status)
	assert.Equal(t, t2.ID, tasks[1].ID)
	assert.Equal(t, "nondeterminism", tasks[1].Reason)

	evs, err := h.store.RunEvents(h.ctx, id)
	require.NoError(t, err)
	stuck := evs[len(evs)-1]
	assert.Equal(t, state.EventRunStuck, stuck.Type)
	assert.Contains(t, string(stuck.Payload), `"seq":2`)

	// Its work carries on, but results don't trigger replays of the same code.
	h.succeedStep(audit)
	steps, err := h.store.ListSteps(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.StepCompleted, steps[1].Status)
	h.pollNothing()
}

func TestReplayMissingACommandIsAContradiction(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a"), stepCmd("b")), nil)
	h.succeedStep(h.poll(TaskStep))

	h.replay(seqs(stepCmd("a")), &RunOutcome{Status: state.RunCompleted})
	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status, "the outcome of a contradicting replay is ignored")
	assert.Equal(t, `replay issued 1 commands but the event log has 2: missing command 2, step "b"`, got.Message)
}

func TestStuckRunResumesAfterFix(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a"), sleep(time.Hour)), nil)
	h.succeedStep(h.poll(TaskStep))
	h.replay(seqs(stepCmd("b")), nil) // contradicts: step "b" where the event log has step "a"
	require.Equal(t, state.RunStuck, h.run(id).Status)

	h.advance(time.Hour) // the timer still fires while stuck
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerFired, tms[0].Status)
	h.pollNothing()

	// The bad build replays again after a resume: still stuck.
	require.NoError(t, h.core.ResumeRun(h.ctx, id))
	h.replay(seqs(stepCmd("b")), nil)
	require.Equal(t, state.RunStuck, h.run(id).Status)

	// The fixed build is consistent: the run continues and finishes.
	require.NoError(t, h.core.ResumeRun(h.ctx, id))
	require.NoError(t, h.core.ResumeRun(h.ctx, id), "a second resume while a replay is queued is a no-op")
	h.replay(seqs(stepCmd("a"), sleep(time.Hour), stepCmd("c")), nil)
	got := h.run(id)
	assert.Equal(t, state.RunRunning, got.Status)
	assert.Empty(t, got.Reason)
	assert.Empty(t, got.Message)

	h.succeedStep(h.poll(TaskStep))
	h.replay(seqs(stepCmd("a"), sleep(time.Hour), stepCmd("c")), &RunOutcome{Status: state.RunCompleted})
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
}

func TestResumeRun(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.poll(TaskRun)
	require.NoError(t, h.core.ResumeRun(h.ctx, id), "not stuck: no-op")
	h.pollNothing()
	assert.ErrorIs(t, h.core.ResumeRun(h.ctx, "nope"), ErrNotFound)

	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	require.NoError(t, h.core.ResumeRun(h.ctx, id), "finished: no-op")
}

func TestCancelStuckRun(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a"), stepCmd("b")), nil)
	a := h.poll(TaskStep)
	h.succeedStep(a)
	h.replay(seqs(stepCmd("z")), nil)
	require.Equal(t, state.RunStuck, h.run(id).Status)

	require.NoError(t, h.core.CancelRun(h.ctx, id, "x"))
	assert.Equal(t, state.RunCancelled, h.run(id).Status)
	h.pollNothing() // b was withdrawn
}

func TestCheckReplay(t *testing.T) {
	logged := seqs(stepCmd("a"), sleep(time.Minute))
	for name, tc := range map[string]struct {
		replayed []Command
		seq      int
	}{
		"same":           {seqs(stepCmd("a"), sleep(time.Minute)), 0},
		"extends":        {seqs(stepCmd("a"), sleep(time.Second), stepCmd("b")), 0},
		"other step":     {seqs(stepCmd("b"), sleep(time.Minute)), 1},
		"other kind":     {seqs(stepCmd("a"), stepCmd("s")), 2},
		"missing":        {seqs(stepCmd("a")), 2},
		"nothing at all": {nil, 1},
	} {
		seq, msg := checkReplay(logged, tc.replayed)
		assert.Equal(t, tc.seq, seq, name)
		assert.Equal(t, tc.seq != 0, msg != "", name)
	}
}

func TestGuardedRunCodeMakesRunStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a")), nil)
	h.succeedStep(h.poll(TaskStep))

	// The worker's guard caught the pipeline code reading the clock.
	ht := h.poll(TaskRun)
	msg := "time.time() called at app.py:7: pipeline code must not read the clock or randomness"
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: ht.ID, BuildID: "b",
		Error: &TaskError{Reason: ReasonNondeterminism, Message: msg}}))
	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status, "stuck, not failed: fixed code can resume it")
	assert.Equal(t, ReasonNondeterminism, got.Reason)
	assert.Equal(t, msg, got.Message)

	require.NoError(t, h.core.ResumeRun(h.ctx, id))
	h.replay(seqs(stepCmd("a")), &RunOutcome{Status: state.RunCompleted})
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
}

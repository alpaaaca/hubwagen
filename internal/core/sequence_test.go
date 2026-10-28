package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func at(cmd Command, source string) Command {
	cmd.Source = source
	return cmd
}

// replay completes the next haul task with cmds.
func (h *harness) replay(cmds []Command, outcome *HaulOutcome) Task {
	h.t.Helper()
	t := h.poll(TaskHaul)
	require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Outcome: outcome}))
	return t
}

func (h *harness) succeedLift(a Task) {
	h.t.Helper()
	require.NoError(h.t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
}

func TestConsistentReplayExtendsTheLogbook(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a")), nil)
	h.succeedLift(h.poll(TaskLift))

	// Arguments, durations and call sites may differ: only kind and target count.
	again := seqs(Command{Kind: CommandLift, Name: "a", Args: json.RawMessage(`[2]`), Source: "new.py:1"}, sleep(time.Minute))
	h.replay(again, nil)
	assert.Equal(t, state.HaulRunning, h.haul(id).Status)
	assert.Equal(t, 2, h.haul(id).Commands)
}

func TestContradictingReplayMakesHaulStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(at(lift("extract"), "app.py:10"), at(lift("audit"), "app.py:11")), nil)
	extract, audit := h.poll(TaskLift), h.poll(TaskLift)
	h.succeedLift(extract)

	// The code changed: the second call is now a different lift.
	t2 := h.replay(seqs(at(lift("extract"), "app.py:10"), at(lift("transform"), "app.py:12")), nil)

	got := h.haul(id)
	assert.Equal(t, state.HaulStuck, got.Status)
	assert.Equal(t, "nondeterminism", got.Reason)
	assert.Equal(t, `command 2: replay issued lift "transform" at app.py:12, the logbook has lift "audit" at app.py:11`, got.Message)
	assert.Equal(t, 2, got.Commands, "nothing from the contradicting replay was applied")

	tasks, err := h.store.ListHaulTasks(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.HaulTaskFailed, tasks[1].Status)
	assert.Equal(t, t2.ID, tasks[1].ID)
	assert.Equal(t, "nondeterminism", tasks[1].Reason)

	evs, err := h.store.HaulEvents(h.ctx, id)
	require.NoError(t, err)
	stuck := evs[len(evs)-1]
	assert.Equal(t, state.EventHaulStuck, stuck.Type)
	assert.Contains(t, string(stuck.Payload), `"seq":2`)

	// Its work carries on, but results don't trigger replays of the same code.
	h.succeedLift(audit)
	lifts, err := h.store.ListLifts(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.LiftCompleted, lifts[1].Status)
	h.pollNothing()
}

func TestReplayMissingACommandIsAContradiction(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a"), lift("b")), nil)
	h.succeedLift(h.poll(TaskLift))

	h.replay(seqs(lift("a")), &HaulOutcome{Status: state.HaulCompleted})
	got := h.haul(id)
	assert.Equal(t, state.HaulStuck, got.Status, "the outcome of a contradicting replay is ignored")
	assert.Equal(t, `replay issued 1 commands but the logbook has 2: missing command 2, lift "b"`, got.Message)
}

func TestStuckHaulResumesAfterFix(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a"), sleep(time.Hour)), nil)
	h.succeedLift(h.poll(TaskLift))
	h.replay(seqs(lift("b")), nil) // contradicts: lift "b" where the logbook has lift "a"
	require.Equal(t, state.HaulStuck, h.haul(id).Status)

	h.advance(time.Hour) // the timer still fires while stuck
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.TimerFired, tms[0].Status)
	h.pollNothing()

	// The bad build replays again after a resume: still stuck.
	require.NoError(t, h.core.ResumeHaul(h.ctx, id))
	h.replay(seqs(lift("b")), nil)
	require.Equal(t, state.HaulStuck, h.haul(id).Status)

	// The fixed build is consistent: the haul runs again and finishes.
	require.NoError(t, h.core.ResumeHaul(h.ctx, id))
	require.NoError(t, h.core.ResumeHaul(h.ctx, id), "a second resume while a replay is queued is a no-op")
	h.replay(seqs(lift("a"), sleep(time.Hour), lift("c")), nil)
	got := h.haul(id)
	assert.Equal(t, state.HaulRunning, got.Status)
	assert.Empty(t, got.Reason)
	assert.Empty(t, got.Message)

	h.succeedLift(h.poll(TaskLift))
	h.replay(seqs(lift("a"), sleep(time.Hour), lift("c")), &HaulOutcome{Status: state.HaulCompleted})
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
}

func TestResumeHaul(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.poll(TaskHaul)
	require.NoError(t, h.core.ResumeHaul(h.ctx, id), "not stuck: no-op")
	h.pollNothing()
	assert.ErrorIs(t, h.core.ResumeHaul(h.ctx, "nope"), ErrNotFound)

	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	require.NoError(t, h.core.ResumeHaul(h.ctx, id), "finished: no-op")
}

func TestCancelStuckHaul(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a"), lift("b")), nil)
	a := h.poll(TaskLift)
	h.succeedLift(a)
	h.replay(seqs(lift("z")), nil)
	require.Equal(t, state.HaulStuck, h.haul(id).Status)

	require.NoError(t, h.core.CancelHaul(h.ctx, id, "x"))
	assert.Equal(t, state.HaulCancelled, h.haul(id).Status)
	h.pollNothing() // b was withdrawn
}

func TestCheckReplay(t *testing.T) {
	logged := seqs(lift("a"), sleep(time.Minute))
	for name, tc := range map[string]struct {
		replayed []Command
		seq      int
	}{
		"same":           {seqs(lift("a"), sleep(time.Minute)), 0},
		"extends":        {seqs(lift("a"), sleep(time.Second), lift("b")), 0},
		"other lift":     {seqs(lift("b"), sleep(time.Minute)), 1},
		"other kind":     {seqs(lift("a"), lift("s")), 2},
		"missing":        {seqs(lift("a")), 2},
		"nothing at all": {nil, 1},
	} {
		seq, msg := checkReplay(logged, tc.replayed)
		assert.Equal(t, tc.seq, seq, name)
		assert.Equal(t, tc.seq != 0, msg != "", name)
	}
}

func TestGuardedHaulCodeMakesHaulStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(lift("a")), nil)
	h.succeedLift(h.poll(TaskLift))

	// The worker's guard caught the haul code reading the clock.
	ht := h.poll(TaskHaul)
	msg := "time.time() called at app.py:7: haul code must not read the clock or randomness"
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: ht.ID, BuildID: "b",
		Error: &TaskError{Reason: ReasonNondeterminism, Message: msg}}))
	got := h.haul(id)
	assert.Equal(t, state.HaulStuck, got.Status, "stuck, not failed: fixed code can resume it")
	assert.Equal(t, ReasonNondeterminism, got.Reason)
	assert.Equal(t, msg, got.Message)

	require.NoError(t, h.core.ResumeHaul(h.ctx, id))
	h.replay(seqs(lift("a")), &HaulOutcome{Status: state.HaulCompleted})
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
}

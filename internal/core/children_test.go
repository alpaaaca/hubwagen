package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startChildCmd(name string, params string, wait bool) Command {
	return Command{Kind: CommandStartRun, Name: name, Args: json.RawMessage(params), Wait: wait}
}

// replayRun completes the next run task, which must be runID's.
func (h *harness) replayRun(runID string, cmds []Command, outcome *RunOutcome) {
	h.t.Helper()
	t := h.poll(TaskRun)
	require.Equal(h.t, runID, t.RunID, "the next run task is another run's")
	require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Outcome: outcome}))
}

// childEvents returns a run's ChildStarted and ChildFinished payloads by seq.
func (h *harness) childEvents(runID string) (map[int]childStartedPayload, map[int]childFinishedPayload) {
	h.t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(h.t, err)
	started, finished := map[int]childStartedPayload{}, map[int]childFinishedPayload{}
	for _, e := range evs {
		switch e.Type {
		case state.EventChildStarted:
			var p childStartedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			started[p.Seq] = p
		case state.EventChildFinished:
			var p childFinishedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			finished[p.Seq] = p
		}
	}
	return started, finished
}

func completed(result string) *RunOutcome {
	return &RunOutcome{Status: state.RunCompleted, Result: json.RawMessage(result)}
}

func TestChildRunResultReachesItsParent(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("x")
	h.replayRun(parent, seqs(startChildCmd("y", `{"day":"2026-10-24","n":10000000000000001}`, true)), nil)

	started, finished := h.childEvents(parent)
	child := started[1].Run
	require.NotEmpty(t, child)
	assert.Empty(t, finished)
	c := h.run(child)
	assert.Equal(t, "y", c.Name)
	assert.Equal(t, parent, c.ParentID)
	assert.Equal(t, 1, c.ParentSeq)
	assert.True(t, c.ParentWaits)
	assert.Equal(t, "run "+parent, c.Trigger)
	assert.Equal(t, json.Number("10000000000000001"), c.Params["n"], "numbers stay exact")

	h.replayRun(child, nil, completed(`{"rows":3}`)) // the parent waits: only the child's task is queued
	_, finished = h.childEvents(parent)
	assert.Equal(t, state.RunCompleted, finished[1].Status)
	assert.Equal(t, child, finished[1].Run)
	assert.JSONEq(t, `{"rows":3}`, string(finished[1].Result))

	h.replayRun(parent, seqs(startChildCmd("y", `{}`, true)), completed(`"done"`)) // woken by it
	assert.Equal(t, state.RunCompleted, h.run(parent).Status)
}

func TestFireAndForgetChild(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("x")
	h.replayRun(parent, seqs(startChildCmd("y", ``, false)), nil)
	started, _ := h.childEvents(parent)
	child := started[1].Run
	assert.False(t, h.run(child).ParentWaits)

	h.replayRun(child, nil, nil)                                             // the child's first replay was queued first
	h.replayRun(parent, seqs(startChildCmd("y", ``, false)), completed(`1`)) // answered at once
	assert.Equal(t, state.RunCompleted, h.run(parent).Status)
	assert.Equal(t, state.RunRunning, h.run(child).Status, "a child it doesn't wait for carries on")
	require.NoError(t, h.core.CancelRun(h.ctx, child, "test"))
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished)
}

func TestChildThatCantStart(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("x")
	h.replayRun(parent, seqs(startChildCmd("nobody_defines_this", `{}`, true)), nil)
	started, finished := h.childEvents(parent)
	assert.Empty(t, started)
	assert.Equal(t, state.RunFailed, finished[1].Status)
	assert.Equal(t, "start_failed", finished[1].Reason)
	assert.Contains(t, finished[1].Message, `no registered build defines run "nobody_defines_this"`)
	h.replayRun(parent, seqs(startChildCmd("nobody_defines_this", `{}`, true)), completed(`null`)) // woken
}

func TestEndingAParentCancelsTheChildrenItWaitsFor(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("x")
	h.replayRun(parent, seqs(startChildCmd("y", ``, false)), nil)
	started, _ := h.childEvents(parent)
	free := started[1].Run
	h.replayRun(free, nil, nil)
	h.replayRun(parent, seqs(startChildCmd("y", ``, false), startChildCmd("y", ``, true)), nil)
	started, _ = h.childEvents(parent)
	waited := started[2].Run

	// The waited-for child has a grandchild it waits for too.
	h.replayRun(waited, seqs(startChildCmd("x", ``, true)), nil)
	started, _ = h.childEvents(waited)
	grandchild := started[1].Run

	require.NoError(t, h.core.CancelRun(h.ctx, parent, "test"))
	for _, id := range []string{waited, grandchild} {
		got := h.run(id)
		assert.Equal(t, state.RunCancelled, got.Status, id)
		assert.Equal(t, "parent_finished", got.Reason)
	}
	assert.Equal(t, state.RunRunning, h.run(free).Status)
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished, "a cancelled parent isn't told")
}

func TestContinueAsNew(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("y")
	h.replayRun(parent, seqs(startChildCmd("x", `{"cursor":0}`, true)), nil)
	started, _ := h.childEvents(parent)
	first := started[1].Run

	cont := &RunOutcome{Status: state.RunContinued, Params: map[string]any{"cursor": 100}}
	h.replayRun(first, seqs(stepCmd("a")), nil)
	h.succeedStep(h.poll(TaskStep))
	h.replayRun(first, seqs(stepCmd("a")), cont)

	old := h.run(first)
	assert.Equal(t, state.RunContinued, old.Status)
	second := old.ContinuedAs
	require.NotEmpty(t, second)
	assert.JSONEq(t, `{"continued_as":"`+second+`"}`, string(old.Result))
	next := h.run(second)
	assert.Equal(t, state.RunRunning, next.Status)
	assert.Equal(t, "x", next.Name)
	assert.Equal(t, "continue "+first, next.Trigger)
	assert.Equal(t, json.Number("100"), next.Params["cursor"])
	assert.Equal(t, parent, next.ParentID, "the parent link carries over")
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished, "the parent waits for the chain, not the first run")

	evs, err := h.store.RunEvents(h.ctx, second)
	require.NoError(t, err)
	assert.Equal(t, state.EventRunStarted, evs[0].Type, "a fresh event log")

	h.replayRun(second, nil, completed(`"end of chain"`))
	_, finished = h.childEvents(parent)
	assert.Equal(t, second, finished[1].Run)
	assert.JSONEq(t, `"end of chain"`, string(finished[1].Result))
}

func TestChildResultAfterRestart(t *testing.T) {
	h := newHarness(t)
	parent := h.startRun("x")
	h.replayRun(parent, seqs(startChildCmd("y", ``, true)), nil)
	started, _ := h.childEvents(parent)

	h.restart()
	h.replayRun(started[1].Run, nil, &RunOutcome{Status: state.RunFailed, Reason: "ValueError", Message: "bad day"})
	_, finished := h.childEvents(parent)
	assert.Equal(t, state.RunFailed, finished[1].Status)
	assert.Equal(t, "bad day", finished[1].Message)
}

func TestStartRunValidation(t *testing.T) {
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(startChildCmd("", ``, true))}), "needs a pipeline name")
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(startChildCmd("y", `[1]`, true))}), "params must be a JSON object")
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Outcome: &RunOutcome{Status: "paused"}}), `"continued"`)
}

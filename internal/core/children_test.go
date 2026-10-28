package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startChildCmd(name string, params string, wait bool) Command {
	return Command{Kind: CommandStartHaul, Name: name, Args: json.RawMessage(params), Wait: wait}
}

// replayHaul completes the next haul task, which must be haulID's.
func (h *harness) replayHaul(haulID string, cmds []Command, outcome *HaulOutcome) {
	h.t.Helper()
	t := h.poll(TaskHaul)
	require.Equal(h.t, haulID, t.HaulID, "the next haul task is another haul's")
	require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Outcome: outcome}))
}

// childEvents returns a haul's ChildStarted and ChildFinished payloads by seq.
func (h *harness) childEvents(haulID string) (map[int]childStartedPayload, map[int]childFinishedPayload) {
	h.t.Helper()
	evs, err := h.store.HaulEvents(h.ctx, haulID)
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

func completed(result string) *HaulOutcome {
	return &HaulOutcome{Status: state.HaulCompleted, Result: json.RawMessage(result)}
}

func TestChildHaulResultReachesItsParent(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("x")
	h.replayHaul(parent, seqs(startChildCmd("y", `{"day":"2026-10-24","n":10000000000000001}`, true)), nil)

	started, finished := h.childEvents(parent)
	child := started[1].Haul
	require.NotEmpty(t, child)
	assert.Empty(t, finished)
	c := h.haul(child)
	assert.Equal(t, "y", c.Name)
	assert.Equal(t, parent, c.ParentID)
	assert.Equal(t, 1, c.ParentSeq)
	assert.True(t, c.ParentWaits)
	assert.Equal(t, "haul "+parent, c.Trigger)
	assert.Equal(t, json.Number("10000000000000001"), c.Params["n"], "numbers stay exact")

	h.replayHaul(child, nil, completed(`{"rows":3}`)) // the parent waits: only the child's task is queued
	_, finished = h.childEvents(parent)
	assert.Equal(t, state.HaulCompleted, finished[1].Status)
	assert.Equal(t, child, finished[1].Haul)
	assert.JSONEq(t, `{"rows":3}`, string(finished[1].Result))

	h.replayHaul(parent, seqs(startChildCmd("y", `{}`, true)), completed(`"done"`)) // woken by it
	assert.Equal(t, state.HaulCompleted, h.haul(parent).Status)
}

func TestFireAndForgetChild(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("x")
	h.replayHaul(parent, seqs(startChildCmd("y", ``, false)), nil)
	started, _ := h.childEvents(parent)
	child := started[1].Haul
	assert.False(t, h.haul(child).ParentWaits)

	h.replayHaul(child, nil, nil)                                             // the child's first replay was queued first
	h.replayHaul(parent, seqs(startChildCmd("y", ``, false)), completed(`1`)) // answered at once
	assert.Equal(t, state.HaulCompleted, h.haul(parent).Status)
	assert.Equal(t, state.HaulRunning, h.haul(child).Status, "a child it doesn't wait for carries on")
	require.NoError(t, h.core.CancelHaul(h.ctx, child, "test"))
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished)
}

func TestChildThatCantStart(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("x")
	h.replayHaul(parent, seqs(startChildCmd("nobody_defines_this", `{}`, true)), nil)
	started, finished := h.childEvents(parent)
	assert.Empty(t, started)
	assert.Equal(t, state.HaulFailed, finished[1].Status)
	assert.Equal(t, "start_failed", finished[1].Reason)
	assert.Contains(t, finished[1].Message, `no registered build defines haul "nobody_defines_this"`)
	h.replayHaul(parent, seqs(startChildCmd("nobody_defines_this", `{}`, true)), completed(`null`)) // woken
}

func TestEndingAParentCancelsTheChildrenItWaitsFor(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("x")
	h.replayHaul(parent, seqs(startChildCmd("y", ``, false)), nil)
	started, _ := h.childEvents(parent)
	free := started[1].Haul
	h.replayHaul(free, nil, nil)
	h.replayHaul(parent, seqs(startChildCmd("y", ``, false), startChildCmd("y", ``, true)), nil)
	started, _ = h.childEvents(parent)
	waited := started[2].Haul

	// The waited-for child has a grandchild it waits for too.
	h.replayHaul(waited, seqs(startChildCmd("x", ``, true)), nil)
	started, _ = h.childEvents(waited)
	grandchild := started[1].Haul

	require.NoError(t, h.core.CancelHaul(h.ctx, parent, "test"))
	for _, id := range []string{waited, grandchild} {
		got := h.haul(id)
		assert.Equal(t, state.HaulCancelled, got.Status, id)
		assert.Equal(t, "parent_finished", got.Reason)
	}
	assert.Equal(t, state.HaulRunning, h.haul(free).Status)
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished, "a cancelled parent isn't told")
}

func TestContinueAsNew(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("y")
	h.replayHaul(parent, seqs(startChildCmd("x", `{"cursor":0}`, true)), nil)
	started, _ := h.childEvents(parent)
	first := started[1].Haul

	cont := &HaulOutcome{Status: state.HaulContinued, Params: map[string]any{"cursor": 100}}
	h.replayHaul(first, seqs(lift("a")), nil)
	h.succeedLift(h.poll(TaskLift))
	h.replayHaul(first, seqs(lift("a")), cont)

	old := h.haul(first)
	assert.Equal(t, state.HaulContinued, old.Status)
	second := old.ContinuedAs
	require.NotEmpty(t, second)
	assert.JSONEq(t, `{"continued_as":"`+second+`"}`, string(old.Result))
	next := h.haul(second)
	assert.Equal(t, state.HaulRunning, next.Status)
	assert.Equal(t, "x", next.Name)
	assert.Equal(t, "continue "+first, next.Trigger)
	assert.Equal(t, json.Number("100"), next.Params["cursor"])
	assert.Equal(t, parent, next.ParentID, "the parent link carries over")
	_, finished := h.childEvents(parent)
	assert.Empty(t, finished, "the parent waits for the chain, not the first haul")

	evs, err := h.store.HaulEvents(h.ctx, second)
	require.NoError(t, err)
	assert.Equal(t, state.EventHaulStarted, evs[0].Type, "a fresh logbook")

	h.replayHaul(second, nil, completed(`"end of chain"`))
	_, finished = h.childEvents(parent)
	assert.Equal(t, second, finished[1].Haul)
	assert.JSONEq(t, `"end of chain"`, string(finished[1].Result))
}

func TestChildResultAfterRestart(t *testing.T) {
	h := newHarness(t)
	parent := h.startHaul("x")
	h.replayHaul(parent, seqs(startChildCmd("y", ``, true)), nil)
	started, _ := h.childEvents(parent)

	h.restart()
	h.replayHaul(started[1].Haul, nil, &HaulOutcome{Status: state.HaulFailed, Reason: "ValueError", Message: "bad day"})
	_, finished := h.childEvents(parent)
	assert.Equal(t, state.HaulFailed, finished[1].Status)
	assert.Equal(t, "bad day", finished[1].Message)
}

func TestStartHaulValidation(t *testing.T) {
	assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(startChildCmd("", ``, true))}), "needs a haul name")
	assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(startChildCmd("y", `[1]`, true))}), "params must be a JSON object")
	assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Outcome: &HaulOutcome{Status: "paused"}}), `"continued"`)
}

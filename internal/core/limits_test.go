package core

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func limited(name string, limits ...ParallelLimit) Command {
	cmd := lift(name)
	cmd.Limits = limits
	return cmd
}

func gatherOf(n int, group string, max int) []Command {
	var cmds []Command
	for i := range n {
		cmds = append(cmds, limited(string(rune('a'+i)), ParallelLimit{Group: group, Max: max}))
	}
	return cmds
}

// pollAll takes every lift task there is right now, by name.
func (h *harness) pollAll() map[string]Task {
	h.t.Helper()
	out := map[string]Task{}
	for {
		t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskLift}})
		require.NoError(h.t, err)
		if !ok {
			return out
		}
		out[t.Name] = t
	}
}

func names(m map[string]Task) []string { return slices.Sorted(maps.Keys(m)) }

// liftAt returns the lift with seq of a haul.
func (h *harness) liftAt(haulID string, seq int) state.Lift {
	h.t.Helper()
	l, err := h.store.GetLift(h.ctx, haulID, seq)
	require.NoError(h.t, err)
	return l
}

func TestGatherLimitRunsAtMostMaxAtOnce(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(gatherOf(5, "gather:1", 2)...), nil)

	running := h.pollAll()
	assert.Equal(t, []string{"a", "b"}, names(running), "two slots, in command order")
	assert.Equal(t, state.LiftScheduled, h.liftAt(id, 3).Status, "the rest wait, not queued")

	h.succeedLift(running["a"])
	assert.Equal(t, []string{"c"}, names(h.pollAll()), "a slot freed: the next one starts")
	h.succeedLift(running["b"])
	next := h.pollAll()
	assert.Equal(t, []string{"d"}, names(next))
	h.succeedLift(next["d"])
	assert.Equal(t, []string{"e"}, names(h.pollAll()))
}

func TestEveryGroupMustHaveRoom(t *testing.T) {
	h := newHarness(t)
	h.startHaul("x")
	haul := ParallelLimit{Group: "haul", Max: 3}
	h.replay(seqs(
		limited("a", haul, ParallelLimit{Group: "gather:1", Max: 1}),
		limited("b", haul, ParallelLimit{Group: "gather:1", Max: 1}),
		limited("c", haul), limited("d", haul), limited("e", haul),
	), nil)
	running := h.pollAll()
	assert.Equal(t, []string{"a", "c", "d"}, names(running), "b waits for its gather, e for the haul")
	h.succeedLift(running["c"])
	assert.Equal(t, []string{"e"}, names(h.pollAll()), "c's slot: b's gather is still full, e fits")
	h.succeedLift(running["a"])
	assert.Equal(t, []string{"b"}, names(h.pollAll()))
}

func TestARetryWaitHoldsNoSlot(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	a := limited("a", ParallelLimit{Group: "g", Max: 1})
	a.Retry = &RetryPolicy{Limit: 1, Initial: time.Minute}
	h.replay(seqs(a, limited("b", ParallelLimit{Group: "g", Max: 1})), nil)
	first := h.pollAll()["a"]
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: first.ID, Status: state.AttemptFailed, Reason: "boom"}))
	assert.Equal(t, state.LiftRetryWait, h.liftAt(id, 1).Status)
	b := h.pollAll()
	assert.Equal(t, []string{"b"}, names(b), "a's slot went to b while a waits to retry")

	h.advance(time.Minute)
	assert.Empty(t, h.pollAll(), "a's retry waits for b's slot")
	h.succeedLift(b["b"])
	assert.Equal(t, []string{"a"}, names(h.pollAll()))
}

func TestLimitsArePerHaul(t *testing.T) {
	h := newHarness(t)
	h.startHaul("x")
	h.replay(seqs(gatherOf(2, "g", 1)...), nil)
	h.startHaul("x")
	h.replay(seqs(gatherOf(2, "g", 1)...), nil)
	hauls := map[string]int{}
	for {
		task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}, Kinds: []TaskKind{TaskLift}})
		require.NoError(t, err)
		if !ok {
			break
		}
		hauls[task.HaulID]++
	}
	assert.Equal(t, []int{1, 1}, slices.Collect(maps.Values(hauls)), "one per haul: the same group in two hauls is two groups")
}

func TestCancellingAHaulDropsThrottledLifts(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(gatherOf(3, "g", 1)...), nil)
	h.pollAll()
	require.NoError(t, h.core.CancelHaul(h.ctx, id, "test"))
	assert.Empty(t, h.pollAll(), "nothing started while cancelling")
	for seq := 1; seq <= 3; seq++ {
		assert.Equal(t, state.LiftCancelled, h.liftAt(id, seq).Status)
	}
}

func TestLimitsSurviveRestart(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(gatherOf(4, "g", 2)...), nil)
	running := h.pollAll()
	require.Equal(t, []string{"a", "b"}, names(running))

	h.restart()
	assert.Empty(t, h.pollAll(), "a and b still hold the slots")
	h.succeedLift(running["a"])
	assert.Equal(t, []string{"c"}, names(h.pollAll()))
	assert.Equal(t, state.LiftScheduled, h.liftAt(id, 4).Status)
}

func TestLimitValidation(t *testing.T) {
	for want, limits := range map[string][]ParallelLimit{
		"needs a group":    {{Max: 1}},
		"at least 1":       {{Group: "g"}},
		`group "g" twice`:  {{Group: "g", Max: 1}, {Group: "g", Max: 2}},
		"at most 8 limits": make([]ParallelLimit, 9),
	} {
		assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(limited("a", limits...))}), want)
	}
}

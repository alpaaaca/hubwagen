package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two projects with different code share the default dock: revenue defines
// haul "rev" and lift "extract", marketing haul "mkt" and lift "load".
func newProjectsHarness(t *testing.T) *harness {
	h := newHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "revenue", Hauls: []string{"rev"}, Lifts: map[string]string{"extract": "v1"}}))
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "marketing", Hauls: []string{"mkt"}, Lifts: map[string]string{"load": "v1"}}))
	return h
}

func (h *harness) pollAs(build string, kind TaskKind) (Task, bool) {
	h.t.Helper()
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: build + "-1", BuildID: build, Docks: []string{DefaultDock}, Kinds: []TaskKind{kind}})
	require.NoError(h.t, err)
	return t, ok
}

func TestHaulTasksGoToWorkersWhoseBuildDefinesThem(t *testing.T) {
	h := newProjectsHarness(t)
	mkt := h.startHaul("mkt")
	rev := h.startHaul("rev")

	task, ok := h.pollAs("revenue", TaskHaul)
	require.True(t, ok)
	assert.Equal(t, rev, task.HaulID, "the older mkt task is skipped, not handed to the wrong code")
	_, ok = h.pollAs("revenue", TaskHaul)
	assert.False(t, ok, "mkt stays queued for its own workers")

	task, ok = h.pollAs("marketing", TaskHaul)
	require.True(t, ok)
	assert.Equal(t, mkt, task.HaulID)
}

func TestLiftsGoToWorkersWhoseBuildDefinesThem(t *testing.T) {
	h := newProjectsHarness(t)
	h.startHaul("rev")
	task, _ := h.pollAs("revenue", TaskHaul)
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: task.ID, BuildID: "revenue", Commands: seqs(lift("extract"))}))

	_, ok := h.pollAs("marketing", TaskLift)
	assert.False(t, ok)
	a, ok := h.pollAs("revenue", TaskLift)
	require.True(t, ok)
	assert.Equal(t, "extract", a.Name)
}

func TestPollersWithoutABuildTakeAnything(t *testing.T) {
	h := newProjectsHarness(t)
	id := h.startHaul("mkt")
	assert.Equal(t, id, h.poll(TaskHaul).HaulID)
}

func TestPollingWithAnUnknownBuildFails(t *testing.T) {
	h := newHarness(t)
	_, _, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", BuildID: "nope", Docks: []string{DefaultDock}})
	assert.ErrorIs(t, err, ErrUnknownBuild)
}

func TestStartHaulNeedsABuildThatDefinesIt(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "nowhere"})
	assert.ErrorIs(t, err, ErrUnknownHaul)
	assert.ErrorContains(t, err, `no registered build defines haul "nowhere"`)
}

func TestBuildHaulsAreImmutable(t *testing.T) {
	h := newHarness(t)
	lifts := map[string]string{"extract": "v1"}
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Hauls: []string{"rev", "rev"}, Lifts: lifts}))
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Hauls: []string{"rev"}, Lifts: lifts}), "the same build again")
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Hauls: []string{"rev", "other"}, Lifts: lifts})
	assert.ErrorContains(t, err, "already registered with other hauls")
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b2", Hauls: []string{""}}), ErrInvalid)
}

func TestABuildRecordedWithoutHaulsGetsThem(t *testing.T) {
	h := newHarness(t)
	lifts := map[string]string{"extract": "v1"}
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "old", Lifts: lifts}))
	h.startHaul("x")
	task, ok := h.pollAs("old", TaskHaul)
	require.True(t, ok, "a build without recorded hauls takes any haul task")
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: task.ID, BuildID: "old", Commands: seqs(lift("extract"))}))

	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "old", Hauls: []string{"rev"}, Lifts: lifts}))
	h.startHaul("x")
	_, ok = h.pollAs("old", TaskHaul)
	assert.False(t, ok, "now it defines rev only")
}

func TestBuildHaulsSurviveRestart(t *testing.T) {
	h := newProjectsHarness(t)
	h.restart()
	mkt := h.startHaul("mkt")
	h.startHaul("rev")
	task, ok := h.pollAs("marketing", TaskHaul)
	require.True(t, ok)
	assert.Equal(t, mkt, task.HaulID)
	_, ok = h.pollAs("marketing", TaskHaul)
	assert.False(t, ok)
}

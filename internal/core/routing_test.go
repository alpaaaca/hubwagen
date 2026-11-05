package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two projects with different code share the default queue: revenue defines
// run "rev" and step "extract", marketing run "mkt" and step "load".
func newProjectsHarness(t *testing.T) *harness {
	h := newHarness(t)
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "revenue", Pipelines: []string{"rev"}, Steps: map[string]string{"extract": "v1"}}))
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "marketing", Pipelines: []string{"mkt"}, Steps: map[string]string{"load": "v1"}}))
	return h
}

func (h *harness) pollAs(build string, kind TaskKind) (Task, bool) {
	h.t.Helper()
	t, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: build + "-1", BuildID: build, Queues: []string{DefaultQueue}, Kinds: []TaskKind{kind}})
	require.NoError(h.t, err)
	return t, ok
}

func TestRunTasksGoToWorkersWhoseBuildDefinesThem(t *testing.T) {
	h := newProjectsHarness(t)
	mkt := h.startRun("mkt")
	rev := h.startRun("rev")

	task, ok := h.pollAs("revenue", TaskRun)
	require.True(t, ok)
	assert.Equal(t, rev, task.RunID, "the older mkt task is skipped, not handed to the wrong code")
	_, ok = h.pollAs("revenue", TaskRun)
	assert.False(t, ok, "mkt stays queued for its own workers")

	task, ok = h.pollAs("marketing", TaskRun)
	require.True(t, ok)
	assert.Equal(t, mkt, task.RunID)
}

func TestStepsGoToWorkersWhoseBuildDefinesThem(t *testing.T) {
	h := newProjectsHarness(t)
	h.startRun("rev")
	task, _ := h.pollAs("revenue", TaskRun)
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: task.ID, BuildID: "revenue", Commands: seqs(stepCmd("extract"))}))

	_, ok := h.pollAs("marketing", TaskStep)
	assert.False(t, ok)
	a, ok := h.pollAs("revenue", TaskStep)
	require.True(t, ok)
	assert.Equal(t, "extract", a.Name)
}

func TestPollersWithoutABuildTakeAnything(t *testing.T) {
	h := newProjectsHarness(t)
	id := h.startRun("mkt")
	assert.Equal(t, id, h.poll(TaskRun).RunID)
}

func TestPollingWithAnUnknownBuildFails(t *testing.T) {
	h := newHarness(t)
	_, _, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", BuildID: "nope", Queues: []string{DefaultQueue}})
	assert.ErrorIs(t, err, ErrUnknownBuild)
}

func TestStartRunNeedsABuildThatDefinesIt(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.StartRun(h.ctx, StartRunRequest{Tenant: "t", Name: "nowhere"})
	assert.ErrorIs(t, err, ErrUnknownPipeline)
	assert.ErrorContains(t, err, `no registered build defines run "nowhere"`)
}

func TestBuildRunsAreImmutable(t *testing.T) {
	h := newHarness(t)
	steps := map[string]string{"extract": "v1"}
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Pipelines: []string{"rev", "rev"}, Steps: steps}))
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Pipelines: []string{"rev"}, Steps: steps}), "the same build again")
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Pipelines: []string{"rev", "other"}, Steps: steps})
	assert.ErrorContains(t, err, "already registered with other pipelines")
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b2", Pipelines: []string{""}}), ErrInvalid)
}

func TestABuildRecordedWithoutRunsGetsThem(t *testing.T) {
	h := newHarness(t)
	steps := map[string]string{"extract": "v1"}
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "old", Steps: steps}))
	h.startRun("x")
	task, ok := h.pollAs("old", TaskRun)
	require.True(t, ok, "a build without recorded runs takes any run task")
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: task.ID, BuildID: "old", Commands: seqs(stepCmd("extract"))}))

	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "old", Pipelines: []string{"rev"}, Steps: steps}))
	h.startRun("x")
	_, ok = h.pollAs("old", TaskRun)
	assert.False(t, ok, "now it defines rev only")
}

func TestBuildRunsSurviveRestart(t *testing.T) {
	h := newProjectsHarness(t)
	h.restart()
	mkt := h.startRun("mkt")
	h.startRun("rev")
	task, ok := h.pollAs("marketing", TaskRun)
	require.True(t, ok)
	assert.Equal(t, mkt, task.RunID)
	_, ok = h.pollAs("marketing", TaskRun)
	assert.False(t, ok)
}

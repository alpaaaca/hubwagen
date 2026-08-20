package tests

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/scheduler"
	"github.com/alper/hubwagen/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// discoverOne is a small helper: write a DAG file to disk and discover it,
// since TriggerManual takes a dag.Discovered (it needs the YAMLHash), not a
// bare *dag.DAG.
func discoverOne(t *testing.T, yamlContent string) dag.Discovered {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "d.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlContent), 0o644))
	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.NoError(t, found[0].ParseErr)
	return found[0]
}

func openMemStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestTriggerManual_StartsAndSucceeds(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	d := discoverOne(t, `
name: trigger_ok
steps:
  - id: s1
    executor: noop
`)
	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}

	result, err := scheduler.TriggerManual(ctx, st, d, executors, io.Discard, "manual")
	require.NoError(t, err)
	assert.False(t, result.Skipped)
	require.NotEmpty(t, result.RunID)

	run, err := st.GetRun(ctx, result.RunID)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", run.Status)
	assert.Equal(t, "manual", run.Trigger)

	// UpsertDAG must have happened too.
	steps, err := st.ListStepRuns(ctx, result.RunID)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	assert.Equal(t, "succeeded", steps[0].Status)
}

func TestTriggerManual_ReturnsRunIDOnFailure(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	d := discoverOne(t, `
name: trigger_fail
steps:
  - id: s1
    executor: noop
`)
	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{ExitCode: 1}}

	result, err := scheduler.TriggerManual(ctx, st, d, executors, io.Discard, "manual")
	assert.Error(t, err, "a failed step must surface as an error")
	assert.False(t, result.Skipped)
	require.NotEmpty(t, result.RunID, "the run still started and should be inspectable even though it failed")

	run, err := st.GetRun(ctx, result.RunID)
	require.NoError(t, err)
	assert.Equal(t, "failed", run.Status)
}

func TestTriggerManual_SkipsWhenAlreadyRunning(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	d := discoverOne(t, `
name: trigger_overlap
steps:
  - id: s1
    executor: noop
`)

	// Seed a run that's still "running" — TriggerManual must not start a
	// second one on top of it (on_overlap: skip, the schema default). The
	// DAG record must exist first (runs.dag_name has a FK to dags.name).
	require.NoError(t, st.UpsertDAG(ctx, "trigger_overlap", "", "", "seed-hash"))
	inProgress := &store.Run{
		ID:      "in-progress-run",
		DAGName: "trigger_overlap",
		Status:  "running",
		Trigger: "manual",
	}
	require.NoError(t, st.CreateRun(ctx, inProgress))

	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}
	result, err := scheduler.TriggerManual(ctx, st, d, executors, io.Discard, "manual")
	require.NoError(t, err)
	assert.True(t, result.Skipped)
	assert.Empty(t, result.RunID)
	assert.Contains(t, result.Reason, "in-progress-run")

	runs, err := st.ListRuns(ctx, "trigger_overlap", 10)
	require.NoError(t, err)
	assert.Len(t, runs, 1, "no second run should have been created")
}

func TestTriggerManual_DoesNotSkipAfterPriorRunFinished(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	d := discoverOne(t, `
name: trigger_after_finish
steps:
  - id: s1
    executor: noop
`)

	require.NoError(t, st.UpsertDAG(ctx, "trigger_after_finish", "", "", "seed-hash"))
	finished := &store.Run{
		ID:      "finished-run",
		DAGName: "trigger_after_finish",
		Status:  "succeeded",
		Trigger: "manual",
	}
	require.NoError(t, st.CreateRun(ctx, finished))

	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}
	result, err := scheduler.TriggerManual(ctx, st, d, executors, io.Discard, "manual")
	require.NoError(t, err)
	assert.False(t, result.Skipped)
	assert.NotEmpty(t, result.RunID)
}

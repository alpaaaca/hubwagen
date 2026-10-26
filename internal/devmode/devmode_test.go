package devmode

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

const pipeline = `apiVersion: hubwagen.dev/v1alpha1
kind: Pipeline
metadata: {name: p, tenant: t}
spec:
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - {id: a, command: [/bin/sh, -c], args: ["echo VERSION"]}
`

func start(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Start(ctx, Config{DataDir: dir, DisableSchedules: true})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

func write(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func TestDataDirIsLocked(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir)
	_, err := Start(ctx, Config{DataDir: dir})
	assert.ErrorIs(t, err, ErrLocked)

	require.NoError(t, e.Close())
	e2 := start(t, dir) // released on Close
	assert.Equal(t, int64(2), e2.Store.Term(), "each start is a new term")
}

func TestLoaderPublishesAndReloads(t *testing.T) {
	data, pipes := t.TempDir(), t.TempDir()
	e := start(t, data)
	file := filepath.Join(pipes, "p.yaml")
	write(t, file, strings.Replace(pipeline, "VERSION", "one", 1))
	write(t, filepath.Join(pipes, "broken.yaml"), "apiVersion: hubwagen.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: Bad, tenant: t}\n")
	write(t, filepath.Join(pipes, ".hidden", "x.yaml"), "not: a pipeline")

	l := NewLoader(e.Core, []string{pipes})
	changed, err := l.Load(ctx)
	require.NoError(t, err)
	require.Len(t, changed, 2, "hidden directories are skipped")
	files := l.Files()
	assert.Contains(t, files[0].Err, "metadata.name", "broken.yaml sorts first and reports its error")
	assert.Equal(t, []string{"t/p"}, files[1].Pipelines)

	again, err := l.Load(ctx)
	require.NoError(t, err)
	assert.Empty(t, again, "unchanged files are not reloaded")

	v1, err := e.Store.GetPipeline(ctx, "t", "p")
	require.NoError(t, err)
	write(t, file, strings.Replace(pipeline, "VERSION", "two", 1))
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(file, future, future))
	changed, err = l.Load(ctx)
	require.NoError(t, err)
	require.Len(t, changed, 1)
	v2, err := e.Store.GetPipeline(ctx, "t", "p")
	require.NoError(t, err)
	assert.NotEqual(t, v1.CurrentVersion, v2.CurrentVersion)
}

func TestRunAndRestart(t *testing.T) {
	dir := t.TempDir()
	e := start(t, dir)
	l := NewLoader(e.Core, []string{writeTemp(t, pipeline)})
	_, err := l.Load(ctx)
	require.NoError(t, err)

	res, err := e.Core.Trigger(ctx, core.TriggerRequest{Tenant: "t", Pipeline: "p", IdempotencyKey: "k"})
	require.NoError(t, err)
	run, err := e.WaitRun(ctx, res.RunID)
	require.NoError(t, err)
	assert.Equal(t, state.RunSucceeded, run.Status)

	var lines []string
	for _, ev := range mustEvents(t, e) {
		if l := Describe(ev); l != "" {
			lines = append(lines, l)
		}
	}
	assert.Contains(t, strings.Join(lines, "\n"), "task a succeeded")

	require.NoError(t, e.Close())
	e2 := start(t, dir)
	again, err := e2.Core.Trigger(ctx, core.TriggerRequest{Tenant: "t", Pipeline: "p", IdempotencyKey: "k"})
	require.NoError(t, err)
	assert.Equal(t, res.RunID, again.RunID, "state survives a restart")
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "p.yaml")
	write(t, p, content)
	return p
}

func mustEvents(t *testing.T, e *Engine) []state.Event {
	t.Helper()
	evs, err := e.Store.EventsSince(ctx, 0, "", 1000)
	require.NoError(t, err)
	return evs
}

func TestServesTheWorkerAPI(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir(), DisableSchedules: true, Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	defer e.Close()
	require.NotEmpty(t, e.APIAddr)

	resp, err := http.Post("http://"+e.APIAddr+"/hubwagen.v1.WorkerService/Heartbeat", "application/json",
		strings.NewReader(`{"workerId":"w1","taskIds":["x"]}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// A long poll in flight doesn't hold up Close.
	go http.Post("http://"+e.APIAddr+"/hubwagen.v1.WorkerService/PollLiftTask", "application/json",
		strings.NewReader(`{"workerId":"w1","docks":["default"],"wait":"30s"}`))
	time.Sleep(100 * time.Millisecond)
	began := time.Now()
	require.NoError(t, e.Close())
	assert.Less(t, time.Since(began), 5*time.Second)
}

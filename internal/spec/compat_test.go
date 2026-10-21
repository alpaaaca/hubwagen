package spec

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every example in the repository must keep compiling: v1 files through the
// compat mapping, v1alpha1 files natively.
func TestExamplesCompile(t *testing.T) {
	root := filepath.Join("..", "..", "dags", "examples")
	var n int
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".yaml" {
			return err
		}
		n++
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		_, err = CompileFile(context.Background(), p, data, Options{})
		assert.NoError(t, err, p)
		return nil
	})
	require.NoError(t, err)
	assert.Greater(t, n, 5)
}

func TestV1Compat(t *testing.T) {
	src := `name: legacy
description: "v1 DAG"
schedule: "0 2 * * *"
timezone: Europe/Berlin
concurrency: {max_runs: 1}
env: {WAREHOUSE: bq, MODE: dag}
retry: {limit: 2, interval_sec: 30, backoff: true}
steps:
  - id: extract
    executor: shell
    command: "run --date={{ .RunDate }} --id {{.RunID}} --dag {{ .DAGName }} # .RunDate outside an action stays"
    env: {MODE: step, DAY: "{{ .RunDate }}"}
    timeout_sec: 300
  - id: load
    depends_on: [extract]
    command: "load"
    retry: {limit: 1}
    on_failure: continue
  - id: check
    executor: noop
    depends_on: [load]
`
	out, err := CompileFile(context.Background(), "legacy.yaml", []byte(src), Options{})
	require.NoError(t, err)
	s := out[0].Spec

	assert.Equal(t, "v1", s.SourceFormat)
	assert.Equal(t, V1Tenant, s.Tenant)
	assert.Equal(t, &Schedule{Cron: "0 2 * * *", Timezone: "Europe/Berlin", Interval: IntervalCron, Catchup: false}, s.Schedule)
	assert.Equal(t, Concurrency{MaxActiveRuns: 1, OnOverlap: "skip"}, s.Concurrency)

	extract := s.Task("extract")
	assert.Equal(t, Runtime{Kind: RuntimeAgent, Pool: V1Pool}, extract.Runtime)
	assert.Equal(t, []string{"/bin/sh", "-c"}, extract.Command)
	assert.Equal(t, []string{"run --date={{ .Run.Date }} --id {{.Run.ID}} --dag {{ .Pipeline }} # .RunDate outside an action stays"}, extract.Args)
	assert.Equal(t, map[string]string{"WAREHOUSE": "bq", "MODE": "step", "DAY": "{{ .Run.Date }}"}, extract.Env)
	assert.Equal(t, int64(300), extract.TimeoutSeconds)
	assert.Equal(t, Retry{Limit: 2, Backoff: "exponential", InitialSeconds: 30, MaxSeconds: 3600}, extract.Retry)

	load := s.Task("load")
	assert.Equal(t, Retry{Limit: 1, Backoff: "fixed", InitialSeconds: 0, MaxSeconds: 3600}, load.Retry, "a step retry block replaces the DAG's")
	assert.Equal(t, "continue", load.OnFailure)
	assert.Equal(t, int64(48*3600), load.TimeoutSeconds, "v1 steps had no timeout")

	assert.Equal(t, []string{"true"}, s.Task("check").Command)
}

func TestV1CompatRejects(t *testing.T) {
	src := `name: legacy
steps:
  - id: notify
    executor: http
    http: {method: POST, url: "https://example.com"}
  - id: q
    executor: sql
  - id: home
    command: "echo {{ .Env.HOME }}"
`
	_, err := CompileFile(context.Background(), "legacy.yaml", []byte(src), Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `steps[0].executor: executor "http" is not supported in v1alpha1`)
	assert.Contains(t, err.Error(), `steps[1].executor: executor "sql" is not supported`)
	assert.Contains(t, err.Error(), `steps[2].command: {{ .Env.X }} is no longer supported`)
}

package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/runtime/localrt"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/alper/hubwagen/internal/taskexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The new engine end to end, as dev mode will wire it: a file-backed store,
// the core, the local runtime and the task wrapper running real processes
// against a file:// bucket.

type engine struct {
	t      *testing.T
	dir    string
	store  *state.Store
	bucket blob.Bucket
	core   *core.Core
	rt     *localrt.Runtime
	stop   func()
}

func startEngine(t *testing.T, dir string, wrap func(localrt.Reporter) localrt.Reporter) *engine {
	t.Helper()
	st, err := state.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	term, err := st.StartTerm(context.Background(), 0)
	require.NoError(t, err)
	b, err := blob.Open(blob.FileURI(filepath.Join(dir, "bucket")))
	require.NoError(t, err)

	c := core.New(core.Config{
		Store: st, Term: term, Clock: clock.Real{}, Layout: layout.New(b.Base()),
		Recovery: taskexec.Recovery{Bucket: b}, AutoCreateTenants: true,
	})
	var reporter localrt.Reporter = c
	if wrap != nil {
		reporter = wrap(c)
	}
	rt := localrt.New(reporter, localrt.Config{Slots: 4, Exec: taskexec.Options{
		Bucket: b, BaseEnv: []string{"PATH=" + os.Getenv("PATH")}, FlushInterval: 50 * time.Millisecond,
	}})
	c.SetRuntime(spec.RuntimeAgent, rt)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	e := &engine{t: t, dir: dir, store: st, bucket: b, core: c, rt: rt}
	stopped := false
	e.stop = func() {
		if stopped {
			return
		}
		stopped = true
		rt.Close()
		cancel()
		require.NoError(t, <-done)
		st.Close()
	}
	t.Cleanup(e.stop)
	require.NoError(t, c.Barrier(context.Background()))
	return e
}

func (e *engine) publish(src string) {
	e.t.Helper()
	out, err := spec.CompileFile(context.Background(), "p.yaml", []byte(src), spec.Options{})
	require.NoError(e.t, err)
	_, err = e.core.Publish(context.Background(), core.PublishRequest{SpecJSON: out[0].Canonical})
	require.NoError(e.t, err)
}

func (e *engine) waitRun(id string) state.Run {
	e.t.Helper()
	var r state.Run
	require.Eventually(e.t, func() bool {
		var err error
		r, err = e.store.GetRun(context.Background(), id)
		require.NoError(e.t, err)
		return state.RunTerminal(r.Status)
	}, 20*time.Second, 20*time.Millisecond, "run %s did not finish", id)
	return r
}

const twoStep = `apiVersion: hubwagen.dev/v1alpha1
kind: Pipeline
metadata: {name: orders, tenant: t}
spec:
  defaults:
    runtime: {kind: agent, pool: local}
  tasks:
    - id: extract
      command: [/bin/sh, -c]
      args: ['printf "1,10\n2,20\n" > "$HUBWAGEN_OUTPUT_ROWS_PATH/rows.csv"; echo extracted {{ .Interval.Start | date "2006-01-02" }}']
      outputs: [{name: rows}]
    - id: total
      depends_on: [extract]
      command: [/bin/sh, -c]
      args: ['awk -F, "{s+=\$2} END {print s}" "$HUBWAGEN_INPUT_ROWS_PATH/rows.csv" > "$HUBWAGEN_OUTPUT_SUM_PATH/sum.txt"']
      inputs: [{name: rows, from: extract.rows}]
      outputs: [{name: sum}]
`

func TestEngineRunsAPipelineEndToEnd(t *testing.T) {
	e := startEngine(t, t.TempDir(), nil)
	e.publish(twoStep)

	start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	res, err := e.core.Trigger(context.Background(), core.TriggerRequest{
		Tenant: "t", Pipeline: "orders", Interval: &spec.Interval{Start: start, End: start.AddDate(0, 0, 1)},
	})
	require.NoError(t, err)
	assert.Equal(t, state.RunSucceeded, e.waitRun(res.RunID).Status)

	outs, err := e.store.ListOutputs(context.Background(), res.RunID)
	require.NoError(t, err)
	require.Len(t, outs, 2)
	var sumURI string
	for _, o := range outs {
		if o.Name == "sum" {
			sumURI = o.URI
		}
	}
	sum, err := e.bucket.Get(context.Background(), sumURI+"sum.txt")
	require.NoError(t, err)
	assert.Equal(t, "30\n", string(sum), "total read extract's committed output")

	atts, err := e.store.ListAttempts(context.Background(), res.RunID, "extract")
	require.NoError(t, err)
	chunks, err := e.bucket.List(context.Background(), atts[0].LogsURI)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)
	first, err := e.bucket.Get(context.Background(), chunks[0])
	require.NoError(t, err)
	assert.Contains(t, string(first), "extracted 2026-10-20")
}

// dropFinal forwards "running" reports but swallows final ones: the leader
// dies after the task committed its output but before hearing about it.
type dropFinal struct{ next localrt.Reporter }

func (d dropFinal) Report(r core.Report) {
	if r.Status == state.AttemptRunning {
		d.next.Report(r)
	}
}

func TestEngineRecoversCommittedAttemptsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	e := startEngine(t, dir, func(next localrt.Reporter) localrt.Reporter { return dropFinal{next} })
	e.publish(twoStep)
	day := &spec.Interval{Start: time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	res, err := e.core.Trigger(context.Background(), core.TriggerRequest{Tenant: "t", Pipeline: "orders", Interval: day})
	require.NoError(t, err)

	// Wait for extract's commit marker, then "crash".
	require.Eventually(t, func() bool {
		list, _ := e.bucket.List(context.Background(), e.bucket.Base()+"/commits/")
		return len(list) == 1
	}, 10*time.Second, 20*time.Millisecond)
	e.stop()

	e2 := startEngine(t, dir, nil)
	assert.Equal(t, state.RunSucceeded, e2.waitRun(res.RunID).Status)

	atts, err := e2.store.ListAttempts(context.Background(), res.RunID, "extract")
	require.NoError(t, err)
	require.Len(t, atts, 1, "extract was not run again")
	assert.Equal(t, state.AttemptSucceeded, atts[0].Status)
	assert.Equal(t, "recovered", atts[0].Reason)

	again, err := e2.core.Trigger(context.Background(), core.TriggerRequest{Tenant: "t", Pipeline: "orders", Interval: day})
	require.NoError(t, err)
	assert.True(t, again.Existing)
}

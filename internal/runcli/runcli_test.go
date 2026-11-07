package runcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/moksha/internal/api"
	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/blob"
	"github.com/alper/moksha/internal/clock"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/layout"
	"github.com/alper/moksha/internal/state"
)

var ctx = context.Background()

func TestExpandParams(t *testing.T) {
	sets, err := ExpandParams([]string{"day=2026-02-27..2026-03-02", "full=false", "region=eu", "n=3", `cfg={"a":1}`})
	require.NoError(t, err)
	var days []any
	for _, s := range sets {
		days = append(days, s["day"])
		assert.Equal(t, false, s["full"])
		assert.Equal(t, "eu", s["region"])
		assert.Equal(t, json.Number("3"), s["n"])
		assert.Equal(t, map[string]any{"a": json.Number("1")}, s["cfg"])
	}
	assert.Equal(t, []any{"2026-02-27", "2026-02-28", "2026-03-01", "2026-03-02"}, days)

	sets, err = ExpandParams([]string{"a=1..2", "b=1..3"})
	require.NoError(t, err)
	assert.Len(t, sets, 6, "ranges multiply")
	assert.Equal(t, json.Number("2"), sets[5]["a"])
	assert.Equal(t, json.Number("3"), sets[5]["b"])

	sets, err = ExpandParams(nil)
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{}}, sets, "no params: one run")

	for _, bad := range [][]string{
		{"day"}, {"=x"}, {"day=2026-03-02..2026-03-01"}, {"day=2026-03-01..soon"},
		{"n=a..b"}, {"n=5..1"}, {"n=0..20000"}, {"a=1..200", "b=1..200"},
	} {
		_, err := ExpandParams(bad)
		assert.Error(t, err, "%v", bad)
	}
}

func TestReport(t *testing.T) {
	step := func(name, status, reason string, reused bool) *mokshav1.Step {
		return &mokshav1.Step{Name: name, Status: status, RunReason: reason, Reused: reused}
	}
	runs := []*mokshav1.Run{
		{Id: "h1", Status: "completed", Steps: []*mokshav1.Step{
			step("extract", "completed", "", true), step("build", "completed", "code changed (v1 → v2)", false)}},
		{Id: "h2", Status: "completed", Steps: []*mokshav1.Step{
			step("extract", "completed", "", true), step("build", "completed", "input changed", false)}},
		{Id: "h3", Status: "failed", Reason: "step_failed", Message: "build: boom", Steps: []*mokshav1.Step{
			step("extract", "completed", "no previous run", false),
			{Name: "build", Status: "failed", RunReason: "input changed", Reason: "error"}}},
	}
	assert.Equal(t, `3 runs
  extract   2 reused, 1 ran (it hasn't run with these inputs before)
  build     1 ran (an input changed), 1 ran (its code changed (v1 → v2)), 1 failed (error)
2 completed, 1 failed
  h3 failed: step_failed build: boom
`, Report(runs))
}

// server runs a core with RunService and WorkerService over HTTP, and a
// worker in the test process that runs a two-step run: extract (memoized,
// one data version per day) then report (not memoized).
type server struct {
	core   *core.Core
	url    string
	bucket blob.Bucket
	build  atomic.Value // the build the worker runs as
}

func start(t *testing.T) *server {
	t.Helper()
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	term, err := st.StartTerm(ctx, 0)
	require.NoError(t, err)
	b, err := blob.Open(blob.FileURI(filepath.Join(dir, "bucket")))
	require.NoError(t, err)
	lay := layout.New(b.Base())
	c := core.New(core.Config{Store: st, Term: term, Clock: clock.Real{}, Layout: lay, DataVersions: dataversion.NewStore(b, lay)})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	require.NoError(t, c.Barrier(ctx))

	mux := http.NewServeMux()
	path, h := api.NewRunHandler(c, st)
	mux.Handle(path, h)
	path, h = api.NewWorkerHandler(c, time.Second, nil)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)

	s := &server{core: c, url: srv.URL, bucket: b}
	require.NoError(t, c.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily_orders"},
		Steps: map[string]string{"extract": "v1", "report": "v1"}}))
	s.build.Store("b1")
	workCtx, stopWork := context.WithCancel(runCtx)
	worked := make(chan struct{})
	go func() { defer close(worked); s.work(workCtx) }()
	t.Cleanup(func() {
		stopWork()
		<-worked
		srv.Close()
		cancel()
		require.NoError(t, <-done)
		st.Close()
	})
	return s
}

func (s *server) work(ctx context.Context) {
	for ctx.Err() == nil {
		task, ok, err := s.core.Poll(ctx, core.PollRequest{WorkerID: "w1", Queues: []string{core.DefaultQueue}, Wait: 50 * time.Millisecond})
		if err != nil || !ok {
			continue
		}
		switch task.Kind {
		case core.TaskRun:
			cmds, outcome := replay(task.EventLog)
			_ = s.core.CompleteRunTask(ctx, core.RunTaskResult{TaskID: task.ID, BuildID: s.build.Load().(string), Commands: cmds, Outcome: outcome})
		case core.TaskStep:
			res := core.StepResult{AttemptID: task.ID, Status: state.AttemptSucceeded}
			if task.Output != nil {
				content := []byte("orders for " + string(task.Args))
				_ = s.bucket.Put(ctx, task.Staging+"orders.csv", content)
				sum := sha256.Sum256(content)
				res.Output = &core.StepOutput{Files: []dataversion.File{{Path: "orders.csv", Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}}}
			}
			_ = s.core.CompleteStep(ctx, res)
		}
	}
}

func replay(eventLog []state.Event) ([]core.Command, *core.RunOutcome) {
	var day string
	done := map[int]bool{}
	for _, e := range eventLog {
		switch e.Type {
		case state.EventRunStarted:
			var p struct {
				Params map[string]any `json:"params"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			day, _ = p.Params["day"].(string)
		case state.EventStepFinished:
			var p struct {
				Seq int `json:"seq"`
			}
			_ = json.Unmarshal(e.Payload, &p)
			done[p.Seq] = true
		}
	}
	args := json.RawMessage(`{"day":"` + day + `"}`)
	cmds := []core.Command{{Seq: 1, Kind: core.CommandStep, Name: "extract", Args: args, Memo: "fingerprint",
		Output: &core.OutputSpec{Dataset: "raw.orders", Partition: day, PartitionKey: "day"}}}
	if !done[1] {
		return cmds, nil
	}
	cmds = append(cmds, core.Command{Seq: 2, Kind: core.CommandStep, Name: "report", Args: args})
	if !done[2] {
		return cmds, nil
	}
	return cmds, &core.RunOutcome{Status: state.RunCompleted}
}

func (s *server) replayRange(t *testing.T, days string, reuse ...string) string {
	t.Helper()
	sets, err := ExpandParams([]string{"day=" + days})
	require.NoError(t, err)
	c := NewClient(s.url)
	var ids []string
	for _, p := range sets {
		id, err := c.Start(ctx, RunRequest{Tenant: "t", Name: "daily_orders", Params: p, Reuse: strings.Join(reuse, "")})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	runs, err := c.Wait(waitCtx, ids, 20*time.Millisecond)
	require.NoError(t, err)
	return Report(runs)
}

func TestReplayOverADateRange(t *testing.T) {
	s := start(t)

	assert.Equal(t, `3 runs
  extract   3 ran (it hasn't run with these inputs before)
  report    3 ran
3 completed
`, s.replayRange(t, "2026-03-01..2026-03-03"))

	// Again, with one more day: the three known days are reused.
	assert.Equal(t, `4 runs
  extract   3 reused, 1 ran (it hasn't run with these inputs before)
  report    4 ran
4 completed
`, s.replayRange(t, "2026-03-01..2026-03-04"))

	// --reuse=none: everything runs, and says why.
	assert.Equal(t, `2 runs
  extract   2 ran (its run was started with --reuse=none)
  report    2 ran
2 completed
`, s.replayRange(t, "2026-03-01..2026-03-02", "none"))
	_, err := NewClient(s.url).Start(ctx, RunRequest{Tenant: "t", Name: "daily_orders", Reuse: "never"})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.ErrorContains(t, err, `reuse "never": want auto or none`)

	// extract's code changed.
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b2", Steps: map[string]string{"extract": "v2", "report": "v1"}}))
	s.build.Store("b2")
	assert.Equal(t, `2 runs
  extract   2 ran (its code changed (v1 → v2))
  report    2 ran
2 completed
`, s.replayRange(t, "2026-03-01..2026-03-02"))
}

func TestSignal(t *testing.T) {
	s := start(t)
	c := NewClient(s.url)
	id, err := c.Start(ctx, RunRequest{Tenant: "t", Name: "daily_orders", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	_, err = c.Wait(ctx, []string{id}, 20*time.Millisecond)
	require.NoError(t, err)
	_, err = c.Signal(ctx, id, "approve", `{"by":"ana"}`)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "the run has finished")
	_, err = c.Signal(ctx, "nope", "approve", "")
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStartAndWait(t *testing.T) {
	s := start(t)
	c := NewClient(s.url)
	id, err := c.Start(ctx, RunRequest{Tenant: "t", Name: "daily_orders", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	runs, err := c.Wait(ctx, []string{id}, 20*time.Millisecond)
	require.NoError(t, err)
	h := runs[0]
	assert.Equal(t, "completed", h.Status)
	assert.JSONEq(t, `{"day":"2026-10-24"}`, h.ParamsJson)
	require.Len(t, h.Steps, 2)
	assert.Equal(t, "extract", h.Steps[0].Name)
	assert.NotEmpty(t, h.Steps[0].DataVersion)
	assert.Equal(t, int32(1), h.Steps[0].Attempts)
	assert.Empty(t, h.Steps[1].DataVersion)

	_, err = c.Wait(ctx, []string{"nope"}, time.Millisecond)
	assert.ErrorContains(t, err, "not found")
}

func TestWaitSaysWhyARunWaits(t *testing.T) {
	s := start(t)
	c := NewClient(s.url)
	c.HintAfter = time.Millisecond
	var hints []string
	c.Hint = func(h string) { hints = append(hints, h) }
	id, err := c.Start(ctx, RunRequest{Tenant: "t", Name: "daily_orders", Queue: "nowhere"}) // the test's worker polls only the default queue
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = c.Wait(waitCtx, []string{id}, 20*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, hints, 1, "each reason once")
	assert.Contains(t, hints[0], `waiting: no worker is connected on queue "nowhere"`)
}

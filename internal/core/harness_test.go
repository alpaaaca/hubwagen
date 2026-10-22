package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC)

// fakeRuntime records what the core asks it to do. Tests answer through
// harness.report.
type fakeRuntime struct {
	mu         sync.Mutex
	dispatched []Assignment
	cancelled  map[string]string // attempt → reason
}

func newFakeRuntime() *fakeRuntime { return &fakeRuntime{cancelled: map[string]string{}} }

func (f *fakeRuntime) Dispatch(a Assignment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, a)
}

func (f *fakeRuntime) Cancel(id, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled[id] = reason
}

type harness struct {
	t     *testing.T
	ctx   context.Context
	store *state.Store
	clock *clock.Fake
	rt    *fakeRuntime
	core  *Core
	stop  func() error
	ids   int
	// expectRunErr: the test makes core.Run fail on purpose.
	expectRunErr bool
}

type option func(*Config)

func newHarness(t *testing.T, opts ...option) *harness {
	t.Helper()
	return newHarnessAt(t, t0, opts...)
}

func newHarnessAt(t *testing.T, now time.Time, opts ...option) *harness {
	t.Helper()
	st, err := state.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	h := &harness{t: t, ctx: context.Background(), store: st, clock: clock.NewFake(now), rt: newFakeRuntime()}
	h.start(opts...)
	return h
}

// start runs a core with a new term over the harness's store.
func (h *harness) start(opts ...option) {
	h.t.Helper()
	term, err := h.store.StartTerm(h.ctx, 0)
	require.NoError(h.t, err)
	cfg := Config{
		Store: h.store, Term: term, Clock: h.clock, Layout: layout.New("file:///bucket"),
		Runtimes:          map[string]Runtime{spec.RuntimeAgent: h.rt, spec.RuntimeJob: h.rt},
		AutoCreateTenants: true,
		NewID: func() string {
			h.ids++
			return fmt.Sprintf("id%04d", h.ids)
		},
	}
	for _, o := range opts {
		o(&cfg)
	}
	h.core = New(cfg)
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- h.core.Run(ctx) }()

	var once sync.Once
	var runErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	h.stop = stop
	h.t.Cleanup(func() {
		if err := stop(); err != nil && !h.expectRunErr {
			h.t.Errorf("core.Run: %v", err)
		}
	})
	h.barrier()
}

// restart stops the core and starts a new one (next term) on the same store.
func (h *harness) restart(opts ...option) {
	h.t.Helper()
	require.NoError(h.t, h.stop())
	h.start(opts...)
}

func (h *harness) barrier() {
	h.t.Helper()
	require.NoError(h.t, h.core.Barrier(h.ctx))
}

// compile turns a pipeline body (the part under spec:) into canonical bytes.
func compile(t *testing.T, name, body string) []byte {
	t.Helper()
	src := "apiVersion: hubwagen.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: " + name + ", tenant: t}\nspec:\n" + body
	out, err := spec.CompileFile(context.Background(), name+".yaml", []byte(src), spec.Options{})
	require.NoError(t, err)
	return out[0].Canonical
}

func (h *harness) publish(name, body string) PublishResult {
	h.t.Helper()
	res, err := h.core.Publish(h.ctx, PublishRequest{SpecJSON: compile(h.t, name, body)})
	require.NoError(h.t, err)
	return res
}

func (h *harness) trigger(req TriggerRequest) TriggerResult {
	h.t.Helper()
	if req.Tenant == "" {
		req.Tenant = "t"
	}
	res, err := h.core.Trigger(h.ctx, req)
	require.NoError(h.t, err)
	return res
}

func day(d int) *spec.Interval {
	start := time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC)
	return &spec.Interval{Start: start, End: start.AddDate(0, 0, 1)}
}

// dispatched returns every assignment for a task, oldest first.
func (h *harness) dispatched(runID, task string) []Assignment {
	h.rt.mu.Lock()
	defer h.rt.mu.Unlock()
	var out []Assignment
	for _, a := range h.rt.dispatched {
		if a.RunID == runID && a.TaskID == task {
			out = append(out, a)
		}
	}
	return out
}

// last returns the newest assignment for a task, failing if there is none.
func (h *harness) last(runID, task string) Assignment {
	h.t.Helper()
	as := h.dispatched(runID, task)
	require.NotEmpty(h.t, as, "task %s was never dispatched", task)
	return as[len(as)-1]
}

func (h *harness) cancelReason(attemptID string) (string, bool) {
	h.rt.mu.Lock()
	defer h.rt.mu.Unlock()
	r, ok := h.rt.cancelled[attemptID]
	return r, ok
}

func (h *harness) report(r Report) {
	h.core.Report(r)
	h.barrier()
}

// succeed reports the task's newest attempt as started and succeeded,
// committing every declared output at its staging URI.
func (h *harness) succeed(runID, task string) {
	h.t.Helper()
	a := h.last(runID, task)
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptRunning, AgentID: "agent-1"})
	var outs []OutputRef
	for name, target := range a.Outputs {
		outs = append(outs, OutputRef{Name: name, URI: target.Staging})
	}
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptSucceeded, Reason: "exit_code", ExitCode: ptr(0), Outputs: outs})
}

func (h *harness) fail(runID, task, reason string) {
	h.t.Helper()
	a := h.last(runID, task)
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptRunning})
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptFailed, Reason: reason, ExitCode: ptr(1)})
}

func (h *harness) advance(d time.Duration) {
	h.clock.Advance(d)
	h.barrier()
}

func (h *harness) run(id string) state.Run {
	h.t.Helper()
	r, err := h.store.GetRun(h.ctx, id)
	require.NoError(h.t, err)
	return r
}

// tasks returns task statuses as "id=status" in task-ID order.
func (h *harness) tasks(runID string) string {
	h.t.Helper()
	trs, err := h.store.ListTaskRuns(h.ctx, runID)
	require.NoError(h.t, err)
	var parts []string
	for _, tr := range trs {
		parts = append(parts, tr.TaskID+"="+tr.Status)
	}
	return strings.Join(parts, " ")
}

func (h *harness) eventTypes(runID string) []state.EventType {
	h.t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(h.t, err)
	var out []state.EventType
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

const agentDefaults = "  defaults:\n    runtime: {kind: agent, pool: local}\n"

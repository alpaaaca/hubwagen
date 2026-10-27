package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC)

type harness struct {
	t     *testing.T
	ctx   context.Context
	store *state.Store
	clock *clock.Fake
	core  *Core
	stop  func() error
	ids   int
	// results overrides what runHaul's lifts return, by lift name (JSON).
	results map[string]string
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
	h := &harness{t: t, ctx: context.Background(), store: st, clock: clock.NewFake(now)}
	h.start(opts...)
	// StartHaul takes only hauls some build defines: these are the tests'.
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "test-hauls", Hauls: testHauls}))
	return h
}

var testHauls = []string{"a", "b", "daily_revenue", "fanout", "fire_and_forget", "ingest", "pipeline", "x", "y"}

// start runs a core with a new term over the harness's store.
func (h *harness) start(opts ...option) {
	h.t.Helper()
	term, err := h.store.StartTerm(h.ctx, 0)
	require.NoError(h.t, err)
	cfg := Config{
		Store: h.store, Term: term, Clock: h.clock, Layout: layout.New("file:///bucket"),
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

func (h *harness) advance(d time.Duration) {
	h.clock.Advance(d)
	h.barrier()
}

// do runs f in the core loop and waits for its batch to commit.
func (h *harness) do(f func(c *Core)) {
	h.t.Helper()
	_, err := submit(h.ctx, h.core, func(c *Core) (struct{}, error) {
		f(c)
		return struct{}{}, nil
	})
	require.NoError(h.t, err)
}

// fakeWorker polls docks like a real worker would and passes every task it
// receives to handle (by default: onto tasks). It stops when the test ends
// or the core stops.
type fakeWorker struct {
	id     string
	tasks  chan Task
	cancel context.CancelFunc
	done   chan struct{}
	err    error // why polling stopped
}

func (h *harness) startWorker(id string, req PollRequest, handle func(Task)) *fakeWorker {
	h.t.Helper()
	req.WorkerID = id
	if req.Wait == 0 {
		req.Wait = time.Hour // on the fake clock: until the test advances it
	}
	w := &fakeWorker{id: id, tasks: make(chan Task, 100), done: make(chan struct{})}
	if handle == nil {
		handle = func(t Task) { w.tasks <- t }
	}
	ctx, cancel := context.WithCancel(h.ctx)
	w.cancel = cancel
	core := h.core
	before := h.waitingPollers()
	go func() {
		defer close(w.done)
		for {
			t, ok, err := core.Poll(ctx, req)
			if err != nil {
				w.err = err
				return
			}
			if ok {
				handle(t)
			}
		}
	}()
	h.t.Cleanup(w.stop)
	// Return once the first poll is waiting, so tests can rely on the order
	// in which workers started.
	require.Eventually(h.t, func() bool { return h.waitingPollers() > before || len(w.tasks) > 0 },
		5*time.Second, time.Millisecond)
	return w
}

func (h *harness) waitingPollers() int {
	h.t.Helper()
	var n int
	h.do(func(c *Core) { n = len(c.pollers) })
	return n
}

// stop ends polling and waits for the worker's loop to exit.
func (w *fakeWorker) stop() {
	w.cancel()
	<-w.done
}

// next waits (in real time) for the worker's next task.
func (w *fakeWorker) next(t *testing.T) Task {
	t.Helper()
	select {
	case task := <-w.tasks:
		return task
	case <-time.After(5 * time.Second):
		t.Fatalf("worker %s received no task", w.id)
		return Task{}
	}
}

// none checks that the worker has received nothing (after a barrier, any
// task handed out has already been sent).
func (w *fakeWorker) none(t *testing.T) {
	t.Helper()
	select {
	case task := <-w.tasks:
		t.Fatalf("worker %s unexpectedly received %s %s", w.id, task.Kind, task.ID)
	case <-time.After(50 * time.Millisecond):
	}
}

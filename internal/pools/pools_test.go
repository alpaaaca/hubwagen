package pools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/state"
)

var ctx = context.Background()

// env is a real core on a fake clock, a launcher that records what it was
// asked, and workers simulated by polling the core.
type env struct {
	t      *testing.T
	core   *core.Core
	clock  *clock.Fake
	launch *launcher
	ids    int
}

type launcher struct {
	mu      sync.Mutex
	started []string
	stopped []string
}

func (l *launcher) Start(_, id string) { l.mu.Lock(); l.started = append(l.started, id); l.mu.Unlock() }
func (l *launcher) Stop(id string)     { l.mu.Lock(); l.stopped = append(l.stopped, id); l.mu.Unlock() }

func (l *launcher) take() (started, stopped []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	started, stopped, l.started, l.stopped = l.started, l.stopped, nil, nil
	return started, stopped
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st, err := state.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	term, err := st.StartTerm(ctx, 0)
	require.NoError(t, err)
	e := &env{t: t, clock: clock.NewFake(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)), launch: &launcher{}}
	e.core = core.New(core.Config{Store: st, Term: term, Clock: e.clock, Layout: layout.New("file:///bucket"),
		NewID: func() string { e.ids++; return fmt.Sprintf("id%04d", e.ids) }})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- e.core.Run(runCtx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.NoError(t, e.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"}, Lifts: map[string]string{"load": "v1"}}))
	return e
}

func (e *env) scaler(pools ...Pool) *Scaler {
	return New(pools, e.launch, e.core, e.clock, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func (e *env) tick(s *Scaler) (started, stopped []string) {
	e.t.Helper()
	require.NoError(e.t, s.Tick(ctx))
	return e.launch.take()
}

func (e *env) startRun() {
	e.t.Helper()
	_, err := e.core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily"})
	require.NoError(e.t, err)
}

// take has worker id take a task without waiting; the task stays running.
func (e *env) take(id string) core.Task {
	e.t.Helper()
	task, ok, err := e.core.Poll(ctx, core.PollRequest{WorkerID: id, BuildID: "b1", Docks: []string{core.DefaultDock}})
	require.NoError(e.t, err)
	require.True(e.t, ok, "%s got no task", id)
	return task
}

// idle has worker id wait for work until the test ends.
func (e *env) idle(id string) {
	e.t.Helper()
	pctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = e.core.Poll(pctx, core.PollRequest{WorkerID: id, BuildID: "b1", Docks: []string{core.DefaultDock}, Wait: 24 * time.Hour})
	}()
	e.t.Cleanup(func() { cancel(); <-done })
	require.Eventually(e.t, func() bool {
		ws, err := e.core.Workers(ctx)
		require.NoError(e.t, err)
		for _, w := range ws {
			if w.ID == id && w.Polling {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
}

func (e *env) pool(name string) core.PoolStatus {
	e.t.Helper()
	ps, err := e.core.Pools(ctx)
	require.NoError(e.t, err)
	for _, p := range ps {
		if p.Name == name {
			return p
		}
	}
	e.t.Fatalf("no pool %s", name)
	return core.PoolStatus{}
}

var py = Pool{Name: "py", Docks: []string{core.DefaultDock}, Lifts: 2, Min: 0, Max: 3, IdleAfter: 5 * time.Minute}

func TestScalesFromZeroForAWaitingRun(t *testing.T) {
	e := newEnv(t)
	s := e.scaler(py)
	started, _ := e.tick(s)
	assert.Empty(t, started, "nothing waits")

	e.startRun()
	started, _ = e.tick(s)
	assert.Equal(t, []string{"py-1"}, started)
	assert.Equal(t, "started py-1: 1 run waits on default", e.pool("py").Decision)
	started, _ = e.tick(s)
	assert.Empty(t, started, "py-1 is still starting")

	// Meanwhile the run says so.
	hauls, err := e.core.Waiting(ctx, []string{"id0001"})
	require.NoError(t, err)
	assert.Equal(t, core.WaitStarting, hauls["id0001"].Reason)
	assert.Equal(t, "py", hauls["id0001"].Pool)

	e.take("py-1")
	started, _ = e.tick(s)
	assert.Empty(t, started)
	assert.Equal(t, 0, e.pool("py").Starting)
}

func TestScalesUpForQueuedStepsUpToMax(t *testing.T) {
	e := newEnv(t)
	s := e.scaler(py)
	e.startRun()
	e.tick(s)
	task := e.take("py-1")
	var cmds []core.Command
	for i := 1; i <= 7; i++ {
		cmds = append(cmds, core.Command{Seq: i, Kind: core.CommandLift, Name: "load", Args: json.RawMessage(fmt.Sprint(i))})
	}
	require.NoError(t, e.core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1", Commands: cmds}))

	// 7 steps wait, py-1 isn't polling: 4 workers' worth, but max is 3.
	started, _ := e.tick(s)
	assert.Equal(t, []string{"py-2", "py-3"}, started)
	assert.Equal(t, "started py-2, py-3: 7 steps wait on default", e.pool("py").Decision)
	e.clock.Advance(StartGrace)
	started, _ = e.tick(s)
	assert.Empty(t, started, "at max")
}

func TestStopsIdleWorkersDownToMin(t *testing.T) {
	e := newEnv(t)
	p := py
	p.Min = 1
	s := e.scaler(p)
	started, _ := e.tick(s)
	assert.Equal(t, []string{"py-1"}, started, "min is started at once")
	e.startRun()
	t1 := e.take("py-1") // busy
	e.startRun()
	started, _ = e.tick(s)
	assert.Equal(t, []string{"py-2"}, started)
	t2 := e.take("py-2")

	// Both finish their runs and wait for work.
	for _, task := range []core.Task{t1, t2} {
		require.NoError(t, e.core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1",
			Outcome: &core.HaulOutcome{Status: state.HaulCompleted}}))
	}
	e.idle("py-1")
	e.idle("py-2")
	e.tick(s)
	e.clock.Advance(4 * time.Minute)
	_, stopped := e.tick(s)
	assert.Empty(t, stopped, "not idle for long enough")
	e.clock.Advance(time.Minute)
	_, stopped = e.tick(s)
	assert.Equal(t, []string{"py-2"}, stopped, "the highest number, down to min")
	assert.Equal(t, "stopped py-2: nothing to run for 5m0s", e.pool("py").Decision)
	assert.Equal(t, []string{"py-1"}, e.pool("py").Workers)
}

func TestTheFirstPoolOnADockTakesItsWork(t *testing.T) {
	e := newEnv(t)
	other := Pool{Name: "other", Docks: []string{core.DefaultDock, "gpu"}, Lifts: 1, Max: 1, IdleAfter: time.Minute}
	s := e.scaler(py, other)
	e.startRun()
	started, _ := e.tick(s)
	assert.Equal(t, []string{"py-1"}, started)
}

func TestAnExternalPoolPublishesHowManyWorkersItWants(t *testing.T) {
	e := newEnv(t)
	k8s := Pool{Name: "k8s", Docks: []string{core.DefaultDock}, Lifts: 2, Max: 5, IdleAfter: 5 * time.Minute, External: true}
	s := e.scaler(k8s)
	e.tick(s)
	assert.Equal(t, 0, e.pool("k8s").Desired)

	e.startRun()
	started, _ := e.tick(s)
	assert.Empty(t, started, "an external pool starts nothing")
	assert.Equal(t, 1, e.pool("k8s").Desired)
	assert.True(t, e.pool("k8s").External)
	assert.Equal(t, "wants 1 worker (was 0): 1 run waits on default", e.pool("k8s").Decision)

	// A pod starts and takes the run: still 1, the one that runs it.
	task := e.take("pod-a")
	e.tick(s)
	assert.Equal(t, 1, e.pool("k8s").Desired)
	assert.Equal(t, []string{"pod-a"}, e.pool("k8s").Workers)

	// It finishes and waits for work: wanted until idle for IdleAfter.
	require.NoError(t, e.core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1",
		Outcome: &core.HaulOutcome{Status: state.HaulCompleted}}))
	e.idle("pod-a")
	e.tick(s)
	e.clock.Advance(4 * time.Minute)
	e.tick(s)
	assert.Equal(t, 1, e.pool("k8s").Desired)
	e.clock.Advance(time.Minute)
	e.tick(s)
	assert.Equal(t, 0, e.pool("k8s").Desired)
	assert.Equal(t, "wants 0 workers (was 1): workers had nothing to run for 5m0s", e.pool("k8s").Decision)
}

func TestAnExternalPoolDoesntCountAProcessPoolsWorkers(t *testing.T) {
	e := newEnv(t)
	k8s := Pool{Name: "k8s", Docks: []string{core.DefaultDock}, Lifts: 1, Max: 5, IdleAfter: time.Minute, External: true}
	p := py
	p.Min = 1
	s := e.scaler(p, k8s)
	e.tick(s)
	e.idle("py-1")
	e.tick(s)
	assert.Equal(t, []string{"py-1"}, e.pool("py").Workers)
	assert.Empty(t, e.pool("k8s").Workers)
}

func TestAKubernetesPoolAppliesItsDesiredCount(t *testing.T) {
	e := newEnv(t)
	var applied []int
	fail := true
	k8s := Pool{Name: "k8s", Docks: []string{core.DefaultDock}, Lifts: 1, Min: 1, Max: 5, IdleAfter: time.Minute, External: true,
		Apply: func(_ context.Context, n int) error {
			applied = append(applied, n)
			if fail {
				return fmt.Errorf("forbidden")
			}
			return nil
		}}
	s := e.scaler(k8s)
	e.tick(s) // min: 1, but the API refuses
	fail = false
	e.tick(s) // tried again
	e.tick(s) // unchanged: not sent again
	// A pod takes a run, which asks for 3 steps: it and 3 more.
	e.startRun()
	task := e.take("pod-a")
	require.NoError(t, e.core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1", Commands: []core.Command{
		{Seq: 1, Kind: core.CommandLift, Name: "load", Args: json.RawMessage(`1`)},
		{Seq: 2, Kind: core.CommandLift, Name: "load", Args: json.RawMessage(`2`)},
		{Seq: 3, Kind: core.CommandLift, Name: "load", Args: json.RawMessage(`3`)},
	}}))
	e.tick(s)
	assert.Equal(t, []int{1, 1, 4}, applied, "min, its retry, then pod-a and one per waiting step")
	assert.Equal(t, 4, e.pool("k8s").Desired)
}

func TestSetPoolsRollsOut(t *testing.T) {
	e := newEnv(t)
	v1 := Pool{Name: "revenue-default-v1", Docks: []string{core.DefaultDock}, Lifts: 2, Min: 1, Max: 3, IdleAfter: time.Minute}
	s := e.scaler(v1)
	started, _ := e.tick(s)
	assert.Equal(t, []string{"revenue-default-v1-1"}, started)

	// A new version: its pool starts, the old one's workers stop.
	v2 := v1
	v2.Name = "revenue-default-v2"
	s.SetPools([]Pool{v2})
	_, stopped := e.launch.take()
	assert.Equal(t, []string{"revenue-default-v1-1"}, stopped)
	started, _ = e.tick(s)
	assert.Equal(t, []string{"revenue-default-v2-1"}, started)

	// The same pool with other bounds keeps its workers.
	v2.Max = 5
	s.SetPools([]Pool{v2})
	started, stopped = e.launch.take()
	assert.Empty(t, started)
	assert.Empty(t, stopped)
	assert.Equal(t, []string{"revenue-default-v2-1"}, e.pool("revenue-default-v2").Workers)
}

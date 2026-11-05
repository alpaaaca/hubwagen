package tests

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alper/moksha/internal/clock"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A run survives leader restarts: one while a step is running on a worker,
// one while the run sleeps. Workers are separate from the leader, so the
// worker here outlives each leader and talks to whichever one is current.

type leader struct {
	store *state.Store
	core  *core.Core
	stop  func()
}

func startLeader(t *testing.T, dir string, clk clock.Clock) *leader {
	t.Helper()
	st, err := state.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	term, err := st.StartTerm(context.Background(), 0)
	require.NoError(t, err)
	c := core.New(core.Config{Store: st, Term: term, Clock: clk})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	require.NoError(t, c.Barrier(context.Background()))

	var once sync.Once
	l := &leader{store: st, core: c}
	l.stop = func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-done)
			st.Close()
		})
	}
	t.Cleanup(l.stop)
	return l
}

// sequentialWorker replays a run that runs steps in order and runs its
// steps, reconnecting to the current leader when one stops.
type sequentialWorker struct {
	steps   []core.Command
	current func() *core.Core
	block   map[string]chan struct{} // step name → closed when the step may finish
	running chan string              // step names as they start

	mu   sync.Mutex
	runs map[string]int
}

func (w *sequentialWorker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		task, ok, err := w.current().Poll(ctx, core.PollRequest{WorkerID: "w1", Queues: []string{core.DefaultQueue}, Wait: time.Hour})
		if err != nil || !ok {
			time.Sleep(5 * time.Millisecond) // leader stopping: try the next one
			continue
		}
		switch task.Kind {
		case core.TaskRun:
			cmds, outcome := w.replay(task.EventLog)
			// If the leader stops first, its successor replaces this run task.
			_ = w.current().CompleteRunTask(ctx, core.RunTaskResult{TaskID: task.ID, BuildID: "b1", Commands: cmds, Outcome: outcome})
		case core.TaskStep:
			w.mu.Lock()
			w.runs[task.Name]++
			w.mu.Unlock()
			w.running <- task.Name
			if ch := w.block[task.Name]; ch != nil {
				<-ch
			}
			res := core.StepResult{AttemptID: task.ID, Status: state.AttemptSucceeded}
			for ctx.Err() == nil { // report to whichever leader is current
				if err := w.current().CompleteStep(ctx, res); !errors.Is(err, core.ErrStopped) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

func (w *sequentialWorker) replay(eventLog []state.Event) ([]core.Command, *core.RunOutcome) {
	done := map[int]bool{}
	for _, e := range eventLog {
		if e.Type == state.EventStepFinished || e.Type == state.EventTimerFired {
			var p struct {
				Seq int `json:"seq"`
			}
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				panic(err)
			}
			done[p.Seq] = true
		}
	}
	var cmds []core.Command
	for i, step := range w.steps {
		step.Seq = i + 1
		cmds = append(cmds, step)
		if !done[step.Seq] {
			return cmds, nil
		}
	}
	return cmds, &core.RunOutcome{Status: state.RunCompleted}
}

func TestRunSurvivesLeaderRestarts(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(time.Date(2026, 10, 25, 3, 0, 0, 0, time.UTC))
	ctx := context.Background()

	var mu sync.Mutex
	l := startLeader(t, dir, clk)
	current := func() *core.Core {
		mu.Lock()
		defer mu.Unlock()
		return l.core
	}
	restart := func() {
		mu.Lock()
		defer mu.Unlock()
		l.stop()
		l = startLeader(t, dir, clk)
	}

	release := make(chan struct{})
	w := &sequentialWorker{
		steps: []core.Command{
			{Kind: core.CommandStep, Name: "extract"},
			{Kind: core.CommandStep, Name: "transform"},
			{Kind: core.CommandSleep, Duration: 15 * time.Minute},
			{Kind: core.CommandStep, Name: "report"},
		},
		current: current,
		block:   map[string]chan struct{}{"transform": release},
		running: make(chan string, 10),
		runs:    map[string]int{},
	}
	wctx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); w.loop(wctx) }()
	t.Cleanup(func() { stopWorker(); <-workerDone })

	require.NoError(t, current().RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily_revenue"}}))
	res, err := current().StartRun(ctx, core.StartRunRequest{Tenant: "t", Name: "daily_revenue"})
	require.NoError(t, err)
	id := res.RunID
	waitFor := func(name string) {
		t.Helper()
		select {
		case got := <-w.running:
			require.Equal(t, name, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("step %s never started", name)
		}
	}
	waitFor("extract")
	waitFor("transform")

	// Restart while transform runs on the worker; it reports to the new leader.
	restart()
	close(release)

	// Restart again while the run sleeps; the new leader keeps the timer.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		timers, err := l.store.ListTimers(ctx, id)
		return err == nil && len(timers) == 1
	}, 5*time.Second, 5*time.Millisecond)
	restart()
	clk.Advance(15 * time.Minute)
	waitFor("report")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		h, err := l.store.GetRun(ctx, id)
		return err == nil && h.Status == state.RunCompleted
	}, 5*time.Second, 5*time.Millisecond)

	w.mu.Lock()
	assert.Equal(t, map[string]int{"extract": 1, "transform": 1, "report": 1}, w.runs, "each step ran once")
	w.mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	steps, err := l.store.ListSteps(ctx, id)
	require.NoError(t, err)
	require.Len(t, steps, 3)
	for _, lf := range steps {
		assert.Equal(t, state.StepCompleted, lf.Status, lf.Name)
		assert.Equal(t, 1, lf.Attempts, lf.Name)
	}
	tasks, err := l.store.ListRunTasks(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.RunTaskCompleted, tasks[len(tasks)-1].Status)
}

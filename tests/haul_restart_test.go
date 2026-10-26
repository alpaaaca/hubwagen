package tests

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A haul survives leader restarts: one while a lift is running on a worker,
// one while the haul sleeps. Workers are separate from the leader, so the
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

// sequentialWorker replays a haul that runs steps in order and runs its
// lifts, reconnecting to the current leader when one stops.
type sequentialWorker struct {
	steps   []core.Command
	current func() *core.Core
	block   map[string]chan struct{} // lift name → closed when the lift may finish
	running chan string              // lift names as they start

	mu   sync.Mutex
	runs map[string]int
}

func (w *sequentialWorker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		task, ok, err := w.current().Poll(ctx, core.PollRequest{WorkerID: "w1", Docks: []string{core.DefaultDock}, Wait: time.Hour})
		if err != nil || !ok {
			time.Sleep(5 * time.Millisecond) // leader stopping: try the next one
			continue
		}
		switch task.Kind {
		case core.TaskHaul:
			cmds, outcome := w.replay(task.Logbook)
			// If the leader stops first, its successor replaces this haul task.
			_ = w.current().CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1", Commands: cmds, Outcome: outcome})
		case core.TaskLift:
			w.mu.Lock()
			w.runs[task.Name]++
			w.mu.Unlock()
			w.running <- task.Name
			if ch := w.block[task.Name]; ch != nil {
				<-ch
			}
			res := core.LiftResult{AttemptID: task.ID, Status: state.AttemptSucceeded}
			for ctx.Err() == nil { // report to whichever leader is current
				if err := w.current().CompleteLift(ctx, res); !errors.Is(err, core.ErrStopped) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

func (w *sequentialWorker) replay(logbook []state.Event) ([]core.Command, *core.HaulOutcome) {
	done := map[int]bool{}
	for _, e := range logbook {
		if e.Type == state.EventLiftFinished || e.Type == state.EventTimerFired {
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
	return cmds, &core.HaulOutcome{Status: state.HaulCompleted}
}

func TestHaulSurvivesLeaderRestarts(t *testing.T) {
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
			{Kind: core.CommandLift, Name: "extract"},
			{Kind: core.CommandLift, Name: "transform"},
			{Kind: core.CommandSleep, Duration: 15 * time.Minute},
			{Kind: core.CommandLift, Name: "report"},
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

	res, err := current().StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily_revenue"})
	require.NoError(t, err)
	id := res.HaulID
	waitFor := func(name string) {
		t.Helper()
		select {
		case got := <-w.running:
			require.Equal(t, name, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("lift %s never started", name)
		}
	}
	waitFor("extract")
	waitFor("transform")

	// Restart while transform runs on the worker; it reports to the new leader.
	restart()
	close(release)

	// Restart again while the haul sleeps; the new leader keeps the timer.
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
		h, err := l.store.GetHaul(ctx, id)
		return err == nil && h.Status == state.HaulCompleted
	}, 5*time.Second, 5*time.Millisecond)

	w.mu.Lock()
	assert.Equal(t, map[string]int{"extract": 1, "transform": 1, "report": 1}, w.runs, "each lift ran once")
	w.mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	lifts, err := l.store.ListLifts(ctx, id)
	require.NoError(t, err)
	require.Len(t, lifts, 3)
	for _, lf := range lifts {
		assert.Equal(t, state.LiftCompleted, lf.Status, lf.Name)
		assert.Equal(t, 1, lf.Attempts, lf.Name)
	}
	tasks, err := l.store.ListHaulTasks(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.HaulTaskCompleted, tasks[len(tasks)-1].Status)
}

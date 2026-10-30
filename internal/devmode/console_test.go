package devmode

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/runlog"
	"github.com/alper/hubwagen/internal/state"
)

func TestConsolePrintsRunsAndWhatLiftsPrinted(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer e.Close()
	var (
		mu    sync.Mutex
		lines []string
	)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go e.Console(cctx, 0, false, func(s string) { mu.Lock(); lines = append(lines, s); mu.Unlock() })
	printed := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), lines...) }

	require.NoError(t, e.Core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"},
		Info: map[string]state.HaulInfo{"daily": {Title: "Daily revenue"}}}))
	res, err := e.Core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)

	// The haul asks for one lift; a worker runs it and prints.
	task, ok, err := e.Core.Poll(ctx, core.PollRequest{WorkerID: "w", Docks: []string{core.DefaultDock}})
	require.NoError(t, err)
	require.True(t, ok)
	lift := core.Command{Seq: 1, Kind: core.CommandLift, Name: "extract", Args: json.RawMessage(`{}`)}
	require.NoError(t, e.Core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1", Commands: []core.Command{lift}}))
	lt, ok, err := e.Core.Poll(ctx, core.PollRequest{WorkerID: "w", Docks: []string{core.DefaultDock}, Kinds: []core.TaskKind{core.TaskLift}})
	require.NoError(t, err)
	require.True(t, ok)
	now := time.Now()
	require.NoError(t, e.Logs.Append(runlog.Attempt{HaulID: res.HaulID, Seq: 1, Number: 1}, []runlog.Line{
		{Time: now, Stream: "stdout", Text: "6 orders"},
		{Time: now, Stream: "log", Level: "WARNING", Logger: "app", Text: "one order has no region"},
		{Time: now, Stream: "stderr", Text: "Traceback:\n  line 1"},
	}))
	require.NoError(t, e.Core.CompleteLift(ctx, core.LiftResult{AttemptID: lt.ID, Status: state.AttemptSucceeded, Result: json.RawMessage(`6`)}))
	task, ok, err = e.Core.Poll(ctx, core.PollRequest{WorkerID: "w", Docks: []string{core.DefaultDock}, Kinds: []core.TaskKind{core.TaskHaul}})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Core.CompleteHaulTask(ctx, core.HaulTaskResult{TaskID: task.ID, BuildID: "b1", Commands: []core.Command{lift},
		Outcome: &core.HaulOutcome{Status: state.HaulCompleted}}))

	require.Eventually(t, func() bool { return len(printed()) == 6 }, 5*time.Second, 10*time.Millisecond, "%q", printed())
	got := printed()
	assert.Equal(t, "▶ Daily revenue day=2026-10-24 started", got[0])
	assert.Equal(t, []string{
		"  day=2026-10-24 › extract │ 6 orders",
		"  day=2026-10-24 › extract │ WARNING one order has no region",
		"  day=2026-10-24 › extract │ Traceback:",
		"  day=2026-10-24 › extract │   line 1",
	}, got[1:5])
	assert.Regexp(t, `^✓ Daily revenue day=2026-10-24 completed in \d`, got[5])
	for _, l := range got {
		assert.NotContains(t, l, "scheduled", "engine events stay out")
	}
}

func TestConsoleSaysWhyARunWaits(t *testing.T) {
	defer func(d time.Duration) { hintAfter = d }(hintAfter)
	hintAfter = 30 * time.Millisecond
	e, err := Start(ctx, Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer e.Close()
	var (
		mu    sync.Mutex
		lines []string
	)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go e.Console(cctx, 0, false, func(s string) { mu.Lock(); lines = append(lines, s); mu.Unlock() })
	printed := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), lines...) }

	require.NoError(t, e.Core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"}}))
	_, err = e.Core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)

	waits := func() []string {
		var out []string
		for _, l := range printed() {
			if strings.Contains(l, "is waiting") {
				out = append(out, l)
			}
		}
		return out
	}
	require.Eventually(t, func() bool { return len(waits()) > 0 }, 5*time.Second, 10*time.Millisecond, "%q", printed())
	time.Sleep(100 * time.Millisecond)
	got := waits()
	require.Len(t, got, 1, "once")
	assert.True(t, strings.HasPrefix(got[0], "… daily day=2026-10-24 is waiting: no worker is connected to run the pipeline's code."), got[0])
}

package state

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// put appends events and runs every write in one transaction.
func put(t *testing.T, s *Store, events []Event, writes ...func(tx *Tx) error) {
	t.Helper()
	_, err := s.Append(ctx, s.Term(), events, func(tx *Tx) error {
		for _, w := range writes {
			if err := w(tx); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func run(id, status string) Run {
	return Run{
		ID: id, Tenant: "t", Name: "daily_revenue", Queue: "default", Trigger: "manual",
		Params: map[string]any{"day": "2026-10-24"}, Status: status, CreatedAt: t0,
	}
}

func step(runID string, seq int, status string) Step {
	return Step{
		RunID: runID, Seq: seq, Name: "extract_orders", Queue: "default",
		Args: json.RawMessage(`[{"day":"2026-10-24"}]`), Status: status, ScheduledAt: t0,
	}
}

func TestRunProjectionsRoundTrip(t *testing.T) {
	s, _ := openFile(t)

	h := run("h1", RunCompleted)
	h.Commands, h.BuildID, h.Result, h.FinishedAt = 2, "b-1", json.RawMessage(`{"rows":3}`), ptr(t0.Add(time.Hour))
	ht := RunTask{
		ID: "ht1", RunID: "h1", Number: 1, Term: s.Term(), WorkerID: "w1", BuildID: "b-1",
		Status: RunTaskCompleted, QueuedAt: t0, DispatchedAt: ptr(t0), FinishedAt: ptr(t0.Add(time.Second)),
	}
	l := step("h1", 1, StepCompleted)
	l.Attempts, l.Result, l.FinishedAt = 2, json.RawMessage(`{"data_version":"p1"}`), ptr(t0.Add(time.Minute))
	l.Timeout, l.Retry = 30*time.Minute, json.RawMessage(`{"limit":2}`)
	l.Output, l.Inputs = json.RawMessage(`{"dataset":"raw"}`), json.RawMessage(`[{"data_version":"p0"}]`)
	l.Fingerprint, l.FPInputs, l.RunReason = "fp1", json.RawMessage(`{"step":"x"}`), "no previous run"
	la := StepAttempt{
		ID: "la1", RunID: "h1", Seq: 1, Number: 1, Term: s.Term(), WorkerID: "w1", Status: AttemptFailed,
		Reason: "error", Message: "boom", LeaseExpiresAt: ptr(t0.Add(time.Minute)), DispatchedAt: t0,
		StartedAt: ptr(t0), FinishedAt: ptr(t0.Add(time.Second)),
	}
	tm := Timer{RunID: "h1", Seq: 2, FireAt: t0.Add(15 * time.Minute), Status: TimerFired, CreatedAt: t0, FiredAt: ptr(t0.Add(15 * time.Minute))}

	put(t, s, []Event{{Type: EventRunFinished, Tenant: "t", RunID: "h1"}},
		func(tx *Tx) error { return tx.PutRun(h) },
		func(tx *Tx) error { return tx.PutRunTask(ht) },
		func(tx *Tx) error { return tx.PutStep(l) },
		func(tx *Tx) error { return tx.PutStepAttempt(la) },
		func(tx *Tx) error { return tx.PutTimer(tm) },
	)

	gotRun, err := s.GetRun(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, h, gotRun)

	hts, err := s.ListRunTasks(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []RunTask{ht}, hts)

	ls, err := s.ListSteps(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []Step{l}, ls)

	las, err := s.ListStepAttempts(ctx, "h1", 1)
	require.NoError(t, err)
	assert.Equal(t, []StepAttempt{la}, las)

	tms, err := s.ListTimers(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []Timer{tm}, tms)

	_, err = s.GetRun(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestRunUnsetJSONStaysNil(t *testing.T) {
	s, _ := openFile(t)
	h := run("h1", RunRunning)
	h.Params = nil
	l := step("h1", 1, StepScheduled)
	l.Args = nil
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(h) }, func(tx *Tx) error { return tx.PutStep(l) })

	got, err := s.GetRun(ctx, "h1")
	require.NoError(t, err)
	assert.Nil(t, got.Params)
	assert.Nil(t, got.Result)
	ls, err := s.ListSteps(ctx, "h1")
	require.NoError(t, err)
	assert.Nil(t, ls[0].Args)
	assert.Nil(t, ls[0].Result)
}

func TestCountSteps(t *testing.T) {
	s, _ := openFile(t)
	writes := []func(tx *Tx) error{
		func(tx *Tx) error { return tx.PutRun(run("h1", RunRunning)) },
		func(tx *Tx) error { return tx.PutRun(run("h2", RunRunning)) },
		func(tx *Tx) error { return tx.PutRun(run("h3", RunRunning)) },
	}
	for i, st := range []string{StepCompleted, StepCompleted, StepFailed, StepCancelled, StepDispatched} {
		l := step("h1", i+1, st)
		l.Reused = i == 0
		writes = append(writes, func(tx *Tx) error { return tx.PutStep(l) })
	}
	writes = append(writes, func(tx *Tx) error { return tx.PutStep(step("h2", 1, StepScheduled)) })
	put(t, s, nil, writes...)

	got, err := s.CountSteps(ctx, []string{"h1", "h2", "h3"})
	require.NoError(t, err)
	assert.Equal(t, map[string]StepCounts{
		"h1": {Total: 5, Completed: 2, Reused: 1, Failed: 1, Cancelled: 1},
		"h2": {Total: 1},
	}, got, "h3 has no steps")
	got, err = s.CountSteps(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestRunUpsertUpdatesMutableFieldsOnly(t *testing.T) {
	s, _ := openFile(t)
	h := run("h1", RunRunning)
	l := step("h1", 1, StepScheduled)
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(h) }, func(tx *Tx) error { return tx.PutStep(l) })

	h.Status, h.Commands, h.Reason = RunStuck, 3, "nondeterminism"
	h.Name = "changed" // identity fields are fixed at creation
	l.Status, l.Attempts, l.RetryAt = StepRetryWait, 1, ptr(t0.Add(time.Minute))
	l.Args = json.RawMessage(`["changed"]`)
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(h) }, func(tx *Tx) error { return tx.PutStep(l) })

	got, err := s.GetRun(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, RunStuck, got.Status)
	assert.Equal(t, 3, got.Commands)
	assert.Equal(t, "daily_revenue", got.Name)

	ls, err := s.ListSteps(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, StepRetryWait, ls[0].Status)
	assert.Equal(t, t0.Add(time.Minute), *ls[0].RetryAt)
	assert.JSONEq(t, `[{"day":"2026-10-24"}]`, string(ls[0].Args))
}

func TestRunEventsIsTheEventLog(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, []Event{
		{Type: EventRunStarted, Tenant: "t", RunID: "h1", Time: t0},
		{Type: EventRunStarted, Tenant: "t", RunID: "h2", Time: t0},
		{Type: EventStepScheduled, Tenant: "t", RunID: "h1", Time: t0, Payload: json.RawMessage(`{"seq":1}`)},
		{Type: EventLeaderTermStarted, Time: t0},
		{Type: EventStepFinished, Tenant: "t", RunID: "h1", Time: t0, Payload: json.RawMessage(`{"seq":1}`)},
	})

	book, err := s.RunEvents(ctx, "h1")
	require.NoError(t, err)
	var types []EventType
	for _, e := range book {
		assert.Equal(t, "h1", e.RunID)
		types = append(types, e.Type)
	}
	assert.Equal(t, []EventType{EventRunStarted, EventStepScheduled, EventStepFinished}, types)
	assert.Less(t, book[0].Seq, book[1].Seq)

	all, err := s.EventsSince(ctx, 0, "", 0)
	require.NoError(t, err)
	require.Len(t, all, 5)
	assert.Equal(t, "h2", all[1].RunID)
	assert.Equal(t, "", all[3].RunID)
}

func TestLoadRuns(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, []Event{{Type: EventRunStarted, RunID: "h1"}},
		func(tx *Tx) error {
			for _, h := range []Run{run("h1", RunRunning), run("h2", RunCompleted), run("h3", RunStuck)} {
				if err := tx.PutRun(h); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			for _, ht := range []RunTask{
				{ID: "t1", RunID: "h1", Number: 1, Term: 1, Status: RunTaskCompleted, QueuedAt: t0},
				{ID: "t2", RunID: "h1", Number: 2, Term: 1, Status: RunTaskQueued, QueuedAt: t0},
				{ID: "t3", RunID: "h2", Number: 1, Term: 1, Status: RunTaskQueued, QueuedAt: t0}, // run closed
			} {
				if err := tx.PutRunTask(ht); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			// h1: step 1 finished, step 2 running with one failed and one
			// running attempt; h2's open step belongs to a closed run.
			for _, l := range []Step{step("h1", 1, StepCompleted), step("h1", 2, StepRunning), step("h2", 1, StepRunning)} {
				if err := tx.PutStep(l); err != nil {
					return err
				}
			}
			for _, a := range []StepAttempt{
				{ID: "a1", RunID: "h1", Seq: 1, Number: 1, Term: 1, Status: AttemptSucceeded, DispatchedAt: t0},
				{ID: "a2", RunID: "h1", Seq: 2, Number: 1, Term: 1, Status: AttemptFailed, DispatchedAt: t0},
				{ID: "a3", RunID: "h1", Seq: 2, Number: 2, Term: 1, Status: AttemptRunning, DispatchedAt: t0},
				{ID: "a4", RunID: "h2", Seq: 1, Number: 1, Term: 1, Status: AttemptRunning, DispatchedAt: t0},
			} {
				if err := tx.PutStepAttempt(a); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			for _, tm := range []Timer{
				{RunID: "h1", Seq: 3, FireAt: t0, Status: TimerFired, CreatedAt: t0},
				{RunID: "h3", Seq: 1, FireAt: t0, Status: TimerPending, CreatedAt: t0},
			} {
				if err := tx.PutTimer(tm); err != nil {
					return err
				}
			}
			return nil
		},
	)

	a, err := s.LoadRuns(ctx)
	require.NoError(t, err)

	var runs, tasks, steps, attempts []string
	for _, h := range a.Runs {
		runs = append(runs, h.ID)
	}
	for _, ht := range a.RunTasks {
		tasks = append(tasks, ht.ID)
	}
	for _, l := range a.Steps {
		steps = append(steps, fmt.Sprintf("%s/%d", l.RunID, l.Seq))
	}
	for _, la := range a.StepAttempts {
		attempts = append(attempts, la.ID)
	}
	assert.Equal(t, []string{"h1", "h3"}, runs, "running and stuck runs are open")
	assert.Equal(t, []string{"t2"}, tasks)
	assert.Equal(t, []string{"h1/2"}, steps)
	assert.Equal(t, []string{"a2", "a3"}, attempts, "every attempt of an open step, so numbering continues")
	require.Len(t, a.Timers, 1)
	assert.Equal(t, "h3", a.Timers[0].RunID)
	assert.Equal(t, int64(1), a.LastSeq)
}

func TestRunForeignKeys(t *testing.T) {
	s, _ := openFile(t)
	for name, w := range map[string]func(tx *Tx) error{
		"step without run": func(tx *Tx) error { return tx.PutStep(step("missing", 1, StepScheduled)) },
		"timer without run": func(tx *Tx) error {
			return tx.PutTimer(Timer{RunID: "missing", Seq: 1, FireAt: t0, Status: TimerPending, CreatedAt: t0})
		},
		"task without run": func(tx *Tx) error {
			return tx.PutRunTask(RunTask{ID: "x", RunID: "missing", Number: 1, Status: RunTaskQueued, QueuedAt: t0})
		},
		"attempt without step": func(tx *Tx) error {
			return tx.PutStepAttempt(StepAttempt{ID: "x", RunID: "missing", Seq: 1, Number: 1, Status: AttemptDispatched, DispatchedAt: t0})
		},
	} {
		_, err := s.Append(ctx, s.Term(), nil, w)
		assert.Error(t, err, name)
	}
}

func TestRunStatusHelpers(t *testing.T) {
	assert.True(t, RunOpen(RunRunning))
	assert.True(t, RunOpen(RunStuck))
	assert.False(t, RunOpen(RunCompleted))
	assert.True(t, RunTaskTerminal(RunTaskLost))
	assert.False(t, RunTaskTerminal(RunTaskDispatched))
	assert.True(t, StepTerminal(StepCancelled))
	assert.False(t, StepTerminal(StepRetryWait))
}

func TestStepRunReasonCanChange(t *testing.T) {
	s, _ := openFile(t)
	l := step("h1", 1, StepWaiting)
	l.RunReason = "no previous run"
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(run("h1", RunRunning)) }, func(tx *Tx) error { return tx.PutStep(l) })
	l.Status, l.RunReason = StepScheduled, "the run it waited for failed"
	put(t, s, nil, func(tx *Tx) error { return tx.PutStep(l) })
	got, err := s.GetStep(ctx, "h1", 1)
	require.NoError(t, err)
	assert.Equal(t, "the run it waited for failed", got.RunReason)
}

func TestMemoResultFindsRunsOnly(t *testing.T) {
	s, _ := openFile(t)
	other := run("h2", RunCompleted)
	other.Tenant = "u"
	finished := func(runID string, seq int, at time.Time, edit func(*Step)) func(tx *Tx) error {
		l := step(runID, seq, StepCompleted)
		l.Fingerprint, l.RunReason, l.Result, l.FinishedAt = "fp", "no previous run", json.RawMessage(fmt.Sprint(seq)), ptr(at)
		if edit != nil {
			edit(&l)
		}
		return func(tx *Tx) error { return tx.PutStep(l) }
	}
	put(t, s, nil,
		func(tx *Tx) error { return tx.PutRun(run("h1", RunCompleted)) },
		func(tx *Tx) error { return tx.PutRun(other) },
		finished("h1", 1, t0, nil),
		finished("h1", 2, t0.Add(time.Minute), func(l *Step) { l.Reused, l.RunReason = true, "" }),
		finished("h1", 3, t0.Add(2*time.Minute), func(l *Step) { l.Output = json.RawMessage(`{"dataset":"raw"}`) }),
		finished("h1", 4, t0.Add(3*time.Minute), func(l *Step) { l.Status = StepFailed }),
		finished("h2", 1, t0.Add(4*time.Minute), nil), // another tenant
	)

	got, err := s.MemoResult(ctx, "t", "fp", time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 1, got.Seq, "not the reuse, the data version's step, the failure or another tenant's run")
	assert.JSONEq(t, "1", string(got.Result))
	_, err = s.MemoResult(ctx, "t", "fp", t0.Add(time.Second))
	assert.ErrorIs(t, err, ErrNotFound, "too old")
	_, err = s.MemoResult(ctx, "t", "other", time.Time{})
	assert.ErrorIs(t, err, ErrNotFound)

	runs, err := s.RecentStepRuns(ctx, "t", "extract_orders", 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, 1, runs[0].Seq)
	assert.False(t, runs[0].Reused)
}

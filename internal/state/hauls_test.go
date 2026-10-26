package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
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

func haul(id, status string) Haul {
	return Haul{
		ID: id, Tenant: "t", Name: "daily_revenue", Dock: "default", Trigger: "manual",
		Params: map[string]any{"day": "2026-10-24"}, Status: status, CreatedAt: t0,
	}
}

func lift(haulID string, seq int, status string) Lift {
	return Lift{
		HaulID: haulID, Seq: seq, Name: "extract_orders", Dock: "default",
		Args: json.RawMessage(`[{"day":"2026-10-24"}]`), Status: status, ScheduledAt: t0,
	}
}

func TestHaulProjectionsRoundTrip(t *testing.T) {
	s, _ := openFile(t)

	h := haul("h1", HaulCompleted)
	h.Commands, h.BuildID, h.Result, h.FinishedAt = 2, "b-1", json.RawMessage(`{"rows":3}`), ptr(t0.Add(time.Hour))
	ht := HaulTask{
		ID: "ht1", HaulID: "h1", Number: 1, Term: s.Term(), WorkerID: "w1", BuildID: "b-1",
		Status: HaulTaskCompleted, QueuedAt: t0, DispatchedAt: ptr(t0), FinishedAt: ptr(t0.Add(time.Second)),
	}
	l := lift("h1", 1, LiftCompleted)
	l.Attempts, l.Result, l.FinishedAt = 2, json.RawMessage(`{"pallet":"p1"}`), ptr(t0.Add(time.Minute))
	l.Timeout, l.Retry = 30*time.Minute, json.RawMessage(`{"limit":2}`)
	l.Output, l.Inputs = json.RawMessage(`{"dataset":"raw"}`), json.RawMessage(`[{"pallet":"p0"}]`)
	l.Fingerprint, l.FPInputs, l.RunReason = "fp1", json.RawMessage(`{"lift":"x"}`), "no previous run"
	la := LiftAttempt{
		ID: "la1", HaulID: "h1", Seq: 1, Number: 1, Term: s.Term(), WorkerID: "w1", Status: AttemptFailed,
		Reason: "error", Message: "boom", LeaseExpiresAt: ptr(t0.Add(time.Minute)), DispatchedAt: t0,
		StartedAt: ptr(t0), FinishedAt: ptr(t0.Add(time.Second)),
	}
	tm := Timer{HaulID: "h1", Seq: 2, FireAt: t0.Add(15 * time.Minute), Status: TimerFired, CreatedAt: t0, FiredAt: ptr(t0.Add(15 * time.Minute))}

	put(t, s, []Event{{Type: EventHaulFinished, Tenant: "t", HaulID: "h1"}},
		func(tx *Tx) error { return tx.PutHaul(h) },
		func(tx *Tx) error { return tx.PutHaulTask(ht) },
		func(tx *Tx) error { return tx.PutLift(l) },
		func(tx *Tx) error { return tx.PutLiftAttempt(la) },
		func(tx *Tx) error { return tx.PutTimer(tm) },
	)

	gotHaul, err := s.GetHaul(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, h, gotHaul)

	hts, err := s.ListHaulTasks(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []HaulTask{ht}, hts)

	ls, err := s.ListLifts(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []Lift{l}, ls)

	las, err := s.ListLiftAttempts(ctx, "h1", 1)
	require.NoError(t, err)
	assert.Equal(t, []LiftAttempt{la}, las)

	tms, err := s.ListTimers(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, []Timer{tm}, tms)

	_, err = s.GetHaul(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestHaulUnsetJSONStaysNil(t *testing.T) {
	s, _ := openFile(t)
	h := haul("h1", HaulRunning)
	h.Params = nil
	l := lift("h1", 1, LiftScheduled)
	l.Args = nil
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(h) }, func(tx *Tx) error { return tx.PutLift(l) })

	got, err := s.GetHaul(ctx, "h1")
	require.NoError(t, err)
	assert.Nil(t, got.Params)
	assert.Nil(t, got.Result)
	ls, err := s.ListLifts(ctx, "h1")
	require.NoError(t, err)
	assert.Nil(t, ls[0].Args)
	assert.Nil(t, ls[0].Result)
}

func TestHaulUpsertUpdatesMutableFieldsOnly(t *testing.T) {
	s, _ := openFile(t)
	h := haul("h1", HaulRunning)
	l := lift("h1", 1, LiftScheduled)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(h) }, func(tx *Tx) error { return tx.PutLift(l) })

	h.Status, h.Commands, h.Reason = HaulStuck, 3, "nondeterminism"
	h.Name = "changed" // identity fields are fixed at creation
	l.Status, l.Attempts, l.RetryAt = LiftRetryWait, 1, ptr(t0.Add(time.Minute))
	l.Args = json.RawMessage(`["changed"]`)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(h) }, func(tx *Tx) error { return tx.PutLift(l) })

	got, err := s.GetHaul(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, HaulStuck, got.Status)
	assert.Equal(t, 3, got.Commands)
	assert.Equal(t, "daily_revenue", got.Name)

	ls, err := s.ListLifts(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, LiftRetryWait, ls[0].Status)
	assert.Equal(t, t0.Add(time.Minute), *ls[0].RetryAt)
	assert.JSONEq(t, `[{"day":"2026-10-24"}]`, string(ls[0].Args))
}

func TestHaulEventsIsTheLogbook(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, []Event{
		{Type: EventHaulStarted, Tenant: "t", HaulID: "h1", Time: t0},
		{Type: EventHaulStarted, Tenant: "t", HaulID: "h2", Time: t0},
		{Type: EventLiftScheduled, Tenant: "t", HaulID: "h1", Time: t0, Payload: json.RawMessage(`{"seq":1}`)},
		{Type: EventCronTicked, Tenant: "t", Time: t0},
		{Type: EventLiftFinished, Tenant: "t", HaulID: "h1", Time: t0, Payload: json.RawMessage(`{"seq":1}`)},
	})

	book, err := s.HaulEvents(ctx, "h1")
	require.NoError(t, err)
	var types []EventType
	for _, e := range book {
		assert.Equal(t, "h1", e.HaulID)
		types = append(types, e.Type)
	}
	assert.Equal(t, []EventType{EventHaulStarted, EventLiftScheduled, EventLiftFinished}, types)
	assert.Less(t, book[0].Seq, book[1].Seq)

	all, err := s.EventsSince(ctx, 0, "", 0)
	require.NoError(t, err)
	require.Len(t, all, 5)
	assert.Equal(t, "h2", all[1].HaulID)
	assert.Equal(t, "", all[3].HaulID)
}

func TestLoadHauls(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, []Event{{Type: EventHaulStarted, HaulID: "h1"}},
		func(tx *Tx) error {
			for _, h := range []Haul{haul("h1", HaulRunning), haul("h2", HaulCompleted), haul("h3", HaulStuck)} {
				if err := tx.PutHaul(h); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			for _, ht := range []HaulTask{
				{ID: "t1", HaulID: "h1", Number: 1, Term: 1, Status: HaulTaskCompleted, QueuedAt: t0},
				{ID: "t2", HaulID: "h1", Number: 2, Term: 1, Status: HaulTaskQueued, QueuedAt: t0},
				{ID: "t3", HaulID: "h2", Number: 1, Term: 1, Status: HaulTaskQueued, QueuedAt: t0}, // haul closed
			} {
				if err := tx.PutHaulTask(ht); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			// h1: lift 1 finished, lift 2 running with one failed and one
			// running attempt; h2's open lift belongs to a closed haul.
			for _, l := range []Lift{lift("h1", 1, LiftCompleted), lift("h1", 2, LiftRunning), lift("h2", 1, LiftRunning)} {
				if err := tx.PutLift(l); err != nil {
					return err
				}
			}
			for _, a := range []LiftAttempt{
				{ID: "a1", HaulID: "h1", Seq: 1, Number: 1, Term: 1, Status: AttemptSucceeded, DispatchedAt: t0},
				{ID: "a2", HaulID: "h1", Seq: 2, Number: 1, Term: 1, Status: AttemptFailed, DispatchedAt: t0},
				{ID: "a3", HaulID: "h1", Seq: 2, Number: 2, Term: 1, Status: AttemptRunning, DispatchedAt: t0},
				{ID: "a4", HaulID: "h2", Seq: 1, Number: 1, Term: 1, Status: AttemptRunning, DispatchedAt: t0},
			} {
				if err := tx.PutLiftAttempt(a); err != nil {
					return err
				}
			}
			return nil
		},
		func(tx *Tx) error {
			for _, tm := range []Timer{
				{HaulID: "h1", Seq: 3, FireAt: t0, Status: TimerFired, CreatedAt: t0},
				{HaulID: "h3", Seq: 1, FireAt: t0, Status: TimerPending, CreatedAt: t0},
			} {
				if err := tx.PutTimer(tm); err != nil {
					return err
				}
			}
			return nil
		},
	)

	a, err := s.LoadHauls(ctx)
	require.NoError(t, err)

	var hauls, tasks, lifts, attempts []string
	for _, h := range a.Hauls {
		hauls = append(hauls, h.ID)
	}
	for _, ht := range a.HaulTasks {
		tasks = append(tasks, ht.ID)
	}
	for _, l := range a.Lifts {
		lifts = append(lifts, fmt.Sprintf("%s/%d", l.HaulID, l.Seq))
	}
	for _, la := range a.LiftAttempts {
		attempts = append(attempts, la.ID)
	}
	assert.Equal(t, []string{"h1", "h3"}, hauls, "running and stuck hauls are open")
	assert.Equal(t, []string{"t2"}, tasks)
	assert.Equal(t, []string{"h1/2"}, lifts)
	assert.Equal(t, []string{"a2", "a3"}, attempts, "every attempt of an open lift, so numbering continues")
	require.Len(t, a.Timers, 1)
	assert.Equal(t, "h3", a.Timers[0].HaulID)
	assert.Equal(t, int64(1), a.LastSeq)
}

func TestHaulForeignKeys(t *testing.T) {
	s, _ := openFile(t)
	for name, w := range map[string]func(tx *Tx) error{
		"lift without haul": func(tx *Tx) error { return tx.PutLift(lift("missing", 1, LiftScheduled)) },
		"timer without haul": func(tx *Tx) error {
			return tx.PutTimer(Timer{HaulID: "missing", Seq: 1, FireAt: t0, Status: TimerPending, CreatedAt: t0})
		},
		"task without haul": func(tx *Tx) error {
			return tx.PutHaulTask(HaulTask{ID: "x", HaulID: "missing", Number: 1, Status: HaulTaskQueued, QueuedAt: t0})
		},
		"attempt without lift": func(tx *Tx) error {
			return tx.PutLiftAttempt(LiftAttempt{ID: "x", HaulID: "missing", Seq: 1, Number: 1, Status: AttemptDispatched, DispatchedAt: t0})
		},
	} {
		_, err := s.Append(ctx, s.Term(), nil, w)
		assert.Error(t, err, name)
	}
}

func TestHaulStatusHelpers(t *testing.T) {
	assert.True(t, HaulOpen(HaulRunning))
	assert.True(t, HaulOpen(HaulStuck))
	assert.False(t, HaulOpen(HaulCompleted))
	assert.True(t, HaulTaskTerminal(HaulTaskLost))
	assert.False(t, HaulTaskTerminal(HaulTaskDispatched))
	assert.True(t, LiftTerminal(LiftCancelled))
	assert.False(t, LiftTerminal(LiftRetryWait))
}

// A v1 database (only 001 applied, with events) upgrades in place; old events
// get an empty HaulID.
func TestMigrateV1Database(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", dsn(path, false))
	require.NoError(t, err)
	v1, err := migrationFS.ReadFile("migrations/001_initial.up.sql")
	require.NoError(t, err)
	for _, q := range []string{
		string(v1),
		`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
		`INSERT INTO schema_migrations (version) VALUES ('001_initial')`,
		`INSERT INTO events (term, ts, type, run_id) VALUES (1, '` + formatTime(t0) + `', 'RunCreated', 'r1')`,
	} {
		_, err := db.Exec(q)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	evs, err := s.EventsSince(ctx, 0, "", 0)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "r1", evs[0].RunID)
	assert.Equal(t, "", evs[0].HaulID)

	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulRunning)) })
}

func TestLiftRunReasonCanChange(t *testing.T) {
	s, _ := openFile(t)
	l := lift("h1", 1, LiftWaiting)
	l.RunReason = "no previous run"
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulRunning)) }, func(tx *Tx) error { return tx.PutLift(l) })
	l.Status, l.RunReason = LiftScheduled, "the run it waited for failed"
	put(t, s, nil, func(tx *Tx) error { return tx.PutLift(l) })
	got, err := s.GetLift(ctx, "h1", 1)
	require.NoError(t, err)
	assert.Equal(t, "the run it waited for failed", got.RunReason)
}

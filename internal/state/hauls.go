package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// v2 projections: hauls, the haul tasks that replay them, the lifts and
// timers their commands create. A lift or timer is identified by its haul
// and the sequence number of the command that created it.

// Haul statuses. A running haul with no queued haul task and no open lift is
// waiting (on a timer or signal); that is derived, not stored.
const (
	HaulRunning   = "running"
	HaulStuck     = "stuck" // non-determinism; continues after a consistent haul task
	HaulCompleted = "completed"
	HaulFailed    = "failed"
	HaulCancelled = "cancelled"
)

// Haul task statuses.
const (
	HaulTaskQueued     = "queued"
	HaulTaskDispatched = "dispatched"
	HaulTaskCompleted  = "completed"
	HaulTaskFailed     = "failed"
	HaulTaskLost       = "lost"
)

// Lift statuses. Lift attempts use the Attempt* statuses.
const (
	LiftScheduled  = "scheduled"
	LiftDispatched = "dispatched"
	LiftRunning    = "running"
	LiftRetryWait  = "retry_wait"
	LiftCompleted  = "completed"
	LiftFailed     = "failed"
	LiftCancelled  = "cancelled"
)

// Timer statuses.
const (
	TimerPending   = "pending"
	TimerFired     = "fired"
	TimerCancelled = "cancelled"
)

// HaulOpen reports whether a haul may still make progress.
func HaulOpen(status string) bool { return status == HaulRunning || status == HaulStuck }

// HaulTaskTerminal reports whether a haul task status is final.
func HaulTaskTerminal(status string) bool {
	return status == HaulTaskCompleted || status == HaulTaskFailed || status == HaulTaskLost
}

// LiftTerminal reports whether a lift status is final.
func LiftTerminal(status string) bool {
	return status == LiftCompleted || status == LiftFailed || status == LiftCancelled
}

type Haul struct {
	ID         string
	Tenant     string
	Name       string
	Dock       string
	Trigger    string
	Params     map[string]any
	Commands   int // commands recorded so far; the next one gets seq Commands+1
	BuildID    string
	Status     string
	Reason     string
	Message    string
	Result     json.RawMessage
	CreatedAt  time.Time
	FinishedAt *time.Time
}

type HaulTask struct {
	ID           string
	HaulID       string
	Number       int
	Term         int64
	WorkerID     string
	BuildID      string
	Status       string
	Reason       string
	Message      string
	QueuedAt     time.Time
	DispatchedAt *time.Time
	FinishedAt   *time.Time
}

type Lift struct {
	HaulID      string
	Seq         int
	Name        string
	Dock        string
	Args        json.RawMessage // encoded by the SDK; opaque to the server
	Timeout     time.Duration   // per attempt; 0 = none
	Retry       json.RawMessage // retry policy, encoded by the core; nil = no retries
	Status      string
	Attempts    int
	RetryAt     *time.Time
	Reason      string
	Message     string
	Result      json.RawMessage
	ScheduledAt time.Time
	FinishedAt  *time.Time
}

type LiftAttempt struct {
	ID             string
	HaulID         string
	Seq            int
	Number         int
	Term           int64
	WorkerID       string
	Status         string
	Reason         string
	Message        string
	LeaseExpiresAt *time.Time
	DispatchedAt   time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type Timer struct {
	HaulID    string
	Seq       int
	FireAt    time.Time
	Status    string
	CreatedAt time.Time
	FiredAt   *time.Time
}

// PutHaul inserts or updates a haul; identity fields are fixed at creation.
func (t *Tx) PutHaul(v Haul) error {
	params, err := jsonObject(v.Params)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO hauls (id, tenant, name, dock, trigger, params_json, commands, build_id, status,
			reason, message, result_json, created_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			commands = excluded.commands, build_id = excluded.build_id, status = excluded.status,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			finished_at = excluded.finished_at`,
		v.ID, v.Tenant, v.Name, v.Dock, v.Trigger, params, v.Commands, v.BuildID, v.Status,
		v.Reason, v.Message, string(v.Result), formatTime(v.CreatedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutHaulTask(v HaulTask) error {
	return t.exec(`
		INSERT INTO haul_tasks (id, haul_id, number, term, worker_id, build_id, status, reason, message,
			queued_at, dispatched_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			worker_id = excluded.worker_id, build_id = excluded.build_id, status = excluded.status,
			reason = excluded.reason, message = excluded.message,
			dispatched_at = excluded.dispatched_at, finished_at = excluded.finished_at`,
		v.ID, v.HaulID, v.Number, v.Term, v.WorkerID, v.BuildID, v.Status, v.Reason, v.Message,
		formatTime(v.QueuedAt), formatTimePtr(v.DispatchedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutLift(v Lift) error {
	return t.exec(`
		INSERT INTO lifts (haul_id, seq, name, dock, args_json, timeout_ns, retry_json, status, attempts,
			retry_at, reason, message, result_json, scheduled_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (haul_id, seq) DO UPDATE SET
			status = excluded.status, attempts = excluded.attempts, retry_at = excluded.retry_at,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			finished_at = excluded.finished_at`,
		v.HaulID, v.Seq, v.Name, v.Dock, string(v.Args), int64(v.Timeout), string(v.Retry), v.Status,
		v.Attempts, formatTimePtr(v.RetryAt),
		v.Reason, v.Message, string(v.Result), formatTime(v.ScheduledAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutLiftAttempt(v LiftAttempt) error {
	return t.exec(`
		INSERT INTO lift_attempts (id, haul_id, seq, number, term, worker_id, status, reason, message,
			lease_expires_at, dispatched_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			worker_id = excluded.worker_id, status = excluded.status, reason = excluded.reason,
			message = excluded.message, lease_expires_at = excluded.lease_expires_at,
			started_at = excluded.started_at, finished_at = excluded.finished_at`,
		v.ID, v.HaulID, v.Seq, v.Number, v.Term, v.WorkerID, v.Status, v.Reason, v.Message,
		formatTimePtr(v.LeaseExpiresAt), formatTime(v.DispatchedAt), formatTimePtr(v.StartedAt),
		formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutTimer(v Timer) error {
	return t.exec(`
		INSERT INTO timers (haul_id, seq, fire_at, status, created_at, fired_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (haul_id, seq) DO UPDATE SET status = excluded.status, fired_at = excluded.fired_at`,
		v.HaulID, v.Seq, formatTime(v.FireAt), v.Status, formatTime(v.CreatedAt), formatTimePtr(v.FiredAt))
}

// ActiveHauls is what a new leader needs to resume hauls: every open haul,
// its unfinished haul tasks, its unfinished lifts with all their attempts,
// and its pending timers. Results of finished lifts are in the logbook
// (HaulEvents) and are read when a haul task is built, not loaded here.
type ActiveHauls struct {
	Hauls        []Haul
	HaulTasks    []HaulTask
	Lifts        []Lift
	LiftAttempts []LiftAttempt
	Timers       []Timer
	LastSeq      int64 // newest event seq at load time
}

// LoadHauls reads ActiveHauls in one read transaction.
func (s *Store) LoadHauls(ctx context.Context) (*ActiveHauls, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	const open = `SELECT id FROM hauls WHERE status IN ('running', 'stuck')`
	a := &ActiveHauls{}
	if a.Hauls, err = queryAll(ctx, tx, scanHaul,
		`SELECT `+haulColumns+` FROM hauls WHERE status IN ('running', 'stuck') ORDER BY id`); err != nil {
		return nil, err
	}
	if a.HaulTasks, err = queryAll(ctx, tx, scanHaulTask,
		`SELECT `+haulTaskColumns+` FROM haul_tasks WHERE haul_id IN (`+open+`)
		 AND status IN ('queued', 'dispatched') ORDER BY haul_id, number`); err != nil {
		return nil, err
	}
	const openLift = `haul_id IN (` + open + `) AND status NOT IN ('completed', 'failed', 'cancelled')`
	if a.Lifts, err = queryAll(ctx, tx, scanLift,
		`SELECT `+liftColumns+` FROM lifts WHERE `+openLift+` ORDER BY haul_id, seq`); err != nil {
		return nil, err
	}
	if a.LiftAttempts, err = queryAll(ctx, tx, scanLiftAttempt,
		`SELECT `+liftAttemptColumns+` FROM lift_attempts
		 WHERE (haul_id, seq) IN (SELECT haul_id, seq FROM lifts WHERE `+openLift+`)
		 ORDER BY haul_id, seq, number`); err != nil {
		return nil, err
	}
	if a.Timers, err = queryAll(ctx, tx, scanTimer,
		`SELECT `+timerColumns+` FROM timers WHERE haul_id IN (`+open+`)
		 AND status = 'pending' ORDER BY haul_id, seq`); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&a.LastSeq); err != nil {
		return nil, err
	}
	return a, nil
}

func (s *Store) GetHaul(ctx context.Context, id string) (Haul, error) {
	v, err := scanHaul(s.r.QueryRowContext(ctx, `SELECT `+haulColumns+` FROM hauls WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// HaulEvents returns a haul's logbook: its events, oldest first.
func (s *Store) HaulEvents(ctx context.Context, haulID string) ([]Event, error) {
	return queryAll(ctx, s.r, scanEvent,
		`SELECT `+eventColumns+` FROM events WHERE haul_id = ? ORDER BY seq`, haulID)
}

// ListHaulTasks returns a haul's haul tasks in order.
func (s *Store) ListHaulTasks(ctx context.Context, haulID string) ([]HaulTask, error) {
	return queryAll(ctx, s.r, scanHaulTask,
		`SELECT `+haulTaskColumns+` FROM haul_tasks WHERE haul_id = ? ORDER BY number`, haulID)
}

// ListLifts returns a haul's lifts in command order.
func (s *Store) ListLifts(ctx context.Context, haulID string) ([]Lift, error) {
	return queryAll(ctx, s.r, scanLift,
		`SELECT `+liftColumns+` FROM lifts WHERE haul_id = ? ORDER BY seq`, haulID)
}

// ListLiftAttempts returns one lift's attempts in order.
func (s *Store) ListLiftAttempts(ctx context.Context, haulID string, seq int) ([]LiftAttempt, error) {
	return queryAll(ctx, s.r, scanLiftAttempt,
		`SELECT `+liftAttemptColumns+` FROM lift_attempts WHERE haul_id = ? AND seq = ? ORDER BY number`, haulID, seq)
}

// ListTimers returns a haul's timers in command order.
func (s *Store) ListTimers(ctx context.Context, haulID string) ([]Timer, error) {
	return queryAll(ctx, s.r, scanTimer,
		`SELECT `+timerColumns+` FROM timers WHERE haul_id = ? ORDER BY seq`, haulID)
}

const haulColumns = `id, tenant, name, dock, trigger, params_json, commands, build_id, status,
	reason, message, result_json, created_at, finished_at`

func scanHaul(sc interface{ Scan(...any) error }) (Haul, error) {
	var (
		v                       Haul
		params, result, created string
		finished                sql.NullString
		err                     error
	)
	if err = sc.Scan(&v.ID, &v.Tenant, &v.Name, &v.Dock, &v.Trigger, &params, &v.Commands, &v.BuildID,
		&v.Status, &v.Reason, &v.Message, &result, &created, &finished); err != nil {
		return v, err
	}
	v.Result = rawOrNil(result)
	if v.CreatedAt, err = parseTime(created); err != nil {
		return v, err
	}
	if v.FinishedAt, err = parseTimePtr(finished); err != nil {
		return v, err
	}
	v.Params, err = decodeObject(params)
	return v, err
}

const haulTaskColumns = `id, haul_id, number, term, worker_id, build_id, status, reason, message,
	queued_at, dispatched_at, finished_at`

func scanHaulTask(sc interface{ Scan(...any) error }) (HaulTask, error) {
	var (
		v                    HaulTask
		queued               string
		dispatched, finished sql.NullString
		err                  error
	)
	if err = sc.Scan(&v.ID, &v.HaulID, &v.Number, &v.Term, &v.WorkerID, &v.BuildID, &v.Status, &v.Reason,
		&v.Message, &queued, &dispatched, &finished); err != nil {
		return v, err
	}
	if v.QueuedAt, err = parseTime(queued); err != nil {
		return v, err
	}
	if v.DispatchedAt, err = parseTimePtr(dispatched); err != nil {
		return v, err
	}
	v.FinishedAt, err = parseTimePtr(finished)
	return v, err
}

const liftColumns = `haul_id, seq, name, dock, args_json, timeout_ns, retry_json, status, attempts,
	retry_at, reason, message, result_json, scheduled_at, finished_at`

func scanLift(sc interface{ Scan(...any) error }) (Lift, error) {
	var (
		v                              Lift
		args, retry, result, scheduled string
		timeout                        int64
		retryAt, finished              sql.NullString
		err                            error
	)
	if err = sc.Scan(&v.HaulID, &v.Seq, &v.Name, &v.Dock, &args, &timeout, &retry, &v.Status, &v.Attempts,
		&retryAt, &v.Reason, &v.Message, &result, &scheduled, &finished); err != nil {
		return v, err
	}
	v.Args, v.Retry, v.Result = rawOrNil(args), rawOrNil(retry), rawOrNil(result)
	v.Timeout = time.Duration(timeout)
	if v.RetryAt, err = parseTimePtr(retryAt); err != nil {
		return v, err
	}
	if v.ScheduledAt, err = parseTime(scheduled); err != nil {
		return v, err
	}
	v.FinishedAt, err = parseTimePtr(finished)
	return v, err
}

const liftAttemptColumns = `id, haul_id, seq, number, term, worker_id, status, reason, message,
	lease_expires_at, dispatched_at, started_at, finished_at`

func scanLiftAttempt(sc interface{ Scan(...any) error }) (LiftAttempt, error) {
	var (
		v                        LiftAttempt
		dispatched               string
		lease, started, finished sql.NullString
		err                      error
	)
	if err = sc.Scan(&v.ID, &v.HaulID, &v.Seq, &v.Number, &v.Term, &v.WorkerID, &v.Status, &v.Reason,
		&v.Message, &lease, &dispatched, &started, &finished); err != nil {
		return v, err
	}
	if v.LeaseExpiresAt, err = parseTimePtr(lease); err != nil {
		return v, err
	}
	if v.DispatchedAt, err = parseTime(dispatched); err != nil {
		return v, err
	}
	if v.StartedAt, err = parseTimePtr(started); err != nil {
		return v, err
	}
	v.FinishedAt, err = parseTimePtr(finished)
	return v, err
}

const timerColumns = `haul_id, seq, fire_at, status, created_at, fired_at`

func scanTimer(sc interface{ Scan(...any) error }) (Timer, error) {
	var (
		v               Timer
		fireAt, created string
		fired           sql.NullString
		err             error
	)
	if err = sc.Scan(&v.HaulID, &v.Seq, &fireAt, &v.Status, &created, &fired); err != nil {
		return v, err
	}
	if v.FireAt, err = parseTime(fireAt); err != nil {
		return v, err
	}
	if v.CreatedAt, err = parseTime(created); err != nil {
		return v, err
	}
	v.FiredAt, err = parseTimePtr(fired)
	return v, err
}

// rawOrNil turns a stored JSON column back into a RawMessage; "" means unset.
func rawOrNil(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	return json.RawMessage(s)
}

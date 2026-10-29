package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
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
	HaulContinued = "continued" // continue_as_new: Haul.ContinuedAs carries on
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
	LiftCommitting = "committing" // succeeded; its pallet is being promoted
	LiftWaiting    = "waiting"    // its fingerprint is running in another lift (single-flight)
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
	// A child haul: the haul and start_haul command that started it, and
	// whether that haul waits for its result.
	ParentID    string
	ParentSeq   int
	ParentWaits bool
	ContinuedAs string // status continued: the haul that carries on
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
	Output      json.RawMessage // where its pallet goes, encoded by the core; nil = no pallet
	Inputs      json.RawMessage // pallets it reads, encoded by the core
	Fingerprint string          // "" if it has none (not memoized, unknown code version)
	FPInputs    json.RawMessage // what went into the fingerprint, encoded by the core
	RunReason   string          // why it ran; "" if it was reused
	Reused      bool            // completed by memoization, without running
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
	Staging        string // where it writes its output; "" if dispatched before this was recorded
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
			reason, message, result_json, created_at, finished_at, parent_id, parent_seq, parent_waits, continued_as)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			commands = excluded.commands, build_id = excluded.build_id, status = excluded.status,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			finished_at = excluded.finished_at, continued_as = excluded.continued_as`,
		v.ID, v.Tenant, v.Name, v.Dock, v.Trigger, params, v.Commands, v.BuildID, v.Status,
		v.Reason, v.Message, string(v.Result), formatTime(v.CreatedAt), formatTimePtr(v.FinishedAt),
		v.ParentID, v.ParentSeq, v.ParentWaits, v.ContinuedAs)
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
		INSERT INTO lifts (haul_id, seq, name, dock, args_json, timeout_ns, retry_json, output_json,
			inputs_json, fingerprint, fingerprint_json, run_reason, reused, status, attempts, retry_at, reason,
			message, result_json, scheduled_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (haul_id, seq) DO UPDATE SET
			status = excluded.status, attempts = excluded.attempts, retry_at = excluded.retry_at,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			run_reason = excluded.run_reason, reused = excluded.reused, finished_at = excluded.finished_at`,
		v.HaulID, v.Seq, v.Name, v.Dock, string(v.Args), int64(v.Timeout), string(v.Retry),
		string(v.Output), string(v.Inputs), v.Fingerprint, string(v.FPInputs), v.RunReason, v.Reused, v.Status,
		v.Attempts, formatTimePtr(v.RetryAt),
		v.Reason, v.Message, string(v.Result), formatTime(v.ScheduledAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutLiftAttempt(v LiftAttempt) error {
	return t.exec(`
		INSERT INTO lift_attempts (id, haul_id, seq, number, term, worker_id, status, reason, message,
			lease_expires_at, dispatched_at, started_at, finished_at, staging)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			worker_id = excluded.worker_id, status = excluded.status, reason = excluded.reason,
			message = excluded.message, lease_expires_at = excluded.lease_expires_at,
			started_at = excluded.started_at, finished_at = excluded.finished_at, staging = excluded.staging`,
		v.ID, v.HaulID, v.Seq, v.Number, v.Term, v.WorkerID, v.Status, v.Reason, v.Message,
		formatTimePtr(v.LeaseExpiresAt), formatTime(v.DispatchedAt), formatTimePtr(v.StartedAt),
		formatTimePtr(v.FinishedAt), v.Staging)
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

// HaulFilter selects hauls for ListHauls. Empty fields don't filter.
type HaulFilter struct {
	Tenant   string
	Name     string
	Statuses []string
	ParentID string // children of this haul
	BeforeID string // pages backwards: only IDs (ULIDs) lower than this
	Limit    int    // default 50
}

// ListHauls returns hauls newest first.
func (s *Store) ListHauls(ctx context.Context, f HaulFilter) ([]Haul, error) {
	var (
		where []string
		args  []any
	)
	if f.Tenant != "" {
		where, args = append(where, "tenant = ?"), append(args, f.Tenant)
	}
	if f.Name != "" {
		where, args = append(where, "name = ?"), append(args, f.Name)
	}
	if f.ParentID != "" {
		where, args = append(where, "parent_id = ?"), append(args, f.ParentID)
	}
	if f.BeforeID != "" {
		where, args = append(where, "id < ?"), append(args, f.BeforeID)
	}
	if len(f.Statuses) > 0 {
		where = append(where, "status IN (?"+strings.Repeat(", ?", len(f.Statuses)-1)+")")
		for _, st := range f.Statuses {
			args = append(args, st)
		}
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	q := `SELECT ` + haulColumns + ` FROM hauls`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	return queryAll(ctx, s.r, scanHaul, q+` ORDER BY id DESC LIMIT ?`, append(args, f.Limit)...)
}

// LiftCounts counts a haul's lifts by where they are.
type LiftCounts struct {
	Total, Completed, Reused, Failed, Cancelled int
}

// CountLifts returns LiftCounts for each of haulIDs that has lifts.
func (s *Store) CountLifts(ctx context.Context, haulIDs []string) (map[string]LiftCounts, error) {
	out := map[string]LiftCounts{}
	if len(haulIDs) == 0 {
		return out, nil
	}
	args := make([]any, len(haulIDs))
	for i, id := range haulIDs {
		args[i] = id
	}
	rows, err := s.r.QueryContext(ctx, `SELECT haul_id, COUNT(*),
		SUM(status = 'completed'), SUM(status = 'completed' AND reused), SUM(status = 'failed'), SUM(status = 'cancelled')
		FROM lifts WHERE haul_id IN (?`+strings.Repeat(", ?", len(haulIDs)-1)+`) GROUP BY haul_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id string
			c  LiftCounts
		)
		if err := rows.Scan(&id, &c.Total, &c.Completed, &c.Reused, &c.Failed, &c.Cancelled); err != nil {
			return nil, err
		}
		out[id] = c
	}
	return out, rows.Err()
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

// liftRuns are completed lifts of a tenant that ran with a fingerprint and
// produced no pallet: what memoization of results looks at.
const liftRuns = `SELECT ` + liftColumns + ` FROM lifts
	WHERE haul_id IN (SELECT id FROM hauls WHERE tenant = ?)
	AND fingerprint <> '' AND reused = 0 AND status = 'completed' AND output_json = ''`

// MemoResult returns the newest lift of a tenant that ran with fingerprint,
// produced no pallet and completed at or after since (zero: any time). Its
// result is what memoization reuses for a lift without an output.
// ErrNotFound if there is none.
func (s *Store) MemoResult(ctx context.Context, tenant, fingerprint string, since time.Time) (Lift, error) {
	v, err := scanLift(s.r.QueryRowContext(ctx, liftRuns+` AND fingerprint = ? AND finished_at >= ?
		ORDER BY finished_at DESC LIMIT 1`, tenant, fingerprint, formatTime(since)))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// RecentLiftRuns returns up to limit lifts of a tenant named name that ran
// as MemoResult counts them, newest first: what a rerun is compared with.
func (s *Store) RecentLiftRuns(ctx context.Context, tenant, name string, limit int) ([]Lift, error) {
	return queryAll(ctx, s.r, scanLift, liftRuns+` AND name = ? ORDER BY finished_at DESC LIMIT ?`,
		tenant, name, limit)
}

// ListLifts returns a haul's lifts in command order.
func (s *Store) ListLifts(ctx context.Context, haulID string) ([]Lift, error) {
	return queryAll(ctx, s.r, scanLift,
		`SELECT `+liftColumns+` FROM lifts WHERE haul_id = ? ORDER BY seq`, haulID)
}

// GetLiftAttempt returns an attempt by its ID.
func (s *Store) GetLiftAttempt(ctx context.Context, id string) (LiftAttempt, error) {
	v, err := scanLiftAttempt(s.r.QueryRowContext(ctx, `SELECT `+liftAttemptColumns+` FROM lift_attempts WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
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
	reason, message, result_json, created_at, finished_at, parent_id, parent_seq, parent_waits, continued_as`

func scanHaul(sc interface{ Scan(...any) error }) (Haul, error) {
	var (
		v                       Haul
		params, result, created string
		finished                sql.NullString
		err                     error
	)
	if err = sc.Scan(&v.ID, &v.Tenant, &v.Name, &v.Dock, &v.Trigger, &params, &v.Commands, &v.BuildID,
		&v.Status, &v.Reason, &v.Message, &result, &created, &finished,
		&v.ParentID, &v.ParentSeq, &v.ParentWaits, &v.ContinuedAs); err != nil {
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

const liftColumns = `haul_id, seq, name, dock, args_json, timeout_ns, retry_json, output_json,
	inputs_json, fingerprint, fingerprint_json, run_reason, reused, status, attempts, retry_at, reason, message,
	result_json, scheduled_at, finished_at`

func scanLift(sc interface{ Scan(...any) error }) (Lift, error) {
	var (
		v                                                        Lift
		args, retry, output, inputs, fpInputs, result, scheduled string
		timeout                                                  int64
		retryAt, finished                                        sql.NullString
		err                                                      error
	)
	if err = sc.Scan(&v.HaulID, &v.Seq, &v.Name, &v.Dock, &args, &timeout, &retry, &output, &inputs,
		&v.Fingerprint, &fpInputs, &v.RunReason, &v.Reused, &v.Status, &v.Attempts,
		&retryAt, &v.Reason, &v.Message, &result, &scheduled, &finished); err != nil {
		return v, err
	}
	v.Args, v.Retry, v.Result = rawOrNil(args), rawOrNil(retry), rawOrNil(result)
	v.Output, v.Inputs, v.FPInputs = rawOrNil(output), rawOrNil(inputs), rawOrNil(fpInputs)
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
	lease_expires_at, dispatched_at, started_at, finished_at, staging`

func scanLiftAttempt(sc interface{ Scan(...any) error }) (LiftAttempt, error) {
	var (
		v                        LiftAttempt
		dispatched               string
		lease, started, finished sql.NullString
		err                      error
	)
	if err = sc.Scan(&v.ID, &v.HaulID, &v.Seq, &v.Number, &v.Term, &v.WorkerID, &v.Status, &v.Reason,
		&v.Message, &lease, &dispatched, &started, &finished, &v.Staging); err != nil {
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

// GetLift returns one lift of a haul.
func (s *Store) GetLift(ctx context.Context, haulID string, seq int) (Lift, error) {
	v, err := scanLift(s.r.QueryRowContext(ctx, `SELECT `+liftColumns+` FROM lifts WHERE haul_id = ? AND seq = ?`, haulID, seq))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

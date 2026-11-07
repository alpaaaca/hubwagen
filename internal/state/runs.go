package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// v2 projections: runs, the run tasks that replay them, the steps and
// timers their commands create. A step or timer is identified by its run
// and the sequence number of the command that created it.

// Run statuses. A run in progress with no queued run task and no open step is
// waiting (on a timer or signal); that is derived, not stored.
const (
	RunRunning   = "running"
	RunStuck     = "stuck" // non-determinism; continues after a consistent run task
	RunCompleted = "completed"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
	RunContinued = "continued" // continue_as_new: Run.ContinuedAs carries on
)

// Run task statuses.
const (
	RunTaskQueued     = "queued"
	RunTaskDispatched = "dispatched"
	RunTaskCompleted  = "completed"
	RunTaskFailed     = "failed"
	RunTaskLost       = "lost"
)

// Step statuses. Step attempts use the Attempt* statuses.
const (
	StepScheduled  = "scheduled"
	StepDispatched = "dispatched"
	StepRunning    = "running"
	StepRetryWait  = "retry_wait"
	StepCommitting = "committing" // succeeded; its data version is being promoted
	StepWaiting    = "waiting"    // its fingerprint is running in another step (single-flight)
	StepCompleted  = "completed"
	StepFailed     = "failed"
	StepCancelled  = "cancelled"
)

// Timer statuses.
const (
	TimerPending   = "pending"
	TimerFired     = "fired"
	TimerCancelled = "cancelled"
)

// RunOpen reports whether a run may still make progress.
func RunOpen(status string) bool { return status == RunRunning || status == RunStuck }

// RunTaskTerminal reports whether a run task status is final.
func RunTaskTerminal(status string) bool {
	return status == RunTaskCompleted || status == RunTaskFailed || status == RunTaskLost
}

// StepTerminal reports whether a step status is final.
func StepTerminal(status string) bool {
	return status == StepCompleted || status == StepFailed || status == StepCancelled
}

type Run struct {
	ID         string
	Tenant     string
	Name       string
	Queue      string
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
	// A child run: the run and start_run command that started it, and
	// whether that run waits for its result.
	ParentID    string
	ParentSeq   int
	ParentWaits bool
	ContinuedAs string // status continued: the run that carries on
	// Reuse: "" (steps with reuse= are reused) or ReuseNone (every step
	// runs, and records its result for later runs).
	Reuse string
}

// ReuseNone is a run's Reuse when it reuses nothing.
const ReuseNone = "none"

type RunTask struct {
	ID           string
	RunID        string
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

type Step struct {
	RunID       string
	Seq         int
	Name        string
	Queue       string
	Args        json.RawMessage // encoded by the SDK; opaque to the server
	Timeout     time.Duration   // per attempt; 0 = none
	Retry       json.RawMessage // retry policy, encoded by the core; nil = no retries
	Output      json.RawMessage // where its data version goes, encoded by the core; nil = no data version
	Inputs      json.RawMessage // data versions it reads, encoded by the core
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

type StepAttempt struct {
	ID             string
	RunID          string
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
	RunID     string
	Seq       int
	FireAt    time.Time
	Status    string
	CreatedAt time.Time
	FiredAt   *time.Time
}

// PutRun inserts or updates a run; identity fields are fixed at creation.
func (t *Tx) PutRun(v Run) error {
	params, err := jsonObject(v.Params)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO runs (id, tenant, name, queue, trigger, params_json, commands, build_id, status,
			reason, message, result_json, created_at, finished_at, parent_id, parent_seq, parent_waits, continued_as, reuse)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			commands = excluded.commands, build_id = excluded.build_id, status = excluded.status,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			finished_at = excluded.finished_at, continued_as = excluded.continued_as`,
		v.ID, v.Tenant, v.Name, v.Queue, v.Trigger, params, v.Commands, v.BuildID, v.Status,
		v.Reason, v.Message, string(v.Result), formatTime(v.CreatedAt), formatTimePtr(v.FinishedAt),
		v.ParentID, v.ParentSeq, v.ParentWaits, v.ContinuedAs, v.Reuse)
}

func (t *Tx) PutRunTask(v RunTask) error {
	return t.exec(`
		INSERT INTO run_tasks (id, run_id, number, term, worker_id, build_id, status, reason, message,
			queued_at, dispatched_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			worker_id = excluded.worker_id, build_id = excluded.build_id, status = excluded.status,
			reason = excluded.reason, message = excluded.message,
			dispatched_at = excluded.dispatched_at, finished_at = excluded.finished_at`,
		v.ID, v.RunID, v.Number, v.Term, v.WorkerID, v.BuildID, v.Status, v.Reason, v.Message,
		formatTime(v.QueuedAt), formatTimePtr(v.DispatchedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutStep(v Step) error {
	return t.exec(`
		INSERT INTO steps (run_id, seq, name, queue, args_json, timeout_ns, retry_json, output_json,
			inputs_json, fingerprint, fingerprint_json, run_reason, reused, status, attempts, retry_at, reason,
			message, result_json, scheduled_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id, seq) DO UPDATE SET
			status = excluded.status, attempts = excluded.attempts, retry_at = excluded.retry_at,
			reason = excluded.reason, message = excluded.message, result_json = excluded.result_json,
			run_reason = excluded.run_reason, reused = excluded.reused, finished_at = excluded.finished_at`,
		v.RunID, v.Seq, v.Name, v.Queue, string(v.Args), int64(v.Timeout), string(v.Retry),
		string(v.Output), string(v.Inputs), v.Fingerprint, string(v.FPInputs), v.RunReason, v.Reused, v.Status,
		v.Attempts, formatTimePtr(v.RetryAt),
		v.Reason, v.Message, string(v.Result), formatTime(v.ScheduledAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutStepAttempt(v StepAttempt) error {
	return t.exec(`
		INSERT INTO step_attempts (id, run_id, seq, number, term, worker_id, status, reason, message,
			lease_expires_at, dispatched_at, started_at, finished_at, staging)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			worker_id = excluded.worker_id, status = excluded.status, reason = excluded.reason,
			message = excluded.message, lease_expires_at = excluded.lease_expires_at,
			started_at = excluded.started_at, finished_at = excluded.finished_at, staging = excluded.staging`,
		v.ID, v.RunID, v.Seq, v.Number, v.Term, v.WorkerID, v.Status, v.Reason, v.Message,
		formatTimePtr(v.LeaseExpiresAt), formatTime(v.DispatchedAt), formatTimePtr(v.StartedAt),
		formatTimePtr(v.FinishedAt), v.Staging)
}

func (t *Tx) PutTimer(v Timer) error {
	return t.exec(`
		INSERT INTO timers (run_id, seq, fire_at, status, created_at, fired_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id, seq) DO UPDATE SET status = excluded.status, fired_at = excluded.fired_at`,
		v.RunID, v.Seq, formatTime(v.FireAt), v.Status, formatTime(v.CreatedAt), formatTimePtr(v.FiredAt))
}

// ActiveRuns is what a new leader needs to resume runs: every open run,
// its unfinished run tasks, its unfinished steps with all their attempts,
// and its pending timers. Results of finished steps are in the event log
// (RunEvents) and are read when a run task is built, not loaded here.
type ActiveRuns struct {
	Runs         []Run
	RunTasks     []RunTask
	Steps        []Step
	StepAttempts []StepAttempt
	Timers       []Timer
	LastSeq      int64 // newest event seq at load time
}

// LoadRuns reads ActiveRuns in one read transaction.
func (s *Store) LoadRuns(ctx context.Context) (*ActiveRuns, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	const open = `SELECT id FROM runs WHERE status IN ('running', 'stuck')`
	a := &ActiveRuns{}
	if a.Runs, err = queryAll(ctx, tx, scanRun,
		`SELECT `+runColumns+` FROM runs WHERE status IN ('running', 'stuck') ORDER BY id`); err != nil {
		return nil, err
	}
	if a.RunTasks, err = queryAll(ctx, tx, scanRunTask,
		`SELECT `+runTaskColumns+` FROM run_tasks WHERE run_id IN (`+open+`)
		 AND status IN ('queued', 'dispatched') ORDER BY run_id, number`); err != nil {
		return nil, err
	}
	const openStep = `run_id IN (` + open + `) AND status NOT IN ('completed', 'failed', 'cancelled')`
	if a.Steps, err = queryAll(ctx, tx, scanStep,
		`SELECT `+stepColumns+` FROM steps WHERE `+openStep+` ORDER BY run_id, seq`); err != nil {
		return nil, err
	}
	if a.StepAttempts, err = queryAll(ctx, tx, scanStepAttempt,
		`SELECT `+stepAttemptColumns+` FROM step_attempts
		 WHERE (run_id, seq) IN (SELECT run_id, seq FROM steps WHERE `+openStep+`)
		 ORDER BY run_id, seq, number`); err != nil {
		return nil, err
	}
	if a.Timers, err = queryAll(ctx, tx, scanTimer,
		`SELECT `+timerColumns+` FROM timers WHERE run_id IN (`+open+`)
		 AND status = 'pending' ORDER BY run_id, seq`); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&a.LastSeq); err != nil {
		return nil, err
	}
	return a, nil
}

// RunFilter selects runs for ListRuns. Empty fields don't filter.
type RunFilter struct {
	Tenant   string
	Name     string
	Statuses []string
	ParentID string // children of this run
	BeforeID string // pages backwards: only IDs (ULIDs) lower than this
	Limit    int    // default 50
}

// ListRuns returns runs newest first.
func (s *Store) ListRuns(ctx context.Context, f RunFilter) ([]Run, error) {
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
	q := `SELECT ` + runColumns + ` FROM runs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	return queryAll(ctx, s.r, scanRun, q+` ORDER BY id DESC LIMIT ?`, append(args, f.Limit)...)
}

// StepCounts counts a run's steps by where they are.
type StepCounts struct {
	Total, Completed, Reused, Failed, Cancelled int
}

// CountSteps returns StepCounts for each of runIDs that has steps.
func (s *Store) CountSteps(ctx context.Context, runIDs []string) (map[string]StepCounts, error) {
	out := map[string]StepCounts{}
	if len(runIDs) == 0 {
		return out, nil
	}
	args := make([]any, len(runIDs))
	for i, id := range runIDs {
		args[i] = id
	}
	rows, err := s.r.QueryContext(ctx, `SELECT run_id, COUNT(*),
		SUM(status = 'completed'), SUM(status = 'completed' AND reused), SUM(status = 'failed'), SUM(status = 'cancelled')
		FROM steps WHERE run_id IN (?`+strings.Repeat(", ?", len(runIDs)-1)+`) GROUP BY run_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id string
			c  StepCounts
		)
		if err := rows.Scan(&id, &c.Total, &c.Completed, &c.Reused, &c.Failed, &c.Cancelled); err != nil {
			return nil, err
		}
		out[id] = c
	}
	return out, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	v, err := scanRun(s.r.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// RunEvents returns a run's event log: its events, oldest first.
func (s *Store) RunEvents(ctx context.Context, runID string) ([]Event, error) {
	return queryAll(ctx, s.r, scanEvent,
		`SELECT `+eventColumns+` FROM events WHERE run_id = ? ORDER BY seq`, runID)
}

// ListRunTasks returns a run's run tasks in order.
func (s *Store) ListRunTasks(ctx context.Context, runID string) ([]RunTask, error) {
	return queryAll(ctx, s.r, scanRunTask,
		`SELECT `+runTaskColumns+` FROM run_tasks WHERE run_id = ? ORDER BY number`, runID)
}

// stepRuns are completed steps of a tenant that ran with a fingerprint and
// produced no data version: what memoization of results looks at.
const stepRuns = `SELECT ` + stepColumns + ` FROM steps
	WHERE run_id IN (SELECT id FROM runs WHERE tenant = ?)
	AND fingerprint <> '' AND reused = 0 AND status = 'completed' AND output_json = ''`

// MemoResult returns the newest step of a tenant that ran with fingerprint,
// produced no data version and completed at or after since (zero: any time). Its
// result is what memoization reuses for a step without an output.
// ErrNotFound if there is none.
func (s *Store) MemoResult(ctx context.Context, tenant, fingerprint string, since time.Time) (Step, error) {
	v, err := scanStep(s.r.QueryRowContext(ctx, stepRuns+` AND fingerprint = ? AND finished_at >= ?
		ORDER BY finished_at DESC LIMIT 1`, tenant, fingerprint, formatTime(since)))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// RecentStepRuns returns up to limit steps of a tenant named name that ran
// as MemoResult counts them, newest first: what a rerun is compared with.
func (s *Store) RecentStepRuns(ctx context.Context, tenant, name string, limit int) ([]Step, error) {
	return queryAll(ctx, s.r, scanStep, stepRuns+` AND name = ? ORDER BY finished_at DESC LIMIT ?`,
		tenant, name, limit)
}

// ListSteps returns a run's steps in command order.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	return queryAll(ctx, s.r, scanStep,
		`SELECT `+stepColumns+` FROM steps WHERE run_id = ? ORDER BY seq`, runID)
}

// ListStepsOf returns the steps of each of runIDs, in seq order.
func (s *Store) ListStepsOf(ctx context.Context, runIDs []string) (map[string][]Step, error) {
	out := map[string][]Step{}
	if len(runIDs) == 0 {
		return out, nil
	}
	args := make([]any, len(runIDs))
	for i, id := range runIDs {
		args[i] = id
	}
	steps, err := queryAll(ctx, s.r, scanStep, `SELECT `+stepColumns+` FROM steps
		WHERE run_id IN (?`+strings.Repeat(", ?", len(runIDs)-1)+`) ORDER BY run_id, seq`, args...)
	if err != nil {
		return nil, err
	}
	for _, l := range steps {
		out[l.RunID] = append(out[l.RunID], l)
	}
	return out, nil
}

// GetStepAttempt returns an attempt by its ID.
func (s *Store) GetStepAttempt(ctx context.Context, id string) (StepAttempt, error) {
	v, err := scanStepAttempt(s.r.QueryRowContext(ctx, `SELECT `+stepAttemptColumns+` FROM step_attempts WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// ListStepAttempts returns one step's attempts in order.
func (s *Store) ListStepAttempts(ctx context.Context, runID string, seq int) ([]StepAttempt, error) {
	return queryAll(ctx, s.r, scanStepAttempt,
		`SELECT `+stepAttemptColumns+` FROM step_attempts WHERE run_id = ? AND seq = ? ORDER BY number`, runID, seq)
}

// ListTimers returns a run's timers in command order.
func (s *Store) ListTimers(ctx context.Context, runID string) ([]Timer, error) {
	return queryAll(ctx, s.r, scanTimer,
		`SELECT `+timerColumns+` FROM timers WHERE run_id = ? ORDER BY seq`, runID)
}

const runColumns = `id, tenant, name, queue, trigger, params_json, commands, build_id, status,
	reason, message, result_json, created_at, finished_at, parent_id, parent_seq, parent_waits, continued_as, reuse`

func scanRun(sc interface{ Scan(...any) error }) (Run, error) {
	var (
		v                       Run
		params, result, created string
		finished                sql.NullString
		err                     error
	)
	if err = sc.Scan(&v.ID, &v.Tenant, &v.Name, &v.Queue, &v.Trigger, &params, &v.Commands, &v.BuildID,
		&v.Status, &v.Reason, &v.Message, &result, &created, &finished,
		&v.ParentID, &v.ParentSeq, &v.ParentWaits, &v.ContinuedAs, &v.Reuse); err != nil {
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

const runTaskColumns = `id, run_id, number, term, worker_id, build_id, status, reason, message,
	queued_at, dispatched_at, finished_at`

func scanRunTask(sc interface{ Scan(...any) error }) (RunTask, error) {
	var (
		v                    RunTask
		queued               string
		dispatched, finished sql.NullString
		err                  error
	)
	if err = sc.Scan(&v.ID, &v.RunID, &v.Number, &v.Term, &v.WorkerID, &v.BuildID, &v.Status, &v.Reason,
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

const stepColumns = `run_id, seq, name, queue, args_json, timeout_ns, retry_json, output_json,
	inputs_json, fingerprint, fingerprint_json, run_reason, reused, status, attempts, retry_at, reason, message,
	result_json, scheduled_at, finished_at`

func scanStep(sc interface{ Scan(...any) error }) (Step, error) {
	var (
		v                                                        Step
		args, retry, output, inputs, fpInputs, result, scheduled string
		timeout                                                  int64
		retryAt, finished                                        sql.NullString
		err                                                      error
	)
	if err = sc.Scan(&v.RunID, &v.Seq, &v.Name, &v.Queue, &args, &timeout, &retry, &output, &inputs,
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

const stepAttemptColumns = `id, run_id, seq, number, term, worker_id, status, reason, message,
	lease_expires_at, dispatched_at, started_at, finished_at, staging`

func scanStepAttempt(sc interface{ Scan(...any) error }) (StepAttempt, error) {
	var (
		v                        StepAttempt
		dispatched               string
		lease, started, finished sql.NullString
		err                      error
	)
	if err = sc.Scan(&v.ID, &v.RunID, &v.Seq, &v.Number, &v.Term, &v.WorkerID, &v.Status, &v.Reason,
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

const timerColumns = `run_id, seq, fire_at, status, created_at, fired_at`

func scanTimer(sc interface{ Scan(...any) error }) (Timer, error) {
	var (
		v               Timer
		fireAt, created string
		fired           sql.NullString
		err             error
	)
	if err = sc.Scan(&v.RunID, &v.Seq, &fireAt, &v.Status, &created, &fired); err != nil {
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

// GetStep returns one step of a run.
func (s *Store) GetStep(ctx context.Context, runID string, seq int) (Step, error) {
	v, err := scanStep(s.r.QueryRowContext(ctx, `SELECT `+stepColumns+` FROM steps WHERE run_id = ? AND seq = ?`, runID, seq))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

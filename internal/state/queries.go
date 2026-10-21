package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

// Read side. Safe to call from any goroutine; on a file database these use
// the read-only pool and see the last committed state.

const runColumns = `id, tenant, pipeline, version, run_key, generation, trigger, priority,
	interval_start, interval_end, params_json, status, created_at, started_at, finished_at`

func scanRun(sc interface{ Scan(...any) error }) (Run, error) {
	var (
		v                                 Run
		ivStart, ivEnd, started, finished sql.NullString
		params, created                   string
		err                               error
	)
	if err = sc.Scan(&v.ID, &v.Tenant, &v.Pipeline, &v.Version, &v.RunKey, &v.Generation, &v.Trigger,
		&v.Priority, &ivStart, &ivEnd, &params, &v.Status, &created, &started, &finished); err != nil {
		return v, err
	}
	if v.IntervalStart, err = parseTimePtr(ivStart); err != nil {
		return v, err
	}
	if v.IntervalEnd, err = parseTimePtr(ivEnd); err != nil {
		return v, err
	}
	if v.CreatedAt, err = parseTime(created); err != nil {
		return v, err
	}
	if v.StartedAt, err = parseTimePtr(started); err != nil {
		return v, err
	}
	if v.FinishedAt, err = parseTimePtr(finished); err != nil {
		return v, err
	}
	v.Params, err = decodeObject(params)
	return v, err
}

// decodeObject decodes a JSON object, keeping numbers as json.Number so
// integer params round-trip exactly. "{}" decodes to nil.
func decodeObject(s string) (map[string]any, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var m map[string]any
	err := dec.Decode(&m)
	return m, err
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	v, err := scanRun(s.r.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// LatestRunByKey returns the highest generation run for a run key.
func (s *Store) LatestRunByKey(ctx context.Context, runKey string) (Run, error) {
	v, err := scanRun(s.r.QueryRowContext(ctx,
		`SELECT `+runColumns+` FROM runs WHERE run_key = ? ORDER BY generation DESC LIMIT 1`, runKey))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// RunFilter selects runs for ListRuns. Empty fields don't filter.
type RunFilter struct {
	Tenant   string
	Pipeline string
	Statuses []string
	// BeforeID pages backwards: only runs with an ID (ULID, so time-ordered)
	// lower than this one.
	BeforeID string
	Limit    int // default 50
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
	if f.Pipeline != "" {
		where, args = append(where, "pipeline = ?"), append(args, f.Pipeline)
	}
	if len(f.Statuses) > 0 {
		where = append(where, "status IN (?"+strings.Repeat(", ?", len(f.Statuses)-1)+")")
		for _, st := range f.Statuses {
			args = append(args, st)
		}
	}
	if f.BeforeID != "" {
		where, args = append(where, "id < ?"), append(args, f.BeforeID)
	}
	q := `SELECT ` + runColumns + ` FROM runs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	return queryAll(ctx, s.r, scanRun, q, args...)
}

const taskRunColumns = `run_id, task_id, status, attempts, ready_at, retry_at, winning_attempt, started_at, finished_at`

func scanTaskRun(sc interface{ Scan(...any) error }) (TaskRun, error) {
	var (
		v                               TaskRun
		ready, retry, started, finished sql.NullString
		err                             error
	)
	if err = sc.Scan(&v.RunID, &v.TaskID, &v.Status, &v.Attempts, &ready, &retry, &v.WinningAttempt,
		&started, &finished); err != nil {
		return v, err
	}
	if v.ReadyAt, err = parseTimePtr(ready); err != nil {
		return v, err
	}
	if v.RetryAt, err = parseTimePtr(retry); err != nil {
		return v, err
	}
	if v.StartedAt, err = parseTimePtr(started); err != nil {
		return v, err
	}
	v.FinishedAt, err = parseTimePtr(finished)
	return v, err
}

// ListTaskRuns returns a run's task runs, ordered by task ID.
func (s *Store) ListTaskRuns(ctx context.Context, runID string) ([]TaskRun, error) {
	return queryAll(ctx, s.r, scanTaskRun,
		`SELECT `+taskRunColumns+` FROM task_runs WHERE run_id = ? ORDER BY task_id`, runID)
}

const attemptColumns = `id, run_id, task_id, number, term, runtime, pool, agent_id, status, reason,
	exit_code, staging_uri, logs_uri, result_json, lease_expires_at, dispatched_at, started_at, finished_at`

func scanAttempt(sc interface{ Scan(...any) error }) (Attempt, error) {
	var (
		v                        Attempt
		exitCode                 sql.NullInt64
		result, dispatched       string
		lease, started, finished sql.NullString
		err                      error
	)
	if err = sc.Scan(&v.ID, &v.RunID, &v.TaskID, &v.Number, &v.Term, &v.Runtime, &v.Pool, &v.AgentID,
		&v.Status, &v.Reason, &exitCode, &v.StagingURI, &v.LogsURI, &result, &lease, &dispatched,
		&started, &finished); err != nil {
		return v, err
	}
	if exitCode.Valid {
		c := int(exitCode.Int64)
		v.ExitCode = &c
	}
	if result != "" {
		v.Result = json.RawMessage(result)
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

// ListAttempts returns a run's attempts (all tasks if taskID is empty),
// ordered by task and attempt number.
func (s *Store) ListAttempts(ctx context.Context, runID, taskID string) ([]Attempt, error) {
	if taskID == "" {
		return queryAll(ctx, s.r, scanAttempt,
			`SELECT `+attemptColumns+` FROM attempts WHERE run_id = ? ORDER BY task_id, number`, runID)
	}
	return queryAll(ctx, s.r, scanAttempt,
		`SELECT `+attemptColumns+` FROM attempts WHERE run_id = ? AND task_id = ? ORDER BY number`, runID, taskID)
}

func scanOutput(sc interface{ Scan(...any) error }) (Output, error) {
	var (
		v    Output
		meta string
	)
	if err := sc.Scan(&v.RunID, &v.TaskID, &v.Name, &v.URI, &v.AttemptID, &meta); err != nil {
		return v, err
	}
	var err error
	v.Metadata, err = decodeObject(meta)
	return v, err
}

// ListOutputs returns a run's committed outputs.
func (s *Store) ListOutputs(ctx context.Context, runID string) ([]Output, error) {
	return queryAll(ctx, s.r, scanOutput,
		`SELECT run_id, task_id, name, uri, attempt_id, metadata_json FROM outputs WHERE run_id = ? ORDER BY task_id, name`, runID)
}

func scanPipeline(sc interface{ Scan(...any) error }) (Pipeline, error) {
	var v Pipeline
	err := sc.Scan(&v.Tenant, &v.Name, &v.CurrentVersion, &v.Paused)
	return v, err
}

// ListPipelines returns pipelines, for one tenant or all if tenant is empty.
func (s *Store) ListPipelines(ctx context.Context, tenant string) ([]Pipeline, error) {
	if tenant == "" {
		return queryAll(ctx, s.r, scanPipeline,
			`SELECT tenant, name, current_version, paused FROM pipelines ORDER BY tenant, name`)
	}
	return queryAll(ctx, s.r, scanPipeline,
		`SELECT tenant, name, current_version, paused FROM pipelines WHERE tenant = ? ORDER BY name`, tenant)
}

func (s *Store) GetPipeline(ctx context.Context, tenant, name string) (Pipeline, error) {
	v, err := scanPipeline(s.r.QueryRowContext(ctx,
		`SELECT tenant, name, current_version, paused FROM pipelines WHERE tenant = ? AND name = ?`, tenant, name))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

const versionColumns = `hash, tenant, pipeline, spec_json, published_at, published_by`

func scanVersion(sc interface{ Scan(...any) error }) (PipelineVersion, error) {
	var (
		v               PipelineVersion
		spec, published string
	)
	if err := sc.Scan(&v.Hash, &v.Tenant, &v.Pipeline, &spec, &published, &v.PublishedBy); err != nil {
		return v, err
	}
	v.SpecJSON = []byte(spec)
	var err error
	v.PublishedAt, err = parseTime(published)
	return v, err
}

func (s *Store) GetVersion(ctx context.Context, hash string) (PipelineVersion, error) {
	v, err := scanVersion(s.r.QueryRowContext(ctx,
		`SELECT `+versionColumns+` FROM pipeline_versions WHERE hash = ?`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// ListVersions returns a pipeline's versions, newest first.
func (s *Store) ListVersions(ctx context.Context, tenant, pipeline string) ([]PipelineVersion, error) {
	return queryAll(ctx, s.r, scanVersion,
		`SELECT `+versionColumns+` FROM pipeline_versions WHERE tenant = ? AND pipeline = ?
		 ORDER BY published_at DESC, hash`, tenant, pipeline)
}

func scanEvent(sc interface{ Scan(...any) error }) (Event, error) {
	var (
		v       Event
		ts, typ string
		payload string
	)
	if err := sc.Scan(&v.Seq, &v.Term, &ts, &v.Tenant, &typ, &v.RunID, &v.TaskID, &v.AttemptID, &payload); err != nil {
		return v, err
	}
	v.Type = EventType(typ)
	v.Payload = json.RawMessage(payload)
	var err error
	v.Time, err = parseTime(ts)
	return v, err
}

// EventsSince returns up to limit events with seq > afterSeq, oldest first,
// for one tenant or all if tenant is empty. Live subscribers use it to catch
// up after a reconnect.
func (s *Store) EventsSince(ctx context.Context, afterSeq int64, tenant string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 500
	}
	const cols = `seq, term, ts, tenant, type, run_id, task_id, attempt_id, payload`
	if tenant == "" {
		return queryAll(ctx, s.r, scanEvent,
			`SELECT `+cols+` FROM events WHERE seq > ? ORDER BY seq LIMIT ?`, afterSeq, limit)
	}
	return queryAll(ctx, s.r, scanEvent,
		`SELECT `+cols+` FROM events WHERE tenant = ? AND seq > ? ORDER BY seq LIMIT ?`, tenant, afterSeq, limit)
}

// RunEvents returns every event of one run, oldest first.
func (s *Store) RunEvents(ctx context.Context, runID string) ([]Event, error) {
	return queryAll(ctx, s.r, scanEvent,
		`SELECT seq, term, ts, tenant, type, run_id, task_id, attempt_id, payload
		 FROM events WHERE run_id = ? ORDER BY seq`, runID)
}

type rowQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryAll[T any](ctx context.Context, db rowQueryer, scan func(interface{ Scan(...any) error }) (T, error), q string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

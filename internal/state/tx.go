package state

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Tx writes projections inside a Store.Append transaction. Every method is an
// upsert keyed by the projection's primary key, except PutVersion (versions
// are immutable) — so the core can write the full current state of an object
// without tracking whether it is new.
type Tx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (t *Tx) exec(query string, args ...any) error {
	_, err := t.tx.ExecContext(t.ctx, query, args...)
	return err
}

func (t *Tx) PutTenant(v Tenant) error {
	return t.exec(`
		INSERT INTO tenants (name, weight, max_running_tasks, created_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (name) DO UPDATE SET
			weight = excluded.weight, max_running_tasks = excluded.max_running_tasks`,
		v.Name, v.Weight, v.MaxRunningTasks, formatTime(v.CreatedAt))
}

// PutVersion records a published version. Republishing an existing hash is a
// no-op: the hash is the content, so the row can't differ.
func (t *Tx) PutVersion(v PipelineVersion) error {
	return t.exec(`
		INSERT INTO pipeline_versions (hash, tenant, pipeline, spec_json, published_at, published_by)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (hash) DO NOTHING`,
		v.Hash, v.Tenant, v.Pipeline, string(v.SpecJSON), formatTime(v.PublishedAt), v.PublishedBy)
}

func (t *Tx) PutPipeline(v Pipeline) error {
	return t.exec(`
		INSERT INTO pipelines (tenant, name, current_version, paused) VALUES (?, ?, ?, ?)
		ON CONFLICT (tenant, name) DO UPDATE SET
			current_version = excluded.current_version, paused = excluded.paused`,
		v.Tenant, v.Name, v.CurrentVersion, v.Paused)
}

func (t *Tx) PutScheduleCursor(v ScheduleCursor) error {
	return t.exec(`
		INSERT INTO schedule_cursors (tenant, pipeline, last_tick) VALUES (?, ?, ?)
		ON CONFLICT (tenant, pipeline) DO UPDATE SET last_tick = excluded.last_tick`,
		v.Tenant, v.Pipeline, formatTime(v.LastTick))
}

// PutRun inserts or updates a run. Inserting a second run with the same
// (RunKey, Generation) fails, which fails the whole Append.
func (t *Tx) PutRun(v Run) error {
	params, err := jsonObject(v.Params)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO runs (id, tenant, pipeline, version, run_key, generation, trigger, priority,
			interval_start, interval_end, params_json, status, created_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			status = excluded.status, started_at = excluded.started_at, finished_at = excluded.finished_at`,
		v.ID, v.Tenant, v.Pipeline, v.Version, v.RunKey, v.Generation, v.Trigger, v.Priority,
		formatTimePtr(v.IntervalStart), formatTimePtr(v.IntervalEnd), params, v.Status,
		formatTime(v.CreatedAt), formatTimePtr(v.StartedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutTaskRun(v TaskRun) error {
	return t.exec(`
		INSERT INTO task_runs (run_id, task_id, status, attempts, ready_at, retry_at, winning_attempt, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id, task_id) DO UPDATE SET
			status = excluded.status, attempts = excluded.attempts, ready_at = excluded.ready_at,
			retry_at = excluded.retry_at, winning_attempt = excluded.winning_attempt,
			started_at = excluded.started_at, finished_at = excluded.finished_at`,
		v.RunID, v.TaskID, v.Status, v.Attempts, formatTimePtr(v.ReadyAt), formatTimePtr(v.RetryAt),
		v.WinningAttempt, formatTimePtr(v.StartedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutAttempt(v Attempt) error {
	return t.exec(`
		INSERT INTO attempts (id, run_id, task_id, number, term, runtime, pool, agent_id, status, reason,
			exit_code, staging_uri, logs_uri, result_json, lease_expires_at, dispatched_at, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			agent_id = excluded.agent_id, status = excluded.status, reason = excluded.reason,
			exit_code = excluded.exit_code, result_json = excluded.result_json,
			lease_expires_at = excluded.lease_expires_at, started_at = excluded.started_at,
			finished_at = excluded.finished_at`,
		v.ID, v.RunID, v.TaskID, v.Number, v.Term, v.Runtime, v.Pool, v.AgentID, v.Status, v.Reason,
		v.ExitCode, v.StagingURI, v.LogsURI, string(v.Result), formatTimePtr(v.LeaseExpiresAt),
		formatTime(v.DispatchedAt), formatTimePtr(v.StartedAt), formatTimePtr(v.FinishedAt))
}

func (t *Tx) PutOutput(v Output) error {
	meta, err := jsonObject(v.Metadata)
	if err != nil {
		return err
	}
	return t.exec(`
		INSERT INTO outputs (run_id, task_id, name, uri, attempt_id, metadata_json) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id, task_id, name) DO UPDATE SET
			uri = excluded.uri, attempt_id = excluded.attempt_id, metadata_json = excluded.metadata_json`,
		v.RunID, v.TaskID, v.Name, v.URI, v.AttemptID, meta)
}

func jsonObject(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	return string(b), err
}

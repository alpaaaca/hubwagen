package state

import (
	"context"
	"database/sql"
)

// Catalog is what a new leader needs to know before scheduling: tenants,
// pipelines, and the specs of every version that is current or still in use
// by an unfinished run.
type Catalog struct {
	Tenants   []Tenant
	Pipelines []Pipeline
	Versions  map[string]PipelineVersion // by hash
	Cursors   []ScheduleCursor
}

// ActiveState is every unfinished run with all of its task runs and
// attempts. Finished runs are never loaded into memory.
type ActiveState struct {
	Runs     []Run
	TaskRuns []TaskRun
	Attempts []Attempt
	LastSeq  int64 // newest event seq at load time
}

// LoadCatalog reads the catalog in one read transaction, so it is consistent
// with itself. Call it before LoadActive on leader start.
func (s *Store) LoadCatalog(ctx context.Context) (*Catalog, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	c := &Catalog{Versions: map[string]PipelineVersion{}}
	if c.Tenants, err = queryAll(ctx, tx, scanTenant,
		`SELECT name, weight, max_running_tasks, created_at FROM tenants ORDER BY name`); err != nil {
		return nil, err
	}
	if c.Pipelines, err = queryAll(ctx, tx, scanPipeline,
		`SELECT tenant, name, current_version, paused FROM pipelines ORDER BY tenant, name`); err != nil {
		return nil, err
	}
	versions, err := queryAll(ctx, tx, scanVersion, `
		SELECT `+versionColumns+` FROM pipeline_versions WHERE hash IN (
			SELECT current_version FROM pipelines
			UNION SELECT version FROM runs WHERE status IN ('queued', 'running'))`)
	if err != nil {
		return nil, err
	}
	for _, v := range versions {
		c.Versions[v.Hash] = v
	}
	if c.Cursors, err = queryAll(ctx, tx, scanCursor,
		`SELECT tenant, pipeline, last_tick FROM schedule_cursors ORDER BY tenant, pipeline`); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadActive reads every unfinished run, its task runs and attempts in one
// read transaction.
func (s *Store) LoadActive(ctx context.Context) (*ActiveState, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	const active = `SELECT id FROM runs WHERE status IN ('queued', 'running')`
	a := &ActiveState{}
	if a.Runs, err = queryAll(ctx, tx, scanRun,
		`SELECT `+runColumns+` FROM runs WHERE status IN ('queued', 'running') ORDER BY id`); err != nil {
		return nil, err
	}
	if a.TaskRuns, err = queryAll(ctx, tx, scanTaskRun,
		`SELECT `+taskRunColumns+` FROM task_runs WHERE run_id IN (`+active+`) ORDER BY run_id, task_id`); err != nil {
		return nil, err
	}
	if a.Attempts, err = queryAll(ctx, tx, scanAttempt,
		`SELECT `+attemptColumns+` FROM attempts WHERE run_id IN (`+active+`) ORDER BY run_id, task_id, number`); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&a.LastSeq); err != nil {
		return nil, err
	}
	return a, nil
}

func scanTenant(sc interface{ Scan(...any) error }) (Tenant, error) {
	var (
		v       Tenant
		created string
	)
	if err := sc.Scan(&v.Name, &v.Weight, &v.MaxRunningTasks, &created); err != nil {
		return v, err
	}
	var err error
	v.CreatedAt, err = parseTime(created)
	return v, err
}

func scanCursor(sc interface{ Scan(...any) error }) (ScheduleCursor, error) {
	var (
		v    ScheduleCursor
		tick string
	)
	if err := sc.Scan(&v.Tenant, &v.Pipeline, &tick); err != nil {
		return v, err
	}
	var err error
	v.LastTick, err = parseTime(tick)
	return v, err
}

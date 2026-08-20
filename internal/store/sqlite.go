package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %q: %w", pragma, err)
		}
	}

	if err := RunMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

// --- Runs ---

func (s *SQLiteStore) CreateRun(ctx context.Context, run *Run) error {
	_, err := s.db.ExecContext(ctx, sqlCreateRun,
		run.ID, run.DAGName, run.Status, run.Trigger,
		toNullTime(run.StartedAt), toNullTime(run.FinishedAt), run.CreatedAt)
	return err
}

func (s *SQLiteStore) UpdateRun(ctx context.Context, run *Run) error {
	_, err := s.db.ExecContext(ctx, sqlUpdateRun,
		run.Status, toNullTime(run.StartedAt), toNullTime(run.FinishedAt), run.ID)
	return err
}

func (s *SQLiteStore) GetRun(ctx context.Context, id string) (*Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, sqlGetRun, id))
}

func (s *SQLiteStore) ListRuns(ctx context.Context, dagName string, limit int) ([]*Run, error) {
	rows, err := s.db.QueryContext(ctx, sqlListRuns, dagName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// --- Steps ---

func (s *SQLiteStore) CreateStep(ctx context.Context, step *StepRun) error {
	meta, err := marshalMeta(step.Meta)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, sqlCreateStep,
		step.ID, step.RunID, step.StepID, step.StepName, step.Status, step.Executor,
		toNullTime(step.StartedAt), toNullTime(step.FinishedAt), toNullInt(step.ExitCode), meta)
	return err
}

func (s *SQLiteStore) UpdateStep(ctx context.Context, step *StepRun) error {
	meta, err := marshalMeta(step.Meta)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, sqlUpdateStep,
		step.Status, toNullTime(step.StartedAt), toNullTime(step.FinishedAt), toNullInt(step.ExitCode), meta, step.ID)
	return err
}

func (s *SQLiteStore) ListStepRuns(ctx context.Context, runID string) ([]*StepRun, error) {
	rows, err := s.db.QueryContext(ctx, sqlListStepRuns, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []*StepRun
	for rows.Next() {
		sr, err := scanStepRun(rows)
		if err != nil {
			return nil, err
		}
		steps = append(steps, sr)
	}
	return steps, rows.Err()
}

// --- Logs ---

func (s *SQLiteStore) AppendLog(ctx context.Context, runID, stepID, line string) error {
	_, err := s.db.ExecContext(ctx, sqlAppendLog, runID, stepID, line)
	return err
}

func (s *SQLiteStore) GetLogs(ctx context.Context, runID, stepID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, sqlGetLogs, runID, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, rows.Err()
}

func (s *SQLiteStore) StreamLogs(ctx context.Context, runID, stepID string) (<-chan string, error) {
	ch := make(chan string, 32)
	terminalStatuses := map[string]bool{
		"succeeded": true, "failed": true, "skipped": true, "cancelled": true,
	}
	go func() {
		defer close(ch)
		var lastID int64
		for {
			rows, err := s.db.QueryContext(ctx, sqlGetNewLogs, runID, stepID, lastID)
			if err == nil {
				for rows.Next() {
					var id int64
					var line string
					if rows.Scan(&id, &line) == nil {
						lastID = id
						select {
						case ch <- line:
						case <-ctx.Done():
							rows.Close()
							return
						}
					}
				}
				rows.Close()
			}

			var status string
			_ = s.db.QueryRowContext(ctx, sqlGetStepStatus, runID, stepID).Scan(&status)
			if terminalStatuses[status] {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
	return ch, nil
}

// --- DAGs ---

func (s *SQLiteStore) UpsertDAG(ctx context.Context, name, description, schedule, yamlHash string) error {
	_, err := s.db.ExecContext(ctx, sqlUpsertDAG, name, description, schedule, yamlHash)
	return err
}

// --- Lifecycle ---

func (s *SQLiteStore) MarkInterruptedRunsFailed(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, sqlSelectRunningRuns)
	if err != nil {
		return fmt.Errorf("query running runs: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx, sqlMarkRunFailed, id); err != nil {
			return fmt.Errorf("mark run %s failed: %w", id, err)
		}
		if _, err := s.db.ExecContext(ctx, sqlAppendLog, id, "engine", "run interrupted"); err != nil {
			return fmt.Errorf("append interrupted log for %s: %w", id, err)
		}
	}
	return nil
}

// --- helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(s rowScanner) (*Run, error) {
	var r Run
	var startedAt, finishedAt sql.NullTime
	if err := s.Scan(&r.ID, &r.DAGName, &r.Status, &r.Trigger,
		&startedAt, &finishedAt, &r.CreatedAt); err != nil {
		return nil, err
	}
	if startedAt.Valid {
		r.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		r.FinishedAt = &finishedAt.Time
	}
	return &r, nil
}

func scanStepRun(s rowScanner) (*StepRun, error) {
	var sr StepRun
	var startedAt, finishedAt sql.NullTime
	var exitCode sql.NullInt64
	var metaJSON sql.NullString
	if err := s.Scan(&sr.ID, &sr.RunID, &sr.StepID, &sr.StepName, &sr.Status, &sr.Executor,
		&startedAt, &finishedAt, &exitCode, &metaJSON); err != nil {
		return nil, err
	}
	if startedAt.Valid {
		sr.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		sr.FinishedAt = &finishedAt.Time
	}
	if exitCode.Valid {
		v := int(exitCode.Int64)
		sr.ExitCode = &v
	}
	if metaJSON.Valid && metaJSON.String != "" {
		_ = json.Unmarshal([]byte(metaJSON.String), &sr.Meta)
	}
	return &sr, nil
}

func toNullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

func toNullInt(i *int) any {
	if i == nil {
		return nil
	}
	return *i
}

func marshalMeta(meta map[string]any) (any, error) {
	if meta == nil {
		return nil, nil
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal meta: %w", err)
	}
	return string(b), nil
}

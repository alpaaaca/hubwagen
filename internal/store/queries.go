package store

const (
	sqlCreateRun = `
		INSERT INTO runs (id, dag_name, status, trigger, started_at, finished_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	sqlUpdateRun = `
		UPDATE runs SET status=?, started_at=?, finished_at=? WHERE id=?`

	sqlGetRun = `
		SELECT id, dag_name, status, trigger, started_at, finished_at, created_at
		FROM runs WHERE id=?`

	sqlListRuns = `
		SELECT id, dag_name, status, trigger, started_at, finished_at, created_at
		FROM runs WHERE dag_name=? ORDER BY created_at DESC LIMIT ?`

	sqlCreateStep = `
		INSERT INTO step_runs (id, run_id, step_id, step_name, status, executor, started_at, finished_at, exit_code, meta)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	sqlUpdateStep = `
		UPDATE step_runs SET status=?, started_at=?, finished_at=?, exit_code=?, meta=? WHERE id=?`

	sqlListStepRuns = `
		SELECT id, run_id, step_id, step_name, status, executor, started_at, finished_at, exit_code, meta
		FROM step_runs WHERE run_id=? ORDER BY rowid ASC`

	sqlAppendLog = `
		INSERT INTO logs (run_id, step_id, line) VALUES (?, ?, ?)`

	sqlGetLogs = `
		SELECT line FROM logs WHERE run_id=? AND step_id=? ORDER BY id ASC`

	sqlGetNewLogs = `
		SELECT id, line FROM logs WHERE run_id=? AND step_id=? AND id>? ORDER BY id ASC`

	sqlGetStepStatus = `
		SELECT status FROM step_runs WHERE run_id=? AND step_id=?`

	sqlUpsertDAG = `
		INSERT OR REPLACE INTO dags (name, description, schedule, yaml_hash, updated_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`

	sqlSelectRunningRuns = `
		SELECT id FROM runs WHERE status='running'`

	sqlMarkRunFailed = `
		UPDATE runs SET status='failed', finished_at=CURRENT_TIMESTAMP WHERE id=?`
)

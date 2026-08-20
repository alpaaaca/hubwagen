CREATE TABLE dags (
    name        TEXT PRIMARY KEY,
    description TEXT,
    schedule    TEXT,
    yaml_hash   TEXT,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE runs (
    id          TEXT PRIMARY KEY,
    dag_name    TEXT NOT NULL,
    status      TEXT NOT NULL,
    trigger     TEXT NOT NULL,
    started_at  DATETIME,
    finished_at DATETIME,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dag_name) REFERENCES dags(name)
);

CREATE TABLE step_runs (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    step_id     TEXT NOT NULL,
    step_name   TEXT NOT NULL,
    status      TEXT NOT NULL,
    executor    TEXT NOT NULL,
    started_at  DATETIME,
    finished_at DATETIME,
    exit_code   INTEGER,
    meta        TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE TABLE logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id  TEXT NOT NULL,
    step_id TEXT NOT NULL,
    line    TEXT NOT NULL,
    ts      DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX idx_runs_dag_name   ON runs(dag_name);
CREATE INDEX idx_step_runs_run_id ON step_runs(run_id);
CREATE INDEX idx_logs_run_step   ON logs(run_id, step_id);

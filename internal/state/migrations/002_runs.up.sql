-- v2: runs, steps and timers. Lives next to the v1 tables until M3c.

-- A run's event log is its events in seq order.
ALTER TABLE events ADD COLUMN run_id TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_events_run ON events(run_id, seq) WHERE run_id <> '';

CREATE TABLE runs (
    id             TEXT PRIMARY KEY,           -- ULID
    tenant         TEXT NOT NULL,
    name           TEXT NOT NULL,              -- registered pipeline function
    queue           TEXT NOT NULL,              -- where its run tasks are queued
    trigger        TEXT NOT NULL,              -- manual|cron|dataset|signal|child|api
    params_json    TEXT NOT NULL DEFAULT '{}',
    commands       INTEGER NOT NULL DEFAULT 0, -- commands recorded so far; the next gets seq commands+1
    build_id       TEXT NOT NULL DEFAULT '',   -- build of the last completed run task
    status         TEXT NOT NULL,
    reason         TEXT NOT NULL DEFAULT '',   -- why failed or stuck
    message        TEXT NOT NULL DEFAULT '',
    result_json    TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    finished_at    TEXT
);
CREATE INDEX idx_runs_name   ON runs(tenant, name, id);
CREATE INDEX idx_runs_active ON runs(status) WHERE status IN ('running', 'stuck');

-- One replay of a run on a worker.
CREATE TABLE run_tasks (
    id             TEXT PRIMARY KEY,           -- ULID
    run_id        TEXT NOT NULL REFERENCES runs(id),
    number         INTEGER NOT NULL,
    term           INTEGER NOT NULL,           -- leadership term that queued it
    worker_id      TEXT NOT NULL DEFAULT '',
    build_id       TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    reason         TEXT NOT NULL DEFAULT '',
    message        TEXT NOT NULL DEFAULT '',
    queued_at      TEXT NOT NULL,
    dispatched_at  TEXT,
    finished_at    TEXT,
    UNIQUE (run_id, number)
);

-- A step is identified by its run and the sequence number of the command
-- that scheduled it.
CREATE TABLE steps (
    run_id        TEXT NOT NULL REFERENCES runs(id),
    seq            INTEGER NOT NULL,
    name           TEXT NOT NULL,              -- registered step function
    queue           TEXT NOT NULL,
    args_json      TEXT NOT NULL DEFAULT '',   -- encoded by the SDK; opaque to the server
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    retry_at       TEXT,
    reason         TEXT NOT NULL DEFAULT '',
    message        TEXT NOT NULL DEFAULT '',
    result_json    TEXT NOT NULL DEFAULT '',
    scheduled_at   TEXT NOT NULL,
    finished_at    TEXT,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE step_attempts (
    id                TEXT PRIMARY KEY,        -- ULID
    run_id           TEXT NOT NULL,
    seq               INTEGER NOT NULL,
    number            INTEGER NOT NULL,
    term              INTEGER NOT NULL,        -- leadership term that dispatched it
    worker_id         TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL,
    reason            TEXT NOT NULL DEFAULT '', -- error|timeout|lost|cancelled|...
    message           TEXT NOT NULL DEFAULT '',
    lease_expires_at  TEXT,
    dispatched_at     TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    UNIQUE (run_id, seq, number),
    FOREIGN KEY (run_id, seq) REFERENCES steps(run_id, seq)
);

-- Durable sleeps and deadlines, identified like steps.
CREATE TABLE timers (
    run_id     TEXT NOT NULL REFERENCES runs(id),
    seq         INTEGER NOT NULL,
    fire_at     TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    fired_at    TEXT,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX idx_timers_pending ON timers(fire_at) WHERE status = 'pending';

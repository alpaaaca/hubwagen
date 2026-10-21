-- Event log: append-only, the source of every projection below.
-- AUTOINCREMENT so sequence numbers are never reused, even after archival
-- deletes rows; live subscribers resume from the last seq they saw.
CREATE TABLE events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    term        INTEGER NOT NULL,
    ts          TEXT    NOT NULL,             -- RFC 3339 (nanoseconds), UTC
    tenant      TEXT    NOT NULL DEFAULT '',
    type        TEXT    NOT NULL,
    run_id      TEXT    NOT NULL DEFAULT '',
    task_id     TEXT    NOT NULL DEFAULT '',
    attempt_id  TEXT    NOT NULL DEFAULT '',
    payload     TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_events_run    ON events(run_id, seq) WHERE run_id <> '';
CREATE INDEX idx_events_tenant ON events(tenant, seq);

-- Singleton values: 'term' (current leadership term).
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE tenants (
    name               TEXT PRIMARY KEY,
    weight             INTEGER NOT NULL DEFAULT 1,
    max_running_tasks  INTEGER NOT NULL DEFAULT 0,  -- 0 = unlimited
    created_at         TEXT NOT NULL
);

CREATE TABLE pipeline_versions (
    hash          TEXT PRIMARY KEY,             -- sha256 of the canonical spec JSON
    tenant        TEXT NOT NULL REFERENCES tenants(name),
    pipeline      TEXT NOT NULL,
    spec_json     TEXT NOT NULL,                -- canonical bytes; specs are small
    published_at  TEXT NOT NULL,
    published_by  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_versions_pipeline ON pipeline_versions(tenant, pipeline, published_at);

CREATE TABLE pipelines (
    tenant          TEXT NOT NULL REFERENCES tenants(name),
    name            TEXT NOT NULL,
    current_version TEXT NOT NULL REFERENCES pipeline_versions(hash),
    paused          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, name)
);

-- Last cron tick turned into a run, per pipeline; catch-up starts after it.
CREATE TABLE schedule_cursors (
    tenant     TEXT NOT NULL,
    pipeline   TEXT NOT NULL,
    last_tick  TEXT NOT NULL,
    PRIMARY KEY (tenant, pipeline),
    FOREIGN KEY (tenant, pipeline) REFERENCES pipelines(tenant, name)
);

CREATE TABLE runs (
    id              TEXT PRIMARY KEY,           -- ULID
    tenant          TEXT NOT NULL,
    pipeline        TEXT NOT NULL,
    version         TEXT NOT NULL REFERENCES pipeline_versions(hash),
    run_key         TEXT NOT NULL,
    generation      INTEGER NOT NULL,
    trigger         TEXT NOT NULL,              -- cron|manual|backfill|asset|api
    priority        TEXT NOT NULL,              -- interactive|scheduled|backfill
    interval_start  TEXT,
    interval_end    TEXT,
    params_json     TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    UNIQUE (run_key, generation)
);
CREATE INDEX idx_runs_pipeline ON runs(tenant, pipeline, id);
CREATE INDEX idx_runs_active   ON runs(status) WHERE status IN ('queued', 'running');

CREATE TABLE task_runs (
    run_id           TEXT NOT NULL REFERENCES runs(id),
    task_id          TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    ready_at         TEXT,
    retry_at         TEXT,
    winning_attempt  TEXT NOT NULL DEFAULT '',
    started_at       TEXT,
    finished_at      TEXT,
    PRIMARY KEY (run_id, task_id)
);

CREATE TABLE attempts (
    id                TEXT PRIMARY KEY,         -- ULID; also the Job/container label value
    run_id            TEXT NOT NULL,
    task_id           TEXT NOT NULL,
    number            INTEGER NOT NULL,
    term              INTEGER NOT NULL,         -- leadership term that dispatched it
    runtime           TEXT NOT NULL,            -- agent|job
    pool              TEXT NOT NULL DEFAULT '',
    agent_id          TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL,
    reason            TEXT NOT NULL DEFAULT '', -- exit_code|timeout|lost|cancelled|exec_error|...
    exit_code         INTEGER,
    staging_uri       TEXT NOT NULL DEFAULT '',
    logs_uri          TEXT NOT NULL DEFAULT '',
    result_json       TEXT NOT NULL DEFAULT '',
    lease_expires_at  TEXT,
    dispatched_at     TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    UNIQUE (run_id, task_id, number),
    FOREIGN KEY (run_id, task_id) REFERENCES task_runs(run_id, task_id)
);

-- Committed outputs: references only, never data.
CREATE TABLE outputs (
    run_id         TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    name           TEXT NOT NULL,
    uri            TEXT NOT NULL,               -- winning attempt's prefix
    attempt_id     TEXT NOT NULL REFERENCES attempts(id),
    metadata_json  TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, task_id, name),
    FOREIGN KEY (run_id, task_id) REFERENCES task_runs(run_id, task_id)
);

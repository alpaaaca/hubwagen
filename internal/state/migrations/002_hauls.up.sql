-- v2: hauls, lifts and timers. Lives next to the v1 tables until M3c.

-- A haul's logbook is its events in seq order.
ALTER TABLE events ADD COLUMN haul_id TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_events_haul ON events(haul_id, seq) WHERE haul_id <> '';

CREATE TABLE hauls (
    id             TEXT PRIMARY KEY,           -- ULID
    tenant         TEXT NOT NULL,
    name           TEXT NOT NULL,              -- registered haul function
    dock           TEXT NOT NULL,              -- where its haul tasks are queued
    trigger        TEXT NOT NULL,              -- manual|cron|dataset|signal|child|api
    params_json    TEXT NOT NULL DEFAULT '{}',
    commands       INTEGER NOT NULL DEFAULT 0, -- commands recorded so far; the next gets seq commands+1
    build_id       TEXT NOT NULL DEFAULT '',   -- build of the last completed haul task
    status         TEXT NOT NULL,
    reason         TEXT NOT NULL DEFAULT '',   -- why failed or stuck
    message        TEXT NOT NULL DEFAULT '',
    result_json    TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    finished_at    TEXT
);
CREATE INDEX idx_hauls_name   ON hauls(tenant, name, id);
CREATE INDEX idx_hauls_active ON hauls(status) WHERE status IN ('running', 'stuck');

-- One replay of a haul on a worker.
CREATE TABLE haul_tasks (
    id             TEXT PRIMARY KEY,           -- ULID
    haul_id        TEXT NOT NULL REFERENCES hauls(id),
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
    UNIQUE (haul_id, number)
);

-- A lift is identified by its haul and the sequence number of the command
-- that scheduled it.
CREATE TABLE lifts (
    haul_id        TEXT NOT NULL REFERENCES hauls(id),
    seq            INTEGER NOT NULL,
    name           TEXT NOT NULL,              -- registered lift function
    dock           TEXT NOT NULL,
    args_json      TEXT NOT NULL DEFAULT '',   -- encoded by the SDK; opaque to the server
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    retry_at       TEXT,
    reason         TEXT NOT NULL DEFAULT '',
    message        TEXT NOT NULL DEFAULT '',
    result_json    TEXT NOT NULL DEFAULT '',
    scheduled_at   TEXT NOT NULL,
    finished_at    TEXT,
    PRIMARY KEY (haul_id, seq)
);

CREATE TABLE lift_attempts (
    id                TEXT PRIMARY KEY,        -- ULID
    haul_id           TEXT NOT NULL,
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
    UNIQUE (haul_id, seq, number),
    FOREIGN KEY (haul_id, seq) REFERENCES lifts(haul_id, seq)
);

-- Durable sleeps and deadlines, identified like lifts.
CREATE TABLE timers (
    haul_id     TEXT NOT NULL REFERENCES hauls(id),
    seq         INTEGER NOT NULL,
    fire_at     TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    fired_at    TEXT,
    PRIMARY KEY (haul_id, seq)
);
CREATE INDEX idx_timers_pending ON timers(fire_at) WHERE status = 'pending';

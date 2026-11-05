-- Event log: append-only, the source of every projection below.
-- AUTOINCREMENT so sequence numbers are never reused, even after archival
-- deletes rows; live subscribers resume from the last seq they saw.
CREATE TABLE events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    term        INTEGER NOT NULL,
    ts          TEXT    NOT NULL,             -- RFC 3339 (nanoseconds), UTC
    tenant      TEXT    NOT NULL DEFAULT '',
    type        TEXT    NOT NULL,
    attempt_id  TEXT    NOT NULL DEFAULT '',
    payload     TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_events_tenant ON events(tenant, seq);

-- Singleton values: 'term' (current leadership term).
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

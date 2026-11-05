-- Builds: the code version of every step in a worker build, registered on
-- worker start. Builds are immutable.
CREATE TABLE builds (
    id             TEXT PRIMARY KEY,
    registered_at  TEXT NOT NULL
);

CREATE TABLE build_steps (
    build_id      TEXT NOT NULL REFERENCES builds(id),
    step          TEXT NOT NULL,
    code_version  TEXT NOT NULL,
    PRIMARY KEY (build_id, step)
);

-- Why a step ran or was reused, and the fingerprint it was looked up by.
ALTER TABLE steps ADD COLUMN fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE steps ADD COLUMN fingerprint_json TEXT NOT NULL DEFAULT ''; -- what went into it, for rerun reasons
ALTER TABLE steps ADD COLUMN run_reason TEXT NOT NULL DEFAULT '';       -- e.g. "code changed (v1 → v2)"; "" when reused

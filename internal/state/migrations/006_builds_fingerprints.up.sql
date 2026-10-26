-- Builds: the code version of every lift in a worker build, registered on
-- worker start. Builds are immutable.
CREATE TABLE builds (
    id             TEXT PRIMARY KEY,
    registered_at  TEXT NOT NULL
);

CREATE TABLE build_lifts (
    build_id      TEXT NOT NULL REFERENCES builds(id),
    lift          TEXT NOT NULL,
    code_version  TEXT NOT NULL,
    PRIMARY KEY (build_id, lift)
);

-- Why a lift ran or was reused, and the fingerprint it was looked up by.
ALTER TABLE lifts ADD COLUMN fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE lifts ADD COLUMN fingerprint_json TEXT NOT NULL DEFAULT ''; -- what went into it, for rerun reasons
ALTER TABLE lifts ADD COLUMN run_reason TEXT NOT NULL DEFAULT '';       -- e.g. "code changed (v1 → v2)"; "" when reused

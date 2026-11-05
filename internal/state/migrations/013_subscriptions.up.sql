-- Dataset subscriptions: the newest build defining a run decides which
-- datasets start it; data versions committed to them wait in a batch until the
-- run starts with them.
CREATE TABLE subscriptions (
    run         TEXT PRIMARY KEY,
    datasets     TEXT NOT NULL,             -- JSON array of dataset names
    debounce_ns  INTEGER NOT NULL,
    max_wait_ns  INTEGER NOT NULL,
    max_batch    INTEGER NOT NULL,
    build_id     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE subscription_data_versions (
    run       TEXT NOT NULL,
    tenant     TEXT NOT NULL,
    data_version_id  TEXT NOT NULL,
    added_at   TEXT NOT NULL,
    PRIMARY KEY (run, tenant, data_version_id)
);

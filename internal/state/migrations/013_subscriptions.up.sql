-- Dataset subscriptions: the newest build defining a haul decides which
-- datasets start it; pallets committed to them wait in a batch until the
-- haul starts with them.
CREATE TABLE subscriptions (
    haul         TEXT PRIMARY KEY,
    datasets     TEXT NOT NULL,             -- JSON array of dataset names
    debounce_ns  INTEGER NOT NULL,
    max_wait_ns  INTEGER NOT NULL,
    max_batch    INTEGER NOT NULL,
    build_id     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE subscription_pallets (
    haul       TEXT NOT NULL,
    tenant     TEXT NOT NULL,
    pallet_id  TEXT NOT NULL,
    added_at   TEXT NOT NULL,
    PRIMARY KEY (haul, tenant, pallet_id)
);

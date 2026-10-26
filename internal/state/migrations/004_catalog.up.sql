-- v2 catalog: datasets, the pallets committed to them, and lineage. Projected
-- from haul events; nobody writes it directly.

CREATE TABLE datasets (
    tenant         TEXT NOT NULL,
    name           TEXT NOT NULL,              -- e.g. marketing.daily_revenue
    partition_key  TEXT NOT NULL DEFAULT '',   -- e.g. day; '' = unpartitioned
    created_at     TEXT NOT NULL,
    PRIMARY KEY (tenant, name)
);

-- A pallet points to data; it never holds it.
CREATE TABLE pallets (
    id            TEXT PRIMARY KEY,            -- ULID
    tenant        TEXT NOT NULL,
    dataset       TEXT NOT NULL,
    partition     TEXT NOT NULL DEFAULT '',    -- '' for unpartitioned datasets
    location      TEXT NOT NULL,               -- e.g. the pallet manifest's URI
    content_hash  TEXT NOT NULL DEFAULT '',    -- from the storage adapter; '' if it has none
    fingerprint   TEXT NOT NULL DEFAULT '',    -- of the lift run that built it
    stats_json    TEXT NOT NULL DEFAULT '{}',  -- rows, bytes, schema, ...
    meta_json     TEXT NOT NULL DEFAULT '{}',  -- set by the lift, e.g. a cursor's max_ts
    haul_id       TEXT NOT NULL DEFAULT '',    -- producer: haul and lift command
    lift_seq      INTEGER NOT NULL DEFAULT 0,
    state         TEXT NOT NULL,               -- pending|committed|invalid|expired
    reason        TEXT NOT NULL DEFAULT '',    -- why invalid or expired
    created_at    TEXT NOT NULL,               -- recorded
    committed_at  TEXT,                        -- promoted, and so visible
    retired_at    TEXT,                        -- became invalid or expired
    FOREIGN KEY (tenant, dataset) REFERENCES datasets(tenant, name)
);
CREATE INDEX idx_pallets_latest ON pallets(tenant, dataset, partition, committed_at)
    WHERE state = 'committed';
CREATE INDEX idx_pallets_dataset ON pallets(tenant, dataset, id);
CREATE INDEX idx_pallets_fingerprint ON pallets(fingerprint) WHERE fingerprint <> '';

-- Lineage: pallet_id was built from input_id.
CREATE TABLE pallet_inputs (
    pallet_id  TEXT NOT NULL REFERENCES pallets(id),
    input_id   TEXT NOT NULL REFERENCES pallets(id),
    name       TEXT NOT NULL DEFAULT '',       -- the lift argument it was passed as
    PRIMARY KEY (pallet_id, input_id)
);
CREATE INDEX idx_pallet_inputs_input ON pallet_inputs(input_id);

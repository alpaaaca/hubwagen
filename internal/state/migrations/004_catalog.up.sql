-- v2 catalog: datasets, the data versions committed to them, and lineage. Projected
-- from run events; nobody writes it directly.

CREATE TABLE datasets (
    tenant         TEXT NOT NULL,
    name           TEXT NOT NULL,              -- e.g. marketing.daily_revenue
    partition_key  TEXT NOT NULL DEFAULT '',   -- e.g. day; '' = unpartitioned
    created_at     TEXT NOT NULL,
    PRIMARY KEY (tenant, name)
);

-- A data version points to data; it never holds it.
CREATE TABLE data_versions (
    id            TEXT PRIMARY KEY,            -- ULID
    tenant        TEXT NOT NULL,
    dataset       TEXT NOT NULL,
    partition     TEXT NOT NULL DEFAULT '',    -- '' for unpartitioned datasets
    location      TEXT NOT NULL,               -- e.g. the data version manifest's URI
    content_hash  TEXT NOT NULL DEFAULT '',    -- from the storage adapter; '' if it has none
    fingerprint   TEXT NOT NULL DEFAULT '',    -- of the step run that built it
    stats_json    TEXT NOT NULL DEFAULT '{}',  -- rows, bytes, schema, ...
    meta_json     TEXT NOT NULL DEFAULT '{}',  -- set by the step, e.g. a cursor's max_ts
    run_id       TEXT NOT NULL DEFAULT '',    -- producer: pipeline and step command
    step_seq      INTEGER NOT NULL DEFAULT 0,
    state         TEXT NOT NULL,               -- pending|committed|invalid|expired
    reason        TEXT NOT NULL DEFAULT '',    -- why invalid or expired
    created_at    TEXT NOT NULL,               -- recorded
    committed_at  TEXT,                        -- promoted, and so visible
    retired_at    TEXT,                        -- became invalid or expired
    FOREIGN KEY (tenant, dataset) REFERENCES datasets(tenant, name)
);
CREATE INDEX idx_data_versions_latest ON data_versions(tenant, dataset, partition, committed_at)
    WHERE state = 'committed';
CREATE INDEX idx_data_versions_dataset ON data_versions(tenant, dataset, id);
CREATE INDEX idx_data_versions_fingerprint ON data_versions(fingerprint) WHERE fingerprint <> '';

-- Lineage: data_version_id was built from input_id.
CREATE TABLE data_version_inputs (
    data_version_id  TEXT NOT NULL REFERENCES data_versions(id),
    input_id   TEXT NOT NULL REFERENCES data_versions(id),
    name       TEXT NOT NULL DEFAULT '',       -- the step argument it was passed as
    PRIMARY KEY (data_version_id, input_id)
);
CREATE INDEX idx_data_version_inputs_input ON data_version_inputs(input_id);

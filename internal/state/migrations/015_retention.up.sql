-- Retention per dataset: the newest build that declares the dataset (an
-- output's mk.Dataset(keep=...)) decides. 0 means that rule is unset.
CREATE TABLE retention (
    dataset        TEXT PRIMARY KEY,
    keep_versions  INTEGER NOT NULL DEFAULT 0,  -- newest N committed data versions per partition
    keep_ns        INTEGER NOT NULL DEFAULT 0,  -- data versions committed less than this long ago
    build_id       TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

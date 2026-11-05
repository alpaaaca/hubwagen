-- The runs each build defines, so run tasks go only to workers that can
-- replay them. Builds registered before this have none recorded.
CREATE TABLE build_pipelines (
    build_id  TEXT NOT NULL REFERENCES builds(id),
    run      TEXT NOT NULL,
    PRIMARY KEY (build_id, run)
);

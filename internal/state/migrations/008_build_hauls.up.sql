-- The hauls each build defines, so haul tasks go only to workers that can
-- replay them. Builds registered before this have none recorded.
CREATE TABLE build_hauls (
    build_id  TEXT NOT NULL REFERENCES builds(id),
    haul      TEXT NOT NULL,
    PRIMARY KEY (build_id, haul)
);

-- v1 (compiled YAML pipelines, runs and tasks) is gone. Its events stay in
-- the log as history; their run and task columns go.
DROP TABLE outputs;
DROP TABLE attempts;
DROP TABLE task_runs;
DROP TABLE runs;
DROP TABLE schedule_cursors;
DROP TABLE pipelines;
DROP TABLE pipeline_versions;
DROP TABLE tenants;

DROP INDEX idx_events_run;
ALTER TABLE events DROP COLUMN run_id;
ALTER TABLE events DROP COLUMN task_id;

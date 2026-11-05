-- Steps without an output are memoized by their recorded result, so steps
-- are looked up by fingerprint, and a reused step is told apart from one
-- that ran (a TTL counts from the run, not from the latest reuse).
ALTER TABLE steps ADD COLUMN reused INTEGER NOT NULL DEFAULT 0;
UPDATE steps SET reused = 1 WHERE fingerprint <> '' AND run_reason = '' AND status = 'completed';
CREATE INDEX idx_steps_fingerprint ON steps(fingerprint, finished_at) WHERE fingerprint <> '' AND reused = 0;
CREATE INDEX idx_steps_runs ON steps(name, finished_at) WHERE fingerprint <> '' AND reused = 0;

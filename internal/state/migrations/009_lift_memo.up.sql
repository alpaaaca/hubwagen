-- Lifts without an output are memoized by their recorded result, so lifts
-- are looked up by fingerprint, and a reused lift is told apart from one
-- that ran (a TTL counts from the run, not from the latest reuse).
ALTER TABLE lifts ADD COLUMN reused INTEGER NOT NULL DEFAULT 0;
UPDATE lifts SET reused = 1 WHERE fingerprint <> '' AND run_reason = '' AND status = 'completed';
CREATE INDEX idx_lifts_fingerprint ON lifts(fingerprint, finished_at) WHERE fingerprint <> '' AND reused = 0;
CREATE INDEX idx_lifts_runs ON lifts(name, finished_at) WHERE fingerprint <> '' AND reused = 0;

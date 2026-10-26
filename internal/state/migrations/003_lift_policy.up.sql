-- How a lift is retried and timed out, so a new leader can carry on.
ALTER TABLE lifts ADD COLUMN timeout_ns INTEGER NOT NULL DEFAULT 0; -- 0 = none
ALTER TABLE lifts ADD COLUMN retry_json TEXT NOT NULL DEFAULT '';   -- encoded by the core; '' = no retries

-- Child runs (start_run) and continue_as_new.
ALTER TABLE runs ADD COLUMN parent_id TEXT NOT NULL DEFAULT '';      -- the run whose start_run started it
ALTER TABLE runs ADD COLUMN parent_seq INTEGER NOT NULL DEFAULT 0;   -- that command's seq
ALTER TABLE runs ADD COLUMN parent_waits INTEGER NOT NULL DEFAULT 0; -- the parent waits for its result (ctx.run)
ALTER TABLE runs ADD COLUMN continued_as TEXT NOT NULL DEFAULT '';   -- continue_as_new: the run that carries on
CREATE INDEX idx_runs_parent ON runs(parent_id) WHERE parent_id <> '';

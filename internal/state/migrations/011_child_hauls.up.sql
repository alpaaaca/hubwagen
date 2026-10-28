-- Child hauls (start_haul) and continue_as_new.
ALTER TABLE hauls ADD COLUMN parent_id TEXT NOT NULL DEFAULT '';      -- the haul whose start_haul started it
ALTER TABLE hauls ADD COLUMN parent_seq INTEGER NOT NULL DEFAULT 0;   -- that command's seq
ALTER TABLE hauls ADD COLUMN parent_waits INTEGER NOT NULL DEFAULT 0; -- the parent waits for its result (ctx.haul)
ALTER TABLE hauls ADD COLUMN continued_as TEXT NOT NULL DEFAULT '';   -- continue_as_new: the haul that carries on
CREATE INDEX idx_hauls_parent ON hauls(parent_id) WHERE parent_id <> '';

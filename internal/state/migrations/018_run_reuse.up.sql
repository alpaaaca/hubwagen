-- Whether a run may reuse steps of earlier runs: '' (as its steps say)
-- or 'none' (moksha start --reuse=none: every step runs).
ALTER TABLE runs ADD COLUMN reuse TEXT NOT NULL DEFAULT '';

-- A step without an output can still produce a data version (a dataframe it
-- returned), so memoization looks data versions up by the step that built them.
CREATE INDEX idx_data_versions_step ON data_versions(run_id, step_seq);

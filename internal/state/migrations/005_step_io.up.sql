-- What a step reads and writes, so a new leader can finish its commit.
ALTER TABLE steps ADD COLUMN output_json TEXT NOT NULL DEFAULT ''; -- dataset + partition; '' = no data version
ALTER TABLE steps ADD COLUMN inputs_json TEXT NOT NULL DEFAULT ''; -- input data versions, for lineage

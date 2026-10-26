-- What a lift reads and writes, so a new leader can finish its commit.
ALTER TABLE lifts ADD COLUMN output_json TEXT NOT NULL DEFAULT ''; -- dataset + partition; '' = no pallet
ALTER TABLE lifts ADD COLUMN inputs_json TEXT NOT NULL DEFAULT ''; -- input pallets, for lineage

-- A lift without an output can still produce a pallet (a dataframe it
-- returned), so memoization looks pallets up by the lift that built them.
CREATE INDEX idx_pallets_lift ON pallets(haul_id, lift_seq);

-- Where each lift attempt writes its output, as it was handed to the worker,
-- so a layout change can't move it under a running attempt. Empty for
-- attempts dispatched before: they used layout.LegacyLiftStaging.
ALTER TABLE lift_attempts ADD COLUMN staging TEXT NOT NULL DEFAULT '';

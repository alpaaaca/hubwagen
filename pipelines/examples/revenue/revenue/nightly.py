"""Nightly revenue: every night at 03:00 UTC, yesterday's Daily revenue.

A scheduled pipeline: the server starts it on its own, once per data
interval (here: the previous day, 00:00 to 00:00 UTC), and it runs `daily`
for that day.
"""

import hubwagen as hw

from .hauls import daily


@hw.pipeline(title="Nightly revenue",
             schedule=hw.Schedule("0 3 * * *", interval="daily", catch_up="latest"))
def nightly(ctx, interval: hw.Interval):
    """Every night, yesterday's revenue: runs Daily revenue for the day that just ended."""
    day = interval.start.date().isoformat()
    return ctx.run(daily, day=day)

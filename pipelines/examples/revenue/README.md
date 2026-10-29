# Example: daily revenue (v2, Python)

One run per day: `extract_orders` → `daily_revenue` → `revenue_report`,
each writing a new version of its dataset (CSV and text files). Source data: `data/orders.csv`. `nightly`
([revenue/nightly.py](revenue/nightly.py)) runs it on a schedule: at 03:00
UTC, for the day that just ended.

```bash
# from the repository root
go build -o hubwagen .
./hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml   # server + 3 workers

# in another terminal
./hubwagen replay daily --param day=2026-10-01..2026-10-03
```

[hubwagen.yaml](hubwagen.yaml) starts three workers (`py-1`..`py-3`), with
data in `.hubwagen/` here. To run a worker by hand instead, from this
directory: `PYTHONPATH=../../../sdk/python/src python3 -m hubwagen.worker`. It
imports the modules listed in [pyproject.toml](pyproject.toml).

What to try:

1. Replay the same days again: `extract_orders` runs (it isn't memoized:
   it reads a source that can change), but its output is identical, so
   `daily_revenue` and `revenue_report` are **reused**.
2. Change `daily_revenue` (say, round to whole units: `Decimal("1")`),
   restart the worker and replay: it runs again because "its code changed
   (py-… → py-…)", and `revenue_report` because "an input changed".
3. Kill a worker (`kill -9 <pid>`; dev prints them): dev restarts it.
   Run `hubwagen dev --no-workers` and `hubwagen workers` (same
   `--config`) in two terminals to restart the server while the workers
   keep going.
4. Add a row for 2026-10-02 to `data/orders.csv` and replay: only that
   day's `daily_revenue` and report run again ("an input changed").

5. Watch what the steps print and log: `hubwagen dev` prints it marked
   with the run and the step, and the web UI (http://127.0.0.1:7070/) shows
   it on each run's page; click a step for its output only. Replay a day
   with no orders (`--param day=2026-10-20`) for a warning.

`sdk/python/tests/test_example.py` runs exactly this as a test.

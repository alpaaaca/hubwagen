# Example: daily revenue (v2, Python)

One haul per day: `extract_orders` → `daily_revenue` → `revenue_report`,
writing CSV and text pallets. Source data: `data/orders.csv`.

```bash
# from the repository root
go build -o hubwagen .
./hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml   # server + 3 workers

# in another terminal
./hubwagen replay daily --param day=2026-10-01..2026-10-03
```

[hubwagen.yaml](hubwagen.yaml) starts three workers (`py-1`..`py-3`), with
data in `.hubwagen/` here. To run a worker by hand instead, from this
directory: `PYTHONPATH=../../../sdk/python/src python3 -m hubwagen.worker revenue.hauls`.

What to try:

1. Replay the same days again: `extract_orders` runs (it isn't memoized:
   it reads a source that can change), but its output is identical, so
   `daily_revenue` and `revenue_report` are **reused**.
2. Change `daily_revenue` (say, round to whole units: `Decimal("1")`),
   restart the worker and replay: it runs again with
   `code changed (src-… → src-…)`, and `revenue_report` with `input changed`.
3. Kill a worker (`kill -9 <pid>`; dev prints them): dev restarts it.
   Run `hubwagen dev --no-workers` and `hubwagen workers` (same
   `--config`) in two terminals to restart the server while the workers
   keep going.
4. Add a row for 2026-10-02 to `data/orders.csv` and replay: only that
   day's `daily_revenue` and report run again (`input changed`).

`sdk/python/tests/test_example.py` runs exactly this as a test.

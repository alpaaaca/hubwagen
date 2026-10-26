# Example: daily revenue (v2, Python)

One haul per day: `extract_orders` → `daily_revenue` → `revenue_report`,
writing CSV and text pallets. Source data: `data/orders.csv`.

```bash
# from the repository root
go build -o hubwagen .
./hubwagen dev                               # server, worker API on 127.0.0.1:7070

# in another terminal, from this directory
PYTHONPATH=../../../sdk/python/src python3 -m hubwagen.worker revenue.hauls

# in a third
./hubwagen replay daily --param day=2026-10-01..2026-10-03
```

What to try:

1. Replay the same days again: `extract_orders` runs (it isn't memoized:
   it reads a source that can change), but its output is identical, so
   `daily_revenue` and `revenue_report` are **reused**.
2. Change `daily_revenue` (say, round to whole units: `Decimal("1")`),
   restart the worker and replay: it runs again with
   `code changed (src-… → src-…)`, and `revenue_report` with `input changed`.
3. Add a row for 2026-10-02 to `data/orders.csv` and replay: only that
   day's `daily_revenue` and report run again (`input changed`).

`sdk/python/tests/test_example.py` runs exactly this as a test.

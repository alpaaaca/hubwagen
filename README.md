# hubwagen

A durable execution engine for data. Write pipelines as plain Python
functions; hubwagen runs them to completion through crashes, deploys and
long waits, and records which versions of data went into and came out of
every step. That record is the lineage graph, the catalog, and a cache:
a rerun or backfill skips every step whose code and inputs didn't change.

One Go binary, no database server, no message broker. Your code runs in
workers you start; the server never runs it.

```python
import hubwagen as hw

@hw.step(output=hw.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict: ...            # writes files to hw.output().path

@hw.step(memo=True, output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: hw.DataVersion, day: str) -> dict: ...

@hw.pipeline
def daily(ctx, day: str):
    orders = ctx.step(extract_orders, day)           # a DataVersion: a saved, never-changed version of raw.orders
    return ctx.step(daily_revenue, orders, day).id   # reused if code and inputs are unchanged
```

## Status

The engine runs on one machine: `hubwagen dev` (server, API and workers in
one terminal) or `hubwagen server` (the server alone, as a service, with
`hubwagen workers` next to it), Python workers, `file://` storage, no
authentication yet. Built so far: durable pipelines (replay until
blocked, timers, retries, timeouts, leader restarts), data versions with
exactly-once commits, memoized steps with single-flight, a catalog with
lineage, the worker and pipeline APIs, dataframes between steps, code
versions from what a step's code reaches, runtime determinism guards,
signals, version markers, fan-out limits, runs that start runs,
continue_as_new, schedules, dataset subscriptions, a linter for pipeline
code, testing tools (scripted runs, replays of recorded runs) and a web UI.
(Inside, the engine calls a pipeline a haul, a step a lift and a data
version a pallet.) Next: M5 (catalog
UI, invalidation, staleness). See
[docs/roadmap.md](docs/roadmap.md) and
[docs/architecture.md](docs/architecture.md).

## Try it

Requires Go 1.26+ and Python 3.12+ (and Node 20+ for the web UI).

```bash
go generate ./internal/webui   # builds the web UI into the binary (optional; needs Node)
go build -o hubwagen .
# the server plus three workers, as pipelines/examples/revenue/hubwagen.yaml says
./hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml
# the web UI: http://127.0.0.1:7070/
```

```bash
# in another terminal
./hubwagen replay daily --param day=2026-10-01..2026-10-03
./hubwagen replay daily --param day=2026-10-01..2026-10-03   # again: what's unchanged is reused
./hubwagen start daily --param day=2026-10-04 --wait
```

`replay` prints, per step, what was reused and what ran and why:

```
3 runs
  extract_orders   3 ran
  daily_revenue    3 reused
  revenue_report   3 reused
3 completed
```

Dev prints each run as it starts and ends, and in between what its steps
print and log, marked with the run and the step:

```
▶ Daily revenue day=2026-10-01 started
  day=2026-10-01 › extract_orders │ reading orders.csv
  day=2026-10-01 › extract_orders │ 6 orders for 2026-10-01: 3 apac, 3 eu
  day=2026-10-01 › daily_revenue │ eu       439.78  78.5%
  day=2026-10-01 › daily_revenue │ apac     120.17  21.5%
  day=2026-10-01 › daily_revenue │ eu brings the most revenue on 2026-10-01
  day=2026-10-01 › revenue_report │ Revenue on 2026-10-01
  ...
✓ Daily revenue day=2026-10-01 completed in 58ms
▶ Daily revenue day=2026-10-01 started
  day=2026-10-01 › extract_orders │ reading orders.csv
  day=2026-10-01 › extract_orders │ 6 orders for 2026-10-01: 3 apac, 3 eu
  day=2026-10-01 › daily_revenue ↺ reused: same inputs and code, nothing ran
  day=2026-10-01 › revenue_report ↺ reused: same inputs and code, nothing ran
✓ Daily revenue day=2026-10-01 completed in 23ms
```

The web UI shows the same output on the run's page, step by step
(`hubwagen dev --events` prints the engine's own events instead). The config file: [docs/config.md](docs/config.md).
Writing pipelines and steps: [sdk/python/README.md](sdk/python/README.md).
The example: [pipelines/examples/revenue](pipelines/examples/revenue).

## Development

```bash
go vet ./...
go test ./...
go test -race ./...
(cd sdk/python && uv run --no-project --with pytest --python 3.12 pytest -q)
go generate ./internal/api    # after changing proto/
```

Pure Go, no CGO: the binary is static and cross-compiles.

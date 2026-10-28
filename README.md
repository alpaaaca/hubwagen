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

@hw.lift(output=hw.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict: ...            # writes files to hw.output().path

@hw.lift(memo=True, output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: hw.Pallet, day: str) -> dict: ...

@hw.haul
def daily(ctx, day: str):
    orders = ctx.lift(extract_orders, day)           # a Pallet: an immutable version of data
    return ctx.lift(daily_revenue, orders, day).id   # reused if code and inputs are unchanged
```

## Status

The engine runs on one machine: `hubwagen dev` (server and API), Python
workers, `file://` storage. Built so far: durable hauls (replay until
blocked, timers, retries, timeouts, leader restarts), pallets with
exactly-once commits, fingerprint memoization with single-flight, a
catalog with lineage, the worker and haul APIs, dataframes between lifts,
code versions from what a lift's code reaches, runtime determinism guards,
and signals. Next (M4): version markers, fan-out limits, child hauls,
schedules, subscriptions and testing tools. See [docs/roadmap.md](docs/roadmap.md) and
[docs/architecture.md](docs/architecture.md).

## Try it

Requires Go 1.26+ and Python 3.12+.

```bash
go build -o hubwagen .
# the server plus three workers, as pipelines/examples/revenue/hubwagen.yaml says
./hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml
```

```bash
# in another terminal
./hubwagen replay daily --param day=2026-10-01..2026-10-03
./hubwagen replay daily --param day=2026-10-01..2026-10-03   # again: what's unchanged is reused
./hubwagen start daily --param day=2026-10-04 --wait
```

`replay` prints, per lift, what was reused and what ran and why:

```
3 hauls
  extract_orders   3 run (not memoized)
  daily_revenue    3 reused
  revenue_report   3 reused
3 completed
```

Each lift dispatch shows in dev's output with the worker that took it
(`lift 2 attempt 1 → py-3`). The config file: [docs/config.md](docs/config.md).
Writing hauls and lifts: [sdk/python/README.md](sdk/python/README.md).
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

# moksha

**Data pipelines as plain Python, without the orchestrator.**

moksha is a durable execution engine for data. You write pipelines as
ordinary Python functions; moksha runs them to completion through crashes,
deploys and long waits, and keeps every output as an immutable, versioned
dataset. Because it knows which versions of data went into and came out of
every step, a rerun or a backfill skips every step whose code and inputs
didn't change.

One Go binary. No database server, no message broker, no scheduler fleet.

```python
import moksha as hw

@hw.step(output=hw.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict: ...            # writes files to hw.output().path

@hw.step(memo=True, output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: hw.DataVersion, day: str) -> dict: ...

@hw.pipeline
def daily(ctx, day: str):
    orders = ctx.step(extract_orders, day)           # a saved, never-changing version of raw.orders
    return ctx.step(daily_revenue, orders, day).id   # reused if its code and inputs are unchanged
```

## Why moksha

*Moksha* is release: liberation from the cycle. This project is about
release from a cycle data teams know well.

You want to run a few Python functions every night, in order, and know
what they produced. To do that today you adopt an orchestrator, and the
orchestrator arrives with a scheduler, a webserver, a metadata database, a
message broker, a pool of executors, a plugin ecosystem and a team to keep
it all running. It can do everything: it runs shell commands, sends
emails, waits on sensors, talks to every cloud. What it does poorly is the
one thing you came for.

- **It doesn't know your data.** It knows that a task ran, not what it
  wrote. Lineage, freshness and "which version is this report built on?"
  are bolted on afterwards, if at all.
- **It reruns everything.** A backfill of a year recomputes a year, even
  for the days and steps where nothing changed.
- **It loses work in a crash.** A pipeline that waits a day for a file,
  or dies halfway, starts over or needs a human to clear task states.
- **Your code is not plain code.** Pipelines are DAG objects with
  operators, templated strings and data passed through a side channel,
  parsed by the scheduler over and over.
- **It is heavy.** Several always-on services and a database before the
  first task runs, and CPU spent on the orchestrator rather than on your
  pipelines.

moksha does less, on purpose, and does it properly:

- **Plain code.** A pipeline is a Python function, a step is a Python
  function. Loops, conditionals and fan-out are just Python.
- **Durable by default.** Every run's history is recorded before anything
  acts on it. A pipeline survives crashes, restarts and deploys, and can
  wait hours or days for a timer, a signal or new data for free.
- **Data is the product.** Every step's output is a versioned, immutable
  data version in a catalog, with lineage back to the code and the inputs
  that made it.
- **Never compute the same thing twice.** A memoized step is reused when
  a readable version with the same code and inputs exists; a backfill only
  runs what changed. When moksha is unsure, it reruns.
- **Small.** One static binary on SQLite. Your code runs in workers you
  start, in your environment; the server never runs it.

## What you get

- Durable pipelines: replay until blocked, timers, retries, timeouts,
  signals, runs that start runs, `continue_as_new`
- Data versions with exactly-once commits, a catalog and lineage
- Memoized steps with single-flight: the same work in flight runs once
- Code versions from what a step's code actually reaches, not just its
  own source
- Dataframes between steps: return a pandas or Polars DataFrame or an
  Arrow table, receive it in the next step as the type you annotate
- Schedules (cron or calendar intervals, DST-correct, catch-up) and dataset
  subscriptions (new data starts a pipeline)
- A linter and runtime guards for pipeline code, and testing tools
  (scripted runs, replays of recorded runs)
- A web UI: runs, what happened in each, what steps printed and logged
- Local directories or S3 for data; workers as processes, containers or
  Kubernetes Deployments that scale with the work waiting

## Quick start

Requires Go 1.26+ and Python 3.12+ (and Node 20+ for the web UI).

```bash
go generate ./internal/webui   # build the web UI into the binary (optional; needs Node)
go build -o moksha .
./moksha dev --config pipelines/examples/revenue/moksha.yaml
```

That starts the server and three Python workers for the
[example project](pipelines/examples/revenue). The web UI is at
http://127.0.0.1:7070/. In another terminal:

```bash
./moksha replay daily --param day=2026-10-01..2026-10-03
./moksha replay daily --param day=2026-10-01..2026-10-03   # again: what's unchanged is reused
./moksha start daily --param day=2026-10-04 --wait
```

`replay` reports, per step, what ran and what was reused:

```
3 runs
  extract_orders   3 ran
  daily_revenue    3 reused
  revenue_report   3 reused
3 completed
```

`moksha dev` shows each run as it goes, with what its steps print and log:

```
▶ Daily revenue day=2026-10-01 started
  day=2026-10-01 › extract_orders │ reading orders.csv
  day=2026-10-01 › extract_orders │ 6 orders for 2026-10-01: 3 apac, 3 eu
  day=2026-10-01 › daily_revenue ↺ reused: same inputs and code, nothing ran
  day=2026-10-01 › revenue_report ↺ reused: same inputs and code, nothing ran
✓ Daily revenue day=2026-10-01 completed in 23ms
```

## How it works

```
          moksha server  (one Go binary)
   ┌──────────────────────────────────────────┐
   │  event log + catalog (SQLite)   web UI   │
   │  scheduler, timers, memoization    API   │
   └──────────────┬──────────────────┬────────┘
                  │ work, results    │ data
          ┌───────┴───────┐   ┌──────┴────────────┐
          │ your workers  │   │ bucket            │
          │ (Python SDK)  │   │ (directory or S3) │
          └───────────────┘   └───────────────────┘
```

- The **server** records everything that happens to a run as events, and
  derives the state of runs, steps and the catalog from them. It schedules,
  answers and stores; it never runs your code.
- **Workers** run your pipelines and steps. A pipeline function is
  replayed from its history until it reaches something new, so it can
  stop anywhere and pick up where it left off.
- The **bucket** holds the data versions. Only the server opens it;
  workers on other machines move data through URLs it hands out.

The design is in [docs/architecture.md](docs/architecture.md). (Inside,
the engine calls a pipeline a haul, a step a lift and a data version a
pallet.)

## Running it

- `moksha dev`: server, API, web UI and workers in one terminal
- `moksha server`: the same as a service, with JSON logs; it starts the
  config's workers, or `--no-workers` and `moksha workers` elsewhere
- Containers: `make images && make up` ([deploy/compose.yaml](deploy/compose.yaml))
- Kubernetes: the Helm chart in [deploy/helm/moksha](deploy/helm/moksha),
  which scales worker Deployments from the work waiting
- `moksha deploy`: ship a project's code to a running server

See [docs/deploy.md](docs/deploy.md) and [docs/config.md](docs/config.md).

## Status

moksha is young and changing fast. It runs on one machine or one leader,
with Python workers and no authentication yet. Next up: the catalog in the
web UI, invalidating data versions, and showing what is stale. See
[docs/roadmap.md](docs/roadmap.md).

## Documentation

- [Writing pipelines and steps](sdk/python/README.md): the Python SDK and the worker
- [Configuration](docs/config.md): `moksha.yaml`
- [Deployment](docs/deploy.md): images, compose, Helm
- [Projects](docs/projects.md): a repo's project file and `moksha deploy`
- [Architecture](docs/architecture.md): hauls, lifts, pallets, memoization, commits
- [Roadmap](docs/roadmap.md)

## Development

```bash
go vet ./...
go test ./...
go test -race ./...
(cd ui && npm ci && npm test && npm run typecheck)
(cd sdk/python && uv run --no-project --with pytest --python 3.12 pytest -q)
go generate ./internal/api    # after changing proto/
```

Pure Go, no CGO: the binary is static and cross-compiles.

# Moksha

[![Build & Test](https://github.com/alpaaaca/moksha/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/alpaaaca/moksha/actions/workflows/test.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Go version](https://img.shields.io/github/go-mod/go-version/alpaaaca/moksha)](go.mod)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](sdk/python)

**Plain data orchestrator, that lets you do your thing.**

**Moksha** (_liberation_ in Sanskrit) is a durable execution engine for data. You write pipelines as
ordinary Python functions; moksha runs them to completion through crashes,
deploys and long waits, and keeps every output as an immutable, versioned
dataset. Because it knows which versions of data went into and came out of
every step, a rerun or a backfill skips every step whose code and inputs
didn't change.

One Go binary. No database server, no message broker, no scheduler fleet.

```python
import moksha as mk

@mk.step(output=mk.Dataset("raw.orders", partition="day"))
def extract_orders(day: str) -> dict: ...            # writes files to mk.output().path

@mk.step(memo=True, output=mk.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: mk.DataVersion, day: str) -> dict: ...

@mk.pipeline
def daily(ctx, day: str):
    orders = ctx.step(extract_orders, day)           # a saved, never-changing version of raw.orders
    return ctx.step(daily_revenue, orders, day).id   # reused if its code and inputs are unchanged
```

## Why Moksha

Moksha started out of frustration with bloated orchestrators like Airflow and Dagster. They often bring high management costs, complicated architectures, and fuzzy concepts. You’re asked to run and babysit a scheduler, web server, metadata database, broker, and fleet of executors—and reshape your code around DAGs and operators. Yet they may not know what your pipelines actually wrote, rerun entire backfills, or lose work in a crash.

Moksha _liberates_ you from that overhead. Write pipelines on your own terms; Moksha handles durable runs, versioned data, avoiding duplicate computation, scheduling, and workers that scale with the work, from a laptop to a Kubernetes cluster. All from one small binary.

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

## Install

Download the archive for your machine from the
[latest release](https://github.com/alpaaaca/moksha/releases/latest)
(macOS and Linux, amd64 and arm64; the web UI is built in) and put
`moksha` on your `PATH`:

```bash
curl -fsSL https://github.com/alpaaaca/moksha/releases/download/v0.2.0/moksha_0.2.0_darwin_arm64.tar.gz | tar -xz moksha
sudo mv moksha /usr/local/bin/
moksha version
```

`checksums.txt` on the release has each archive's SHA-256. On macOS, a
binary downloaded with a browser (not `curl`) is quarantined:
`xattr -d com.apple.quarantine moksha`. Or build it from source (see
[Development](#development)).

## Quick start

The example project, a daily revenue pipeline (extract → transform →
report), lives in [moksha-demo](https://github.com/alpaaaca/moksha-demo).
With `moksha` on your `PATH` and Python 3.12+:

```bash
git clone https://github.com/alpaaaca/moksha-demo && cd moksha-demo
python3 -m venv .venv && . .venv/bin/activate
pip install "git+https://github.com/alpaaaca/moksha@v0.2.0#subdirectory=sdk/python"
moksha dev                                  # the server, web UI on http://127.0.0.1:7070/; it deploys the project
```

`moksha dev` reads the project file (`moksha.yaml`, `kind: Project`),
runs its code in place, and deploys it again when you change it. In
another terminal:

```bash
moksha replay daily --param day=2026-10-01..2026-10-03
moksha replay daily --param day=2026-10-01..2026-10-03   # again: what's unchanged is reused
```

`replay` reports, per step, what ran and what was reused:

```
3 runs
  extract_orders   3 ran
  daily_revenue    3 reused
  revenue_report   3 reused
3 completed
```

`moksha dev` shows each run as it goes, with what its steps print and
log:

```
▶ Daily revenue day=2026-10-01 started
  day=2026-10-01 › extract_orders │ reading orders.csv
  day=2026-10-01 › extract_orders │ 6 orders for 2026-10-01: 3 apac, 3 eu
  day=2026-10-01 › daily_revenue ↺ reused: same inputs and code, nothing ran
  day=2026-10-01 › revenue_report ↺ reused: same inputs and code, nothing ran
✓ Daily revenue day=2026-10-01 completed in 23ms
```

The demo's README goes on to a kind cluster: `helm install` the chart,
`moksha deploy --push`, and pods that scale from zero for a backfill.

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

The design is in [docs/architecture.md](docs/architecture.md).

## Running it

- `moksha dev`: server, API, web UI and workers in one terminal; in a
  project's directory it deploys the project to itself and again when the
  code changes ([docs/projects.md](docs/projects.md))
- `moksha server`: the same as a service, with JSON logs; it runs the
  workers of the projects deployed to it (`moksha deploy`)
- Containers: `make images && make up` ([deploy/compose.yaml](deploy/compose.yaml))
- Kubernetes: `helm install moksha oci://ghcr.io/alpaaaca/charts/moksha`
  installs the server; `moksha deploy --push` runs a project on it as
  Deployments scaled from the work waiting ([docs/deploy.md](docs/deploy.md#kubernetes-the-helm-chart))
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
- [Architecture](docs/architecture.md): runs, steps, data versions, memoization, commits
- [Roadmap](docs/roadmap.md)

## Development

Requires Go 1.26+, Python 3.12+ and Node 20+ (for the web UI).

```bash
go generate ./internal/webui   # build the web UI into the binary (optional; needs Node)
go build -o moksha .
go vet ./...
go test ./...
go test -race ./...
(cd ui && npm ci && npm test && npm run typecheck)
(cd sdk/python && uv run --no-project --with pytest --python 3.12 pytest -q)
go generate ./internal/api    # after changing proto/
```

Pure Go, no CGO: the binary is static and cross-compiles.

## License

moksha is licensed under the [Apache License 2.0](LICENSE).

# hubwagen

A workflow orchestrator for data teams. Single Go binary, no database server,
no message broker.

Pipelines are YAML, compiled into immutable, versioned specs. One event-driven
scheduler records every state change before acting on it, runs each task as
soon as its own dependencies finish, retries and times out tasks, runs
schedules with explicit data intervals (catching up after downtime, correct
across DST changes), and commits each task's outputs exactly once.

## Status

> **Redesign in progress.** hubwagen is moving to a durable execution engine
> for data: pipelines as code, memoized steps, versioned outputs with lineage.
> See [docs/architecture.md](docs/architecture.md). Everything below describes
> the current (v1) engine, which keeps working until the new one replaces it.

The redesigned engine runs on one machine today (`hubwagen dev`, `hubwagen
run`): tasks are host processes, state is SQLite, logs and outputs live in a
local directory. Container tasks, remote agents, an API, plain-server and
Kubernetes deployments, and multi-tenancy come next — see
[docs/roadmap.md](docs/roadmap.md) and [docs/architecture.md](docs/architecture.md).

## Build

Requires Go 1.22+ (pure Go, no CGO — cross-compiles cleanly).

```bash
go build -o hubwagen .
GOOS=linux GOARCH=amd64 go build -o hubwagen-linux-amd64 .   # cross-compile
```

## Quickstart

```bash
# Run one pipeline once and wait for it (logs stream to the terminal)
./hubwagen run pipelines/examples/hello_world.yaml

# A pipeline with a daily data interval, for one day
./hubwagen run pipelines/examples/daily_orders.yaml --date 2026-10-20

# The same day again returns the earlier run; --rerun makes a new generation
./hubwagen run pipelines/examples/daily_orders.yaml --date 2026-10-20 --rerun --param full_refresh=false

# Run the scheduler with the dashboard: loads every pipeline under the
# given directories (default: pipelines), reloads them on change, runs schedules
./hubwagen dev pipelines/examples

# Same, without the dashboard: prints events and task logs
./hubwagen dev --headless pipelines/examples
```

Dashboard keys: `↑/↓` select, `enter` drill in, `t` trigger the selected
pipeline, `/` filter, `v` graph view, `g/G` log top/bottom, `b` back, `?`
help, `q` quit.

Everything lives in `--data-dir` (default `.hubwagen/`): `state.db` (event log
and run history), `bucket/` (task logs, outputs, commit markers) and
`dev.log` (engine log while the dashboard is open). One process uses a data
directory at a time. Stopping `hubwagen dev` terminates running tasks; on the
next start, tasks that had already committed their outputs are recognised and
the rest are retried.

## Writing a pipeline

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Pipeline
metadata: {name: daily_orders, tenant: analytics}
spec:
  schedule: {cron: "0 2 * * *", timezone: Europe/Berlin, interval: daily}
  defaults:
    runtime: {kind: agent, pool: local}
    retry: {limit: 2, initial: 30s}
  tasks:
    - id: extract
      command: [python, -m, jobs.extract]
      args: ["--from={{ .Interval.Start }}", "--to={{ .Interval.End }}"]
      outputs: [{name: orders}]
    - id: load
      depends_on: [extract]
      command: [python, -m, jobs.load]
      inputs: [{name: orders, from: extract.orders}]
```

Tasks get their data interval, inputs and output directories as environment
variables (`HUBWAGEN_INPUT_ORDERS_PATH`, `HUBWAGEN_OUTPUT_ORDERS_PATH`, …).
References: [pipeline format](docs/pipeline-format.md) (fields, templates,
execution semantics, schedules) and [task contract](docs/task-contract.md)
(what a task process can rely on).

## Compiling for CI

```bash
./hubwagen compile pipelines/ --check                 # lint: all errors, nothing written
./hubwagen compile pipelines/ --out build/specs       # canonical specs, image tags pinned to digests
```

Each spec is written to `build/specs/<tenant>/<pipeline>/<sha256>.json`; the
file's hash is its version ID.

## Development

```bash
go vet ./...
go test ./...
go test -race ./...
```

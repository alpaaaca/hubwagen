# hubwagen

A workflow orchestrator for data teams. Single Go binary, no database server,
no message broker.

## Status

hubwagen is being redesigned around compiled pipeline specs, an event-driven
control plane, container tasks and multi-tenancy, while still running as one
binary on a laptop or a plain server. See [docs/architecture.md](docs/architecture.md)
for the design and [docs/roadmap.md](docs/roadmap.md) for progress.

- **Works today (v1)**: `hubwagen run` executes a YAML DAG of shell steps with
  parallelism, retries, timeouts, `on_failure` policies and SQLite-backed
  history; `hubwagen tui` is a terminal dashboard over that history.
- **New**: `hubwagen compile` validates and compiles pipelines into versioned
  specs (below). The engine that runs them is the next milestone.

## Install / Build

Requires Go 1.22+ (pure Go, no CGO — cross-compiles cleanly).

```bash
git clone <this-repo>
cd hubwagen
go build -o hubwagen ./main.go
./hubwagen version
```

Cross-compile for Linux from macOS:

```bash
GOOS=linux GOARCH=amd64 go build -o hubwagen-linux-amd64 ./main.go
```

## Quickstart

```bash
# Validate a DAG without running it
./hubwagen validate ./dags/examples/hello_world.yaml

# See the wave-by-wave execution plan without running anything
./hubwagen run --dry-run ./dags/examples/hello_world.yaml

# Run it for real (creates ./hubwagen.db on first run)
./hubwagen run ./dags/examples/hello_world.yaml

# Run the parallel-steps example (two independent steps run concurrently)
./hubwagen run ./dags/examples/parallel_steps.yaml
```

Every run, step, and log line is recorded in SQLite (`./hubwagen.db` by default):

```bash
sqlite3 hubwagen.db "select id, dag_name, status from runs order by created_at desc limit 1;"
sqlite3 hubwagen.db "select step_id, status, exit_code from step_runs where run_id = '<run-id>';"
sqlite3 hubwagen.db "select step_id, line from logs where run_id = '<run-id>';"
```

If the process is killed mid-run, the next `hubwagen run` invocation automatically marks
any stale `running` rows as `failed` (reason: `run interrupted`) before proceeding.

## CLI flags

| Flag | Applies to | Default | Description |
|---|---|---|---|
| `--db` | all commands | `./hubwagen.db` | Path to the SQLite database file |
| `--log-level` | all commands | `info` | `debug`, `info`, `warn`, or `error` — structured logs go to stderr |
| `--dry-run` | `run` | `false` | Parse + validate only; print the execution plan, don't open the DB or execute |

## Compiling pipelines

The redesign replaces "run YAML directly" with compiled, versioned specs
(format reference: [docs/pipeline-format.md](docs/pipeline-format.md)):

```bash
# Validate everything (CI lint mode); v1 DAG files compile via a compat mapping
./hubwagen compile dags/examples --check --no-resolve

# Write canonical specs to build/specs/<tenant>/<pipeline>/<sha256>.json,
# resolving container image tags to digests
./hubwagen compile dags/examples --out build/specs
```

New-format examples live in `dags/examples/v1alpha1/`. `hubwagen run` still
executes v1 files with the old engine until the new control plane lands.

## Writing a DAG

This section describes the v1 format that `hubwagen run` executes. Minimal example:

```yaml
name: my_pipeline
steps:
  - id: extract
    executor: shell
    command: "echo hello"
  - id: transform
    executor: shell
    depends_on: [extract]
    command: "echo world"
```

Template variables available in `command` and `env` values: `{{ .RunDate }}`,
`{{ .RunID }}`, `{{ .DAGName }}`, `{{ .Env.VAR_NAME }}` — rendered at run time, not
parse time.

## Development

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

# hubwagen

A lightweight, self-contained workflow orchestration engine for data engineers.
Single Go binary. No Docker. No external database. No message broker.
Workflows are defined in YAML and run with goroutine-per-step parallelism.

## Status

Phase 1 (core engine) is complete: `hubwagen run` executes a YAML DAG end-to-end with
parallel step execution, retries, timeouts, `on_failure` policies, template
interpolation, and full SQLite-backed run/step/log history with crash recovery.
The scheduler and terminal UI (`hubwagen tui`, Phase 2+) are not yet built.

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

## Writing a DAG

See [`docs/dag-schema.md`](./docs/dag-schema.md) if present. Minimal example:

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

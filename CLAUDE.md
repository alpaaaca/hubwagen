# CLAUDE.md — hubwagen

## What this is

hubwagen is a workflow orchestrator for data teams, in the middle of a redesign:

- **v1 (still the runtime today)**: one Go binary that runs YAML DAGs of shell
  steps with goroutine-per-step parallelism, stores history in SQLite, and has a
  terminal dashboard.
- **Redesign (in progress)**: compiled, versioned pipeline specs; one
  event-driven leader that is the only writer to SQLite (replicated by
  Litestream); container tasks run by warm agents or Kubernetes Jobs under one
  task contract; multi-tenant fair share; idempotent runs with explicit data
  intervals; built-in observability. Three deployment profiles from one binary:
  `dev` (laptop), `standalone` (plain servers, no Kubernetes) and `kubernetes`.

Read before larger changes:

- [docs/architecture.md](docs/architecture.md) — goals, decisions and why
- [docs/roadmap.md](docs/roadmap.md) — what is done, what is next, open decisions
- [docs/pipeline-format.md](docs/pipeline-format.md) — pipeline YAML and the compiled spec
- [docs/task-contract.md](docs/task-contract.md) — what a task process can rely on (design)

Planning is lightweight: a design note in `docs/` when a decision needs one,
the next milestone broken into PR-sized steps in `docs/roadmap.md`. No
spec-kit; `specs/` is legacy and gitignored.

## Current code

| Path | What | Fate |
|---|---|---|
| `internal/spec` | Pipeline compiler: parsing (v1alpha1 + v1 compat), validation, restricted templates, data intervals, canonical JSON + sha256, image digest resolution | **New design, keep** |
| `cmd/compile.go` | `hubwagen compile` | Keep |
| `internal/dag`, `engine`, `executor`, `scheduler`, `log` | v1 engine (wave-based execution, shell executor, `TriggerManual`) | Replaced and deleted in M2; logic is reused (shell exec, log streaming, retry semantics) |
| `internal/state` | New store: event log + projections, term fencing, leader-start loaders (M2 step 1) | **New design, keep** |
| `internal/core` | Scheduler core: event loop, state machines, triggers, retries, timeouts, cancellation, restore (M2 step 2) | **New design, keep** |
| `internal/clock`, `internal/layout` | Real/fake clock; bucket path layout shared by core and task wrapper | **New design, keep** |
| `internal/blob` | Bucket access by URI; `file://` with atomic create-if-absent (M2 step 3) | **New design, keep** |
| `internal/taskexec` | The task contract: env, process groups, log shipping, result file, commit markers, restart recovery (M2 step 3) | **New design, keep** |
| `internal/store` | v1 SQLite store (update-in-place tables) | Deleted in M2 step 5 |
| `internal/tui` | Terminal dashboard (bubbletea) | Kept; data source moves to the new store (M2), then the API (M3) |
| `cmd/run.go`, `cmd/validate.go` | v1 commands | Deleted in M2 |
| `dags/examples/*.yaml` | v1 examples (compile via compat) | Keep while compat exists |
| `dags/examples/v1alpha1/` | New-format examples | Keep; `TestExamplesCompile` compiles every example |

## Rules that hold everywhere

- **The control plane never runs user code.** It reads compiled specs and only
  renders allow-listed template variables into strings.
- **Write before act.** Record a state change durably before acting on it
  (dispatching, creating a Job, cancelling).
- **Single writer.** Only the leader's core goroutine writes scheduler state.
  Inside `internal/core`, mutate state only from the loop (inputs, timers via
  `c.after`, reports via `Report`); side effects go through `c.effect` so they
  run after the batch commits.
- **Keep the core portable.** No Postgres-only, Kubernetes-only or
  cloud-specific calls in the core; they go behind the interfaces listed in
  `docs/architecture.md`. `internal/profile` (to come) is the only place that
  knows the active profile.
- **Spec JSON field names are part of the version hash.** Renaming or adding a
  non-omitempty field in `internal/spec/spec.go` changes every version ID —
  treat it as a format change.
- **Pure Go, no CGO.** The binary must stay static and cross-compile
  (`modernc.org/sqlite`, not `mattn/go-sqlite3`).

## Conventions

- Raw `database/sql`, no ORM; migrations are embedded SQL files.
- IDs are ULIDs (`github.com/oklog/ulid/v2`).
- Errors for users name the location (`file: spec.tasks[1].depends_on[0]: …`)
  and report all problems at once where possible.
- Structured logs via `log/slog` to stderr; command output to stdout.
- Tests: `testify` (`assert`/`require`); no mocking frameworks — use real
  implementations (`:memory:` SQLite, temp dirs, `file://` blob, real
  subprocesses, fake clock). Unit tests next to the code; cross-package
  integration tests in `tests/`. Pure logic (`internal/spec`, the coming
  `internal/core`) gets table tests with no I/O.
- Keep docs in step with code: update `docs/roadmap.md` status and any
  affected doc in the same PR.

## Commands

```bash
go build -o hubwagen .
go vet ./...
go test ./...
go test -race ./...

./hubwagen compile dags/examples --check --no-resolve   # validate all examples, offline
./hubwagen compile dags/examples --out build/specs      # write canonical specs
./hubwagen run dags/examples/hello_world.yaml           # v1 engine (until M2)
./hubwagen tui                                          # v1 dashboard
```

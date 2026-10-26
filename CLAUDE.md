# CLAUDE.md — hubwagen

## What this is

hubwagen is becoming a durable execution engine for data (v2): pipelines as
plain code (hauls) that survive crashes and long waits, side-effecting steps
(lifts) memoized by fingerprint, and every output an immutable, versioned
pallet in a catalog derived from the event history. One Go server that never
runs user code, single leader on SQLite (Litestream later); workers in the
user's environment, Python SDK first.

**Today:** `hubwagen dev` / `hubwagen run` still run the v1 engine (M1–M2):
compiled YAML pipelines, host-process tasks, `file://` bucket; it keeps
working until M3c. The v2 haul engine (M3a) is in `internal/core`, driven by
Go tests only. **Next:** pallets and memoization (M3b), then the worker API
and Python SDK (M3c).

Read before larger changes:

- [docs/architecture.md](docs/architecture.md) — v2 design: hauls, lifts, pallets, memoization, commits
- [docs/roadmap.md](docs/roadmap.md) — what is done, what is next, open decisions
- [docs/v2-migration.md](docs/v2-migration.md) — what v2 keeps, rewrites and removes from v1, per package
- [docs/pipeline-format.md](docs/pipeline-format.md) — v1 pipeline YAML and the compiled spec (removed in M3c)
- [docs/task-contract.md](docs/task-contract.md) — v1 task contract (parked until pod lifts)
- [docs/config.md](docs/config.md) — instance configuration (design, to be revised for v2)

Planning is lightweight: a design note in `docs/` when a decision needs one,
the next milestone broken into PR-sized steps in `docs/roadmap.md`. No
spec-kit.

## Current code (v1; fate of each package in docs/v2-migration.md)

| Path | What |
|---|---|
| `internal/spec` | Pipeline compiler: parsing, validation, restricted templates, data intervals/calendars, canonical JSON + sha256, image digest resolution |
| `internal/state` | Store: event log + projections, term fencing, leader-start loaders |
| `internal/core` | Scheduler core: event loop, state machines, triggers, retries, timeouts, cancellation, cron, restore. v2: docks (`docks.go`) and the haul engine (`hauls.go`) |
| `internal/clock`, `internal/layout` | Real/fake clock; bucket path layout shared by core and task wrapper |
| `internal/blob` | Bucket access by URI; `file://` with atomic create-if-absent |
| `internal/taskexec` | The task contract: env, process groups, log shipping, result file, commit markers, restart recovery |
| `internal/runtime/localrt` | Dev-mode in-process agent: slots, pools, cancellation |
| `internal/devmode` | Wires store, bucket, core and local runtime for one process; data-dir lock; pipeline loader/watcher |
| `internal/tui` | Terminal dashboard; reads and acts through `tui.Source` (`EngineSource` today, the API client in M3) |
| `cmd/` | `compile`, `dev`, `run`, `version` |
| `pipelines/examples/` | Example pipelines; `TestExamplesCompile` compiles every example |
| `tests/` | Cross-package end-to-end tests of the engine |

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
- **Spec JSON field names are part of the version hash** (v1, until
  `internal/spec` is removed). Renaming or adding a non-omitempty field in
  `internal/spec/spec.go` changes every version ID — treat it as a format
  change.
- **Never reuse what can't be read.** A lift is reused only when a committed,
  readable, non-invalid pallet matches its fingerprint; when unsure, rerun.
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
  integration tests in `tests/`. Pure logic (`internal/spec`, `internal/core`
  with a fake clock) gets tests with no real processes or timers.
- Keep docs in step with code: update `docs/roadmap.md` status and any
  affected doc in the same PR.

## Commands

```bash
go build -o hubwagen .
go vet ./...
go test ./...
go test -race ./...

./hubwagen compile pipelines/examples --check --no-resolve   # validate all examples, offline
./hubwagen compile pipelines/examples --out build/specs      # write canonical specs
./hubwagen run pipelines/examples/hello_world.yaml  # run one pipeline once (data in .hubwagen/)
./hubwagen dev pipelines/examples                            # scheduler + dashboard; --headless for plain output
```

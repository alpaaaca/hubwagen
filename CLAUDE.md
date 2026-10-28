# CLAUDE.md — hubwagen

## What this is

hubwagen is a durable execution engine for data: pipelines as plain code
(hauls) that survive crashes and long waits, side-effecting steps (lifts)
memoized by fingerprint, and every output an immutable, versioned pallet in a
catalog derived from the event history. One Go server that never runs user
code, single leader on SQLite (Litestream later); workers in the user's
environment, Python SDK first.

**Today (M3c done):** `hubwagen dev` serves the API; Python workers
(`sdk/python`) run hauls and lifts, with code versions from what a lift's
code reaches and runtime guards in haul code; `hubwagen start`/`replay`
start them and report reuse. The v1 engine (compiled YAML pipelines) was
removed in M3c; it is in git history up to commit 33ca1ba. **Next:** M4
(fan-out limits, signals, schedules, testing); the reuse-by-default
decision in docs/api-simplification.md.

Read before larger changes:

- [docs/architecture.md](docs/architecture.md) — v2 design: hauls, lifts, pallets, memoization, commits
- [docs/roadmap.md](docs/roadmap.md) — what is done, what is next, open decisions
- [docs/v2-migration.md](docs/v2-migration.md) — what happened to each v1 package
- [docs/failover.md](docs/failover.md) — design: nodes, leader election and state handoff (not built)
- [docs/api-simplification.md](docs/api-simplification.md) — design: lifts return values and dataframes, reuse by default (steps 1–2 built)
- [docs/spikes/](docs/spikes/) — spike findings (S1: Python replay, S2: code versions)
- [sdk/python/README.md](sdk/python/README.md) — writing hauls and lifts, the worker
- [docs/config.md](docs/config.md) — instance configuration (`hubwagen.yaml`)

Planning is lightweight: a design note in `docs/` when a decision needs one,
the next milestone broken into PR-sized steps in `docs/roadmap.md`. No
spec-kit.

## Current code

| Path | What |
|---|---|
| `internal/core` | The core: one event loop; hauls (replay until blocked, sequence check, stuck), lifts (retries, timeouts, leases), timers, docks (long-poll), commits, memoization, single-flight, restore |
| `internal/state` | Store: event log + projections (hauls, lifts, timers, catalog, builds), term fencing, leader-start loader |
| `internal/pallet` | Storage adapter for pallets as files: manifests, content hash, idempotent promote, discard |
| `internal/api` | Connect services over HTTP (`WorkerService`, `HaulService`); generated code in `gen/` from `proto/` |
| `internal/devmode` | Wires store, bucket, core and API for one process; data-dir lock; event descriptions for `hubwagen dev` |
| `internal/haulcli` | What `start` and `replay` do: `--param` ranges, starting and waiting over `HaulService`, the replay report |
| `internal/schedule` | Calendar and cron math (DST-correct intervals, what is due since a cursor); the core's haul schedules use it |
| `internal/clock`, `internal/layout`, `internal/blob` | Real/fake clock; bucket paths (lift staging, pallet manifests); bucket access with atomic create-if-absent |
| `internal/config` | `hubwagen.yaml`: data dir, listen address, worker processes |
| `internal/supervise` | Runs and restarts worker processes for `dev` and `workers` (prefixed output, SIGTERM then kill) |
| `cmd/` | `dev`, `workers`, `start`, `replay`, `signal`, `schedules`, `version` |
| `sdk/python/` | Python SDK (stdlib only, Python ≥ 3.12): `@haul`, `@lift`, replay engine, worker; pytest runs it against a real `hubwagen dev` |
| `pipelines/examples/revenue/` | Example Python project; `sdk/python/tests/test_example.py` is its demo |
| `tests/` | Cross-package end-to-end tests (leader restarts) |

## Rules that hold everywhere

- **The control plane never runs user code.** Workers run hauls and lifts;
  the server only records, schedules and answers.
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
- **Fingerprint inputs are a format.** Changing what goes into a lift's
  fingerprint (`fingerprintInputs` in `internal/core/memo.go`) invalidates
  every memoized pallet — treat it as a format change.
- **Never reuse what can't be read.** A lift is reused only when a committed,
  readable, non-invalid pallet matches its fingerprint; when unsure, rerun.
- **Pure Go, no CGO.** The binary must stay static and cross-compile
  (`modernc.org/sqlite`, not `mattn/go-sqlite3`).

## Conventions

- Raw `database/sql`, no ORM; migrations are embedded SQL files.
- IDs are ULIDs (`github.com/oklog/ulid/v2`).
- Errors for users name the location (`commands[2]: …`, `app/hauls.py:42`)
  and report all problems at once where possible.
- Structured logs via `log/slog` to stderr; command output to stdout.
- Tests: `testify` (`assert`/`require`); no mocking frameworks — use real
  implementations (`:memory:` SQLite, temp dirs, `file://` blob, real
  subprocesses, fake clock). Unit tests next to the code; cross-package
  integration tests in `tests/`. Pure logic (`internal/schedule`, `internal/core`
  with a fake clock) gets tests with no real processes or timers.
- Keep docs in step with code: update `docs/roadmap.md` status and any
  affected doc in the same PR.

## Commands

```bash
go build -o hubwagen .
go vet ./...
go test ./...
go test -race ./...
go generate ./internal/api    # after changing proto/: buf lint + generate (via go run, nothing to install)
(cd sdk/python && uv run --no-project --with pytest --with polars --with pandas --with pyarrow --python 3.12 pytest -q)   # Python SDK, incl. against hubwagen dev and dataframes

./hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml   # server + 3 workers; API on 127.0.0.1:7070
./hubwagen replay daily --param day=2026-10-01..2026-10-03   # start hauls, report what was reused
```

# v1 → v2

What changes when hubwagen moves from the compiled-YAML orchestrator (v1,
M1–M2) to the durable execution engine described in
[architecture.md](architecture.md), and what carries over. The milestones
that do the work are in [roadmap.md](roadmap.md).

## Why

v1 is a solid declarative orchestrator, but that space is crowded (Airflow,
Dagster, Argo). v2 targets a gap: durable execution with data versions,
lineage and memoization built in. The invariants stay the same — no user
code in the control plane, write before act, one writer, a portable core,
one static binary — so most of the engine is reused, not rewritten.

## Concepts

| v1 | v2 | Note |
|---|---|---|
| Pipeline (YAML, compiled spec) | Haul (code, registered by workers) | No compiled manifest; a deploy is a new worker build |
| Task | Lift | Lifts are called from haul code, not declared in a graph |
| Run, run key, generation | Haul execution, haul parameters | Idempotency moves from run keys to fingerprints |
| Output, commit marker | Pallet, pallet manifest | Pallets are versioned and recorded in the catalog |
| Asset | Dataset | A stream of pallets |
| Pool, agent | Dock, worker | Workers run in the user's environment and pull work |
| Data interval | Haul parameter (e.g. `day`) | Cron passes the interval as parameters |
| Spec version ID | Build id + per-lift code version | |

## Package by package

| Package | Fate | How |
|---|---|---|
| `internal/state` | **Kept, extended** | Event log, projections, term fencing and leader-start loaders stay. Logbook and catalog tables are added (M3a, M3b); v1 tables are dropped in M3c |
| `internal/core` | **Kept, extended** | The single-goroutine loop, group commit, `c.after`/`c.effect`, retries, timeouts, cancellation and restore carry over. Run/task state machines become haul/lift state machines; single-flight and memoization lookups are added |
| `internal/core/cron.go` | **Kept** | Cursors, catch-up and DST-safe intervals become haul schedules |
| `internal/clock` | **Kept** | |
| `internal/blob` | **Kept** | `PutIfAbsent` is the basis of pallet manifest commits |
| `internal/layout` | **Rewritten** | Paths keyed by fingerprint and attempt instead of run key and generation |
| `internal/taskexec` | **Parked** | Commit-marker and recovery logic informs M3b. The process wrapper and contract return for pod lifts (later) |
| `internal/runtime/localrt` | **Parked** | Returns as the local pod-lift launcher |
| `internal/devmode` | **Rewritten** | Same role (one process, data-dir lock), but it serves the worker API instead of loading YAML |
| `internal/tui` | **Kept, extended** | Stays on `tui.Source`; views change to hauls, logbooks and the catalog (M5) |
| `internal/spec` | **Removed** (M3c) | The compiler, YAML format and spec hash go away. The template allow-list may be reused for SQL lifts, image digest resolution for code versions of pod lifts |
| `cmd/compile` | **Removed** (M3c) | Replaced by `hubwagen lint` and worker registration |
| `cmd/dev`, `cmd/run` | **Rewritten** | `dev` serves the API; `run` is replaced by `start`/`replay` |
| `pipelines/examples/` | **Replaced** (M3c) | Python example projects |
| `tests/` | **Kept** | Engine end-to-end tests are rewritten for hauls |

## Docs

| Doc | Fate |
|---|---|
| [architecture.md](architecture.md) | Rewritten for v2 |
| [roadmap.md](roadmap.md) | Rewritten for v2 |
| [pipeline-format.md](pipeline-format.md) | Describes v1; removed with `internal/spec` in M3c. Replaced by an SDK reference |
| [task-contract.md](task-contract.md) | Describes v1; parked until pod lifts |
| [config.md](config.md) | Design for v1's M3; to be revised for the v2 server and worker settings before `internal/config` is built |

## Until M3c

v1 keeps working: `hubwagen compile`, `dev` and `run` behave as today while
M3a and M3b add v2 tables and state machines next to the v1 ones. M3c removes
v1 in one step, once the Python example runs end to end.

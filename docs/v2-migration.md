# v1 → v2

What changes when moksha moves from the compiled-YAML orchestrator (v1,
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
| Pipeline (YAML, compiled spec) | Run (code, registered by workers) | No compiled manifest; a deploy is a new worker build |
| Task | Step | Steps are called from pipeline code, not declared in a graph |
| Run, run key, generation | Run execution, run parameters | Idempotency moves from run keys to fingerprints |
| Output, commit marker | Data version, data version manifest | Data versions are versioned and recorded in the catalog |
| Asset | Dataset | A stream of data versions |
| Pool, agent | Queue, worker | Workers run in the user's environment and pull work |
| Data interval | Run parameter (e.g. `day`) | Cron passes the interval as parameters |
| Spec version ID | Build id + per-step code version | |

## Package by package

| Package | Fate | How |
|---|---|---|
| `internal/state` | **Kept, extended** | Event log, projections, term fencing and leader-start loaders stay. Event log and catalog tables were added (M3a, M3b); v1 tables were dropped in M3c (migration 007) |
| `internal/core` | **Kept, extended** | The single-goroutine loop, group commit, `c.after`/`c.effect`, retries, timeouts, cancellation and restore carry over. Run/task state machines become run/step state machines; single-flight and memoization lookups are added |
| `internal/core/cron.go` | **Extracted** (M3c) | The calendar math and the cursor loop moved to `internal/schedule` (`Calendar`, `Due`), free of pipelines; M4 wires it to runs |
| `internal/clock` | **Kept** | |
| `internal/blob` | **Kept** | `PutIfAbsent` is the basis of data version manifest commits |
| `internal/layout` | **Rewritten** | Paths keyed by fingerprint and attempt instead of run key and generation |
| `internal/taskexec` | **Removed** (M3c) | Its commit-marker ideas became `internal/dataversion`. The process wrapper and task contract come back from git history for pod steps (later) |
| `internal/runtime/localrt` | **Removed** (M3c) | Comes back from git history as the local pod-step launcher |
| `internal/devmode` | **Rewritten** (M3c) | Same role (one process, data-dir lock); it serves the API instead of loading YAML. Since M5 the wiring is `internal/server` (shared with `moksha server`) and devmode only prints |
| `internal/tui` | **Removed** (M3c) | Its views were all v1. M5 builds a web UI in TypeScript (`ui/`, served by `moksha dev`) for runs, event logs and the catalog on the API; `moksha dev` prints events meanwhile |
| `internal/spec` | **Removed** (M3c) | The compiler, YAML format and spec hash go away. The template allow-list may be reused for SQL steps, image digest resolution for code versions of pod steps |
| `cmd/compile` | **Removed** (M3c) | Replaced by the SDK's lint (`python -m moksha.lint`) and worker registration |
| `cmd/dev`, `cmd/run` | **Rewritten / removed** (M3c) | `dev` serves the API; `run` is replaced by `start`/`replay` |
| `pipelines/examples/` | **Replaced** (M3c), **moved** | `revenue/`, a Python project; now in [moksha-demo](https://github.com/alpaaaca/moksha-demo), and the tests' copy in `sdk/python/tests/projects/revenue` |
| `tests/` | **Kept** | The v1 engine test is gone; the leader-restart test covers runs |

## Docs

| Doc | Fate |
|---|---|
| [architecture.md](architecture.md) | Rewritten for v2 |
| [roadmap.md](roadmap.md) | Rewritten for v2 |
| pipeline-format.md | Removed in M3c; the SDK README (`sdk/python/README.md`) replaces it |
| task-contract.md | Removed in M3c; returns from git history with pod steps |
| [config.md](config.md) | Design for v1's M3; to be revised for the v2 server and worker settings before `internal/config` is built |

## Done

v1 was removed in M3c (#28). The last commit with it is 33ca1ba: `git checkout 33ca1ba`
to see or run it. A v1 data directory upgrades in place: the v1 tables are
dropped, v1 events stay in the log as history.

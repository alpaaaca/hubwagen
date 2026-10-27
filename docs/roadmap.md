# Roadmap

What is built, what is next, and what is still undecided. Design rationale is
in [architecture.md](architecture.md); what v2 keeps and drops from the v1
engine is in [v2-migration.md](v2-migration.md). Keep this file current:
update a milestone's status in the same PR that changes it. Each
milestone's PR-sized steps are GitHub issues under a milestone of the same
name; the links below point to them.

## Direction

v1 (M1–M2) built a compiled-YAML orchestrator with container tasks. v2 turns
hubwagen into a durable execution engine for data: hauls as code, lifts
memoized by fingerprint, pallets in a catalog derived from logbooks.

The order is chosen to prove the differentiator early. Durable hauls alone
are what Temporal already offers; the reason to use hubwagen is a rerun or
backfill that skips unchanged work. So the first goal is the smallest system
that shows that on one machine: the Go core first (tested with a fake
worker), then pallets and memoization, then a minimal Python SDK. Everything
that only matters at scale waits.

## Status

| Milestone | Status |
|---|---|
| M1: Spec compiler (v1) | **Done** |
| M2: v1 engine in dev mode | **Done** |
| M3a: Haul engine in the core | **Done** |
| M3b: Pallets, catalog, memoization | **Done** |
| M3c: Worker API + minimal Python SDK | **In progress** |
| M4: Full haul surface (fan-out, signals, triggers, testing) | — |
| M5: Catalog in the UI, invalidation, staleness | — |
| Later | — |

M3a → M3b → M3c are sequential; the end of M3c is the first demo. M4 and M5
can run in parallel.

## M1–M2: v1 engine (done)

Compiler (`internal/spec`), event store with term fencing (`internal/state`),
single-goroutine core with retries, timeouts, cancellation and DST-safe cron
(`internal/core`), `file://` bucket with atomic create-if-absent
(`internal/blob`), the task contract with commit markers and restart recovery
(`internal/taskexec`), in-process runtime (`internal/runtime/localrt`),
`hubwagen dev`/`run` with the dashboard. The per-step history is in git. [v2-migration.md](v2-migration.md) says which parts v2 reused; the rest was
removed in M3c and is in git history (commit 33ca1ba).

## M3a: Haul engine in the core

Goal: a haul with several lifts, a timer and a retry survives a leader
restart, driven entirely from Go tests with the fake clock and a fake worker.
No Python yet.

1. ✅ **Logbook events and projections** (`internal/state`): haul, haul task,
   lift, lift attempt and timer tables; events carry a haul ID, so a haul's
   logbook is its events in order. Events `HaulStarted`,
   `HaulTaskQueued`/`Dispatched`/`Completed` (commands)/`Failed`,
   `HaulStuck`, `HaulFinished`, `LiftScheduled`,
   `LiftAttemptDispatched`/`Started`/`Finished`, `LiftRetryScheduled`,
   `LiftFinished` (result), `TimerStarted`/`Fired`/`Cancelled`; payloads are
   defined by the core. Migrations next to the v1 tables, which are dropped
   in M3c. ([#12](https://github.com/alpaaaca/hubwagen/issues/12))
2. ✅ **Haul state machine** (`internal/core`): replay-until-blocked protocol on
   the server side — schedule a haul task when a result arrives, apply the
   returned commands, dispatch lifts, arm timers. Retries and timeouts for
   lifts reuse the v1 logic. Restore on leader start. ([#14](https://github.com/alpaaaca/hubwagen/issues/14), [#15](https://github.com/alpaaaca/hubwagen/issues/15), [#17](https://github.com/alpaaaca/hubwagen/issues/17))
3. ✅ **Docks**: in-memory queues per dock, long-poll semantics behind an
   interface the API will expose; the fake worker in tests consumes it. ([#13](https://github.com/alpaaaca/hubwagen/issues/13))
4. ✅ **Sequence check on the server**: a haul task's commands must extend the
   logbook, never contradict it; a contradiction marks the haul `stuck`
   until `ResumeHaul` and a consistent replay. ([#16](https://github.com/alpaaaca/hubwagen/issues/16))

Done when: an end-to-end test in `tests/` runs a three-lift haul with a sleep,
kills and restarts the leader mid-haul, and the haul completes with each lift
run once.

## M3b: Pallets, catalog, memoization

Goal: running the same haul twice reuses every lift, in a Go test.

1. ✅ **Pallets and datasets**: catalog tables projected from `LiftFinished`
   and commit events; pallet states `pending` (recorded, not yet promoted),
   `committed`, `expired`, `invalid`; latest pallet per partition; upstream
   and downstream lineage. ([#18](https://github.com/alpaaaca/hubwagen/issues/18))
2. ✅ **Commit protocol** on `file://` (`internal/pallet`): staging per
   attempt, pallet manifests promoted with `PutIfAbsent` off the core loop,
   promote redone on restart from the manifest in the logbook, staging of
   failed attempts discarded. ([#19](https://github.com/alpaaaca/hubwagen/issues/19))
3. ✅ **Fingerprints**: computed by the server from what the worker registers
   (`RegisterBuild`: code version per lift) and what the command carries
   (inputs, canonical args, output); content hash for `file://` inputs;
   opt-in memoization for lifts with an output (`memo: fingerprint` or
   `ttl:…`); `LiftReused` events with the matched pallet; rerun reasons
   (`not memoized`, `no previous run`, `code changed (a → b)`,
   `input changed`, `params changed`, `expired`, `unknown code version`). ([#20](https://github.com/alpaaaca/hubwagen/issues/20))
4. ✅ **Single-flight**: a memoized lift whose fingerprint is already running
   waits for it (`LiftWaiting`) and reuses its pallet; if the run fails, the
   first waiter runs itself. Flights are in memory and rebuilt on restore
   (committed fingerprints are the catalog's own index). ([#21](https://github.com/alpaaaca/hubwagen/issues/21))

Done when: a test runs a haul of memoized lifts, reruns it with one lift's
code version changed, and only that lift and its downstream run (a lift
without `memo=True` runs both times); two concurrent hauls with the same
fingerprint run the lift once.

## M3c: Worker API + minimal Python SDK

Goal: the first demo — `hubwagen replay` on a real Python example shows
reuse.

1. ✅ **Worker API** (Connect, HTTP/JSON-callable): `RegisterBuild`,
   `PollHaulTask`, `CompleteHaulTask`, `PollLiftTask`, `CompleteLiftTask`,
   `Heartbeat` (leases; tells workers what to stop). `hubwagen dev` serves it
   (`--listen`, default 127.0.0.1:7070), with `HaulService` (`StartHaul`,
   `GetHaul`) next to it; `hubwagen start` and `hubwagen replay` call it. ([#22](https://github.com/alpaaaca/hubwagen/issues/22), [#23](https://github.com/alpaaaca/hubwagen/issues/23))
2. **Python SDK** (`sdk/python`): ✅ `@haul`, `@lift`, `ctx.lift`,
   `lift_async`/`gather`, `ctx.sleep`, `ctx.state`, `ctx.latest_pallet`
   (answered and recorded by the server), `ctx.now`, `ctx.random`; replay
   until blocked; client-side sequence check with file and line; the worker
   (`python -m hubwagen.worker`, fixed hash seed, heartbeats); writing a
   lift's output files, and reading input pallets through their manifests
   (`Pallet.files()`, `path()`). Still to do: runtime guards and code
   version from imports + lockfile (#25). ([#24](https://github.com/alpaaaca/hubwagen/issues/24), [#25](https://github.com/alpaaaca/hubwagen/issues/25), [#26](https://github.com/alpaaaca/hubwagen/issues/26))
   ✅ `hubwagen.yaml` (server settings and worker processes; `hubwagen dev`
   starts and restarts them, `hubwagen workers` runs them alone), see
   [config.md](config.md).
   ✅ Routing by build: builds register their hauls; workers get only the
   hauls and lifts their build defines; `StartHaul` refuses unknown hauls.
   The worker reads its modules from `[tool.hubwagen]` in `pyproject.toml`.
3. ✅ **Example** in `pipelines/examples/revenue/`: a small Python project with
   an extract → transform → report haul that writes CSV, and the demo test
   (`sdk/python/tests/test_example.py`): `hubwagen dev` + a worker, replays
   over a date range, a code change and a data change, each reporting what
   was reused and what ran and why. ([#27](https://github.com/alpaaaca/hubwagen/issues/27))
4. ✅ **Remove v1**: YAML compiler, pipeline loader, v1 state machines and
   tables, the task wrapper and local runtime, the dashboard (back in M5),
   `compile` and `run`. The calendar math moved to `internal/schedule` for
   M4 (see [v2-migration.md](v2-migration.md)). ([#28](https://github.com/alpaaaca/hubwagen/issues/28))

Done when: `hubwagen dev` + a local worker run the example, and a replay over
a date range reports reused and rerun lifts with reasons.

## M4: Full haul surface

`lift_async` and `gather` with `max_parallel`; signals (`wait_signal`,
`hubwagen signal`); cron schedules on hauls with catch-up policy (on
`internal/schedule`, extracted from v1's cron); dataset subscriptions with debounce; child hauls; `continue_as_new`;
version markers; `hubwagen lint`; `hubwagen test` with a fake ctx and
`--replay <logbook>`.

## M5: Catalog in the UI, invalidation, staleness

The dashboard on top of the API (through `tui.Source`): hauls, logbooks,
datasets, pallets and lineage. `hubwagen pallet invalidate` with transitive
marking; staleness per dataset and partition; retention and expiry of
pallets as events.

## Later

Roughly in order of expected need:

- OpenLineage export.
- SQL lifts (`lift.sql`) and a dbt lift.
- Object-store buckets (`gs://`, `s3://`) and warehouse/table-format adapters.
- Pod lifts: local containers in single-node mode, Kubernetes Jobs in a
  cluster (starting from the v1 task contract).
- Auth, tenants and per-dock budgets; Litestream replication and failover.
- `hubwagen deploy --dry-run` (rebuild preview), `hubwagen verify`,
  `keep_fresh`, data modes, build pinning.
- Go SDK.
- Sharding, Postgres, Kubernetes Lease election.

## Spikes

| | Question | Blocks | Result |
|---|---|---|---|
| [S1](https://github.com/alpaaaca/hubwagen/issues/10) | Python replay prototype: how slow is full replay for a 1,000-event logbook? Does patching clock/random in a running interpreter catch typical mistakes without breaking libraries imported by haul modules? | M3c | **Done**: full replay is ~1 ms for 1,000 events (O(n²) over a haul's life); guard with `sys.monitoring`, not patching (13/13 caught, 0 libraries broken); call sites via `sys._getframe` are free. [Findings](spikes/s1-python-replay.md) |
| [S2](https://github.com/alpaaaca/hubwagen/issues/11) | Code version from imports: can the SDK resolve a lift's project-local imports reliably (relative imports, `importlib`, namespace packages), and how often does the lockfile term cause reruns in a real project? | M3c | |
| [S3](https://github.com/alpaaaca/hubwagen/issues/29) | Load: one leader with group commit at the target load (first v1 signal: ~60k events/s in batches of 128 on an M1 Mac). Re-measure with logbook events. | Load target sign-off | |
| [S4](https://github.com/alpaaaca/hubwagen/issues/30) | Litestream as a library with a replica path per leader term (from v1's R1). | Failover | |

## Open decisions

| Decision | Proposed default | Needed by |
|---|---|---|
| CLI name | Keep `hubwagen`; maybe add `hw` as a short alias | M3c |
| Python SDK packaging | `sdk/python` in this repo, published as `hubwagen` on PyPI | M3c |
| Memoization default | Opt-in per lift (`memo=True`); SQL lifts on by default | M3b |
| Code version default | Source + imported project modules + lockfile, overridable with `version=` | M3b |
| dbt integration | One lift per project, one pallet per model | Later |
| Catalog interoperability | OpenLineage export first; Iceberg REST catalog later, if at all | Later |
| `standalone` profile | Single-node mode covers it; revisit with auth and multi-host | Later |
| Load target for one leader | 100 lift transitions/s sustained, 2,000 running lifts | S3 |
| Licence and distribution | Open | Before first release |

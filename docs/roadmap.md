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
| M3c: Worker API + minimal Python SDK | **Done** |
| M4: Full haul surface (fan-out, signals, triggers, testing) | **Done** |
| M5: Catalog in the UI, invalidation, staleness | **In progress** |
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
   (`Pallet.files()`, `path()`). ✅ Code versions from the code a lift
   reaches and the packages it uses, no lockfile term (spike S2); haul code
   versions in the build ID; runtime guards in haul code with
   `sys.monitoring`, which make a haul stuck (#25). ([#24](https://github.com/alpaaaca/hubwagen/issues/24), [#25](https://github.com/alpaaaca/hubwagen/issues/25), [#26](https://github.com/alpaaaca/hubwagen/issues/26))
   ✅ `hubwagen.yaml` (server settings and worker processes; `hubwagen dev`
   starts and restarts them, `hubwagen workers` runs them alone), see
   [config.md](config.md).
   ✅ Routing by build: builds register their hauls; workers get only the
   hauls and lifts their build defines; `StartHaul` refuses unknown hauls.
   The worker reads its modules from `[tool.hubwagen]` in `pyproject.toml`.
   ✅ Memoized results: lifts without an output can be memoized (their
   result is reused); a pallet's content hash covers the lift's result.
   ✅ Dataframes: lifts return pandas/Polars frames and Arrow tables (written
   as Parquet into a pallet, in the lift's implicit dataset `lift:<name>`
   when it has no output) and take them as arguments, loaded by annotation.
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

Goal: the hauls in [architecture.md](architecture.md) (a scheduled daily
batch with an approval gate, incremental ingestion, dynamic fan-out) run as
written. Steps, in order; each is one PR:

1. ✅ **Signals**: `ctx.wait_signal(name, timeout=)` returns the payload or
   raises `SignalTimeout`; `SignalHaul` on `HaulService` and
   `hubwagen signal <haul> <name> [--data JSON]`. Signals sent before the
   haul waits are kept, and each is delivered to one wait, oldest first. ([#56](https://github.com/alpaaaca/hubwagen/issues/56))
2. ✅ **Version markers**: `ctx.version(name, max=)`, recorded the first time,
   so old logbooks replay the old branch and new hauls take the new one. ([#57](https://github.com/alpaaaca/hubwagen/issues/57))
3. ✅ **Fan-out limits**: `gather(..., max_parallel=)` and a per-haul limit on
   running lifts, held by the server. ([#58](https://github.com/alpaaaca/hubwagen/issues/58))
4. ✅ **Child hauls and `continue_as_new`**: `ctx.haul(...)` (start and wait
   for the result), `ctx.start_haul(...)` (fire and forget), and
   `ctx.continue_as_new(**params)` for hauls that loop. ([#59](https://github.com/alpaaaca/hubwagen/issues/59))
5. ✅ **Schedules**: `@hw.haul(schedule="0 3 * * *", catch_up="latest")` on
   `internal/schedule`; builds register schedules, the leader keeps a
   cursor per schedule and starts what is due (`skip`, `latest`, `all`). ([#60](https://github.com/alpaaaca/hubwagen/issues/60))
6. ✅ **Dataset subscriptions**: start a haul when a pallet is committed to a
   dataset or partition, with debounce and batching. ([#61](https://github.com/alpaaaca/hubwagen/issues/61))
7. ✅ **`hubwagen lint`**: AST checks for clock, randomness, set iteration,
   I/O and environment variables, `except:`/`except BaseException`, and lifts
   called directly in haul code. It is `python -m hubwagen.lint`, in the
   SDK, since it reads Python. Checking SQL table references that bypass
   `input()` waits for SQL lifts. ([#63](https://github.com/alpaaaca/hubwagen/issues/63))
8. ✅ **`hubwagen test`**: `hubwagen.testing.run(haul, params, lifts=...,
   signals=..., children=...)` plays the server in memory against scripted
   results; `python -m hubwagen.testing --replay ID` or `--replay-recent N`
   replays recorded logbooks (`GetLogbook`, `ListHauls`) against the local
   code. Both in the SDK, since they run Python. ([#64](https://github.com/alpaaaca/hubwagen/issues/64))

Done when: the architecture's daily-revenue haul (schedule, quality gate
with an approval signal, fan-out with a limit) runs end to end on
`hubwagen dev`, and `hubwagen test --replay` catches a non-deterministic
change to it. ✅ `sdk/python/tests/test_m4.py` (the schedule's firing is
tested with a fake clock in the core) and `test_testing.py`.

## M5: Catalog in the UI, invalidation, staleness

Goal: the catalog is something people use, not just a table: they can see
what exists and what it was built from, mark bad data, see what is out of
date and why, and old versions go away on their own. Steps, in order;
each is one PR:

1. ✅ **Catalog API and CLI**: a `CatalogService` (datasets, a dataset's
   partitions with their latest pallets, a pallet with its manifest, its
   inputs and what was built from it) and `hubwagen datasets`,
   `hubwagen pallets <dataset>`, `hubwagen pallet <id>` (lineage up and
   down). Everything after this reads through it. ([#73](https://github.com/alpaaaca/hubwagen/issues/73))
2. ✅ **Invalidation**: `hubwagen pallet invalidate <id> --reason …` marks the
   pallet and everything built from it, transitively, invalid
   (`PalletInvalidated` events with the cause), after showing what it will
   mark (`--dry-run`). Invalid pallets are never reused, so a replay of the
   affected hauls, which the command prints, recomputes exactly that set. ([#74](https://github.com/alpaaaca/hubwagen/issues/74))
3. ✅ **Staleness**: a partition's latest pallet is stale when one of its
   inputs' datasets has a newer pallet for that partition, or when the
   newest build's code version of the lift that built it differs. The
   catalog API answers it per dataset and partition with the reason, and
   `hubwagen stale [dataset]` lists them with the replay that refreshes
   them. ([#75](https://github.com/alpaaaca/hubwagen/issues/75))
4. **Bucket layout by dataset**: a lift with an output stages under
   `data/<tenant>/<dataset>/<partition>/<attempt>/` instead of the haul's
   ID, so the bucket reads like the catalog; existing pallets keep their
   paths (manifests point at them). ([#76](https://github.com/alpaaaca/hubwagen/issues/76))
5. **Retention and expiry**: per dataset, keep the latest pallet of each
   partition plus those younger than an age or among the newest N
   (`hw.Dataset(..., keep=...)`, registered with the build). A leader timer
   and `hubwagen gc` expire the rest as events (`PalletExpired`, then the
   files go), never a pallet an open haul reads or the latest of a
   partition; expired pallets stay in the catalog as lineage records. ([#77](https://github.com/alpaaaca/hubwagen/issues/77))
6. **Dashboard: hauls**: `hubwagen ui`, a terminal UI on the API (a
   `tui.Source` over `HaulService`, so it works against any server): hauls
   with their status, a haul's lifts and logbook, live updates. ([#78](https://github.com/alpaaaca/hubwagen/issues/78))
7. **Dashboard: catalog**: datasets, partitions and pallets with stale and
   invalid marked, lineage up and down, and invalidating from the UI. ([#79](https://github.com/alpaaaca/hubwagen/issues/79))

Done when: in the revenue example, invalidating a `raw.orders` pallet
marks the downstream pallets invalid, `hubwagen stale` and the dashboard
show what to recompute, the replay it suggests recomputes exactly that,
and retention removes old versions while keeping the latest and anything
in use.

Open questions, to settle in the step that needs them:

- Dashboard: a terminal UI first (v1 had one; pure Go, nothing to serve),
  or a web UI served by `hubwagen dev`. Proposed: terminal first, on the
  API, so a web UI can follow on the same calls.
- Retention: declared in code (`hw.Dataset(keep=...)`, versioned with the
  build) or in `hubwagen.yaml` (an operator's choice). Proposed: code, with
  a config override later.
- Staleness by code version: "current" is the newest build that defines
  the lift; a stale pallet from an old build may still be what an older
  worker would produce.

## Later

Roughly in order of expected need:

- OpenLineage export.
- SQL lifts (`lift.sql`) and a dbt lift.
- Object-store buckets (`gs://`, `s3://`) and warehouse/table-format adapters.
- Pod lifts: local containers in single-node mode, Kubernetes Jobs in a
  cluster (starting from the v1 task contract).
- Auth, tenants and per-dock budgets; Litestream replication and failover
  (design: [failover.md](failover.md)).
- `hubwagen deploy --dry-run` (rebuild preview), `hubwagen verify`,
  `keep_fresh`, data modes, build pinning.
- Go SDK.
- Sharding, Postgres, Kubernetes Lease election.

## Spikes

| | Question | Blocks | Result |
|---|---|---|---|
| [S1](https://github.com/alpaaaca/hubwagen/issues/10) | Python replay prototype: how slow is full replay for a 1,000-event logbook? Does patching clock/random in a running interpreter catch typical mistakes without breaking libraries imported by haul modules? | M3c | **Done**: full replay is ~1 ms for 1,000 events (O(n²) over a haul's life); guard with `sys.monitoring`, not patching (13/13 caught, 0 libraries broken); call sites via `sys._getframe` are free. [Findings](spikes/s1-python-replay.md) |
| [S2](https://github.com/alpaaaca/hubwagen/issues/11) | Code version from imports: can the SDK resolve a lift's project-local imports reliably (relative imports, `importlib`, namespace packages), and how often does the lockfile term cause reruns in a real project? | M3c | **Done**: walk live objects from the lift (`co_names`, globals, closures, annotations), not import statements; AST hashes per function, class and constant; per-lift distribution versions instead of the lockfile, which would rerun everything on a dev-tool bump. [Findings](spikes/s2-code-version.md) |
| [S3](https://github.com/alpaaaca/hubwagen/issues/29) | Load: one leader with group commit at the target load (first v1 signal: ~60k events/s in batches of 128 on an M1 Mac). Re-measure with logbook events. | Load target sign-off | |
| [S4](https://github.com/alpaaaca/hubwagen/issues/30) | Litestream as a library with a replica path per leader term (from v1's R1). | Failover ([failover.md](failover.md), option A) | |

## Open decisions

| Decision | Proposed default | Needed by |
|---|---|---|
| CLI name | Keep `hubwagen`; maybe add `hw` as a short alias | M3c |
| Python SDK packaging | `sdk/python` in this repo, published as `hubwagen` on PyPI | M3c |
| Memoization default | Opt-in per lift (`memo=True`); SQL lifts on by default. Results and dataframes are memoized without `hw.output()`; under review: reuse by default with `live=True` to opt out ([api-simplification.md](api-simplification.md)) | Before the first release |
| Code version default | Decided ([S2](spikes/s2-code-version.md)): code the lift reaches + versions of the packages it uses + Python version + `deps=`, overridable with `version=` | M3b |
| dbt integration | One lift per project, one pallet per model | Later |
| Catalog interoperability | OpenLineage export first; Iceberg REST catalog later, if at all | Later |
| Failover | Nodes that supervise workers and can lead; bucket lease + Litestream first, embedded Raft if no-loss is needed ([failover.md](failover.md)) | Failover |
| `standalone` profile | Single-node mode covers it; revisit with auth and multi-host | Later |
| Load target for one leader | 100 lift transitions/s sustained, 2,000 running lifts | S3 |
| Licence and distribution | Open | Before first release |

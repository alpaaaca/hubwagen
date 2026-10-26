# Architecture

Where hubwagen is going and why. What is built and what is next is tracked in
[roadmap.md](roadmap.md); what changes from the M1–M2 engine is in
[v2-migration.md](v2-migration.md).

> **Status:** design (v2). The code in the repository is still the v1 engine
> (compiled YAML pipelines, container-task contract). Building v2 starts with
> M3a. Where this page and the code disagree, the code describes v1.

## Overview

hubwagen is a durable execution engine for data, with its own Go server. Data
engineers and scientists write pipelines as ordinary code. hubwagen guarantees
that the code runs to completion and records which data versions went in and
came out of every step.

That record does three jobs at once: it is the lineage graph, the data
catalog, and a cache that makes reruns and backfills skip work whose inputs
did not change.

**Problem.** Data pipelines are long-running processes. They wait for files,
keep cursors, retry flaky APIs, pause for human approval and get backfilled.
Declarative orchestrators model them as static graphs and push the process
state into side tables, sensors and XComs. General durable-execution engines
handle the process but know nothing about data: no versions, no lineage, no
partitions, no reuse across runs.

### Goals

- Pipelines as plain code that survives crashes, deploys and long waits.
- Every data output is an immutable, versioned **pallet** with recorded inputs.
- Memoization across runs: a step whose code and input versions are unchanged
  is not executed again.
- Exactly-once commits of data outputs, even when steps are retried.
- Waiting, timers, signals and durable state as first-class operations.
- A Go server that never runs user code.
- Workers in any language; Python first.
- Still one binary on a laptop or a plain server, with no database server and
  no message broker.

### Non-goals

- A distributed compute engine. Heavy compute stays in BigQuery, Spark, Ray
  and similar systems.
- A general-purpose workflow engine for application backends (payments,
  orders).
- A declarative asset framework at the core. "Keep this dataset fresh" is a
  library haul built on top.
- Moving data. Pallets point to data; hubwagen never touches the bytes.

## Mental model

Execution is the source of truth, and data state is derived from it. That is
the reverse of declarative orchestrators, where declared data is the source
of truth and runs are derived.

| | Declarative (Dagster) | Durable execution (Temporal) | hubwagen |
|---|---|---|---|
| What you author | The data that should exist | A process that must complete | A process that produces data |
| Source of truth | Declared asset graph | Event history | Event history, with data versions in every event |
| Graph known | Before running | Never as a graph | After running |
| Dynamic fan-out | Special primitives | A loop | A loop |
| Waiting and long-lived state | Sensors and side tables | First-class | First-class |
| Lineage and catalog | Built in | None | Derived from history |
| Reuse across runs | Skip if materialized | None | Skip if fingerprint matches |

**Data state is a projection.** "What is the latest version of
`marketing.daily_revenue` for 2026-10-24?", "what was it built from?" and
"what is stale?" are answered from the catalog, a projection of the logbooks
of all hauls. Nobody writes to it directly.

## Core concepts

A hubwagen moves pallets: hauls orchestrate, lifts do the work, pallets are
the data they move.

| Concept | What it is | Temporal analogue |
|---|---|---|
| **Haul** | Deterministic orchestration code. Survives crashes and deploys by replaying its logbook. | Workflow |
| **Lift** | One side-effecting unit of work: a query, an API call, a training step. Retried by policy, memoized by fingerprint. | Activity |
| **Pallet** | An immutable version of data a lift produced: location, schema, stats, input pallets, the fingerprint that made it. | — |
| **Dataset** | A named, optionally partitioned stream of pallets, such as `marketing.daily_revenue`. | — |
| **Fingerprint** | Hash of a lift's code version, inputs and parameters. Equal fingerprints mean equal output. | — |
| **Logbook** | The event history of one haul: lifts scheduled, completed, failed or reused; timers; signals; state changes. | History |
| **Dock** | A queue workers pull haul tasks and lift tasks from; one per team, image or resource class. | Task queue |
| **Worker** | A process in the user's environment that registers hauls and lifts and pulls work from docks. | Worker |
| **Signal** | External input delivered to a running haul (`approve`, `file_arrived`). | Signal |
| **Timer** | A durable sleep or deadline held by the server. | Timer |
| **State** | Small durable key-value state of a haul, recorded in its logbook. | Workflow variables |

## Writing hauls and lifts

A haul calls lifts, waits and branches. A lift touches the outside world and
returns a pallet or a small value. Examples use the Python SDK.

### A daily batch with a quality gate

```python
from hubwagen import haul, lift, Dataset, Pallet

@haul(schedule="0 3 * * *", params=lambda t: {"day": t.date() - timedelta(days=1)})
def daily_revenue(ctx, day: date):
    orders = ctx.lift(extract_orders, day=day)
    report = ctx.lift(check_quality, orders)
    if report.failed:
        ctx.alert(owner="marketing", report=report)
        ctx.wait_signal("approve", timeout="24h")    # durable across deploys
    return ctx.lift(build_revenue, orders, day)

@lift(memo=True)                    # a closed day's orders no longer change
def extract_orders(day: date) -> Pallet: ...

@lift(memo=True)
def check_quality(orders: Pallet) -> QualityReport: ...

@lift(memo=True, output=Dataset("marketing.daily_revenue", partition="day"))
def build_revenue(orders: Pallet, day: date) -> Pallet: ...
```

### Incremental ingestion with a cursor from the catalog

```python
@haul(schedule="*/15 * * * *")
def ingest_orders(ctx):
    latest = ctx.latest_pallet(Dataset("raw.orders"))
    cursor = latest.meta["max_ts"] if latest else "1970-01-01T00:00:00Z"
    ctx.lift(fetch_since, cursor=cursor)             # commits a pallet with meta max_ts

@lift(output=Dataset("raw.orders"))  # not memoized: reads a live source
def fetch_since(cursor: str) -> Pallet: ...
```

The cursor is the newest pallet's metadata, so there is no `etl_state` table,
no sensor and no haul that runs forever. `ctx.state` exists for state that
belongs to one haul execution (a retry counter, an approval note), and
`continue_as_new` for hauls that really must loop; neither is needed for
incremental ingestion.

### Dynamic fan-out

```python
@haul
def partner_files(ctx, day: date):
    files = ctx.lift(list_landed_files, "gs://partner-drop", day=day)
    parsed = ctx.gather([ctx.lift_async(parse_file, f) for f in files], max_parallel=20)
    return ctx.lift(merge_partner_day, parsed, day)
```

The number of files is unknown until runtime. That is a loop, not a special
primitive.

### SQL lifts

```python
build_revenue = lift.sql(
    "sql/daily_revenue.sql",
    engine="bigquery",
    output=Dataset("marketing.daily_revenue", partition="day"),
)
```

The SQL uses `{{ input("orders") }}` and `{{ partition }}`. The worker
resolves inputs to the exact locations of the input pallets, so a replay reads
the same versions (while they are readable, see Commits).

## Haul execution

### Replay until blocked

A haul task is stateless:

1. The worker receives the haul's logbook (or the part after the last
   `continue_as_new`).
2. It runs the haul function from the top. Each `ctx.*` call either returns
   its result from the logbook or is recorded as a new **command**.
3. The run stops when the function blocks on a call with no result yet: a
   lift not finished, a sleep, a signal, a `gather` with open handles. All
   commands issued up to that point (several, with `lift_async`) are
   collected.
4. The worker sends the commands to the server and forgets the haul.

The server appends the commands to the logbook, dispatches lifts and arms
timers, and schedules the next haul task when a result arrives. No haul is
held in worker memory; caching replays (sticky workers) is an optimisation for
later, only if profiling asks for it.

### Determinism

Replay works only if the haul makes the same calls in the same order every
time. Rules for haul code:

- No clock, randomness, network or file access. Use `ctx.now()`,
  `ctx.random()` and lifts.
- No iteration over unordered collections whose order decides which lifts are
  called.
- Logic changes that affect running hauls need a version marker (see Deploys).

**Detection.** Every `ctx.*` call has a sequence number. On replay the SDK
checks that call *n* has the same kind and target (lift name, timer, signal
name) as logbook entry *n*. A mismatch raises a non-determinism error naming
the file and line, captured when the call was made, and the haul becomes
**stuck**, not failed: deploying fixed code (or adding a version marker) lets
it continue.

**Guards**, cheapest first, instead of a full sandbox:

- While haul code runs, the SDK patches `time.time`, `datetime.now`,
  `random.*` and `uuid.uuid4` to raise.
- `hubwagen lint`: an AST check for the same calls and for iteration over sets.
- `hubwagen test --replay`: replay recent production logbooks against the new
  code in CI, without running lifts.

The guideline for users: keep hauls thin, put real work in lifts.

### Haul states

`running → completed | failed | cancelled`, with `waiting` (blocked on a
timer or signal; no worker involved) and `stuck` (non-determinism; needs a
deploy) as sub-states of running.

## Memoization

Before dispatching a lift, the server computes its fingerprint and looks it
up. If a readable committed pallet with that fingerprint exists, the lift is
not run: the logbook records `lift_reused` and the haul continues with that
pallet.

### Fingerprint

```
fingerprint = hash(
    lift name,
    code version,
    input keys,             # one per input pallet
    parameters,             # canonical JSON
    output dataset + partition,
)
```

- **Code version** defaults to a hash of the lift's source, the project
  modules it imports (resolved through the AST), and the worker's lockfile.
  `@lift(deps=["sql/macros/"])` adds non-Python files. `@lift(version="3")`
  replaces the computed value when the user knows better.
- **Input keys** are the pallet's content hash when its storage adapter can
  provide one cheaply (a `file://` manifest hashes its file checksums), and the
  pallet id otherwise. With content hashes, an upstream rebuild that produces
  identical data does not ripple downstream.
- **Bias: strict.** A needless rerun costs money; a wrong reuse silently
  serves stale data. Every rerun reports its reason (`code changed`,
  `lockfile changed`, `input changed`, `params changed`), so over-strictness
  is visible and can be relaxed per lift with `version=`.

### Which lifts are memoized

The server cannot tell from outside whether a lift's output depends only on
what the fingerprint covers. A lift that reads a pallet and calls an API, or
reads a config table, looks exactly like one that reads a pallet and does pure
arithmetic. So memoization is **opt-in**: a forgotten annotation costs a
rerun, never correctness.

- `@lift` (default): never reused; runs every time.
- `@lift(memo=True)`: the author promises the output depends only on the
  lift's code, input pallets and parameters. Use it for transforms over
  pallets, and for reads that a parameter pins down (a closed day's orders,
  a cursor range on an append-only source).
- `@lift(memo="ttl:1h")`: reuse a result up to that age. For APIs and live
  sources where slightly stale data is acceptable.
- **SQL lifts** (`lift.sql`) default to `memo=True`: hubwagen resolves every
  `{{ input(...) }}` itself, so it knows all inputs. `hubwagen lint` flags
  table references that bypass `input()`, since those are hidden inputs.

`hubwagen replay` names lifts that ran only because they are not memoized,
with a hint, so users learn where `memo=True` would help.

### Hidden inputs

A file, table or API that a lift reads without going through a pallet or a
parameter is a hidden input; memoizing such a lift is a wrong reuse waiting
to happen. Make it visible instead:

- **Read it in its own lift** that is not memoized and returns a pallet. The
  pallet's content hash then carries the change downstream:

  ```python
  @lift                                   # cheap, runs every time
  def load_fx_rates() -> Pallet: ...

  @lift(memo=True)
  def convert(orders: Pallet, rates: Pallet) -> Pallet: ...
  ```

  If the rates did not change, the content hash is the same and `convert` is
  reused; if they did, it reruns.
- **Files in the repository** (config, SQL macros): `@lift(deps=[...])` adds
  them to the code version.

### Single-flight

The fingerprint index holds `running`, `committed` and `failed` entries. A
request for a fingerprint that is already running waits for that run instead
of dispatching a second one; the logbook records which run it waited on. If
the run fails, the first waiter's retry policy decides whether to start a new
attempt. With one leader writing all state, single-flight lives in the core
loop.

## Commits

### Protocol

1. The server starts a lift attempt and gives it a staging location derived
   from fingerprint and attempt number.
2. The lift writes its output only to staging and reports `completed` with an
   output descriptor.
3. The server records completion in the logbook, then runs the commit: an
   atomic promote, through the dataset's storage adapter.
4. The pallet becomes visible in the catalog only after the commit succeeds.
   Commits are keyed by pallet id, so a crash between 3 and 4 is repaired by
   retrying the commit.
5. Staging output of failed or abandoned attempts is garbage-collected.

### Storage adapters

hubwagen does not own the storage, so what a commit guarantees depends on
where a lift writes. Each adapter declares two things:

- **Promote**: atomic (exactly-once visible output) or none (at-least-once;
  the fingerprint is passed as an idempotency key).
- **Old versions**: whether earlier pallets of a partition stay readable after
  a newer one is committed, and for how long (a retention setting where the
  adapter supports one).

The core follows one rule: **only a readable pallet can be reused or read by a
replay.** Lineage is always kept; an expired pallet stays in the catalog as a
record. "Replays read the same versions" holds while the input pallets are
readable.

v1 has one adapter, `file://` with pallet manifests: atomic, and every version
stays readable until retention removes it. Warehouse and table-format adapters
come later; their capabilities are documented with each adapter.

## Backfills, replays and invalidation

A backfill starts hauls for a range of parameters; memoization makes it cheap.

```
$ hubwagen replay daily_revenue --day 2026-03-01..2026-03-31
  31 hauls started
  extract_orders   31 reused
  check_quality    31 reused
  build_revenue    31 run      (code changed: 7c1e → a04b)
  report_extract   31 run      (input changed)
```

`hubwagen pallet invalidate <id> --reason …` marks a pallet and everything
built from it, directly or transitively, invalid. Invalid pallets are never
reused, so a replay recomputes exactly that set.

## Triggers and freshness

Hauls start from a schedule, a new pallet in a dataset, a signal from outside
(`hubwagen start partner_files --day 2026-10-24`, or HTTP), or another haul
(`ctx.start_haul(...)`).

- **Schedule**: cron with an explicit catch-up policy after downtime (`skip`,
  `latest`, `all`). The v1 cron implementation (persisted cursors, DST-safe
  intervals) carries over.
- **Dataset subscription**: starts when a pallet is committed to a dataset or
  partition, with debounce and batching.

**Keep-fresh** is a library haul, not a separate engine:

```python
keep_fresh(
    Dataset("marketing.daily_revenue", partition="day"),
    builder=daily_revenue,
    max_age="2h",
    when_upstream=["finance.orders"],
)
```

It subscribes to upstream datasets, asks the catalog for stale partitions and
starts the builder for them. Over-triggering is cheap when the builder's
lifts are memoized.

A pallet is **stale** when a newer pallet exists for one of its inputs'
datasets and partitions, or when its lift's current code version differs from
the one that built it.

## Deploys and versioning

There is no compiled manifest. A deploy rolls out a new worker image; on
startup, workers register their hauls, lifts, code versions and datasets
under a **build id**.

- **Version markers** are the default for logic changes that reach running
  hauls:

  ```python
  if ctx.version("quality-gate", max=2) >= 2:
      report = ctx.lift(check_quality, orders)
  ```

  Old logbooks replay the old branch; new executions take the new one.
  `hubwagen versions` lists markers still in use.
- **Drain**: most data hauls finish within hours, so rolling out new workers
  after running hauls finish (or reach a marker) covers most deploys.
- **Replay test** (`hubwagen test --replay`) in CI replays recent logbooks
  against the new code and catches non-determinism before it ships.
- **Build pinning** (old builds kept running until their hauls drain) is a
  later option for teams with long-running hauls.

`hubwagen deploy --dry-run` (later) adds a rebuild preview: which lifts
changed code version and which datasets will recompute.

## Server

```
  schedules, dataset events,           ┌──────────────────────────────────┐
  signals, API calls  ───────────────▶ │ Leader (hubwagen server)          │
                                       │  one core goroutine owns state:   │──▶ SQLite (WAL)
                                       │  logbooks, docks, catalog,        │      │ Litestream
                                       │  fingerprint index, timers        │      ▼
                                       └───────┬─────────────────▲─────────┘   bucket
                                     long-poll │                 │ commands, results
                                     haul/lift │                 │
                                       ┌───────▼─────────────────┴─────────┐
                                       │ Workers (user environment)        │
                                       │  Python SDK: replay hauls,        │──▶ user data systems
                                       │  run lifts, write to staging      │
                                       └───────────────────────────────────┘
```

One leader process holds four services in one core goroutine:

- **Logbook**: appends haul events, decides when a haul needs a new haul task
  or lift tasks.
- **Docks**: task queues with concurrency limits, rate limits and budgets per
  team. Workers long-poll them.
- **Catalog**: pallets, datasets, lineage and the fingerprint index, projected
  from logbook events in the same transaction.
- **Timers and triggers**: durable timers, cron and dataset subscriptions,
  firing events into logbooks.

Workers never connect to the database and the server never runs user code.

### Storage: SQLite + Litestream, single writer

Unchanged from v1. The leader is the only writer, so SQLite in WAL mode on
local disk gives fast writes and no server to run; Litestream copies it to
object storage or a second disk. Every state change is an event appended in
the same transaction that updates the projections (haul and lift tables,
catalog). **Side effects only happen after their events commit** (write
before act). Closed logbooks past retention are archived to the bucket.

### The core

One goroutine fed by channels (worker results, API commands, timers),
deterministic with a fake clock, as in v1. Term fencing from v1 stays: a
deposed leader cannot write state, and workers reject commands from older
terms.

### Worker API

Connect services (protobuf, also callable as plain HTTP/JSON, which keeps
the Python SDK free of gRPC tooling): `RegisterBuild`, `PollHaulTask`,
`CompleteHaulTask` (commands), `PollLiftTask`, `CompleteLiftTask`
(output descriptor), `Heartbeat`. Long-poll, so workers behind NAT work.

### Workers and execution

- **Long-lived workers** (default): one image per project, polling one or
  more docks. They run haul tasks and lifts.
- **Pod lifts** (later): `@lift(execution="pod", resources=…)` sends a lift to
  a pod dock; each attempt runs as a Kubernetes Job (or a local container in
  single-node mode). The v1 task contract and `internal/taskexec` are the
  starting point for this path.
- Haul tasks always run on long-lived workers; they are short replays and
  never wait for a pod.

## Local dev and single-node mode

```
$ hubwagen dev                                # server + UI, SQLite in .hubwagen/
$ python -m hubwagen.worker app.hauls         # local worker on the default dock
$ hubwagen start daily_revenue --day 2026-10-24
$ hubwagen test                               # hauls against a fake ctx, lifts with fake pallets
```

Single-node mode is also a supported production setup. Because hauls are
deterministic, a test scripts lift results and signals and asserts on the
lifts called and their order; `hubwagen test --replay <logbook>` turns a
production incident into a local test.

Data modes (later): `--data=prod-readonly` resolves inputs from the
production catalog read-only and commits to a personal namespace;
`--data=local` uses only local pallets.

## Internal interfaces

| Interface | Implementations |
|---|---|
| `Store` (events + projections) | `internal/state` (SQLite); Postgres later behind the same interface |
| `Replicator` | `litestream`, `noop` |
| `Elector` | `single`; `k8slease` later |
| `Blob` (incl. `PutIfAbsent`) | `internal/blob`: `file://`; `gs://`/`s3://` later |
| Storage adapter (promote, readability) | `file://` manifests; others later |
| Pod launcher | local subprocess/container; Kubernetes Jobs later |
| `Clock` | real, fake |

## How this scales (later)

None of this is in the first milestones; it is the direction, so the core
avoids choices that block it.

- **Sharding.** Haul ids hashed into a fixed number of shards, each owned by
  one replica (Kubernetes Leases, or embedded Raft outside Kubernetes). The
  hard part is not logbooks but what crosses hauls: the fingerprint index
  (single-flight) and dataset subscriptions need an owner of their own.
  Single-leader SQLite scales far enough that this waits until it hurts.
- **Storage tiers.** Postgres partitioned by shard for logbooks and catalog;
  Cassandra/ScyllaDB for logbooks only if Postgres is outgrown; closed
  logbooks as Parquet in the bucket.
- **Profiles.** v1 planned `dev`, `standalone` and `kubernetes`. v2 commits to
  dev and single-node first; whether `standalone` survives as a separate
  profile is an open decision in [roadmap.md](roadmap.md).

## Comparison

| | Airflow | Dagster | Temporal | hubwagen |
|---|---|---|---|---|
| Core model | Task DAG on a schedule | Declared assets | Durable code | Durable code that produces versioned data |
| Dynamic fan-out | Dynamic task mapping | Dynamic outputs | Loops | Loops |
| Waiting, signals, approval | Sensors, deferrable operators | Sensors | First-class | First-class |
| Incremental cursors | Variables or side tables | Cursors in sensors | Workflow state | Pallet metadata or haul state |
| Lineage and catalog | Add-ons (OpenLineage) | Built in, declared | None | Built in, recorded |
| Reuse across runs | None | Skip materialized partitions | None | Fingerprint memoization |
| Exactly-once data commits | No | No | No data concept | Staged write + atomic promote |
| User code in control plane | Scheduler parses DAG files | Code servers import it | Never | Never |

The closest existing pattern is teams running Temporal for data pipelines and
building versioning, lineage and idempotent writes themselves.

## Risks

- **Wrong reuse.** A code version that misses a dependency, or a hidden input
  in a lift marked `memo=True`, reuses an outdated pallet. Mitigations:
  memoization is opt-in, strict default hashing with the lockfile, rerun
  reasons, the hidden-inputs pattern, lint for SQL table references, and
  later `hubwagen verify` (recompute a sample of reused lifts and compare).
- **Determinism burden.** Users will write non-deterministic hauls.
  Mitigations: sequence checks with line numbers, runtime guards, lint,
  replay tests, and the stuck state so a mistake is fixable.
- **Logbook growth.** Bounded by keeping hauls short (cursors from the
  catalog), `continue_as_new` and archiving.
- **Side effects outside atomic adapters** stay at-least-once; each adapter
  documents its guarantee.
- **Adoption.** Teams have Airflow DAGs and dbt projects; a dbt lift and a
  migration guide are needed early.
- **Scope.** A durable execution engine is years of work; the roadmap proves
  the memoized rerun on one machine before anything else.

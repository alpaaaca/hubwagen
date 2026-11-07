# Architecture

Where moksha is going and why. What is built and what is next is tracked in
[roadmap.md](roadmap.md); what changes from the M1–M2 engine is in
[v2-migration.md](v2-migration.md).

> **Status:** built through M3c: the run engine (runs, steps, timers,
> queues, retries, the sequence check, restore), data versions, the catalog and
> memoization (file:// commits, fingerprints, single-flight), the worker and
> run APIs, `moksha dev`/`start`/`replay`, and the Python SDK with an
> example project. Runtime guards and import-based code versions (#25) are
> next; then M4.

## Overview

moksha is a durable execution engine for data, with its own Go server. Data
engineers and scientists write pipelines as ordinary code. moksha guarantees
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
- Every data output is an immutable, versioned **data version** with recorded inputs.
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
  library run built on top.
- Moving data. Data versions point to data; moksha never touches the bytes.

## Mental model

Execution is the source of truth, and data state is derived from it. That is
the reverse of declarative orchestrators, where declared data is the source
of truth and runs are derived.

| | Declarative (Dagster) | Durable execution (Temporal) | moksha |
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
"what is stale?" are answered from the catalog, a projection of the event logs
of all runs. Nobody writes to it directly. `CatalogService` reads it
(`moksha datasets`, `moksha versions <dataset>`, and `moksha data <id>`
with its files and its lineage both ways).

**Logs are not state.** What a step prints and logs is captured by the
worker per attempt and sent as it runs (`LogService`); the server keeps it
apart from the event log, capped per attempt, and decides nothing by it. A
lost batch is lost output, never a wrong result.

## Core concepts

Pipelines orchestrate, steps do the work, data versions are the data
they produce.

| Concept | What it is | Temporal analogue |
|---|---|---|
| **Pipeline** | Deterministic orchestration code. | Workflow type |
| **Run** | One execution of a pipeline. Survives crashes and deploys by replaying its event log. | Workflow execution |
| **Step** | One side-effecting unit of work: a query, an API call, a training step. Retried by policy, memoized by fingerprint. | Activity |
| **Data version** | An immutable version of data a step produced: location, schema, stats, input data versions, the fingerprint that made it. | — |
| **Dataset** | A named, optionally partitioned stream of data versions, such as `marketing.daily_revenue`. | — |
| **Fingerprint** | Hash of a step's code version, inputs and parameters. Equal fingerprints mean equal output. | — |
| **Event log** | The event history of one run: steps scheduled, completed, failed or reused; timers; signals; state changes. | History |
| **Queue** | Where workers pull run tasks and step tasks from; one per team, image or resource class. | Task queue |
| **Worker** | A process in the user's environment that registers pipelines and steps and pulls work from queues. | Worker |
| **Signal** | External input delivered to a run in progress (`approve`, `file_arrived`). | Signal |
| **Timer** | A durable sleep or deadline held by the server. | Timer |
| **State** | Small key-value state of a run, rebuilt by every replay from the event log, so it is as durable as the event log. | Workflow variables |

**In front of people** (the SDK, the web UI, the CLI, what `moksha dev`
prints, error messages) a memoized step is **reused** ("same inputs and
same code, so it didn't run again"), and a run's event log is told as its
**history**, in sentences by `internal/history`, with the engine's
bookkeeping (run tasks, attempts that succeeded) shown only on request.
The UI explains each word where it appears (`ui/src/words.tsx`). The
SDK's names are the same words: `@mk.pipeline`, `@mk.step`, `ctx.step`,
`ctx.run`, `mk.DataVersion`.

## Writing pipelines and steps

A run calls steps, waits and branches. A step touches the outside world and
returns a data version or a small value. Examples use the Python SDK.

### A daily batch with a quality gate

```python
from moksha import pipeline, step, Dataset, Interval, DataVersion, Schedule

@pipeline(schedule=Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin"))
def daily_revenue(ctx, interval: Interval):   # yesterday, midnight to midnight in Berlin
    day = interval.start.date()
    orders = ctx.step(extract_orders, day=day)
    report = ctx.step(check_quality, orders)
    if report.failed:
        ctx.alert(owner="marketing", report=report)
        ctx.wait_signal("approve", timeout=timedelta(hours=24))  # durable across deploys
    return ctx.step(build_revenue, orders, day)

@step(reuse=True)                    # a closed day's orders no longer change
def extract_orders(day: date) -> DataVersion: ...

@step(reuse=True)
def check_quality(orders: DataVersion) -> QualityReport: ...

@step(reuse=True, output=Dataset("marketing.daily_revenue", partition="day"))
def build_revenue(orders: DataVersion, day: date) -> DataVersion: ...
```

### Incremental ingestion with a cursor from the catalog

```python
@pipeline(schedule="*/15 * * * *")
def ingest_orders(ctx):
    latest = ctx.latest_version(Dataset("raw.orders"))
    cursor = latest.meta["max_ts"] if latest else "1970-01-01T00:00:00Z"
    ctx.step(fetch_since, cursor=cursor)             # commits a data version with meta max_ts

@step(output=Dataset("raw.orders"))  # not memoized: reads a live source
def fetch_since(cursor: str) -> DataVersion: ...
```

`ctx.latest_version` is a command like `ctx.step`: the server answers it from
the catalog at once and records the answer in the event log (`QueryAnswered`),
so every replay of this run sees the same data version even after newer ones are
committed.

The cursor is the newest data version's metadata, so there is no `etl_state` table,
no sensor and no run that runs forever. `ctx.state` exists for state that
belongs to one run execution (a retry counter, an approval note), and
`continue_as_new` for runs that really must loop; neither is needed for
incremental ingestion.

### Dynamic fan-out

```python
@pipeline
def partner_files(ctx, day: date):
    files = ctx.step(list_landed_files, "gs://partner-drop", day=day)
    parsed = ctx.gather([ctx.step_async(parse_file, f) for f in files], max_parallel=20)
    return ctx.step(merge_partner_day, parsed, day)
```

The number of files is unknown until runtime. That is a loop, not a special
primitive.

`max_parallel` is held by the server, not the replay: all the steps are
recorded at once, each carrying a limit (a group per gather, plus a
run-wide group with `@pipeline(max_parallel=N)`), and the core starts a step
only while each of its groups has fewer than the maximum queued or running.
The rest wait in order and start as slots free up; a step waiting to retry
holds no slot. Queues' own limits apply on top.

### Dataframes between steps

```python
@step(reuse=True)
def orders(day: date) -> pl.DataFrame: ...

@step(reuse=True)
def totals(orders: pl.LazyFrame) -> pl.DataFrame: ...

@pipeline
def daily(ctx, day: date):
    return ctx.step(totals, ctx.step(orders, day))
```

A returned dataframe or Arrow table is written as Parquet by the worker into
the attempt's staging prefix and committed as a data version: in the step's output
dataset, or without one, in its implicit dataset `step:<name>`
(unpartitioned; declared outputs can't use the `step:` prefix). The run
only ever holds the handle; the next step gets the data, loaded as its
parameter's annotation asks. Every step attempt gets a staging prefix for
this, and the server commits whatever files a succeeded attempt reports.

### SQL steps

```python
build_revenue = step.sql(
    "sql/daily_revenue.sql",
    engine="bigquery",
    output=Dataset("marketing.daily_revenue", partition="day"),
)
```

The SQL uses `{{ input("orders") }}` and `{{ partition }}`. The worker
resolves inputs to the exact locations of the input data versions, so a replay reads
the same versions (while they are readable, see Commits).

## Run execution

### Replay until blocked

A run task is stateless:

1. The worker receives the run's event log (or the part after the last
   `continue_as_new`).
2. It runs the pipeline function from the top. Each `ctx.*` call either returns
   its result from the event log or is recorded as a new **command**.
3. The run stops when the function blocks on a call with no result yet: a
   step not finished, a sleep, a signal, a `gather` with open handles. All
   commands issued up to that point (several, with `step_async`) are
   collected.
4. The worker sends the commands to the server and forgets the run.

The server appends the commands to the event log, dispatches steps and arms
timers, and schedules the next run task when a result arrives. No run is
held in worker memory; caching replays (sticky workers) is an optimisation for
later, only if profiling asks for it.

### Determinism

Replay works only if the run makes the same calls in the same order every
time. (The "blocked" signal the SDK raises to stop a replay is a
`BaseException`, so pipeline code's `except Exception:` can't swallow it.)
Rules for pipeline code:

- No clock, randomness, network or file access. Use `ctx.now()`,
  `ctx.random()` and steps.
- No iteration over unordered collections whose order decides which steps are
  called.
- Logic changes that affect running pipelines need a version marker (see Deploys).

**Detection.** Every `ctx.*` call has a sequence number. On replay the SDK
checks that call *n* has the same kind and target (step name, timer, signal
name) as event log entry *n*. A mismatch raises a non-determinism error naming
the file and line, captured when the call was made, and the run becomes
**stuck**, not failed: deploying fixed code (or adding a version marker) lets
it continue.

**Guards**, cheapest first, instead of a full sandbox:

- While pipeline code runs, the SDK watches calls with `sys.monitoring`
  (Python ≥ 3.12) and raises when *user* code calls `time.time`,
  `datetime.now`, the module-level `random` functions or `uuid.uuid4`,
  however they were imported; library code that reads the clock (logging)
  is left alone. Patching module attributes was tried and rejected: it
  misses `from time import time` and breaks logging
  ([spike S1](spikes/s1-python-replay.md)).
- The worker runs with a fixed `PYTHONHASHSEED`, so iterating a set gives
  the same order in every replay.
- Lint (`python -m moksha.lint`, part of the Python SDK since it reads
  Python): an AST check of pipeline code, without importing it, for the same
  calls plus iteration over sets, I/O and environment variables,
  `except:`/`except BaseException` (which swallow the replay's control
  flow), and steps called directly instead of through `ctx.step`.
- `python -m moksha.testing --replay-recent N`: replay recent production event logs against the new
  code in CI, without running steps.

The guideline for users: keep runs thin, put real work in steps.

### Signals

A signal is input from outside for one run: `moksha signal <run> approve
--data '{"by": "ana"}'` (or `SignalRun` on the API) records it in the run's
event log, and `ctx.wait_signal("approve", timeout=...)` returns its payload.
A signal sent before the run waits is kept until a wait for that name takes
it; each signal answers one wait, oldest wait first. A timeout is a timer:
if it fires first, the wait raises `SignalTimeout` in the run. Deliveries
and timeouts are recorded (`SignalDelivered`), so replays see the same
answer, and a new leader rebuilds waits and kept signals from the event log.

### Child runs and continue_as_new

`ctx.run(other, **params)` starts another run and waits for its result;
`ctx.start_run(other, **params)` starts one and returns its ID. A child
has its own event log and run tasks; the parent's event log records only that
it started (`ChildStarted`) and, for a waited-for child, how it ended
(`ChildFinished`, raised as `RunFailed` unless it completed). Ending or
cancelling a parent cancels the children it waits for; others carry on.
The parent link is on the child's run record, so restarts need nothing
else.

`ctx.continue_as_new(**params)` ends a run as `continued` and starts a new
run of the same name with those params and a fresh event log, keeping its
parent link: a waiting parent gets the result of the last run in the
chain, and `moksha start --wait` follows the chain. Runs that loop use
it to keep their event logs, and so their replays, short.

### Run states

`running → completed | failed | cancelled | continued`, with `waiting` (blocked on a
timer or signal; no worker involved) and `stuck` (non-determinism; needs a
deploy) as sub-states of running.

## Memoization

Before dispatching a step, the server computes its fingerprint and looks it
up. If a readable committed data version with that fingerprint exists, the step is
not run: the event log records `step_reused` and the run continues with that
data version. A step without an output is reused the same way, by the result of
the newest step that ran with that fingerprint and completed; results live
in the server's store, so they are always readable.

### Fingerprint

```
fingerprint = hash(
    step name,
    code version,
    input keys,             # one per input data version
    parameters,             # canonical JSON
    output dataset + partition,
)
```

- **Code version** defaults to a hash of the code the step can run, found by
  walking the live objects its function refers to: its own AST and that of
  every project function, class and constant it reaches, in any module;
  the versions of the installed distributions that code uses (with their
  requirements); the Python version. Comments, docstrings and the step's
  decorator don't count. There is no lockfile term: a dev-tool bump would
  rerun everything ([spike S2](spikes/s2-code-version.md)).
  `@step(deps=["sql/macros/"])` adds non-Python files. `@step(version="3")`
  replaces the computed value when the user knows better. Runs get code
  versions the same way (steps count by name), and a build ID covers both.
- **Input keys** are the data version's content hash when its storage adapter can
  provide one cheaply (a `file://` manifest hashes its file checksums and the
  step's result, unless that is null), and the data version id otherwise. With
  content hashes, an upstream rebuild that produces identical data does not
  ripple downstream.
- **Bias: strict.** A needless rerun costs money; a wrong reuse silently
  serves stale data. Every rerun reports its reason (`code changed`,
  `input changed`, `params changed`), so over-strictness is visible and can
  be relaxed per step with `version=`; `python -m moksha.worker
  --versions` shows what a code version covers.

### Which steps are reused

The server cannot tell from outside whether a step's output depends only on
what the fingerprint covers. A step that reads a data version and calls an API, or
reads a config table, looks exactly like one that reads a data version and does pure
arithmetic. So reuse across runs is **opt-in**, as `reuse=`: a forgotten
annotation costs a rerun, never correctness (decided in
[api-simplification.md](api-simplification.md#4-reuse-opt-in-as-reuse-with-moksha-pointing-out-where-it-helps)).
Inside one run a completed step is never run again, with or without it.

- `@step` (default): never reused; runs every time.
- `@step(reuse=True)`: the author promises the output depends only on the
  step's code, input data versions and parameters. Use it for transforms over
  data versions, and for reads that a parameter pins down (a closed day's orders,
  a cursor range on an append-only source).
- `@step(reuse="1h")`: reuse a result up to that age. For APIs and live
  sources where slightly stale data is acceptable.
- `memo=` is the old name: `memo=True` is `reuse=True`, `memo="ttl:1h"`
  is `reuse="1h"`. It still works for a release, with a warning, and lint
  flags it (MK007).
- **A run can reuse nothing**: `moksha start` and `replay` with
  `--reuse=none` (`StartRun.reuse`) run every step of the run, those with
  `reuse=` too, after a source changed in a way the fingerprint can't see. Its
  steps still record their fingerprints and results, so the runs after it
  reuse what it made; its child runs and continuations reuse nothing
  either, and it doesn't wait for another run's step with the same
  fingerprint (single-flight).
- A step with an output reuses a data version; one without reuses the data version it
  built in its implicit dataset (a dataframe it returned, see below) or else
  the result of a step that ran (not one that was itself reused, so a TTL counts from the
  run). On the wire a step command carries its `reuse=` as `memo: ""`
  (never), `fingerprint` or `ttl:<duration>`; the SDK passes data versions as inputs rather than inside
  the arguments, so the fingerprint sees their content hashes.
- **SQL steps** (`step.sql`) default to `reuse=True`: moksha resolves every
  `{{ input(...) }}` itself, so it knows all inputs. lint (later, with SQL steps) flags
  table references that bypass `input()`, since those are hidden inputs.

`moksha replay` will name steps that ran only because they have no
`reuse=`, with a hint, so users learn where `reuse=True` would help
([api-simplification.md](api-simplification.md#steps), step 3).

### Hidden inputs

A file, table or API that a step reads without going through a data version or a
parameter is a hidden input; giving such a step `reuse=` is a wrong reuse waiting
to happen. Make it visible instead:

- **Read it in its own step** without `reuse=` that returns a data version. The
  data version's content hash then carries the change downstream:

  ```python
  @step                                   # cheap, runs every time
  def load_fx_rates() -> DataVersion: ...

  @step(reuse=True)
  def convert(orders: DataVersion, rates: DataVersion) -> DataVersion: ...
  ```

  If the rates did not change, the content hash is the same and `convert` is
  reused; if they did, it reruns.
- **Files in the repository** (config, SQL macros): `@step(deps=[...])` adds
  them to the code version.

### Single-flight

The fingerprint index holds `running`, `committed` and `failed` entries. A
request for a fingerprint that is already running waits for that run instead
of dispatching a second one; the event log records which run it waited on. If
the run fails, the first waiter's retry policy decides whether to start a new
attempt. With one leader writing all state, single-flight lives in the core
loop.

## Commits

### Protocol

1. The server starts a step attempt and gives it a staging location of its
   own, under the dataset and partition its data version goes to
   (`data/<tenant>/<dataset>/<partition>/<attempt>/`; `_` when
   unpartitioned), and records it with the attempt. The files never move:
   once committed, the manifest (`data versions/<tenant>/<dataset>/<partition>/
   <data version>.json`) points there, so the bucket reads like the catalog.
   Attempts dispatched before this layout keep their old paths
   (`steps/<tenant>/<run>/<seq>/<attempt>/`).
2. The step writes its output only to staging and reports `completed` with an
   output descriptor. A worker that doesn't share the server's filesystem
   uploads the files first, with URLs the server hands out for the
   attempt; only the server opens the bucket
   ([remote-storage.md](remote-storage.md)).
3. The server records completion in the event log, then runs the commit: an
   atomic promote, through the dataset's storage adapter.
4. The data version becomes visible in the catalog only after the commit succeeds.
   Commits are keyed by data version id, so a crash between 3 and 4 is repaired by
   retrying the commit.
5. Staging output of failed or abandoned attempts is garbage-collected.

### Storage adapters

moksha does not own the storage, so what a commit guarantees depends on
where a step writes. Each adapter declares two things:

- **Promote**: atomic (exactly-once visible output) or none (at-least-once;
  the fingerprint is passed as an idempotency key).
- **Old versions**: whether earlier data versions of a partition stay readable after
  a newer one is committed, and for how long (a retention setting where the
  adapter supports one).

The core follows one rule: **only a readable data version can be reused or read by a
replay.** Lineage is always kept; an expired data version stays in the catalog as a
record. "Replays read the same versions" holds while the input data versions are
readable. Readable means the catalog says committed *and* the storage still
has it: before reusing a data version, the core checks that its manifest and every
file it lists are in the bucket (`DataVersionStore.Check`), because files can be
deleted outside moksha. A data version that fails the check isn't reused; the
step runs again ("the earlier run's output can't be read: its files are
gone"), and its new data version stands in from then on.

v1 has one adapter, data version manifests in the bucket (`file://` or `s3://`):
atomic (create-if-absent; on an object store a conditional PUT, which the
server checks the store supports), and every version stays readable until
retention removes it. Warehouse and table-format adapters
come later; their capabilities are documented with each adapter.

### Retention

A dataset's retention is declared in code with its outputs,
`Dataset("mart.revenue", partition="day", keep=Keep(versions=3, age=30d))`,
and registered with the build; the newest build that declares the dataset
decides, and one that declares it without `keep` removes it (everything
stays, the default). Per partition the latest committed data version always stays,
and so do those among the newest `versions` or younger than `age`; the rest
expire, except what something running may read: data versions built by open
runs, data versions open runs got as step results or from `latest_version`,
inputs of unfinished steps and data versions waiting in a subscription batch. The
leader expires every hour and on `moksha gc` (`--dry-run` shows): first
a `DataVersionExpired` event records the data version as expired, then its data is
deleted off the loop. Its manifest and lineage stay; expired data versions are
never reused or read, so a replay that needs one reruns its step.

## Backfills, replays and invalidation

A backfill starts runs for a range of parameters; memoization makes it cheap.

```
$ moksha replay daily_revenue --param day=2026-03-01..2026-03-31
31 runs started
31 runs
  extract_orders   31 reused
  check_quality    31 reused
  build_revenue    31 ran (its code changed (7c1e → a04b))
  report_extract   31 ran (an input changed)
31 completed
```

`moksha start <run> --param …` starts one run (`--wait` waits and
reports the same way). Both talk to `RunService` (`StartRun`, `GetRun`)
on the server.

`moksha data invalidate <id> --reason …` marks a data version and everything
built from it, directly or transitively, invalid. Invalid data versions are never
reused, never a dataset's latest and never a step's input, so a replay
recomputes exactly that set; the command shows what it will mark, asks,
and prints the `moksha start` per run that built them (the rerun says
`earlier data version invalidated`). Each data version gets a `DataVersionInvalidated`
event with the cause and who asked; the events belong to the tenant, not
to a run's event log, so no replay changes. A data version still being promoted is
marked too, and its promote records it as invalid, also across a leader
restart.

## Triggers and freshness

Runs start from a schedule, a new data version in a dataset, a signal from outside
(`moksha start partner_files --day 2026-10-24`, or HTTP), or another run
(`ctx.start_run(...)`).

- **Schedule**: cron in a timezone, with the data interval each run covers
  (`hourly`, `daily`, `weekly`, `monthly`: the previous full period,
  DST-correct; or `cron`: previous tick to this one) and a catch-up policy
  for runs missed while no leader ran (`skip`, `latest`, `all`). A schedule
  is declared in code (`@pipeline(schedule=...)`) and registered with the build;
  the newest build that defines a pipeline decides its schedule. The leader
  keeps a cursor per schedule (`internal/schedule`) and starts one run per
  due interval, passing it as the `interval` parameter, in the same batch
  that moves the cursor. The server can't run a params function, so the
  run derives what it needs (`interval.start.date()`) itself.
  `moksha schedules` lists them with their next runs.
- **Dataset subscription**: `@pipeline(subscribe=Subscription("raw.orders",
  debounce=60))` starts the run when data versions are committed to the
  datasets, in batches: once none came for `debounce`, once `max_batch`
  wait, or once the first has waited `max_wait`. The run gets the batch as
  `data versions` (handles; passed to steps they are inputs, so lineage follows).
  Waiting data versions are recorded, so a new leader keeps the batch; starting
  the run and clearing the batch is one commit. A run's own data versions don't
  start it, and memoized steps commit nothing, so a rerun that changes
  nothing starts nothing downstream. Registered like schedules: the newest
  build that defines the pipeline decides.

**Keep-fresh** is a library run, not a separate engine:

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
steps are memoized.

A data version is **stale** when a newer data version with other content exists for one
of its inputs' datasets and partitions, or when its step's current code
version (in the newest registered build that defines the step) differs from
the one that built it. `moksha stale [dataset]` lists the stale latest
data versions with their reasons and the `moksha start` that refreshes each;
memoization reuses the rest. Staleness is reported where it starts: a
data version built from a stale one isn't listed until its input is rebuilt,
since the start that refreshes the input rebuilds it too.

## Deploys and versioning

There is no compiled manifest. A deploy rolls out a new worker image; on
startup, workers register their runs, steps, code versions and datasets
under a **build id**.

- **Version markers** are the default for logic changes that reach running
  runs:

  ```python
  if ctx.version("quality-gate", max=2) >= 2:
      report = ctx.step(check_quality, orders)
  ```

  Old event logs replay the old branch; new executions take the new one. A
  marker is recorded by name, outside the command sequence, so adding one
  doesn't contradict the event log of a run in progress: a run whose recorded
  commands go past the marker started on the old code and gets `min`
  (default 1); a run reaching it for the first time at the end of its
  history gets `max`. The decision is recorded with the run task, every
  replay gets the same, and a replay deciding differently makes the run
  stuck. Raising `min` drops the old branch; a run still on it becomes
  stuck. (Later: `moksha versions` lists markers still in use.)
- **Drain**: most data runs finish within hours, so rolling out new workers
  after running pipelines finish (or reach a marker) covers most deploys.
- **Replay test** (`python -m moksha.testing --replay-recent N`) in CI replays recent event logs
  against the new code and catches non-determinism before it ships.
- **Build pinning** (old builds kept running until their runs drain) is a
  later option for teams with long-running pipelines.

`moksha deploy --dry-run` (later) adds a rebuild preview: which steps
changed code version and which datasets will recompute.

## Server

```
  schedules, dataset events,           ┌──────────────────────────────────┐
  signals, API calls  ───────────────▶ │ Leader (moksha server)          │
                                       │  one core goroutine owns state:   │──▶ SQLite (WAL)
                                       │  event logs, queues, catalog,        │      │ Litestream
                                       │  fingerprint index, timers        │      ▼
                                       └───────┬─────────────────▲─────────┘   bucket
                                     long-poll │                 │ commands, results
                                     run/step │                 │
                                       ┌───────▼─────────────────┴─────────┐
                                       │ Workers (user environment)        │
                                       │  Python SDK: replay runs,        │──▶ user data systems
                                       │  run steps, write to staging      │
                                       └───────────────────────────────────┘
```

One leader process holds four services in one core goroutine:

- **Event log**: appends run events, decides when a run needs a new run task
  or step tasks.
- **Queues**: task queues with concurrency limits, rate limits and budgets per
  team. Workers long-poll them.
- **Catalog**: data versions, datasets, lineage and the fingerprint index, projected
  from event log events in the same transaction.
- **Timers and triggers**: durable timers, cron and dataset subscriptions,
  firing events into event logs.

Workers never connect to the database and the server never runs user code.

### Storage: SQLite + Litestream, single writer

Unchanged from v1. The leader is the only writer, so SQLite in WAL mode on
local disk gives fast writes and no server to run; Litestream copies it to
object storage or a second disk. Every state change is an event appended in
the same transaction that updates the projections (pipeline and step tables,
catalog). **Side effects only happen after their events commit** (write
before act). Closed event logs past retention are archived to the bucket.

### The core

One goroutine fed by channels (worker results, API commands, timers),
deterministic with a fake clock, as in v1. Term fencing from v1 stays: a
deposed leader cannot write state, and workers reject commands from older
terms.

On leader start the core reloads open runs and rebuilds each run's
recorded commands from its event log. Workers outlive leaders, so a step
attempt a worker had taken stays valid (its result is accepted by the new
leader, its timeout keeps what was left); a run task a worker had taken is
replaced, since replays are cheap. Queued work goes back on its queues;
timers and retry waits keep their remaining time and fire at once if it
passed while no leader was up.

### Worker API

`moksha.v1.WorkerService` (`proto/moksha/v1/worker.proto`, served by
`internal/api`): Connect, so also callable as plain HTTP/JSON
(`POST /moksha.v1.WorkerService/<Method>`), which keeps the Python SDK free
of gRPC tooling. `RegisterBuild` (the pipelines a build defines and its steps'
code versions), `PollRunTask`, `CompleteRunTask` (commands),
`PollStepTask`, `CompleteStepTask` (output descriptor), `Heartbeat`. Polls
are long polls (capped at 30 s), so workers behind NAT work.

Polls carry the worker's build, and a worker gets only the run tasks and
steps its build defines. Projects with different code can therefore share
queues; queues separate resources (GPU, memory), not codebases. `StartRun`
refuses a run that no registered build defines. JSON that belongs to user code (arguments, results, params, event
payloads) travels as JSON text in `*_json` fields, so numbers stay exact.

Heartbeats are leases: with a heartbeat timeout (one minute in dev mode), a
dispatched step attempt or run task whose worker stops sending heartbeats
is lost — the attempt goes through its step's retry policy, the run task is
replaced. The heartbeat response lists the tasks a worker should stop
(cancelled, timed out, finished elsewhere). Leases aren't persisted; a new
leader gives restored attempts fresh ones.

### Workers and execution

- **Long-lived workers** (default): one image per project, polling one or
  more queues. They take run tasks and step tasks.
- **Pod steps** (later): `@step(execution="pod", resources=…)` sends a step to
  a pod queue; each attempt runs as a Kubernetes Job (or a local container in
  single-node mode). The v1 task contract and `internal/taskexec` are the
  starting point for this path.
- Run tasks always run on long-lived workers; they are short replays and
  never wait for a pod.

## Local dev and single-node mode

```
$ moksha dev                                # server + web UI on the same port, SQLite in .moksha/
$ python -m moksha.worker                   # local worker; modules from pyproject.toml
$ moksha start daily_revenue --day 2026-10-24
$ pytest                                      # runs against scripted results: moksha.testing.run
```

Single-node mode is also a supported production setup: `moksha server`
runs the same server and workers as dev without its console, under a
service manager, with `/healthz` and JSON logs ([config.md](config.md)). Because runs are
deterministic, a test scripts step results and signals and asserts on the
steps called and their order (`moksha.testing.run`, which plays the
server in memory); `python -m moksha.testing --replay <run-id>` replays
a production run's event log against local code, so an incident becomes a
local test.

Data modes (later): `--data=prod-readonly` resolves inputs from the
production catalog read-only and commits to a personal namespace;
`--data=local` uses only local data versions.

## Internal interfaces

| Interface | Implementations |
|---|---|
| `Store` (events + projections) | `internal/state` (SQLite); Postgres later behind the same interface |
| `Replicator` | `litestream`, `noop` |
| `Elector` | `single`; `bucketlease`, `k8slease` later ([failover.md](failover.md)) |
| `Blob` (incl. `PutIfAbsent`, presigned URLs) | `internal/blob`: `file://`, `s3://` (S3-compatible); `gs://` later ([remote-storage.md](remote-storage.md)) |
| Storage adapter (promote, readability) | `file://` manifests; others later |
| Pod launcher | local subprocess/container; Kubernetes Jobs later |
| `Clock` | real, fake |

## How this scales (later)

None of this is in the first milestones; it is the direction, so the core
avoids choices that block it.

- **Sharding.** Run ids hashed into a fixed number of shards, each owned by
  one replica (Kubernetes Leases, or embedded Raft outside Kubernetes). The
  hard part is not event logs but what crosses runs: the fingerprint index
  (single-flight) and dataset subscriptions need an owner of their own.
  Single-leader SQLite scales far enough that this waits until it hurts.
- **Storage tiers.** Postgres partitioned by shard for event logs and catalog;
  Cassandra/ScyllaDB for event logs only if Postgres is outgrown; closed
  event logs as Parquet in the bucket.
- **Profiles.** v1 planned `dev`, `standalone` and `kubernetes`. v2 commits to
  dev and single-node first; `standalone` isn't a separate profile:
  `moksha server` is the single-node server.

## Comparison

| | Airflow | Dagster | Temporal | moksha |
|---|---|---|---|---|
| Core model | Task DAG on a schedule | Declared assets | Durable code | Durable code that produces versioned data |
| Dynamic fan-out | Dynamic task mapping | Dynamic outputs | Loops | Loops |
| Waiting, signals, approval | Sensors, deferrable operators | Sensors | First-class | First-class |
| Incremental cursors | Variables or side tables | Cursors in sensors | Workflow state | Data version metadata or run state |
| Lineage and catalog | Add-ons (OpenLineage) | Built in, declared | None | Built in, recorded |
| Reuse across runs | None | Skip materialized partitions | None | Fingerprint memoization |
| Exactly-once data commits | No | No | No data concept | Staged write + atomic promote |
| User code in control plane | Scheduler parses DAG files | Code servers import it | Never | Never |

The closest existing pattern is teams running Temporal for data pipelines and
building versioning, lineage and idempotent writes themselves.

## Risks

- **Wrong reuse.** A code version that misses a dependency, or a hidden input
  in a step marked `reuse=True`, reuses an outdated data version. Mitigations:
  reuse is opt-in, code versions that follow everything a step's code
  reaches (with the versions of the packages it uses), rerun reasons, the hidden-inputs pattern, lint for SQL table references, and
  later `moksha verify` (recompute a sample of reused steps and compare).
- **Determinism burden.** Users will write non-deterministic runs.
  Mitigations: sequence checks with line numbers, runtime guards, lint,
  replay tests, and the stuck state so a mistake is fixable.
- **Event log growth.** Bounded by keeping runs short (cursors from the
  catalog), `continue_as_new` and archiving.
- **Side effects outside atomic adapters** stay at-least-once; each adapter
  documents its guarantee.
- **Adoption.** Teams have Airflow DAGs and dbt projects; a dbt step and a
  migration guide are needed early.
- **Scope.** A durable execution engine is years of work; the roadmap proves
  the memoized rerun on one machine before anything else.

# Simplifying the authoring API

Design note; steps 1 and 2 are built (see Steps), the rest is not.
Decided (2026-11-07): reuse across runs stays opt-in, as `reuse=`
([section 4](#4-reuse-opt-in-as-reuse-with-moksha-pointing-out-where-it-helps)).
Proposes how the Python SDK can drop most of the
ceremony around steps, memoization and outputs while the core keeps its
guarantees. Decisions here would change [architecture.md](architecture.md)
(Core concepts, Writing pipelines and steps, Memoization, Commits) and
[sdk/python/README.md](../sdk/python/README.md).

## The problem

Today a pipeline author has to make three decisions per step that have
little to do with whether the pipeline works:

1. **`memo=True` or not.** It looks as if this is what makes a pipeline
   durable. It isn't: inside one run a completed step is never rerun, with
   or without it. `memo` only controls reuse *across* runs (tomorrow's
   rerun, a backfill), but the API doesn't say so.
2. **`output=mk.Dataset(...)` and `mk.output().path`.** To be memoized, a
   step must write files into a staging directory moksha hands it. A step
   that returns a value can't be memoized at all (`ValueError` at import).
3. **Data versions as arguments.** Downstream steps take an `mk.DataVersion` and open
   files by name (`orders.path("orders.csv")`), instead of getting the data
   they need.

This fits CSV files and fights the way most people work: pandas or Polars
dataframes passed between steps, Spark jobs that write their own tables,
warehouse queries. And the workaround (declare a dataset, write nothing)
commits empty data versions whose content hashes are all equal, so downstream
fingerprints can't tell different results apart.

What authors care about first is that the pipeline runs and survives
crashes. Reuse, lineage and versions matter later: the first expensive
backfill, the first wrong number on a dashboard. The API should cost
nothing until then and already have the guarantees in place when it's
needed.

## Goals

- A pipeline is plain functions with two decorators, `@mk.pipeline` and
  `@mk.step`, and no other required options.
- Steps take and return ordinary values: JSON-able values, dataframes,
  Arrow tables, or references to tables that live elsewhere.
- Reuse across runs is one annotation, `reuse=True`, on the steps that
  read nothing but their inputs and arguments, and moksha points out the
  steps where it would help. A step without it is never wrong, only
  slower.
- The core rules stay: the server never runs user code, data never passes
  through the server, a step is reused only when what it would return can
  still be read.

Non-goals: changing run semantics (replay, determinism), the worker API's
shape, or the event log.

## Proposal

### 1. Every step result is a data version

Every completed step commits a data version, whatever it returns. "Data version" stops
being something authors handle and becomes the catalog's record of a
result: its value or files, the inputs it was built from, its fingerprint.

| Step returns | Stored as | Content hash covers |
|---|---|---|
| `None`, numbers, strings, dicts, lists, dates | The JSON value, in the data version record | The canonical JSON |
| pandas / Polars DataFrame, Arrow table | Parquet in the step's staging prefix, written by the SDK | The files (see Hashing) |
| `mk.table(uri, version=...)` | A reference: URI and version token | The URI and version token |
| Files under `mk.output().path` (kept as an escape hatch) | The files, as today | The files |

The content hash covers the result value as well as the files, so two
results that differ never look the same downstream. This replaces today's
"only steps with an output can be memoized".

`output=mk.Dataset(...)` becomes optional. It names the result in the
catalog (`mart.daily_revenue`, partition `day`) so that `ctx.latest_version`,
invalidation and the UI can find it by name. Without it, results are still
recorded and reusable, under the step's name.

### 2. Values in, values out

A downstream step gets the value, not a handle:

```python
import polars as pl
import moksha as mk

@mk.step                                          # reads a source that changes: runs every time
def extract_orders(day: str) -> pl.DataFrame:
    return pl.read_database(QUERY, conn, execute_options={"parameters": [day]})

@mk.step(reuse=True, output=mk.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: pl.DataFrame, day: str) -> pl.DataFrame:
    return orders.group_by("region").agg(pl.col("amount").sum())

@mk.step(reuse=True)
def revenue_report(revenue: pl.DataFrame, day: str) -> str:
    return render(revenue, day)

@mk.pipeline
def daily(ctx, day: str):
    orders = ctx.step(extract_orders, day)
    revenue = ctx.step(daily_revenue, orders, day)
    return ctx.step(revenue_report, revenue, day)
```

- **In the run**, `ctx.step` still returns a handle (today's `DataVersion`),
  never the loaded data. A run is replayed often and runs on whatever
  worker picks it up; loading a dataframe there would make every replay
  read it. Passing the handle to the next step is what the run does with
  it. Small JSON results stay available on the handle (`.result`), as
  today, so a run can branch on them.
- **In a step**, the SDK loads each handle argument according to the
  parameter's annotation: `pl.DataFrame`, `pl.LazyFrame` (scans the
  Parquet without reading it), `pd.DataFrame`, `pa.Table`, a plain value,
  or `mk.DataVersion` for the files themselves (today's behaviour).
- Handles still travel as inputs, not inside the arguments, so lineage and
  fingerprints work exactly as they do now.

Codecs are pluggable (`mk.register_codec(name, types, write, read)`), with
built-in ones for pandas, Polars and Arrow imported lazily: the SDK stays
stdlib-only, and a project that doesn't use pandas never imports it.

### 3. Spark and warehouses: references, not copies

Data that already lives in a table format or a warehouse shouldn't be
copied into moksha's bucket. The step writes it where it belongs and
returns a reference:

```python
@mk.step(output=mk.Dataset("mart.revenue", partition="day"))
def revenue(day: str):
    spark.sql(REVENUE_SQL, day=day).writeTo("lake.mart.revenue").overwritePartitions()
    snapshot = current_snapshot("lake.mart.revenue")    # e.g. from the table's snapshots metadata
    return mk.table("iceberg://lake.mart.revenue", version=snapshot)
```

The reference's version token (an Iceberg or Delta snapshot, an object
version, a warehouse snapshot time) is what goes into downstream
fingerprints. Before reusing such a data version, the server asks a storage
adapter whether that version still exists ("never reuse what can't be
read"); an adapter that can't tell answers no, and the step reruns. Adapters
run on the worker side when they need credentials the server doesn't have.

A Spark `DataFrame` returned directly is written as Parquet into the
staging prefix by the executors (`df.write.parquet(staging)`), never
collected to the driver.

### 4. Reuse: opt-in, as `reuse=`, with moksha pointing out where it helps

**Decided** (2026-11-07). The alternative was reuse by default with
`live=True` to opt out; it is not taken, for these reasons:

- **The two mistakes aren't alike.** Under opt-in, a forgotten
  annotation reruns a step that could have been reused: slower, never
  wrong. Under reuse by default, a forgotten `live=True` reuses a result
  that is out of date, and nothing says so. For a data tool the second is
  the one that hurts, and "never reuse what can't be read; when unsure,
  rerun" already says which way moksha leans.
- **The fingerprint can't see what a step reads.** It covers code,
  packages, input data versions and arguments, not a file opened by path,
  a database, `now()`, the environment or a config file. The example
  project's own `extract_orders` reads `data/orders.csv`: with reuse by
  default and no `live=True`, a new row in the file would change nothing.
- **The mitigations can't close it.** Lint and the runtime guards see
  Python, but database drivers (libpq) and readers (Polars, Arrow) do
  their I/O in C or Rust, where neither looks.
- **It's what teams know.** Prefect, Flyte, Dagster and Hamilton cache
  across runs only when asked.

What the opt-in model keeps from the proposal, and what it fixes in
today's API:

- **`reuse=` instead of `memo=`.** `memo=True` looked like what makes a
  pipeline durable (problem 1); `reuse=` says what it does: across runs,
  a step that comes again with the same code, inputs and arguments isn't
  run, its earlier result is used. `@mk.step(reuse=True)`, or
  `reuse="1h"` for results up to that age (today's `memo="ttl:1h"`).
  `memo=` keeps working for a release, with a deprecation warning that
  says what to write. Inside a run nothing changes: a completed step is
  never rerun, with or without it.
- **Moksha points out where it would help.** The server fingerprints
  every step, not just those with `reuse=`, and records it; nothing is
  looked up for a step without `reuse=`, so it still runs every time.
  When a step without it has run again with a fingerprint it ran with
  before and returned the same content, the replay report and the web UI
  say so, once per step: "daily_revenue ran 3 times with the same code,
  inputs and arguments and returned the same result each time. If it
  reads nothing but those, `@mk.step(reuse=True)` would reuse it." The
  same result isn't proof (the CSV may just not have changed yet), so it
  is a hint, never automatic. A step turned to `reuse=True` is reused
  from its earlier runs right away, since they carry the fingerprint.
- **The run decides too.** `moksha start` and `replay` take
  `--reuse=auto|none`: `none` recomputes everything in that run (and
  records the result, so later runs can reuse it). `moksha replay
  --dry-run` starts nothing and says, for each run it would start, what
  the latest earlier run with those parameters did and which of its steps
  the current code would run again (code changed, or no `reuse=`): the
  same comparison `moksha deploy --dry-run` makes. It can't foresee more:
  which steps a run calls is known only when its pipeline code runs, on
  a worker, and a step after one that returns something new runs again
  too.
- **Every reuse is reported**, as today: in the event log, the run's
  history ("reused the existing version …: same inputs and same code")
  and the replay report.

### 5. Hashing large outputs

Hashing every file by reading it is fine for a CSV and wrong for 500 GB.

- **Files the SDK writes** (Parquet from a codec) are hashed while they are
  written.
- **Object stores** keep a checksum per object (S3 ETag or CRC32C, GCS
  CRC32C). The SDK lists the staging prefix and uses those, without reading
  the data.
- **References** use their version token; nothing is hashed.

The manifest records which kind of checksum each file has. Comparing
content hashes across kinds is never needed: the same step writes the same
way every time.

## What changes where

| Area | Change |
|---|---|
| Core (`internal/core`) | Memoize steps without an output; result in the content hash; a fingerprint for every step, looked up only with `reuse=`; the reuse hint; `--reuse` on `StartRun` |
| Data versions (`internal/dataversion`) | Manifests with a result, references (URI + version) and checksum kinds |
| Fingerprints (`internal/core/memo.go`) | Content hash now covers results: a **format change**, every existing memoized data version stops matching once |
| Worker API (`proto/`) | Step results carry a reference; `reuse` on `StartRun`; adapters' existence check |
| Python SDK | `reuse=` (`memo=` deprecated for a release); codecs and annotation-based loading; `mk.table`; `mk.output()` kept as the escape hatch |
| Docs | architecture.md (Core concepts, Writing pipelines and steps, Memoization, Commits), SDK README, the example project |

Nothing changes in runs, replay, queues, timers, the event log's shape, or
the rule that the server never runs user code.

## Steps

PR-sized, in order:

1. ✅ **Result data versions.** Memoize steps without an output; content hash
   covers the result. Keep `memo=` as the switch. Small, and removes the
   empty data version workaround. *Built differently:* a plain result is reused
   from the steps table (the newest run with the fingerprint), not from a
   data version, since the catalog has no data versions without a dataset; a data version's
   manifest records the result and its content hash covers it unless null.
2. ✅ **Codecs.** Dataframes and Arrow tables in and out of steps,
   annotation-based loading. *As built:* every step attempt gets a staging
   prefix; a frame returned by a step without an output becomes a data version in
   its implicit dataset `step:<name>` (unpartitioned; the prefix is
   reserved); hashing still reads the written file afterwards, not while
   writing; a frame must be returned on its own, not inside a value.
3. **Reuse, opt-in** (decided in section 4), three PRs:
   1. **`reuse=`**: the SDK takes `reuse=True` / `reuse="1h"` and sends
      the same policy as today (`fingerprint`, `ttl:…`), so the core and
      the wire don't change; `memo=` warns once per step with what to
      write, and the linter flags it. The example project, the SDK
      README and architecture.md say `reuse=`.
   2. ✅ **`--reuse=none` and `replay --dry-run`** (two PRs; `--dry-run`
      through its own RPC, `PlanRuns`): `StartRun` takes a
      reuse mode for the run (proto, `RunService`), recorded on the run;
      `--dry-run` reports, from the latest earlier run with the same
      parameters, which of its steps the current code would run again,
      without starting runs (the server runs no pipeline code to know
      more).
   3. **The hint**: every step gets a fingerprint (the same
      `fingerprintInputs`, so not a format change); a step without
      `reuse=` is still never looked up. The replay report and the
      pipeline's page in the web UI say when such a step returned the
      same content for a fingerprint it had before.
4. **References.** `mk.table`, version tokens, the adapter existence check,
   starting with Iceberg or Delta.
5. **Object-store checksums**, with the `s3://` / `gs://` buckets.

1 and 2 are useful on their own and don't depend on the reuse decision
(step 3).

## Open questions

- **Names.** Authors would only meet `run` and `step`; "data version" would
  appear in the catalog and UI. Whether `run`/`step` themselves should be
  plainer (`flow`/`step`) is a separate question.
- **Where codecs write without a dataset.** Settled: the implicit dataset
  `step:<name>`, unpartitioned, and since M5 the bucket keeps it like any
  dataset (`data/<tenant>/step:<name>/_/<attempt>/`). Retention is M5's.
- **Spark detection.** Whether returning a Spark `DataFrame` should be
  supported at all, or always require `mk.table`.

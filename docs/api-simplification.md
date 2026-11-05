# Simplifying the authoring API

Design note; steps 1 and 2 are built (see Steps), the rest is not.
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
- Reuse across runs is safe by default and needs at most one annotation, on
  the rare step that reads a live source.
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

@mk.step(live=True)                               # reads a source that changes
def extract_orders(day: str) -> pl.DataFrame:
    return pl.read_database(QUERY, conn, execute_options={"parameters": [day]})

@mk.step(output=mk.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: pl.DataFrame, day: str) -> pl.DataFrame:
    return orders.group_by("region").agg(pl.col("amount").sum())

@mk.step
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

### 4. Reuse: on by default, `live=True` to opt out

Today memoization is opt-in per step because the server can't tell a pure
transform from a step that also reads an API. The proposal flips the
annotation to the case that is rarer and easier to spot: **a step that
reads something the fingerprint doesn't cover says so.**

- `@mk.step` (default): reused when code version, input content hashes and
  arguments match.
- `@mk.step(live=True)`: reads a live source (a table that changes, an API,
  "now"). Never reused; runs every time. Downstream steps are still reused
  when it returns the same content, because their fingerprints use its
  content hash.
- `@mk.step(live="1h")`: reused up to that age (today's `memo="ttl:1h"`).

Which steps are live is usually obvious: the ones at the edge that read
from outside. In the example above, only `extract_orders`.

The cost is the risk the opt-in model avoided: a forgotten `live=True` is
a wrong reuse, not a needless rerun. The mitigations:

- **Inputs make most steps safe by construction.** A step whose only
  inputs are other steps' results and its arguments is reusable. The risky
  step is one that reads from outside *and* isn't marked, and those are
  the ones at the edges.
- **The run decides too.** `moksha start` and `replay` take
  `--reuse=auto|none`, `none` for a run that must recompute everything.
- **Lint and guards.** Lint (`python -m moksha.lint`) would flag steps
  that open connections or call HTTP clients without `live=`; the runtime
  guards from #25 can do the same while a step runs, as a warning.
- **Reports.** Every reuse is in the event log and the replay report, and
  `moksha replay --dry-run` shows what would be reused before anything
  runs.

This is the decision in this note most worth arguing about; see Open
questions.

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
| Core (`internal/core`) | Memoize steps without an output; result in the content hash; memo policy `live` / `ttl` instead of `fingerprint` / `ttl`; `--reuse` on `StartRun` |
| Data versions (`internal/dataversion`) | Manifests with a result, references (URI + version) and checksum kinds |
| Fingerprints (`internal/core/memo.go`) | Content hash now covers results: a **format change**, every existing memoized data version stops matching once |
| Worker API (`proto/`) | Step results carry a reference; poll answers carry reuse policy; adapters' existence check |
| Python SDK | `live=`; codecs and annotation-based loading; `mk.table`; `memo=` and `output=` path kept and deprecated for a release |
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
3. **Reuse by default.** `live=` replaces `memo=`; `--reuse`; replay report
   and `--dry-run`. Example project and docs rewritten without `memo=` and
   `mk.output()`.
4. **References.** `mk.table`, version tokens, the adapter existence check,
   starting with Iceberg or Delta.
5. **Object-store checksums**, with the `s3://` / `gs://` buckets.

1 and 2 are useful on their own and don't depend on the reuse decision
(step 3).

## Open questions

- **Reuse by default vs opt-in.** The proposal trades a needless rerun for
  a possible wrong reuse when `live=True` is forgotten. The alternative is
  to keep `memo=` opt-in but make it cheap (steps 1–2 only) and have
  `moksha replay` suggest it. Decide before step 3.
- **Names.** Authors would only meet `run` and `step`; "data version" would
  appear in the catalog and UI. Whether `run`/`step` themselves should be
  plainer (`flow`/`step`) is a separate question.
- **Where codecs write without a dataset.** Settled: the implicit dataset
  `step:<name>`, unpartitioned, and since M5 the bucket keeps it like any
  dataset (`data/<tenant>/step:<name>/_/<attempt>/`). Retention is M5's.
- **Spark detection.** Whether returning a Spark `DataFrame` should be
  supported at all, or always require `mk.table`.

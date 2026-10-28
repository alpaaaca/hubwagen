# Simplifying the authoring API

Design note; steps 1 and 2 are built (see Steps), the rest is not.
Proposes how the Python SDK can drop most of the
ceremony around lifts, memoization and outputs while the core keeps its
guarantees. Decisions here would change [architecture.md](architecture.md)
(Core concepts, Writing hauls and lifts, Memoization, Commits) and
[sdk/python/README.md](../sdk/python/README.md).

## The problem

Today a pipeline author has to make three decisions per lift that have
little to do with whether the pipeline works:

1. **`memo=True` or not.** It looks as if this is what makes a pipeline
   durable. It isn't: inside one haul a completed lift is never rerun, with
   or without it. `memo` only controls reuse *across* hauls (tomorrow's
   rerun, a backfill), but the API doesn't say so.
2. **`output=hw.Dataset(...)` and `hw.output().path`.** To be memoized, a
   lift must write files into a staging directory hubwagen hands it. A lift
   that returns a value can't be memoized at all (`ValueError` at import).
3. **Pallets as arguments.** Downstream lifts take an `hw.Pallet` and open
   files by name (`orders.path("orders.csv")`), instead of getting the data
   they need.

This fits CSV files and fights the way most people work: pandas or Polars
dataframes passed between steps, Spark jobs that write their own tables,
warehouse queries. And the workaround (declare a dataset, write nothing)
commits empty pallets whose content hashes are all equal, so downstream
fingerprints can't tell different results apart.

What authors care about first is that the pipeline runs and survives
crashes. Reuse, lineage and versions matter later: the first expensive
backfill, the first wrong number on a dashboard. The API should cost
nothing until then and already have the guarantees in place when it's
needed.

## Goals

- A pipeline is plain functions with two decorators, `@hw.haul` and
  `@hw.lift`, and no other required options.
- Lifts take and return ordinary values: JSON-able values, dataframes,
  Arrow tables, or references to tables that live elsewhere.
- Reuse across hauls is safe by default and needs at most one annotation, on
  the rare lift that reads a live source.
- The core rules stay: the server never runs user code, data never passes
  through the server, a lift is reused only when what it would return can
  still be read.

Non-goals: changing haul semantics (replay, determinism), the worker API's
shape, or the event log.

## Proposal

### 1. Every lift result is a pallet

Every completed lift commits a pallet, whatever it returns. "Pallet" stops
being something authors handle and becomes the catalog's record of a
result: its value or files, the inputs it was built from, its fingerprint.

| Lift returns | Stored as | Content hash covers |
|---|---|---|
| `None`, numbers, strings, dicts, lists, dates | The JSON value, in the pallet record | The canonical JSON |
| pandas / Polars DataFrame, Arrow table | Parquet in the lift's staging prefix, written by the SDK | The files (see Hashing) |
| `hw.table(uri, version=...)` | A reference: URI and version token | The URI and version token |
| Files under `hw.output().path` (kept as an escape hatch) | The files, as today | The files |

The content hash covers the result value as well as the files, so two
results that differ never look the same downstream. This replaces today's
"only lifts with an output can be memoized".

`output=hw.Dataset(...)` becomes optional. It names the result in the
catalog (`mart.daily_revenue`, partition `day`) so that `ctx.latest_pallet`,
invalidation and the UI can find it by name. Without it, results are still
recorded and reusable, under the lift's name.

### 2. Values in, values out

A downstream lift gets the value, not a handle:

```python
import polars as pl
import hubwagen as hw

@hw.lift(live=True)                               # reads a source that changes
def extract_orders(day: str) -> pl.DataFrame:
    return pl.read_database(QUERY, conn, execute_options={"parameters": [day]})

@hw.lift(output=hw.Dataset("mart.daily_revenue", partition="day"))
def daily_revenue(orders: pl.DataFrame, day: str) -> pl.DataFrame:
    return orders.group_by("region").agg(pl.col("amount").sum())

@hw.lift
def revenue_report(revenue: pl.DataFrame, day: str) -> str:
    return render(revenue, day)

@hw.haul
def daily(ctx, day: str):
    orders = ctx.lift(extract_orders, day)
    revenue = ctx.lift(daily_revenue, orders, day)
    return ctx.lift(revenue_report, revenue, day)
```

- **In the haul**, `ctx.lift` still returns a handle (today's `Pallet`),
  never the loaded data. A haul is replayed often and runs on whatever
  worker picks it up; loading a dataframe there would make every replay
  read it. Passing the handle to the next lift is what the haul does with
  it. Small JSON results stay available on the handle (`.result`), as
  today, so a haul can branch on them.
- **In a lift**, the SDK loads each handle argument according to the
  parameter's annotation: `pl.DataFrame`, `pl.LazyFrame` (scans the
  Parquet without reading it), `pd.DataFrame`, `pa.Table`, a plain value,
  or `hw.Pallet` for the files themselves (today's behaviour).
- Handles still travel as inputs, not inside the arguments, so lineage and
  fingerprints work exactly as they do now.

Codecs are pluggable (`hw.register_codec(name, types, write, read)`), with
built-in ones for pandas, Polars and Arrow imported lazily: the SDK stays
stdlib-only, and a project that doesn't use pandas never imports it.

### 3. Spark and warehouses: references, not copies

Data that already lives in a table format or a warehouse shouldn't be
copied into hubwagen's bucket. The lift writes it where it belongs and
returns a reference:

```python
@hw.lift(output=hw.Dataset("mart.revenue", partition="day"))
def revenue(day: str):
    spark.sql(REVENUE_SQL, day=day).writeTo("lake.mart.revenue").overwritePartitions()
    snapshot = current_snapshot("lake.mart.revenue")    # e.g. from the table's snapshots metadata
    return hw.table("iceberg://lake.mart.revenue", version=snapshot)
```

The reference's version token (an Iceberg or Delta snapshot, an object
version, a warehouse snapshot time) is what goes into downstream
fingerprints. Before reusing such a pallet, the server asks a storage
adapter whether that version still exists ("never reuse what can't be
read"); an adapter that can't tell answers no, and the lift reruns. Adapters
run on the worker side when they need credentials the server doesn't have.

A Spark `DataFrame` returned directly is written as Parquet into the
staging prefix by the executors (`df.write.parquet(staging)`), never
collected to the driver.

### 4. Reuse: on by default, `live=True` to opt out

Today memoization is opt-in per lift because the server can't tell a pure
transform from a lift that also reads an API. The proposal flips the
annotation to the case that is rarer and easier to spot: **a lift that
reads something the fingerprint doesn't cover says so.**

- `@hw.lift` (default): reused when code version, input content hashes and
  arguments match.
- `@hw.lift(live=True)`: reads a live source (a table that changes, an API,
  "now"). Never reused; runs every time. Downstream lifts are still reused
  when it returns the same content, because their fingerprints use its
  content hash.
- `@hw.lift(live="1h")`: reused up to that age (today's `memo="ttl:1h"`).

Which lifts are live is usually obvious: the ones at the edge that read
from outside. In the example above, only `extract_orders`.

The cost is the risk the opt-in model avoided: a forgotten `live=True` is
a wrong reuse, not a needless rerun. The mitigations:

- **Inputs make most lifts safe by construction.** A lift whose only
  inputs are other lifts' results and its arguments is reusable. The risky
  lift is one that reads from outside *and* isn't marked, and those are
  the ones at the edges.
- **The run decides too.** `hubwagen start` and `replay` take
  `--reuse=auto|none`, `none` for a run that must recompute everything.
- **Lint and guards.** Lint (`python -m hubwagen.lint`) would flag lifts
  that open connections or call HTTP clients without `live=`; the runtime
  guards from #25 can do the same while a lift runs, as a warning.
- **Reports.** Every reuse is in the logbook and the replay report, and
  `hubwagen replay --dry-run` shows what would be reused before anything
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
content hashes across kinds is never needed: the same lift writes the same
way every time.

## What changes where

| Area | Change |
|---|---|
| Core (`internal/core`) | Memoize lifts without an output; result in the content hash; memo policy `live` / `ttl` instead of `fingerprint` / `ttl`; `--reuse` on `StartHaul` |
| Pallets (`internal/pallet`) | Manifests with a result, references (URI + version) and checksum kinds |
| Fingerprints (`internal/core/memo.go`) | Content hash now covers results: a **format change**, every existing memoized pallet stops matching once |
| Worker API (`proto/`) | Lift results carry a reference; poll answers carry reuse policy; adapters' existence check |
| Python SDK | `live=`; codecs and annotation-based loading; `hw.table`; `memo=` and `output=` path kept and deprecated for a release |
| Docs | architecture.md (Core concepts, Writing hauls and lifts, Memoization, Commits), SDK README, the example project |

Nothing changes in hauls, replay, docks, timers, the event log's shape, or
the rule that the server never runs user code.

## Steps

PR-sized, in order:

1. ✅ **Result pallets.** Memoize lifts without an output; content hash
   covers the result. Keep `memo=` as the switch. Small, and removes the
   empty pallet workaround. *Built differently:* a plain result is reused
   from the lifts table (the newest run with the fingerprint), not from a
   pallet, since the catalog has no pallets without a dataset; a pallet's
   manifest records the result and its content hash covers it unless null.
2. ✅ **Codecs.** Dataframes and Arrow tables in and out of lifts,
   annotation-based loading. *As built:* every lift attempt gets a staging
   prefix; a frame returned by a lift without an output becomes a pallet in
   its implicit dataset `lift:<name>` (unpartitioned; the prefix is
   reserved); hashing still reads the written file afterwards, not while
   writing; a frame must be returned on its own, not inside a value.
3. **Reuse by default.** `live=` replaces `memo=`; `--reuse`; replay report
   and `--dry-run`. Example project and docs rewritten without `memo=` and
   `hw.output()`.
4. **References.** `hw.table`, version tokens, the adapter existence check,
   starting with Iceberg or Delta.
5. **Object-store checksums**, with the `s3://` / `gs://` buckets.

1 and 2 are useful on their own and don't depend on the reuse decision
(step 3).

## Open questions

- **Reuse by default vs opt-in.** The proposal trades a needless rerun for
  a possible wrong reuse when `live=True` is forgotten. The alternative is
  to keep `memo=` opt-in but make it cheap (steps 1–2 only) and have
  `hubwagen replay` suggest it. Decide before step 3.
- **Names.** Authors would only meet `haul` and `lift`; "pallet" would
  appear in the catalog and UI. Whether `haul`/`lift` themselves should be
  plainer (`flow`/`step`) is a separate question.
- **Where codecs write without a dataset.** Partly settled by step 2: the
  implicit dataset `lift:<name>`, unpartitioned. The files still sit under
  the attempt's staging prefix; the bucket layout and retention are
  undecided.
- **Spark detection.** Whether returning a Spark `DataFrame` should be
  supported at all, or always require `hw.table`.

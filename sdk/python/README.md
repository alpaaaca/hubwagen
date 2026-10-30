# hubwagen Python SDK

Write pipelines and their steps as plain Python; a worker runs them against
a hubwagen server. Python ≥ 3.12, no dependencies outside the standard
library.

```python
import hubwagen as hw

@hw.step(memo=True, output=hw.Dataset("raw.orders", partition="day"))
def extract(day: str) -> dict:
    out = hw.output()                      # this attempt's staging directory
    (out.path / "orders.csv").write_text(fetch_orders(day))
    out.meta["max_ts"] = f"{day}T23:59:00Z"
    return {"rows": 1}

@hw.step(memo=True, output=hw.Dataset("mart.revenue", partition="day"))
def revenue(orders: hw.DataVersion, day: str) -> dict: ...

@hw.pipeline
def daily_revenue(ctx, day: str):
    orders = ctx.step(extract, day)        # a DataVersion of raw.orders; reused if nothing changed
    return ctx.step(revenue, orders, day).id
```

```bash
hubwagen dev                                            # server + worker API on 127.0.0.1:7070
python -m hubwagen.worker                               # modules from pyproject.toml; registers the build, polls
hubwagen start daily_revenue --param day=2026-10-24 --wait
hubwagen replay daily_revenue --param day=2026-10-01..2026-10-24
```

The words:

- A **pipeline** is the code that decides which steps run, with which
  inputs. One execution of it, for some inputs, is a **run**.
- A **step** is one unit of real work (a query, an API call, a
  computation), run on a worker and retried by policy.
- What a step writes is a **data version**: a new version of a
  **dataset**, saved and never changed afterwards.
- A memoized step that comes again with the same code, inputs and
  arguments is **reused**: what it produced before stands in, and it
  doesn't run again.

The engine (the server, its API and docs/architecture.md) calls a pipeline
a *haul*, a step a *lift* and a data version a *pallet*. Those names still
work in the SDK: `hw.haul`, `hw.lift`, `hw.Pallet`, `hw.LiftFailed`,
`hw.ChildFailed`, `ctx.lift`, `ctx.lift_async`, `ctx.haul`,
`ctx.start_haul` and `ctx.latest_pallet` are the same as the names below.

## Pipelines

A run is replayed from the top of the pipeline function whenever it has
something new to see (a step finished, a timer fired); each `ctx` call
returns its recorded result or stops the replay until there is one. So
pipeline code must make the same calls in the same order every time:

- **No clock, randomness, network or files** in pipeline code. Use
  `ctx.now()` (the time of the newest result the replay has used, the same
  on every replay), `ctx.random()` / `ctx.rng` (seeded by the run), and
  steps. While pipeline code runs, calling `time.time`, `datetime.now`,
  `date.today`, the module-level `random` functions, `uuid1`/`uuid4` or
  `os.urandom` from your code raises `hubwagen.NondeterminismError` with
  the line, and the run becomes **stuck** (fix the code; the new build
  resumes it). Libraries that read the clock, such as `logging`, are left
  alone, and so are steps. Network and files aren't guarded: keep them in
  steps.
- `python -m hubwagen.lint` checks pipeline code for all of this before it
  runs (the clock, randomness, set iteration, I/O, `except:`, steps called
  directly), from the source, for the modules in `pyproject.toml` or the
  paths given. `# noqa: HW003` silences one line.
- **Don't swallow `hubwagen.Blocked`.** It is a `BaseException`, so
  `except Exception:` is safe; a bare `except:` is not.
- `ctx.state` is a dict rebuilt by every replay; it holds what the run
  computed so far, nothing more.
- A replay that contradicts what the run already did (another step where
  one was recorded) makes the run **stuck**, with both call sites in its
  message. Deploying fixed code (a new build) resumes it.

`ctx` offers `step`, `step_async` + `gather` (fan-out;
`gather(handles, max_parallel=N)` runs at most N of them at once, and
`@hw.pipeline(max_parallel=N)` caps all of a run's steps), `sleep` (durable,
held by the server), `wait_signal` and `version` (below), `latest_version`
(the newest saved version of a dataset partition, recorded so every replay
sees the same one), `now`, `random` and `state`. A failed step raises
`hubwagen.StepFailed` in the pipeline.

What a step prints (stdout, stderr) and logs (`logging`) is captured with
it: the web UI shows it on the run's page, step by step, and `hubwagen dev`
prints it marked with the run and the step; a failure's traceback is part
of it. Capture goes by thread, so output from threads the step starts, from
subprocesses or from C libraries writing to the file descriptor isn't
captured. Standalone workers still echo it to their terminal, marked with
the step (`HUBWAGEN_ECHO_LOGS=0` turns that off).

The web UI calls a pipeline by its title:
`@hw.pipeline(title="Daily revenue")`, or else the first line of its
docstring; the rest of the docstring describes it. Without either, it shows
the pipeline's name.

Pipelines can run pipelines: `ctx.run(other, **inputs)` waits for that
run's result (raising `hubwagen.RunFailed` if it fails),
`ctx.start_run(other, **inputs)` starts it and returns its ID. Ending a run
cancels the runs it waits for. `ctx.continue_as_new(**inputs)` ends the run
and starts the pipeline again with new inputs and a fresh history, for
pipelines that loop.

```python
@hw.pipeline
def daily(ctx, day: str):
    report = ctx.step(check_quality, day)
    if report["failed"]:
        try:
            approval = ctx.wait_signal("approve", timeout=timedelta(hours=24))
        except hw.SignalTimeout:
            return "not approved"
    return ctx.step(publish, day)
```

```bash
hubwagen signal <run-id> approve --data '{"by": "ana"}'   # or Client.signal_haul(run_id, "approve", {...})
```

A signal sent before the run waits for it is kept; each signal answers one
wait. The wait is held by the server, like a sleep: no worker is busy while
the run waits, and it survives restarts and deploys.

### Schedules

```python
@hw.pipeline(schedule=hw.Schedule("0 3 * * *", interval="daily", catch_up="all"))
def daily_revenue(ctx, interval: hw.Interval):
    day = interval.start.date()          # yesterday (UTC); interval.end is today 00:00 UTC
    ...
```

The server starts a scheduled pipeline once per data interval and passes
it as the `interval` input (`start`, `end`, timezone-aware). Schedules are
in UTC unless you give `timezone="Europe/Berlin"` (IANA): then the cron
fires on that clock and intervals are its days, DST-correct. Whatever the
schedule's timezone, hubwagen shows every time in UTC. `interval` is
`hourly`, `daily`, `weekly` or `monthly` (the previous full period,
DST-correct) or `cron` (previous tick to this one; the default, and what a
plain cron string, `schedule="*/15 * * * *"`, gets). `catch_up` decides
what happens to runs that came due while no server ran: `latest` (default)
starts the newest, `all` every one, `skip` none. The schedule is part of the
build: a new build with another schedule, or none, replaces it.
`hubwagen schedules` lists them. To start one by hand, give it an interval;
"Start a run" in the web UI fills in the latest.

### Subscriptions

```python
@hw.pipeline(subscribe=hw.Subscription("raw.orders", debounce=60, max_batch=100))
def on_new_orders(ctx, versions: list[hw.DataVersion]):
    for v in versions:
        ctx.step(load_into_warehouse, v)
```

New versions in the datasets start the pipeline, a batch at a time: once
none came for `debounce` seconds (default: at once), once `max_batch` wait,
or once the first has waited `max_wait` (default: ten debounces). What a
pipeline's own runs write doesn't start it. `hubwagen schedules` lists
subscriptions too, with how many new versions wait. (A pipeline that names
its input `pallets` gets them under that name.)

### Changing pipeline code while runs are going on

An open run replays on whatever code the workers have now. To change what
a pipeline does without breaking the runs already going, put the change
behind a version marker:

```python
if ctx.version("quality-gate", max=2) >= 2:
    report = ctx.step(check_quality, orders)     # new runs
# runs that started before the marker existed skip it, as they did
```

A run that started on the old code gets `min` (default 1), one that
reaches the marker fresh gets `max`; the choice is recorded and kept. To
change it again, raise `max` (`max=3`, and branch on `>= 3`). Once no open
run is on an old version, raise `min` and delete its branch; a run still
on it becomes stuck rather than taking the wrong path.

## Steps

`@hw.step(...)` options: `memo=True` or `memo="ttl:1h"` (reuse what an
earlier run produced when code, inputs and arguments are unchanged: its
data version, or for a step without an output, its result),
`output=hw.Dataset(name, partition="arg", keep=hw.Keep(versions=3, age=timedelta(days=30)))`
(`keep`: retention; per partition the latest and those kept by either rule
stay, the rest expire; without it every version stays), `version=` (pin the
code version), `timeout=` (seconds), `retry=hw.Retry(limit, backoff,
initial, max)` or an int, `dock=`. Pass data versions as arguments: they
travel as inputs (lineage, and content hashes in the fingerprint), not
inside the arguments. Raise `hubwagen.NonRetryable` for failures retrying
won't fix.

Arguments and results are JSON; `date` and `datetime` round-trip.

### Code versions

A memoized step is reused only while its code version is unchanged. The
version covers the code the step can run: its own body and every function,
class and module-level constant of your project it reaches, in any module
(comments, docstrings, formatting and the `@hw.step(...)` line don't
count); the installed versions of the packages that code uses, with their
requirements; the Python version; and the files under `deps=`. Changing a
helper reruns the steps that call it and no others; bumping pandas reruns
the steps that use pandas; bumping pytest reruns nothing.

It can't see code reached through a computed name
(`importlib.import_module(name)`, `getattr(module, name)`), data a module
reads at import time, or the state of objects. Name such files with
`deps=["config/rules.yaml"]` (relative to the project root, the directory
of `pyproject.toml`), or pin the version yourself with `version="3"`.

`python -m hubwagen.worker --versions` prints what each step's and
pipeline's version covers, to find out why a step reran because "its code
changed".

### Dataframes and tables

A step can return a pandas or Polars DataFrame, a Polars LazyFrame or an
Arrow Table. The worker writes it as Parquet (`data.parquet`) into the
attempt's staging prefix, so it becomes a data version: in the step's
`output=` dataset if it has one, else in its implicit dataset
`lift:<name>`. The pipeline gets a handle (a `DataVersion` whose `.result`
is `{"$frame": "polars", "rows": …, "columns": […]}`), never the data; pass
it to the next step, which gets the data loaded by its parameter's
annotation:

```python
import pandas as pd
import polars as pl

@hw.step(memo=True)
def orders(day: str) -> pl.DataFrame: ...

@hw.step(memo=True)
def totals(orders: pl.LazyFrame) -> pl.DataFrame:     # scans the Parquet
    return orders.group_by("region").agg(pl.col("amount").sum()).collect()

@hw.step
def report(totals: pd.DataFrame) -> str: ...           # converted on load

@hw.pipeline
def daily(ctx, day: str):
    return ctx.step(report, ctx.step(totals, ctx.step(orders, day)))
```

- Annotation `pl.DataFrame`, `pl.LazyFrame`, `pd.DataFrame` or `pa.Table`:
  loaded as that type. `hw.DataVersion`: the handle (files via `.files()`,
  data via `.load(as_=…)`). None: loaded as it was written.
- Memoization works as for files: the version's content hash covers the
  Parquet bytes, so an identical frame upstream lets downstream steps be
  reused. A frame can't be an argument or part of a result; it is returned
  on its own.
- pandas, Polars and pyarrow are imported only when a step uses them; the
  SDK needs none of them. `hw.register_codec(name, types, write, read,
  describe)` adds a type; codecs write Parquet, so each can read what the
  others wrote.
- Needs a server with a bucket (`hubwagen dev` has one).

## Worker

`python -m hubwagen.worker [app.pipelines more.modules] [--server URL]
[--dock NAME] [--lifts N] [--worker-id ID]` imports the modules, registers
the build, replays runs and runs steps (`--lifts`: how many at once), and
sends heartbeats for running steps. The build is the pipelines the modules
define and each step's code version (`version=` or a hash of its source).
It re-executes itself with `PYTHONHASHSEED=0` so set iteration order is the
same in every replay.

Without module names it imports the ones the project lists, so the command
is the same for every project:

```toml
# pyproject.toml
[tool.hubwagen]
modules = ["app.pipelines", "app.backfills"]
```

The worker polls with its build, and the server hands it only the pipelines
and steps that build defines. Projects with different code can therefore
share a server and its docks. The server refuses to start a pipeline that
no registered build defines, so start a worker for it first. If the server
doesn't know the worker's build (a leader that lost it), the worker
registers again.

A run whose work no worker takes says why, in the web UI and in
`hubwagen start --wait`, `replay` and `dev`: no worker is connected to
its dock, the connected workers' code doesn't define the pipeline or step
(restart them with the new code), or they are all busy.

If the server is down when a step finishes, the worker keeps the result,
and keeps the attempt's heartbeats going, until the server takes it. A
restarted leader therefore doesn't rerun the step. Ctrl-C stops polling and
lets running steps finish. Started by `hubwagen dev`, `server` or `workers`,
the worker takes its docks and lifts from the worker group in
`hubwagen.yaml` (`$HUBWAGEN_DOCKS`, `$HUBWAGEN_LIFTS`; flags win), and also
stops that way when that process is gone (`$HUBWAGEN_SUPERVISOR_PID`). `--worker-id` (or `$HUBWAGEN_WORKER_ID`) names
the worker in a run's history; the default is `<hostname>-<pid>`. `--build-json` prints what
the worker registers (its pipelines, steps' code versions, schedules), as
`hubwagen deploy` sends it with a deployment.

### Workers on other machines

A worker needs to reach only the server's API: no shared filesystem and no
storage credentials. On the server's machine it reads and writes the
server's bucket directory directly. Anywhere else (or with an `s3://`
bucket) it moves data through URLs the server hands out for each running
step: what a step writes to `hw.output().path` (a local directory) is
uploaded when it returns, and a data version's `files()`, `path()` and
`load()` download what they read into a local directory first, checked
against the version's manifest. Both directories are removed when the step
ends. Step code is the same either way.

`--data auto` (the default; or `$HUBWAGEN_DATA`) picks: `local` when the
worker finds the server's bucket directory (it holds a `.hubwagen-bucket`
file with the ID the task names), else `remote`. Pass `--data remote` to
force URLs, `--data local` to insist on the directory.

```bash
python -m hubwagen.worker --server http://hubwagen.internal:7070
```

## Testing pipelines

```python
from hubwagen import testing

def test_gate():
    r = testing.run(daily, {"day": "2026-10-24"},
                    steps={"check_quality": {"failed": True}, "build": 42},   # or functions, or exceptions
                    signals={"approve": {"by": "ana"}})
    assert (r.status, r.result) == ("completed", 42)
    assert [c.name for c in r.steps] == ["check_quality", "build"]
```

`testing.run` plays the server in memory: it replays the pipeline, answers
each command from the script (`steps`, `signals`, `versions` for
`latest_version`, `children` for the pipelines it runs; `run_steps=True`
calls the real steps), sleeps on a clock that only moves when the run
waits, and fails if the pipeline isn't deterministic.

Before deploying, replay what the server recorded against the new code:

```bash
python -m hubwagen.testing --replay-recent 50 --server http://hubwagen:7070   # or --replay <run-id>, --pipeline <name>
```

A recorded run the new code contradicts would be stuck after the deploy; it
is reported and the command exits 1.

## Tests

```bash
uv run --no-project --with pytest --python 3.12 pytest -q        # from sdk/python
uv run --no-project --with pytest --with polars --with pandas --with pyarrow --python 3.12 pytest -q   # incl. dataframes
```

`tests/test_frames.py` is skipped without polars, pandas and pyarrow.

`tests/test_replay.py` tests replay on hand-built histories;
`tests/test_dev.py` builds the Go binary (or uses `$HUBWAGEN_BIN`), runs
`hubwagen dev` and a real worker, and runs pipelines end to end.

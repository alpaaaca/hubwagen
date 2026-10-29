# hubwagen Python SDK

Write hauls and lifts as plain Python; a worker runs them against a
hubwagen server. Python ≥ 3.12, no dependencies outside the standard
library.

```python
import hubwagen as hw

@hw.lift(memo=True, output=hw.Dataset("raw.orders", partition="day"))
def extract(day: str) -> dict:
    out = hw.output()                      # this attempt's staging directory
    (out.path / "orders.csv").write_text(fetch_orders(day))
    out.meta["max_ts"] = f"{day}T23:59:00Z"
    return {"rows": 1}

@hw.lift(memo=True, output=hw.Dataset("mart.revenue", partition="day"))
def revenue(orders: hw.Pallet, day: str) -> dict: ...

@hw.haul
def daily_revenue(ctx, day: str):
    orders = ctx.lift(extract, day)        # a Pallet; reused if nothing changed
    return ctx.lift(revenue, orders, day).id
```

```bash
hubwagen dev                                            # server + worker API on 127.0.0.1:7070
python -m hubwagen.worker                               # modules from pyproject.toml; registers the build, polls
hubwagen start daily_revenue --param day=2026-10-24 --wait
hubwagen replay daily_revenue --param day=2026-10-01..2026-10-24
```

## Hauls

A haul is replayed from the top whenever it has something new to see (a
lift finished, a timer fired); each `ctx` call returns its recorded result
or stops the replay until there is one. So haul code must make the same
calls in the same order every time:

- **No clock, randomness, network or files** in haul code. Use `ctx.now()`
  (the time of the newest result the replay has used, the same on every
  replay), `ctx.random()` / `ctx.rng` (seeded by the haul), and lifts.
  While haul code runs, calling `time.time`, `datetime.now`, `date.today`,
  the module-level `random` functions, `uuid1`/`uuid4` or `os.urandom`
  from your code raises `hubwagen.NondeterminismError` with the line, and
  the haul becomes **stuck** (fix the code; the new build resumes it).
  Libraries that read the clock, such as `logging`, are left alone, and so
  are lifts. Network and files aren't guarded: keep them in lifts.
- `python -m hubwagen.lint` checks haul code for all of this before it
  runs (the clock, randomness, set iteration, I/O, `except:`, lifts called
  directly), from the source, for the modules in `pyproject.toml` or the
  paths given. `# noqa: HW003` silences one line.
- **Don't swallow `hubwagen.Blocked`.** It is a `BaseException`, so
  `except Exception:` is safe; a bare `except:` is not.
- `ctx.state` is a dict rebuilt by every replay; it holds what the haul
  computed so far, nothing more.
- A replay that contradicts the logbook (another lift where one was
  recorded) makes the haul **stuck**, with both call sites in its message.
  Deploying fixed code (a new build) resumes it.

`ctx` offers `lift`, `lift_async` + `gather` (fan-out;
`gather(handles, max_parallel=N)` runs at most N of them at once, and
`@hw.haul(max_parallel=N)` caps all of a haul's lifts), `sleep` (durable,
held by the server), `wait_signal` and `version` (below), `latest_pallet` (the newest
committed pallet of a dataset partition, recorded so every replay sees the
same one), `now`, `random` and `state`. A failed lift raises
`hubwagen.LiftFailed` in the haul.

What a lift prints (stdout, stderr) and logs (`logging`) is captured with
it: the web UI shows it on the run's page, step by step, and `hubwagen dev`
prints it marked with the run and the lift; a failure's traceback is part
of it. Capture goes by thread, so output from threads the lift starts, from
subprocesses or from C libraries writing to the file descriptor isn't
captured. Standalone workers still echo it to their terminal, marked with
the lift (`HUBWAGEN_ECHO_LOGS=0` turns that off).

The web UI calls a haul by its title: `@hw.haul(title="Daily revenue")`,
or else the first line of its docstring; the rest of the docstring
describes it. Without either, it shows the haul's name.

Hauls can start hauls: `ctx.haul(other, **params)` waits for the child's
result (raising `hubwagen.ChildFailed` if it fails), `ctx.start_haul(other,
**params)` starts it and returns its ID. Ending a haul cancels the children
it waits for. `ctx.continue_as_new(**params)` ends the haul and starts it
again with new params and a fresh logbook, for hauls that loop.

```python
@hw.haul
def daily(ctx, day: str):
    report = ctx.lift(check_quality, day)
    if report["failed"]:
        try:
            approval = ctx.wait_signal("approve", timeout=timedelta(hours=24))
        except hw.SignalTimeout:
            return "not approved"
    return ctx.lift(publish, day)
```

```bash
hubwagen signal <haul-id> approve --data '{"by": "ana"}'   # or Client.signal_haul(haul_id, "approve", {...})
```

A signal sent before the haul waits for it is kept; each signal answers one
wait. The wait is held by the server, like a sleep: no worker is busy while
the haul waits, and it survives restarts and deploys.

### Schedules

```python
@hw.haul(schedule=hw.Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin", catch_up="all"))
def daily_revenue(ctx, interval: hw.Interval):
    day = interval.start.date()          # yesterday in Berlin; interval.end is today 00:00
    ...
```

The server starts a scheduled haul once per data interval and passes it as
`interval` (`start`, `end`, timezone-aware). `interval` is `hourly`,
`daily`, `weekly` or `monthly` (the previous full period, DST-correct) or
`cron` (previous tick to this one; the default, and what a plain cron
string, `schedule="*/15 * * * *"`, gets). `catch_up` decides what happens
to runs that came due while no server ran: `latest` (default) starts the
newest, `all` every one, `skip` none. The schedule is part of the build: a
new build with another schedule, or none, replaces it.
`hubwagen schedules` lists them.

### Subscriptions

```python
@hw.haul(subscribe=hw.Subscription("raw.orders", debounce=60, max_batch=100))
def on_new_orders(ctx, pallets: list[hw.Pallet]):
    for p in pallets:
        ctx.lift(load_into_warehouse, p)
```

New pallets in the datasets start the haul, a batch at a time: once none
came for `debounce` seconds (default: at once), once `max_batch` wait, or
once the first has waited `max_wait` (default: ten debounces). A haul's
own pallets don't start it. `hubwagen schedules` lists subscriptions too,
with how many pallets wait.

### Changing haul code while hauls run

A running haul replays its logbook on whatever code the workers have now.
To change what a haul does without breaking the ones already running, put
the change behind a version marker:

```python
if ctx.version("quality-gate", max=2) >= 2:
    report = ctx.lift(check_quality, orders)     # new hauls
# hauls that started before the marker existed skip it, as they did
```

A haul that started on the old code gets `min` (default 1), one that
reaches the marker fresh gets `max`; the choice is recorded and kept. To
change it again, raise `max` (`max=3`, and branch on `>= 3`). Once no
running haul is on an old version, raise `min` and delete its branch; a
haul still on it becomes stuck rather than taking the wrong path.

## Lifts

`@hw.lift(...)` options: `memo=True` or `memo="ttl:1h"` (reuse what an
earlier run produced when code, inputs and arguments are unchanged: its
pallet, or for a lift without an output, its result),
`output=hw.Dataset(name, partition="arg", keep=hw.Keep(versions=3, age=timedelta(days=30)))`
(`keep`: retention; per partition the latest and those kept by either rule
stay, the rest expire; without it every version stays), `version=` (pin the
code version), `timeout=` (seconds), `retry=hw.Retry(limit, backoff,
initial, max)` or an int, `dock=`. Pass pallets as arguments: they travel
as inputs (lineage, and content hashes in the fingerprint), not inside the
arguments. Raise `hubwagen.NonRetryable` for failures retrying won't fix.

Arguments and results are JSON; `date` and `datetime` round-trip.

### Code versions

A memoized lift is reused only while its code version is unchanged. The
version covers the code the lift can run: its own body and every function,
class and module-level constant of your project it reaches, in any module
(comments, docstrings, formatting and the `@hw.lift(...)` line don't
count); the installed versions of the packages that code uses, with their
requirements; the Python version; and the files under `deps=`. Changing a
helper reruns the lifts that call it and no others; bumping pandas reruns
the lifts that use pandas; bumping pytest reruns nothing.

It can't see code reached through a computed name
(`importlib.import_module(name)`, `getattr(module, name)`), data a module
reads at import time, or the state of objects. Name such files with
`deps=["config/rules.yaml"]` (relative to the project root, the directory
of `pyproject.toml`), or pin the version yourself with `version="3"`.

`python -m hubwagen.worker --versions` prints what each lift's and haul's
version covers, to find out why a lift reran with `code changed`.

### Dataframes and tables

A lift can return a pandas or Polars DataFrame, a Polars LazyFrame or an
Arrow Table. The worker writes it as Parquet (`data.parquet`) into the
attempt's staging prefix, so it becomes a pallet: in the lift's
`output=` dataset if it has one, else in its implicit dataset
`lift:<name>`. The haul gets a handle (a `Pallet` whose `.result` is
`{"$frame": "polars", "rows": …, "columns": […]}`), never the data; pass
it to the next lift, which gets the data loaded by its parameter's
annotation:

```python
import pandas as pd
import polars as pl

@hw.lift(memo=True)
def orders(day: str) -> pl.DataFrame: ...

@hw.lift(memo=True)
def totals(orders: pl.LazyFrame) -> pl.DataFrame:     # scans the Parquet
    return orders.group_by("region").agg(pl.col("amount").sum()).collect()

@hw.lift
def report(totals: pd.DataFrame) -> str: ...           # converted on load

@hw.haul
def daily(ctx, day: str):
    return ctx.lift(report, ctx.lift(totals, ctx.lift(orders, day)))
```

- Annotation `pl.DataFrame`, `pl.LazyFrame`, `pd.DataFrame` or `pa.Table`:
  loaded as that type. `hw.Pallet`: the handle (files via `.files()`,
  data via `.load(as_=…)`). None: loaded as it was written.
- Memoization works as for files: the pallet's content hash covers the
  Parquet bytes, so an identical frame upstream lets downstream lifts be
  reused. A frame can't be an argument or part of a result; it is returned
  on its own.
- pandas, Polars and pyarrow are imported only when a lift uses them; the
  SDK needs none of them. `hw.register_codec(name, types, write, read,
  describe)` adds a type; codecs write Parquet, so each can read what the
  others wrote.
- Needs a server with a bucket (`hubwagen dev` has one).

## Worker

`python -m hubwagen.worker [app.hauls more.modules] [--server URL]
[--dock NAME] [--lifts N] [--worker-id ID]` imports the modules, registers
the build, replays hauls and runs lifts, and sends heartbeats for running
lifts. The build is the hauls the modules define and each lift's code
version (`version=` or a hash of its source). It re-executes itself with
`PYTHONHASHSEED=0` so set iteration order is the same in every replay.

Without module names it imports the ones the project lists, so the command
is the same for every project:

```toml
# pyproject.toml
[tool.hubwagen]
modules = ["app.hauls", "app.backfills"]
```

The worker polls with its build, and the server hands it only the hauls and
lifts that build defines. Projects with different code can therefore share
a server and its docks. The server refuses to start a haul that no
registered build defines, so start a worker for it first. If the server
doesn't know the worker's build (a leader that lost it), the worker
registers again.

If the server is down when a lift finishes, the worker keeps the result,
and keeps the attempt's heartbeats going, until the server takes it. A
restarted leader therefore doesn't rerun the lift. Ctrl-C stops polling and
lets running lifts finish. Started by `hubwagen dev` or `hubwagen workers`,
the worker also stops that way when that process is gone
(`$HUBWAGEN_SUPERVISOR_PID`). `--worker-id` (or `$HUBWAGEN_WORKER_ID`) names
the worker in the logbook; the default is `<hostname>-<pid>`.

## Testing hauls

```python
from hubwagen import testing

def test_gate():
    r = testing.run(daily, {"day": "2026-10-24"},
                    lifts={"check_quality": {"failed": True}, "build": 42},   # or functions, or exceptions
                    signals={"approve": {"by": "ana"}})
    assert (r.status, r.result) == ("completed", 42)
    assert [c.name for c in r.lifts] == ["check_quality", "build"]
```

`testing.run` plays the server in memory: it replays the haul, answers
each command from the script (`lifts`, `signals`, `pallets` for
`latest_pallet`, `children` for child hauls; `run_lifts=True` calls the
real lifts), sleeps on a clock that only moves when the haul waits, and
fails if the haul isn't deterministic.

Before deploying, replay what the server recorded against the new code:

```bash
python -m hubwagen.testing --replay-recent 50 --server http://hubwagen:7070   # or --replay <haul-id>, --haul <name>
```

A haul whose logbook the new code contradicts would be stuck after the
deploy; it is reported and the command exits 1.

## Tests

```bash
uv run --no-project --with pytest --python 3.12 pytest -q        # from sdk/python
uv run --no-project --with pytest --with polars --with pandas --with pyarrow --python 3.12 pytest -q   # incl. dataframes
```

`tests/test_frames.py` is skipped without polars, pandas and pyarrow.

`tests/test_replay.py` tests replay on hand-built logbooks;
`tests/test_dev.py` builds the Go binary (or uses `$HUBWAGEN_BIN`), runs
`hubwagen dev` and a real worker, and runs hauls end to end.

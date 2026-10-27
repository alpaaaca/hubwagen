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
  Runtime guards for this arrive with #25.
- **Don't swallow `hubwagen.Blocked`.** It is a `BaseException`, so
  `except Exception:` is safe; a bare `except:` is not.
- `ctx.state` is a dict rebuilt by every replay; it holds what the haul
  computed so far, nothing more.
- A replay that contradicts the logbook (another lift where one was
  recorded) makes the haul **stuck**, with both call sites in its message.
  Deploying fixed code (a new build) resumes it.

`ctx` offers `lift`, `lift_async` + `gather` (fan-out), `sleep` (durable,
held by the server), `latest_pallet` (the newest committed pallet of a
dataset partition, recorded so every replay sees the same one), `now`,
`random` and `state`. A failed lift raises `hubwagen.LiftFailed` in the
haul.

## Lifts

`@hw.lift(...)` options: `memo=True` or `memo="ttl:1h"` (only with an
output: reuse the earlier pallet when code, inputs and arguments are
unchanged), `output=hw.Dataset(name, partition="arg")`, `version=` (pin the
code version), `timeout=` (seconds), `retry=hw.Retry(limit, backoff,
initial, max)` or an int, `dock=`. Pass pallets as arguments: they travel
as inputs (lineage, and content hashes in the fingerprint), not inside the
arguments. Raise `hubwagen.NonRetryable` for failures retrying won't fix.

Arguments and results are JSON; `date` and `datetime` round-trip.

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

## Tests

```bash
uv run --no-project --with pytest --python 3.12 pytest -q        # from sdk/python
```

`tests/test_replay.py` tests replay on hand-built logbooks;
`tests/test_dev.py` builds the Go binary (or uses `$HUBWAGEN_BIN`), runs
`hubwagen dev` and a real worker, and runs hauls end to end.

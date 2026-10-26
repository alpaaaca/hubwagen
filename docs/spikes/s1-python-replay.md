# Spike S1: Python replay prototype

Issue [#10](https://github.com/alpaaaca/hubwagen/issues/10). Prototype and
scripts: [`spikes/s1-python-replay/`](../../spikes/s1-python-replay/)
(`python3 bench.py`, `python3 guards_check.py`). Measured on an Apple M4,
Python 3.14.7 (Homebrew) and 3.13.2 (pyenv); the two agree within noise.
Not run on 3.12.

## Questions and answers

### How slow is full replay-until-blocked for a 1,000-event logbook?

Fast. Decoding the logbook dominates; running the haul function is cheap.

| Logbook | Size | Decode | Decode + replay |
|---|---|---|---|
| 143 lifts, 1,002 events | 0.18 MB | 0.7 ms | 0.8 ms |
| 1,000 lifts, 7,001 events | 1.25 MB | 5.3 ms | 5.8 ms |
| 5,000 lifts, 35,001 events | 6.4 MB | 29 ms | 33 ms |
| fan-out: `gather` over 10,000 `lift_async` | | | 50 ms |

The catch is the haul's **lifetime**: a sequential haul replays once per
result, each time over the whole logbook so far, so total work is O(n²).

| Sequential haul | Worker CPU over its life | Logbook bytes sent |
|---|---|---|
| 143 lifts (~1,000 events) | 58 ms | 13 MB |
| 500 lifts | 0.7 s | — |
| 1,000 lifts | 2.9 s | 627 MB |

Sending only the events a replay reads (`HaulStarted`, `LiftFinished`,
`TimerFired`, later signals and markers) instead of every event cuts a
1,000-lift logbook from 1.25 MB to 0.24 MB and the replay from 5.8 to
2.2 ms; the 1,000-lift haul would send ~118 MB instead of 627 MB.

### Does patching `time.time`, `datetime.now`, `random.*` and `uuid.uuid4` catch typical mistakes without breaking libraries?

**No — patching module attributes is the wrong tool.** `guards_check.py`
runs 13 typical mistakes and 6 legitimate uses (stdlib code that reads the
clock or randomness internally, seeded `Random`, date arithmetic, `uuid5`)
under two guards:

| | Mistakes caught (of 13) | Legitimate uses broken (of 6) | Overhead, 1,000-lift replay |
|---|---|---|---|
| A. Patch module attributes while haul code runs | 8 | 2 (`logging`, `email.utils.make_msgid`) | none |
| B. `sys.monitoring` CALL events, raise only when the caller is user code | **13** | **0** | +1.2–1.5 ms (~20%) |

Patching misses everything bound before the patch (`from time import
time`, `from datetime import datetime`, `from random import randint`) and
can't tell user code from library code, so `logging.warning()` in a haul
raises (a `LogRecord` reads `time.time_ns()`).

`sys.monitoring` (Python ≥ 3.12) sees the callable itself, however it was
imported, and the calling code object, so it only flags calls made *from*
user code; library locations are `DISABLE`d after their first event and cost
nothing afterwards. Errors name the call site:
`datetime.now() called at app/hauls.py:24; use ctx.now()/ctx.random() or a lift`.

Two portability details the spike hit:

- **Library paths:** Homebrew's `sysconfig` paths go through a symlink
  (`/opt/homebrew/opt/…`) while module files report the Cellar path;
  compare `os.path.realpath`s, or every stdlib file looks like user code.
- **Call shapes differ by version:** 3.13 reports `random.choice(…)` as a
  bound method; 3.14 as the function `Random.choice` with the instance as
  `arg0`. Check both.

### Can the call site be captured cheaply for every `ctx.*` call?

Yes, with `sys._getframe`: **0.2 µs** per call (0.2 ms per 1,000 calls).
`traceback.extract_stack(limit=…)` costs 7 µs and `inspect.stack()` 77 µs;
don't use either.

## Found along the way

- **`Blocked` must be a `BaseException`.** Haul code with
  `try: ctx.lift(…) except Exception: …` swallows an `Exception`-based
  blocked signal and carries on with a fallback value, silently diverging
  from the logbook. With `BaseException` the replay blocks as it should.
- **Set iteration order changes between processes.** Iterating a set of
  strings gave 6 different orders in 6 runs by default and 1 with
  `PYTHONHASHSEED=0`. A haul that iterates a set to issue lifts replays in
  another order in another worker process. The guards can't see this; a
  fixed hash seed fixes it.
- Not covered by any runtime guard: environment variables, files, network.
  Those stay with `hubwagen lint` and the docs. `time.monotonic` and
  `perf_counter` are left alone (libraries use them for timeouts).

## Recommendations for the SDK (#24, #25)

1. **Keep full replay, no sticky caching** in v1: a replay of a
   realistic logbook costs about a millisecond.
2. **Bound the O(n²):** have `PollHaulTask` return only replay-relevant
   events (a server change with the SDK work), and document that hauls with
   more than ~1,000 sequential commands should use `continue_as_new` or
   fan-out (`lift_async` + `gather`, one replay for all). Incremental
   logbooks for sticky workers only if real hauls need them.
3. **Guard with `sys.monitoring`, not patching.** Forbid `time.time`,
   `time.time_ns`, `datetime.now/utcnow/today`, `date.today`, the
   module-level `random` functions, `uuid.uuid1/uuid4`, `os.urandom`; only
   when called from user code; compare real paths; handle both call shapes;
   report the exact line from the event's offset (`co_positions`), not the
   function's first line; take a free tool ID rather than `PROFILER_ID`, so
   profilers and coverage keep working. This makes **Python ≥ 3.12** a
   requirement of the SDK.
4. **Capture call sites with `sys._getframe`** on every `ctx.*` call; it is
   free.
5. **`Blocked` is a `BaseException`**, and the docs say why.
6. **The worker fixes the hash seed:** `python -m hubwagen.worker` re-execs
   itself with `PYTHONHASHSEED=0` if it isn't set, and the hash seed is part
   of what a build pins, along with the Python version.

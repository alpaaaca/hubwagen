"""Runs the integration tests run on a real worker."""

import logging
import random
import sys
import time
from pathlib import Path

import moksha as mk


@mk.step
def add(a: int, b: int) -> int:
    return a + b


@mk.step
def other(a: int, b: int) -> int:
    return a - b


@mk.pipeline
def arithmetic(ctx, x: int):
    y = ctx.step(add, x, 1)
    ctx.sleep(1)
    started = ctx.now()
    z = ctx.step(add, y, 10)
    ctx.state["last"] = z
    return {"z": z, "after_sleep": started.isoformat(), "draw": ctx.random()}


@mk.step(memo=True)
def exchange_rate(day: str) -> float:
    """A result without an output, memoized: a draw, so a rerun shows."""
    return random.random()


@mk.pipeline
def rates(ctx, day: str):
    return ctx.step(exchange_rate, day)


@mk.step(memo=True, output=mk.Dataset("raw.orders", partition="day"))
def extract(day: str) -> dict:
    out = mk.output()
    (out.path / "orders.csv").write_text(f"order,day\n1,{day}\n")
    out.meta["max_ts"] = f"{day}T23:59:00Z"
    out.stats["rows"] = 1
    return {"rows": 1}


@mk.step(memo=True, output=mk.Dataset("mart.revenue", partition="day"))
def revenue(orders: mk.DataVersion, day: str) -> dict:
    out = mk.output()
    (out.path / "revenue.csv").write_text(f"built from {orders.id}\n")
    return {"from": orders.id}


@mk.pipeline
def daily(ctx, day: str):
    orders = ctx.step(extract, day)
    rev = ctx.step(revenue, orders, day)
    return {"orders": orders.id, "revenue": rev.id, "reused": [orders.reused, rev.reused], "from": rev.result["from"]}


@mk.step(output=mk.Dataset("raw.events"))
def fetch_since(cursor: int) -> int:
    out = mk.output()
    new = cursor + 1
    (out.path / "events.csv").write_text(f"event {new}\n")
    out.meta["cursor"] = new
    return new


@mk.pipeline
def ingest(ctx):
    latest = ctx.latest_version(mk.Dataset("raw.events"))
    cursor = latest.meta["cursor"] if latest else 0
    return ctx.step(fetch_since, cursor).result


@mk.step(retry=mk.Retry(limit=1, initial=0.1))
def flaky(marker: str) -> str:
    p = Path(marker)
    if not p.exists():
        p.write_text("tried once")
        raise RuntimeError("the first attempt fails")
    return "second attempt"


@mk.pipeline
def retried(ctx, marker: str):
    return ctx.step(flaky, marker)


@mk.step
def gated(started: str, release: str) -> str:
    """Signals that it runs, then waits for the release file."""
    Path(started).touch()
    while not Path(release).exists():
        time.sleep(0.02)
    return "released"


@mk.pipeline
def wait_for_release(ctx, started: str, release: str):
    return ctx.step(gated, started, release)


@mk.step(retry=3)
def bad_input() -> None:
    raise mk.NonRetryable("this will never work")


@mk.pipeline
def handles_failure(ctx):
    try:
        ctx.step(bad_input)
    except mk.StepFailed as e:
        return f"caught {e.reason}"


_replays = {"n": 0}


@mk.pipeline
def nondeterministic(ctx):
    _replays["n"] += 1  # module state: differs between replays
    first = add if _replays["n"] == 1 else other
    ctx.step(first, 1, 1)
    ctx.step(add, 2, 2)


@mk.step(output=mk.Dataset("mart.copy"))
def copy_orders(orders: mk.DataVersion) -> dict:
    out = mk.output()
    names = []
    for f in orders.files():
        (out.path / f.name).write_text(f.read_text().upper())
        names.append(f.name)
    return {"files": names, "manifest_files": len(orders.manifest()["files"]),
            "first": orders.path(names[0]).read_text()}


@mk.pipeline
def reads_inputs(ctx, day: str):
    orders = ctx.step(extract, day)
    return ctx.step(copy_orders, orders).result


@mk.pipeline
def reads_clock(ctx):
    """Reads the clock in pipeline code: the guard makes the run stuck."""
    return ctx.step(add, int(time.time()) % 2, 0)


@mk.pipeline
def approval(ctx, timeout: float | None = None):
    """Waits for an `approve` signal; returns its payload, or "timed out"."""
    try:
        decision = ctx.wait_signal("approve", timeout=timeout)
    except mk.SignalTimeout:
        return "timed out"
    return {"decision": decision, "sum": ctx.step(add, decision["n"], 1)}


@mk.step
def timed(marker_dir: str, i: int) -> int:
    """Records when it ran, for checking how many ran at once."""
    start = time.monotonic()
    time.sleep(0.3)
    Path(marker_dir, f"{i}.txt").write_text(f"{start} {time.monotonic()}")
    return i


@mk.pipeline
def limited_fanout(ctx, marker_dir: str, n: int, limit: int):
    return ctx.gather([ctx.step_async(timed, marker_dir, i) for i in range(n)], max_parallel=limit)


@mk.pipeline
def child_sum(ctx, a: int, b: int):
    return ctx.step(add, a, b)


@mk.pipeline
def child_fails(ctx):
    raise ValueError("no such day")


@mk.pipeline
def countdown(ctx, n: int):
    """Continues as new until n reaches 0."""
    if n > 0:
        ctx.continue_as_new(n=n - 1)
    return "liftoff"


@mk.pipeline
def family(ctx, n: int):
    total = ctx.run(child_sum, a=n, b=1)
    try:
        ctx.run(child_fails)
        failed = None
    except mk.RunFailed as e:
        failed = [e.status, e.reason, e.message]
    side = ctx.start_run(child_sum, a=0, b=0)
    chain = ctx.run(countdown, n=2)
    return {"total": total, "failed": failed, "side": side, "chain": chain}


@mk.pipeline(schedule=mk.Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin", catch_up="all"))
def nightly(ctx, interval: mk.Interval):
    return ctx.step(add, interval.start.day, 0)


@mk.step(output=mk.Dataset("feed.orders"))
def feed_extract(n: int) -> int:
    (mk.output().path / "orders.csv").write_text(f"order {n}\n")
    return n


@mk.pipeline
def feed_producer(ctx, n: int):
    return ctx.step(feed_extract, n).id


@mk.step(output=mk.Dataset("feed.summary"))
def feed_summarize(data_version_ids: list) -> int:
    out = mk.output()
    (out.path / "summary.txt").write_text(",".join(data_version_ids))
    out.meta["data_versions"] = data_version_ids
    return len(data_version_ids)


@mk.pipeline(subscribe=mk.Subscription("feed.orders", debounce=1))
def feed_consumer(ctx, versions: list):
    """Started by new feed.orders data versions, a second's quiet after the last."""
    return ctx.step(feed_summarize, [p.id for p in versions]).id


@mk.pipeline
def feed_latest(ctx):
    p = ctx.latest_version("feed.summary")
    return p.meta["data_versions"] if p else None


@mk.step
def m4_quality(day: str) -> dict:
    return {"failed": True, "day": day}


@mk.step
def m4_region(day: str, region: str) -> int:
    return len(region)


@mk.pipeline(schedule=mk.Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin"), max_parallel=4)
def m4_daily(ctx, interval: mk.Interval):
    """The architecture's daily batch: a quality gate that waits for approval,
    then a fan-out with a limit."""
    day = interval.start.date().isoformat()
    approval = {"by": None}
    if ctx.step(m4_quality, day)["failed"]:
        approval = ctx.wait_signal("approve", timeout=3600)
    regions = ["eu", "us", "apac", "latam", "mena"]
    sizes = ctx.gather([ctx.step_async(m4_region, day, r) for r in regions], max_parallel=2)
    return {"day": day, "approved_by": approval["by"], "total": sum(sizes)}


@mk.step(output=mk.Dataset("keep.events", keep=mk.Keep(versions=1)))
def keep_event(n: int) -> int:
    (mk.output().path / "event.txt").write_text(str(n))
    return n


@mk.pipeline
def keep_producer(ctx, n: int):
    return ctx.step(keep_event, n).id


_log = logging.getLogger("example.chatty")


@mk.step
def chatty(n: int) -> int:
    """Prints, logs and fails, for the captured output."""
    print("loading", n, "rows")
    _log.warning("row %d looks odd", 2)
    sys.stderr.write("partial ")
    sys.stderr.write("line\n")
    print("no newline at the end", end="")
    if n < 0:
        raise ValueError("n must not be negative")
    return n


@mk.pipeline
def chatter(ctx, n: int):
    return ctx.step(chatty, n)

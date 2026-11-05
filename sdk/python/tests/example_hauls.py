"""Hauls the integration tests run on a real worker."""

import logging
import random
import sys
import time
from pathlib import Path

import moksha as mk


@mk.lift
def add(a: int, b: int) -> int:
    return a + b


@mk.lift
def other(a: int, b: int) -> int:
    return a - b


@mk.haul
def arithmetic(ctx, x: int):
    y = ctx.lift(add, x, 1)
    ctx.sleep(1)
    started = ctx.now()
    z = ctx.lift(add, y, 10)
    ctx.state["last"] = z
    return {"z": z, "after_sleep": started.isoformat(), "draw": ctx.random()}


@mk.lift(memo=True)
def exchange_rate(day: str) -> float:
    """A result without an output, memoized: a draw, so a rerun shows."""
    return random.random()


@mk.haul
def rates(ctx, day: str):
    return ctx.lift(exchange_rate, day)


@mk.lift(memo=True, output=mk.Dataset("raw.orders", partition="day"))
def extract(day: str) -> dict:
    out = mk.output()
    (out.path / "orders.csv").write_text(f"order,day\n1,{day}\n")
    out.meta["max_ts"] = f"{day}T23:59:00Z"
    out.stats["rows"] = 1
    return {"rows": 1}


@mk.lift(memo=True, output=mk.Dataset("mart.revenue", partition="day"))
def revenue(orders: mk.Pallet, day: str) -> dict:
    out = mk.output()
    (out.path / "revenue.csv").write_text(f"built from {orders.id}\n")
    return {"from": orders.id}


@mk.haul
def daily(ctx, day: str):
    orders = ctx.lift(extract, day)
    rev = ctx.lift(revenue, orders, day)
    return {"orders": orders.id, "revenue": rev.id, "reused": [orders.reused, rev.reused], "from": rev.result["from"]}


@mk.lift(output=mk.Dataset("raw.events"))
def fetch_since(cursor: int) -> int:
    out = mk.output()
    new = cursor + 1
    (out.path / "events.csv").write_text(f"event {new}\n")
    out.meta["cursor"] = new
    return new


@mk.haul
def ingest(ctx):
    latest = ctx.latest_pallet(mk.Dataset("raw.events"))
    cursor = latest.meta["cursor"] if latest else 0
    return ctx.lift(fetch_since, cursor).result


@mk.lift(retry=mk.Retry(limit=1, initial=0.1))
def flaky(marker: str) -> str:
    p = Path(marker)
    if not p.exists():
        p.write_text("tried once")
        raise RuntimeError("the first attempt fails")
    return "second attempt"


@mk.haul
def retried(ctx, marker: str):
    return ctx.lift(flaky, marker)


@mk.lift
def gated(started: str, release: str) -> str:
    """Signals that it runs, then waits for the release file."""
    Path(started).touch()
    while not Path(release).exists():
        time.sleep(0.02)
    return "released"


@mk.haul
def wait_for_release(ctx, started: str, release: str):
    return ctx.lift(gated, started, release)


@mk.lift(retry=3)
def bad_input() -> None:
    raise mk.NonRetryable("this will never work")


@mk.haul
def handles_failure(ctx):
    try:
        ctx.lift(bad_input)
    except mk.LiftFailed as e:
        return f"caught {e.reason}"


_replays = {"n": 0}


@mk.haul
def nondeterministic(ctx):
    _replays["n"] += 1  # module state: differs between replays
    first = add if _replays["n"] == 1 else other
    ctx.lift(first, 1, 1)
    ctx.lift(add, 2, 2)


@mk.lift(output=mk.Dataset("mart.copy"))
def copy_orders(orders: mk.Pallet) -> dict:
    out = mk.output()
    names = []
    for f in orders.files():
        (out.path / f.name).write_text(f.read_text().upper())
        names.append(f.name)
    return {"files": names, "manifest_files": len(orders.manifest()["files"]),
            "first": orders.path(names[0]).read_text()}


@mk.haul
def reads_inputs(ctx, day: str):
    orders = ctx.lift(extract, day)
    return ctx.lift(copy_orders, orders).result


@mk.haul
def reads_clock(ctx):
    """Reads the clock in haul code: the guard makes the haul stuck."""
    return ctx.lift(add, int(time.time()) % 2, 0)


@mk.haul
def approval(ctx, timeout: float | None = None):
    """Waits for an `approve` signal; returns its payload, or "timed out"."""
    try:
        decision = ctx.wait_signal("approve", timeout=timeout)
    except mk.SignalTimeout:
        return "timed out"
    return {"decision": decision, "sum": ctx.lift(add, decision["n"], 1)}


@mk.lift
def timed(marker_dir: str, i: int) -> int:
    """Records when it ran, for checking how many ran at once."""
    start = time.monotonic()
    time.sleep(0.3)
    Path(marker_dir, f"{i}.txt").write_text(f"{start} {time.monotonic()}")
    return i


@mk.haul
def limited_fanout(ctx, marker_dir: str, n: int, limit: int):
    return ctx.gather([ctx.lift_async(timed, marker_dir, i) for i in range(n)], max_parallel=limit)


@mk.haul
def child_sum(ctx, a: int, b: int):
    return ctx.lift(add, a, b)


@mk.haul
def child_fails(ctx):
    raise ValueError("no such day")


@mk.haul
def countdown(ctx, n: int):
    """Continues as new until n reaches 0."""
    if n > 0:
        ctx.continue_as_new(n=n - 1)
    return "liftoff"


@mk.haul
def family(ctx, n: int):
    total = ctx.haul(child_sum, a=n, b=1)
    try:
        ctx.haul(child_fails)
        failed = None
    except mk.ChildFailed as e:
        failed = [e.status, e.reason, e.message]
    side = ctx.start_haul(child_sum, a=0, b=0)
    chain = ctx.haul(countdown, n=2)
    return {"total": total, "failed": failed, "side": side, "chain": chain}


@mk.haul(schedule=mk.Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin", catch_up="all"))
def nightly(ctx, interval: mk.Interval):
    return ctx.lift(add, interval.start.day, 0)


@mk.lift(output=mk.Dataset("feed.orders"))
def feed_extract(n: int) -> int:
    (mk.output().path / "orders.csv").write_text(f"order {n}\n")
    return n


@mk.haul
def feed_producer(ctx, n: int):
    return ctx.lift(feed_extract, n).id


@mk.lift(output=mk.Dataset("feed.summary"))
def feed_summarize(pallet_ids: list) -> int:
    out = mk.output()
    (out.path / "summary.txt").write_text(",".join(pallet_ids))
    out.meta["pallets"] = pallet_ids
    return len(pallet_ids)


@mk.haul(subscribe=mk.Subscription("feed.orders", debounce=1))
def feed_consumer(ctx, pallets: list):
    """Started by new feed.orders pallets, a second's quiet after the last."""
    return ctx.lift(feed_summarize, [p.id for p in pallets]).id


@mk.haul
def feed_latest(ctx):
    p = ctx.latest_pallet("feed.summary")
    return p.meta["pallets"] if p else None


@mk.lift
def m4_quality(day: str) -> dict:
    return {"failed": True, "day": day}


@mk.lift
def m4_region(day: str, region: str) -> int:
    return len(region)


@mk.haul(schedule=mk.Schedule("0 3 * * *", interval="daily", timezone="Europe/Berlin"), max_parallel=4)
def m4_daily(ctx, interval: mk.Interval):
    """The architecture's daily batch: a quality gate that waits for approval,
    then a fan-out with a limit."""
    day = interval.start.date().isoformat()
    approval = {"by": None}
    if ctx.lift(m4_quality, day)["failed"]:
        approval = ctx.wait_signal("approve", timeout=3600)
    regions = ["eu", "us", "apac", "latam", "mena"]
    sizes = ctx.gather([ctx.lift_async(m4_region, day, r) for r in regions], max_parallel=2)
    return {"day": day, "approved_by": approval["by"], "total": sum(sizes)}


@mk.lift(output=mk.Dataset("keep.events", keep=mk.Keep(versions=1)))
def keep_event(n: int) -> int:
    (mk.output().path / "event.txt").write_text(str(n))
    return n


@mk.haul
def keep_producer(ctx, n: int):
    return ctx.lift(keep_event, n).id


_log = logging.getLogger("example.chatty")


@mk.lift
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


@mk.haul
def chatter(ctx, n: int):
    return ctx.lift(chatty, n)

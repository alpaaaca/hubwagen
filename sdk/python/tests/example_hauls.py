"""Hauls the integration tests run on a real worker."""

import random
import time
from pathlib import Path

import hubwagen as hw


@hw.lift
def add(a: int, b: int) -> int:
    return a + b


@hw.lift
def other(a: int, b: int) -> int:
    return a - b


@hw.haul
def arithmetic(ctx, x: int):
    y = ctx.lift(add, x, 1)
    ctx.sleep(1)
    started = ctx.now()
    z = ctx.lift(add, y, 10)
    ctx.state["last"] = z
    return {"z": z, "after_sleep": started.isoformat(), "draw": ctx.random()}


@hw.lift(memo=True)
def exchange_rate(day: str) -> float:
    """A result without an output, memoized: a draw, so a rerun shows."""
    return random.random()


@hw.haul
def rates(ctx, day: str):
    return ctx.lift(exchange_rate, day)


@hw.lift(memo=True, output=hw.Dataset("raw.orders", partition="day"))
def extract(day: str) -> dict:
    out = hw.output()
    (out.path / "orders.csv").write_text(f"order,day\n1,{day}\n")
    out.meta["max_ts"] = f"{day}T23:59:00Z"
    out.stats["rows"] = 1
    return {"rows": 1}


@hw.lift(memo=True, output=hw.Dataset("mart.revenue", partition="day"))
def revenue(orders: hw.Pallet, day: str) -> dict:
    out = hw.output()
    (out.path / "revenue.csv").write_text(f"built from {orders.id}\n")
    return {"from": orders.id}


@hw.haul
def daily(ctx, day: str):
    orders = ctx.lift(extract, day)
    rev = ctx.lift(revenue, orders, day)
    return {"orders": orders.id, "revenue": rev.id, "reused": [orders.reused, rev.reused], "from": rev.result["from"]}


@hw.lift(output=hw.Dataset("raw.events"))
def fetch_since(cursor: int) -> int:
    out = hw.output()
    new = cursor + 1
    (out.path / "events.csv").write_text(f"event {new}\n")
    out.meta["cursor"] = new
    return new


@hw.haul
def ingest(ctx):
    latest = ctx.latest_pallet(hw.Dataset("raw.events"))
    cursor = latest.meta["cursor"] if latest else 0
    return ctx.lift(fetch_since, cursor).result


@hw.lift(retry=hw.Retry(limit=1, initial=0.1))
def flaky(marker: str) -> str:
    p = Path(marker)
    if not p.exists():
        p.write_text("tried once")
        raise RuntimeError("the first attempt fails")
    return "second attempt"


@hw.haul
def retried(ctx, marker: str):
    return ctx.lift(flaky, marker)


@hw.lift
def gated(started: str, release: str) -> str:
    """Signals that it runs, then waits for the release file."""
    Path(started).touch()
    while not Path(release).exists():
        time.sleep(0.02)
    return "released"


@hw.haul
def wait_for_release(ctx, started: str, release: str):
    return ctx.lift(gated, started, release)


@hw.lift(retry=3)
def bad_input() -> None:
    raise hw.NonRetryable("this will never work")


@hw.haul
def handles_failure(ctx):
    try:
        ctx.lift(bad_input)
    except hw.LiftFailed as e:
        return f"caught {e.reason}"


_replays = {"n": 0}


@hw.haul
def nondeterministic(ctx):
    _replays["n"] += 1  # module state: differs between replays
    first = add if _replays["n"] == 1 else other
    ctx.lift(first, 1, 1)
    ctx.lift(add, 2, 2)


@hw.lift(output=hw.Dataset("mart.copy"))
def copy_orders(orders: hw.Pallet) -> dict:
    out = hw.output()
    names = []
    for f in orders.files():
        (out.path / f.name).write_text(f.read_text().upper())
        names.append(f.name)
    return {"files": names, "manifest_files": len(orders.manifest()["files"]),
            "first": orders.path(names[0]).read_text()}


@hw.haul
def reads_inputs(ctx, day: str):
    orders = ctx.lift(extract, day)
    return ctx.lift(copy_orders, orders).result


@hw.haul
def reads_clock(ctx):
    """Reads the clock in haul code: the guard makes the haul stuck."""
    return ctx.lift(add, int(time.time()) % 2, 0)


@hw.haul
def approval(ctx, timeout: float | None = None):
    """Waits for an `approve` signal; returns its payload, or "timed out"."""
    try:
        decision = ctx.wait_signal("approve", timeout=timeout)
    except hw.SignalTimeout:
        return "timed out"
    return {"decision": decision, "sum": ctx.lift(add, decision["n"], 1)}

"""The replay engine on hand-built logbooks: no server."""

import datetime as dt
import json

import pytest

import hubwagen as hw
from hubwagen import _model, _replay


def ev(type_, payload, time="2026-10-26T10:00:00Z"):
    return {"type": type_, "time": time, "payloadJson": json.dumps(payload)}


START = ev("HaulStarted", {"name": "h", "params": {}}, "2026-10-26T09:00:00Z")


def finished(seq, result=None, status="completed", time="2026-10-26T10:00:00Z", **kw):
    return ev("LiftFinished", {"seq": seq, "status": status, "result": result, **kw}, time)


@hw.lift
def add(a: int, b: int) -> int:
    return a + b


@hw.lift(memo=True, output=hw.Dataset("raw.orders", partition="day"), timeout=60, retry=hw.Retry(limit=2, initial=5))
def extract(day: dt.date) -> int:
    return 1


@hw.lift(memo="ttl:1h", output=hw.Dataset("mart.revenue"))
def build(orders: hw.Pallet, factor: float = 1.0) -> int:
    return 2


def run(fn, logbook, params=None, haul_id="h1"):
    # Not registered: test functions share names.
    return _replay.replay(_model.HaulDef(fn.__name__, fn, None), haul_id, params or {}, [START, *logbook])


def test_blocks_at_the_first_call_without_a_result():
    def seq_haul(ctx):
        x = ctx.lift(add, 1, 2)
        return ctx.lift(add, x, 10)

    r = run(seq_haul, [])
    assert r.outcome is None
    assert [c["lift"]["name"] for c in r.commands] == ["add"]
    assert json.loads(r.commands[0]["lift"]["argsJson"]) == {"a": 1, "b": 2}
    assert r.commands[0]["source"] == f"{__file__}:{seq_haul.__code__.co_firstlineno + 1}", "the line of the call"

    r = run(seq_haul, [finished(1, 3)])
    assert [c["seq"] for c in r.commands] == [1, 2]
    assert json.loads(r.commands[1]["lift"]["argsJson"]) == {"a": 3, "b": 10}
    assert r.outcome is None

    r = run(seq_haul, [finished(1, 3), finished(2, 13)])
    assert r.outcome == {"status": "STATUS_COMPLETED", "resultJson": "13"}


def test_lift_command_carries_policy_output_and_memo():
    def h(ctx):
        return ctx.lift(extract, dt.date(2026, 10, 24))

    lift = run(h, []).commands[0]["lift"]
    assert lift["memo"] == "fingerprint"
    assert lift["timeout"] == "60s"
    assert lift["retry"] == {"limit": 2, "backoff": "fixed", "initial": "5s"}
    assert lift["output"] == {"dataset": "raw.orders", "partitionKey": "day", "partition": "2026-10-24"}
    assert json.loads(lift["argsJson"]) == {"day": {"$date": "2026-10-24"}}


def test_pallets_become_inputs_and_results_become_pallets():
    def h(ctx):
        orders = ctx.lift(extract, dt.date(2026, 10, 24))
        return ctx.lift(build, orders, factor=2.0)

    r = run(h, [finished(1, 1, pallet="p1", reused=True)])
    lift = r.commands[1]["lift"]
    assert lift["inputs"] == [{"name": "orders", "pallet": "p1"}]
    assert json.loads(lift["argsJson"]) == {"orders": {"$input": "orders"}, "factor": 2.0}, "the pallet ID stays out of the args"
    assert lift["memo"] == "ttl:1h"

    r = run(h, [finished(1, 1, pallet="p1", reused=True), finished(2, 7, pallet="p2")])
    assert json.loads(r.outcome["resultJson"]) == {"$pallet": "p2", "dataset": "mart.revenue", "partition": ""}


def test_returned_pallet_details():
    seen = {}

    def h(ctx):
        seen["p"] = ctx.lift(extract, dt.date(2026, 10, 24))

    run(h, [finished(1, {"rows": 3}, pallet="p1", reused=True)])
    p = seen["p"]
    assert (p.id, p.dataset, p.partition, p.result, p.reused) == ("p1", "raw.orders", "2026-10-24", {"rows": 3}, True)


def test_failed_lift_raises_in_the_haul():
    def h(ctx):
        try:
            ctx.lift(add, 1, 2)
        except hw.LiftFailed as e:
            return f"fallback after {e.reason}"

    r = run(h, [finished(1, status="failed", reason="ValueError", message="boom")])
    assert json.loads(r.outcome["resultJson"]) == "fallback after ValueError"

    def unhandled(ctx):
        ctx.lift(add, 1, 2)

    r = run(unhandled, [finished(1, status="failed", reason="ValueError", message="boom")])
    assert r.outcome["status"] == "STATUS_FAILED"
    assert r.outcome["reason"] == "LiftFailed"
    assert "add failed (ValueError): boom" in r.outcome["message"]


def test_except_exception_cannot_swallow_blocked():
    def careful(ctx):
        try:
            return ctx.lift(add, 1, 2)
        except Exception:
            return "made up"

    r = run(careful, [])
    assert r.outcome is None, "the replay blocked instead of returning a made-up value"


def test_fan_out_with_gather():
    def h(ctx):
        hs = [ctx.lift_async(add, i, i) for i in range(3)]
        return sum(ctx.gather(hs))

    r = run(h, [])
    assert len(r.commands) == 3 and r.outcome is None
    r = run(h, [finished(1, 0), finished(3, 4)])
    assert r.outcome is None, "one result still missing"
    r = run(h, [finished(1, 0), finished(2, 2), finished(3, 4)])
    assert json.loads(r.outcome["resultJson"]) == 6


def test_sleep_and_now():
    times = []

    def h(ctx):
        times.append(ctx.now())
        ctx.sleep(dt.timedelta(minutes=15))
        times.append(ctx.now())
        ctx.lift(add, 1, 1)
        times.append(ctx.now())

    r = run(h, [])
    assert r.commands[0]["sleep"] == {"duration": "900s"}
    assert r.commands[0]["source"] == f"{__file__}:{h.__code__.co_firstlineno + 2}"
    times.clear()
    run(h, [ev("TimerFired", {"seq": 1}, "2026-10-26T09:15:00Z"), finished(2, 2, time="2026-10-26T09:15:03.123456789Z")])
    assert times == [
        dt.datetime(2026, 10, 26, 9, 0, tzinfo=dt.UTC),
        dt.datetime(2026, 10, 26, 9, 15, tzinfo=dt.UTC),
        dt.datetime(2026, 10, 26, 9, 15, 3, 123456, tzinfo=dt.UTC),
    ]


def test_random_is_the_same_on_every_replay_of_a_haul():
    draws = []

    def h(ctx):
        draws.append((ctx.random(), ctx.rng.randint(1, 1000)))
        ctx.lift(add, 1, 1)

    run(h, [], haul_id="h1")
    run(h, [], haul_id="h1")
    run(h, [], haul_id="h2")
    assert draws[0] == draws[1] != draws[2]


def test_latest_pallet():
    seen = []

    def h(ctx):
        seen.append(ctx.latest_pallet(hw.Dataset("raw.orders"), partition=dt.date(2026, 10, 24)))
        return "ok"

    r = run(h, [])
    assert r.commands[0]["latestPallet"] == {"dataset": "raw.orders", "partition": "2026-10-24"}
    run(h, [ev("QueryAnswered", {"seq": 1, "pallet": None})])
    run(h, [ev("QueryAnswered", {"seq": 1, "pallet": {"id": "p9", "dataset": "raw.orders", "location": "file:///m.json", "meta": {"max_ts": "x"}}})])
    assert seen[0] is None and seen[1].id == "p9" and seen[1].meta == {"max_ts": "x"}


def test_contradicting_the_logbook_stops_the_replay():
    calls = []

    def h(ctx):
        calls.append("before")
        ctx.lift(add, 1, 2)
        calls.append("after")

    recorded = ev("HaulTaskCompleted", {"number": 1, "commands": [{"seq": 1, "kind": "lift", "name": "extract", "source": "old.py:7"}]})
    r = run(h, [recorded, finished(1, 3)])
    assert r.outcome is None
    assert calls == ["before"], "nothing after the contradiction ran"
    assert len(r.commands) == 1, "the contradicting command is sent, so the server marks the haul stuck"
    assert r.mismatch.startswith("command 1: replay issued lift 'add' at ")
    assert r.mismatch.endswith("the logbook has lift 'extract' at old.py:7")


def test_params_are_decoded():
    seen = {}

    def h(ctx, day, n):
        seen.update(day=day, n=n)
        return day

    r = run(h, [], params={"day": {"$date": "2026-10-24"}, "n": 3})
    assert seen == {"day": dt.date(2026, 10, 24), "n": 3}
    assert json.loads(r.outcome["resultJson"]) == {"$date": "2026-10-24"}


def test_memo_without_output():
    @hw.lift(memo=True)
    def rate(day: str) -> float:
        return 1.08

    r = run(lambda ctx: ctx.lift(rate, "2026-10-24"), [])
    lift = r.commands[0]["lift"]
    assert lift["memo"] == "fingerprint"
    assert "output" not in lift


def test_decorator_checks():
    with pytest.raises(ValueError, match="memo="):
        hw.lift(lambda: 1, name="y", memo="always", output=hw.Dataset("d"))
    with pytest.raises(ValueError, match="not one of its arguments"):
        hw.lift(lambda x: 1, name="z", output=hw.Dataset("d", partition="day"))
    r = run(lambda ctx: ctx.lift(print), [])
    assert r.outcome["reason"] == "TypeError" and "not a lift" in r.outcome["message"]
    assert add(1, 2) == 3, "a lift is still a plain function"


def test_wait_signal():
    def gate(ctx):
        approval = ctx.wait_signal("approve", timeout=dt.timedelta(hours=24))
        return ctx.lift(add, approval["n"], 1)

    r = run(gate, [])
    assert r.commands == [{"seq": 1, "source": r.commands[0]["source"], "waitSignal": {"name": "approve", "timeout": "86400s"}}]
    assert r.outcome is None, "blocked until the signal comes"

    got = ev("SignalDelivered", {"seq": 1, "name": "approve", "signal": "s1", "payload": {"n": 2}})
    r = run(gate, [got])
    assert r.commands[1]["lift"]["name"] == "add"
    assert json.loads(r.commands[1]["lift"]["argsJson"]) == {"a": 2, "b": 1}


def test_wait_signal_timeout_raises():
    def gate(ctx):
        try:
            return ctx.wait_signal("approve", timeout=60)
        except hw.SignalTimeout as e:
            return f"gave up: {e}"

    r = run(gate, [ev("SignalDelivered", {"seq": 1, "name": "approve", "timed_out": True})])
    assert json.loads(r.outcome["resultJson"]) == "gave up: no signal 'approve' within 60s"
    assert "timeout" not in run(lambda ctx: ctx.wait_signal("x"), []).commands[0]["waitSignal"]
    bad = run(lambda ctx: ctx.wait_signal("x", timeout=0), []).outcome
    assert (bad["reason"], bad["message"]) == ("ValueError", "wait_signal('x'): timeout must be positive")


def completed(commands, markers=None):
    return ev("HaulTaskCompleted", {"number": 1, "commands": commands, **({"markers": markers} if markers else {})})


def test_version_new_haul_takes_max():
    def h(ctx):
        return ctx.version("gate", max=2)

    r = run(h, [])
    assert json.loads(r.outcome["resultJson"]) == 2
    assert r.markers == [{"name": "gate", "version": 2}]


def test_version_inside_recorded_history_takes_min():
    """A haul that ran before the marker existed keeps the old branch."""
    def h(ctx):
        if ctx.version("gate", max=2) >= 2:
            return ctx.lift(add, 10, 10)
        return ctx.lift(add, 1, 1)

    old = completed([{"seq": 1, "kind": "lift", "name": "add"}])
    r = run(h, [old, finished(1, 2)])
    assert json.loads(r.outcome["resultJson"]) == 2
    assert r.markers == [{"name": "gate", "version": 1}]


def test_version_recorded_is_kept_and_checked():
    def h(ctx):
        return [ctx.version("gate", max=3), ctx.version("gate", max=3)]

    r = run(h, [completed([], [{"name": "gate", "version": 2}])])
    assert json.loads(r.outcome["resultJson"]) == [2, 2], "the recorded decision, every time"

    def dropped_old(ctx):
        return ctx.version("gate", min=3, max=3)

    r = run(dropped_old, [completed([], [{"name": "gate", "version": 2}])])
    assert r.nondeterministic, "a haul on a version the code no longer has is stuck"
    assert "took version 2, the code supports 3 to 3" in r.mismatch

    bad = run(lambda ctx: ctx.version("gate", min=2, max=1), []).outcome
    assert bad["reason"] == "ValueError"


def test_gather_max_parallel_limits_the_new_lifts():
    def fan(ctx):
        return ctx.gather([ctx.lift_async(add, i, 0) for i in range(3)], max_parallel=2)

    r = run(fan, [])
    assert [c["lift"]["limits"] for c in r.commands] == [[{"group": "gather:1", "max": 2}]] * 3

    # Recorded commands were sent with their limits already: not again.
    recorded = ev("HaulTaskCompleted", {"number": 1, "commands": [{"seq": i, "kind": "lift", "name": "add"} for i in (1, 2, 3)]})
    r = run(fan, [recorded])
    assert all("limits" not in c["lift"] for c in r.commands)

    bad = run(lambda ctx: ctx.gather([], max_parallel=0), []).outcome
    assert bad["reason"] == "ValueError"


def test_haul_max_parallel_limits_every_lift():
    @hw.haul(name="limited_haul", max_parallel=3)
    def limited(ctx):
        ctx.lift_async(add, 1, 2)
        return ctx.gather([ctx.lift_async(add, 3, 4)], max_parallel=1)

    r = _replay.replay(limited.__hubwagen_haul__, "h1", {}, [START])
    assert r.commands[0]["lift"]["limits"] == [{"group": "haul", "max": 3}]
    assert r.commands[1]["lift"]["limits"] == [{"group": "haul", "max": 3}, {"group": "gather:2", "max": 1}]
    with pytest.raises(ValueError, match="max_parallel"):
        hw.haul(lambda ctx: 1, name="bad_limit", max_parallel=0)


@hw.haul(name="replay_child")
def replay_child(ctx, day: str, n: int = 1):
    return n


def test_child_haul_waits_for_its_result():
    def parent(ctx):
        return ctx.haul(replay_child, day="2026-10-24", n=2)

    r = run(parent, [])
    assert r.commands[0]["startHaul"] == {"name": "replay_child", "paramsJson": '{"day": "2026-10-24", "n": 2}', "wait": True}
    assert r.outcome is None

    started = ev("ChildStarted", {"seq": 1, "name": "replay_child", "haul": "c1"})
    assert run(parent, [started]).outcome is None, "started, not finished: still waiting"
    done = ev("ChildFinished", {"seq": 1, "name": "replay_child", "haul": "c1", "status": "completed", "result": 2})
    assert json.loads(run(parent, [started, done]).outcome["resultJson"]) == 2


def test_child_failure_raises():
    def parent(ctx):
        try:
            return ctx.haul("replay_child", day="x")
        except hw.ChildFailed as e:
            return [e.status, e.reason, e.message, e.haul_id]

    failed = ev("ChildFinished", {"seq": 1, "name": "replay_child", "haul": "c1", "status": "failed",
                                  "reason": "ValueError", "message": "bad day"})
    assert json.loads(run(parent, [failed]).outcome["resultJson"]) == ["failed", "ValueError", "bad day", "c1"]


def test_start_haul_returns_the_child_id():
    def parent(ctx):
        return ctx.start_haul(replay_child, day="x")

    r = run(parent, [])
    assert "wait" not in r.commands[0]["startHaul"]
    started = ev("ChildStarted", {"seq": 1, "name": "replay_child", "haul": "c1"})
    assert json.loads(run(parent, [started]).outcome["resultJson"]) == "c1"
    cant = ev("ChildFinished", {"seq": 1, "name": "replay_child", "status": "failed", "reason": "start_failed",
                                "message": "no registered build defines haul"})
    assert run(parent, [cant]).outcome["reason"] == "ChildFailed"


def test_child_params_are_checked_against_its_signature():
    r = run(lambda ctx: ctx.haul(replay_child, nope=1), [])
    assert r.outcome["reason"] == "TypeError"
    assert r.commands == [], "nothing was started"


def test_continue_as_new():
    def looping(ctx, cursor: int = 0):
        try:
            ctx.continue_as_new(cursor=cursor + 10)
        except Exception:  # noqa: BLE001 - must not catch it
            return "swallowed"

    r = run(looping, [], params={"cursor": 5})
    assert r.outcome == {"status": "STATUS_CONTINUED", "paramsJson": '{"cursor": 15}'}


def test_scheduled_haul_gets_its_interval():
    with pytest.raises(ValueError, match="takes an `interval` parameter"):
        hw.haul(lambda ctx, day: 1, name="no_interval", schedule="0 3 * * *")
    with pytest.raises(ValueError, match="catch_up"):
        hw.Schedule("0 3 * * *", catch_up="sometimes")
    with pytest.raises(ValueError, match="5 fields"):
        hw.Schedule("daily")

    def daily(ctx, interval: hw.Interval):
        return [interval.start.date(), (interval.end - interval.start).total_seconds()]

    params = {"interval": {"$interval": {"start": "2026-10-24T00:00:00+02:00", "end": "2026-10-25T00:00:00+01:00"}}}
    r = run(daily, [], params=params)
    assert json.loads(r.outcome["resultJson"])[0] == {"$date": "2026-10-24"}
    iv = _model.decode(params)["interval"]
    assert iv.end - iv.start == dt.timedelta(hours=25), "the day the clocks go back"
    assert _model.decode(_model.encode(iv)) == iv


def test_subscription_declaration():
    s = hw.Subscription(["raw.b", hw.Dataset("raw.a"), "raw.b"], debounce=dt.timedelta(minutes=1))
    assert (s.datasets, s.debounce, s.max_wait, s.max_batch) == (("raw.a", "raw.b"), 60.0, None, 100)
    with pytest.raises(ValueError, match="max_wait >= debounce"):
        hw.Subscription("raw.a", debounce=60, max_wait=10)
    with pytest.raises(ValueError, match="`pallets` parameter"):
        hw.haul(lambda ctx: 1, name="no_pallets", subscribe="raw.a")

    def on_orders(ctx, pallets: list):
        return [p.id for p in pallets]

    r = run(on_orders, [], params={"pallets": [{"$pallet": "p1", "dataset": "raw.a", "partition": ""}]})
    assert json.loads(r.outcome["resultJson"]) == ["p1"]


def test_keep():
    assert hw.Keep(versions=3).versions == 3
    assert hw.Keep(age=dt.timedelta(days=1)).age == 86400.0
    for bad in ({}, {"versions": 0}, {"age": -1}):
        with pytest.raises(ValueError, match="Keep"):
            hw.Keep(**bad)
    assert hw.Dataset("a", keep=hw.Keep(versions=1)) == hw.Dataset("a", keep=hw.Keep(versions=1))


def test_lifts_must_agree_on_a_datasets_keep():
    from hubwagen import worker

    saved = dict(_model.LIFTS)
    try:
        hw.lift(lambda: 1, name="k1", output=hw.Dataset("k.ds", keep=hw.Keep(versions=2)))
        hw.lift(lambda: 1, name="k2", output=hw.Dataset("k.ds"))  # no keep: takes the other's
        assert worker.datasets()["k.ds"] == hw.Keep(versions=2)
        hw.lift(lambda: 1, name="k3", output=hw.Dataset("k.ds", keep=hw.Keep(versions=5)))
        with pytest.raises(SystemExit, match="dataset k.ds: lifts k1 and k3 declare different keep"):
            worker.datasets()
    finally:
        _model.LIFTS.clear()
        _model.LIFTS.update(saved)


def test_haul_title_and_description():
    @hw.haul(name="titled_from_doc")
    def from_doc(ctx):
        """Daily revenue

        Orders for one day, and a report."""

    @hw.haul(name="titled", title="Weekly revenue")
    def titled(ctx):
        """Seven days of orders."""

    @hw.haul(name="untitled")
    def untitled(ctx):
        pass

    got = {n: (_model.HAULS[n].title, _model.HAULS[n].description) for n in ("titled_from_doc", "titled", "untitled")}
    assert got == {"titled_from_doc": ("Daily revenue", "Orders for one day, and a report."),
                   "titled": ("Weekly revenue", "Seven days of orders."),
                   "untitled": ("", "")}

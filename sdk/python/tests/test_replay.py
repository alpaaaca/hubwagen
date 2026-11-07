"""The replay engine on hand-built event logs: no server."""

import datetime as dt
import json

import pytest

import moksha as mk
from moksha import _model, _replay


def ev(type_, payload, time="2026-10-26T10:00:00Z"):
    return {"type": type_, "time": time, "payloadJson": json.dumps(payload)}


START = ev("RunStarted", {"name": "h", "params": {}}, "2026-10-26T09:00:00Z")


def finished(seq, result=None, status="completed", time="2026-10-26T10:00:00Z", **kw):
    return ev("StepFinished", {"seq": seq, "status": status, "result": result, **kw}, time)


@mk.step
def add(a: int, b: int) -> int:
    return a + b


@mk.step(reuse=True, output=mk.Dataset("raw.orders", partition="day"), timeout=60, retry=mk.Retry(limit=2, initial=5))
def extract(day: dt.date) -> int:
    return 1


@mk.step(reuse="1h", output=mk.Dataset("mart.revenue"))
def build(orders: mk.DataVersion, factor: float = 1.0) -> int:
    return 2


def run(fn, event_log, params=None, run_id="h1"):
    # Not registered: test functions share names.
    return _replay.replay(_model.PipelineDef(fn.__name__, fn, None), run_id, params or {}, [START, *event_log])


def test_blocks_at_the_first_call_without_a_result():
    def seq_run(ctx):
        x = ctx.step(add, 1, 2)
        return ctx.step(add, x, 10)

    r = run(seq_run, [])
    assert r.outcome is None
    assert [c["step"]["name"] for c in r.commands] == ["add"]
    assert json.loads(r.commands[0]["step"]["argsJson"]) == {"a": 1, "b": 2}
    assert r.commands[0]["source"] == f"{__file__}:{seq_run.__code__.co_firstlineno + 1}", "the line of the call"

    r = run(seq_run, [finished(1, 3)])
    assert [c["seq"] for c in r.commands] == [1, 2]
    assert json.loads(r.commands[1]["step"]["argsJson"]) == {"a": 3, "b": 10}
    assert r.outcome is None

    r = run(seq_run, [finished(1, 3), finished(2, 13)])
    assert r.outcome == {"status": "STATUS_COMPLETED", "resultJson": "13"}


def test_step_command_carries_policy_output_and_reuse():
    def h(ctx):
        return ctx.step(extract, dt.date(2026, 10, 24))

    step = run(h, []).commands[0]["step"]
    assert step["memo"] == "fingerprint"
    assert step["timeout"] == "60s"
    assert step["retry"] == {"limit": 2, "backoff": "fixed", "initial": "5s"}
    assert step["output"] == {"dataset": "raw.orders", "partitionKey": "day", "partition": "2026-10-24"}
    assert json.loads(step["argsJson"]) == {"day": {"$date": "2026-10-24"}}


def test_data_versions_become_inputs_and_results_become_data_versions():
    def h(ctx):
        orders = ctx.step(extract, dt.date(2026, 10, 24))
        return ctx.step(build, orders, factor=2.0)

    r = run(h, [finished(1, 1, data_version="p1", reused=True)])
    step = r.commands[1]["step"]
    assert step["inputs"] == [{"name": "orders", "data_version": "p1"}]
    assert json.loads(step["argsJson"]) == {"orders": {"$input": "orders"}, "factor": 2.0}, "the data version ID stays out of the args"
    assert step["memo"] == "ttl:1h"

    r = run(h, [finished(1, 1, data_version="p1", reused=True), finished(2, 7, data_version="p2")])
    assert json.loads(r.outcome["resultJson"]) == {"$data_version": "p2", "dataset": "mart.revenue", "partition": ""}


def test_returned_data_version_details():
    seen = {}

    def h(ctx):
        seen["p"] = ctx.step(extract, dt.date(2026, 10, 24))

    run(h, [finished(1, {"rows": 3}, data_version="p1", reused=True)])
    p = seen["p"]
    assert (p.id, p.dataset, p.partition, p.result, p.reused) == ("p1", "raw.orders", "2026-10-24", {"rows": 3}, True)


def test_failed_step_raises_in_the_run():
    def h(ctx):
        try:
            ctx.step(add, 1, 2)
        except mk.StepFailed as e:
            return f"fallback after {e.reason}"

    r = run(h, [finished(1, status="failed", reason="ValueError", message="boom")])
    assert json.loads(r.outcome["resultJson"]) == "fallback after ValueError"

    def unhandled(ctx):
        ctx.step(add, 1, 2)

    r = run(unhandled, [finished(1, status="failed", reason="ValueError", message="boom")])
    assert r.outcome["status"] == "STATUS_FAILED"
    assert r.outcome["reason"] == "StepFailed"
    assert "add failed (ValueError): boom" in r.outcome["message"]


def test_except_exception_cannot_swallow_blocked():
    def careful(ctx):
        try:
            return ctx.step(add, 1, 2)
        except Exception:
            return "made up"

    r = run(careful, [])
    assert r.outcome is None, "the replay blocked instead of returning a made-up value"


def test_fan_out_with_gather():
    def h(ctx):
        hs = [ctx.step_async(add, i, i) for i in range(3)]
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
        ctx.step(add, 1, 1)
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


def test_random_is_the_same_on_every_replay_of_a_run():
    draws = []

    def h(ctx):
        draws.append((ctx.random(), ctx.rng.randint(1, 1000)))
        ctx.step(add, 1, 1)

    run(h, [], run_id="h1")
    run(h, [], run_id="h1")
    run(h, [], run_id="h2")
    assert draws[0] == draws[1] != draws[2]


def test_latest_data_version():
    seen = []

    def h(ctx):
        seen.append(ctx.latest_version(mk.Dataset("raw.orders"), partition=dt.date(2026, 10, 24)))
        return "ok"

    r = run(h, [])
    assert r.commands[0]["latestDataVersion"] == {"dataset": "raw.orders", "partition": "2026-10-24"}
    run(h, [ev("QueryAnswered", {"seq": 1, "data_version": None})])
    run(h, [ev("QueryAnswered", {"seq": 1, "data_version": {"id": "p9", "dataset": "raw.orders", "location": "file:///m.json", "meta": {"max_ts": "x"}}})])
    assert seen[0] is None and seen[1].id == "p9" and seen[1].meta == {"max_ts": "x"}


def test_contradicting_the_event_log_stops_the_replay():
    calls = []

    def h(ctx):
        calls.append("before")
        ctx.step(add, 1, 2)
        calls.append("after")

    recorded = ev("RunTaskCompleted", {"number": 1, "commands": [{"seq": 1, "kind": "step", "name": "extract", "source": "old.py:7"}]})
    r = run(h, [recorded, finished(1, 3)])
    assert r.outcome is None
    assert calls == ["before"], "nothing after the contradiction ran"
    assert len(r.commands) == 1, "the contradicting command is sent, so the server marks the run stuck"
    assert r.mismatch.startswith("command 1: replay issued step 'add' at ")
    assert r.mismatch.endswith("the event log has step 'extract' at old.py:7")


def test_params_are_decoded():
    seen = {}

    def h(ctx, day, n):
        seen.update(day=day, n=n)
        return day

    r = run(h, [], params={"day": {"$date": "2026-10-24"}, "n": 3})
    assert seen == {"day": dt.date(2026, 10, 24), "n": 3}
    assert json.loads(r.outcome["resultJson"]) == {"$date": "2026-10-24"}


def test_reuse_without_output():
    @mk.step(reuse=True)
    def rate(day: str) -> float:
        return 1.08

    r = run(lambda ctx: ctx.step(rate, "2026-10-24"), [])
    step = r.commands[0]["step"]
    assert step["memo"] == "fingerprint", "on the wire, reuse= is still the memo policy"
    assert "output" not in step


def test_reuse_ages():
    def policy(**kw):
        return mk.step(lambda: 1, name="age", **kw).__moksha_step__.memo

    assert policy(reuse="1h30m") == "ttl:1h30m"
    assert policy(reuse=dt.timedelta(minutes=90)) == "ttl:5400s"
    assert policy() == "", "not reused unless it says so"
    assert policy(reuse=False) == "never", "not reused, on purpose: no reuse hint"
    for bad in ("ttl:1h", "1d", "0s", "soon", 3):
        with pytest.raises(ValueError, match="reuse="):
            policy(reuse=bad)


def test_memo_is_the_old_name_of_reuse():
    with pytest.warns(FutureWarning, match=r"step old: memo= is now reuse=; write reuse=True"):
        @mk.step(memo=True)
        def old(): ...
    assert old.__moksha_step__.memo == "fingerprint"
    with pytest.warns(FutureWarning, match=r"write reuse='1h'"):
        assert mk.step(lambda: 1, name="ttl", memo="ttl:1h").__moksha_step__.memo == "ttl:1h"
    with pytest.raises(ValueError, match="both given"):
        mk.step(lambda: 1, name="both", memo=True, reuse=True)
    with pytest.raises(ValueError, match="memo= is now reuse="):
        mk.step(lambda: 1, name="y", memo="always")


def test_decorator_checks():
    with pytest.raises(ValueError, match="not one of its arguments"):
        mk.step(lambda x: 1, name="z", output=mk.Dataset("d", partition="day"))
    r = run(lambda ctx: ctx.step(print), [])
    assert r.outcome["reason"] == "TypeError" and "not a step" in r.outcome["message"]
    assert add(1, 2) == 3, "a step is still a plain function"


def test_wait_signal():
    def gate(ctx):
        approval = ctx.wait_signal("approve", timeout=dt.timedelta(hours=24))
        return ctx.step(add, approval["n"], 1)

    r = run(gate, [])
    assert r.commands == [{"seq": 1, "source": r.commands[0]["source"], "waitSignal": {"name": "approve", "timeout": "86400s"}}]
    assert r.outcome is None, "blocked until the signal comes"

    got = ev("SignalDelivered", {"seq": 1, "name": "approve", "signal": "s1", "payload": {"n": 2}})
    r = run(gate, [got])
    assert r.commands[1]["step"]["name"] == "add"
    assert json.loads(r.commands[1]["step"]["argsJson"]) == {"a": 2, "b": 1}


def test_wait_signal_timeout_raises():
    def gate(ctx):
        try:
            return ctx.wait_signal("approve", timeout=60)
        except mk.SignalTimeout as e:
            return f"gave up: {e}"

    r = run(gate, [ev("SignalDelivered", {"seq": 1, "name": "approve", "timed_out": True})])
    assert json.loads(r.outcome["resultJson"]) == "gave up: no signal 'approve' within 60s"
    assert "timeout" not in run(lambda ctx: ctx.wait_signal("x"), []).commands[0]["waitSignal"]
    bad = run(lambda ctx: ctx.wait_signal("x", timeout=0), []).outcome
    assert (bad["reason"], bad["message"]) == ("ValueError", "wait_signal('x'): timeout must be positive")


def completed(commands, markers=None):
    return ev("RunTaskCompleted", {"number": 1, "commands": commands, **({"markers": markers} if markers else {})})


def test_version_new_run_takes_max():
    def h(ctx):
        return ctx.version("gate", max=2)

    r = run(h, [])
    assert json.loads(r.outcome["resultJson"]) == 2
    assert r.markers == [{"name": "gate", "version": 2}]


def test_version_inside_recorded_history_takes_min():
    """A run that ran before the marker existed keeps the old branch."""
    def h(ctx):
        if ctx.version("gate", max=2) >= 2:
            return ctx.step(add, 10, 10)
        return ctx.step(add, 1, 1)

    old = completed([{"seq": 1, "kind": "step", "name": "add"}])
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
    assert r.nondeterministic, "a run on a version the code no longer has is stuck"
    assert "took version 2, the code supports 3 to 3" in r.mismatch

    bad = run(lambda ctx: ctx.version("gate", min=2, max=1), []).outcome
    assert bad["reason"] == "ValueError"


def test_gather_max_parallel_limits_the_new_steps():
    def fan(ctx):
        return ctx.gather([ctx.step_async(add, i, 0) for i in range(3)], max_parallel=2)

    r = run(fan, [])
    assert [c["step"]["limits"] for c in r.commands] == [[{"group": "gather:1", "max": 2}]] * 3

    # Recorded commands were sent with their limits already: not again.
    recorded = ev("RunTaskCompleted", {"number": 1, "commands": [{"seq": i, "kind": "step", "name": "add"} for i in (1, 2, 3)]})
    r = run(fan, [recorded])
    assert all("limits" not in c["step"] for c in r.commands)

    bad = run(lambda ctx: ctx.gather([], max_parallel=0), []).outcome
    assert bad["reason"] == "ValueError"


def test_run_max_parallel_limits_every_step():
    @mk.pipeline(name="limited_run", max_parallel=3)
    def limited(ctx):
        ctx.step_async(add, 1, 2)
        return ctx.gather([ctx.step_async(add, 3, 4)], max_parallel=1)

    r = _replay.replay(limited.__moksha_pipeline__, "h1", {}, [START])
    assert r.commands[0]["step"]["limits"] == [{"group": "run", "max": 3}]
    assert r.commands[1]["step"]["limits"] == [{"group": "run", "max": 3}, {"group": "gather:2", "max": 1}]
    with pytest.raises(ValueError, match="max_parallel"):
        mk.pipeline(lambda ctx: 1, name="bad_limit", max_parallel=0)


@mk.pipeline(name="replay_child")
def replay_child(ctx, day: str, n: int = 1):
    return n


def test_child_run_waits_for_its_result():
    def parent(ctx):
        return ctx.run(replay_child, day="2026-10-24", n=2)

    r = run(parent, [])
    assert r.commands[0]["startRun"] == {"name": "replay_child", "paramsJson": '{"day": "2026-10-24", "n": 2}', "wait": True}
    assert r.outcome is None

    started = ev("ChildStarted", {"seq": 1, "name": "replay_child", "run": "c1"})
    assert run(parent, [started]).outcome is None, "started, not finished: still waiting"
    done = ev("ChildFinished", {"seq": 1, "name": "replay_child", "run": "c1", "status": "completed", "result": 2})
    assert json.loads(run(parent, [started, done]).outcome["resultJson"]) == 2


def test_child_failure_raises():
    def parent(ctx):
        try:
            return ctx.run("replay_child", day="x")
        except mk.RunFailed as e:
            return [e.status, e.reason, e.message, e.run_id]

    failed = ev("ChildFinished", {"seq": 1, "name": "replay_child", "run": "c1", "status": "failed",
                                  "reason": "ValueError", "message": "bad day"})
    assert json.loads(run(parent, [failed]).outcome["resultJson"]) == ["failed", "ValueError", "bad day", "c1"]


def test_start_run_returns_the_child_id():
    def parent(ctx):
        return ctx.start_run(replay_child, day="x")

    r = run(parent, [])
    assert "wait" not in r.commands[0]["startRun"]
    started = ev("ChildStarted", {"seq": 1, "name": "replay_child", "run": "c1"})
    assert json.loads(run(parent, [started]).outcome["resultJson"]) == "c1"
    cant = ev("ChildFinished", {"seq": 1, "name": "replay_child", "status": "failed", "reason": "start_failed",
                                "message": "no registered build defines run"})
    assert run(parent, [cant]).outcome["reason"] == "RunFailed"


def test_child_params_are_checked_against_its_signature():
    r = run(lambda ctx: ctx.run(replay_child, nope=1), [])
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


def test_scheduled_run_gets_its_interval():
    with pytest.raises(ValueError, match="takes an `interval` input"):
        mk.pipeline(lambda ctx, day: 1, name="no_interval", schedule="0 3 * * *")
    with pytest.raises(ValueError, match="catch_up"):
        mk.Schedule("0 3 * * *", catch_up="sometimes")
    with pytest.raises(ValueError, match="5 fields"):
        mk.Schedule("daily")

    def daily(ctx, interval: mk.Interval):
        return [interval.start.date(), (interval.end - interval.start).total_seconds()]

    params = {"interval": {"$interval": {"start": "2026-10-24T00:00:00+02:00", "end": "2026-10-25T00:00:00+01:00"}}}
    r = run(daily, [], params=params)
    assert json.loads(r.outcome["resultJson"])[0] == {"$date": "2026-10-24"}
    iv = _model.decode(params)["interval"]
    assert iv.end - iv.start == dt.timedelta(hours=25), "the day the clocks go back"
    assert _model.decode(_model.encode(iv)) == iv


def test_subscription_declaration():
    s = mk.Subscription(["raw.b", mk.Dataset("raw.a"), "raw.b"], debounce=dt.timedelta(minutes=1))
    assert (s.datasets, s.debounce, s.max_wait, s.max_batch) == (("raw.a", "raw.b"), 60.0, None, 100)
    with pytest.raises(ValueError, match="max_wait >= debounce"):
        mk.Subscription("raw.a", debounce=60, max_wait=10)
    with pytest.raises(ValueError, match="`versions` input"):
        mk.pipeline(lambda ctx: 1, name="no_data_versions", subscribe="raw.a")

    # The new data versions come as `versions`.
    def on_versions(ctx, versions: list[mk.DataVersion]):
        return [v.id for v in versions]

    r = run(on_versions, [], params={"versions": [{"$data_version": "p2", "dataset": "raw.a", "partition": ""}]})
    assert json.loads(r.outcome["resultJson"]) == ["p2"]


def test_keep():
    assert mk.Keep(versions=3).versions == 3
    assert mk.Keep(age=dt.timedelta(days=1)).age == 86400.0
    for bad in ({}, {"versions": 0}, {"age": -1}):
        with pytest.raises(ValueError, match="Keep"):
            mk.Keep(**bad)
    assert mk.Dataset("a", keep=mk.Keep(versions=1)) == mk.Dataset("a", keep=mk.Keep(versions=1))


def test_steps_must_agree_on_a_datasets_keep():
    from moksha import worker

    saved = dict(_model.STEPS)
    try:
        mk.step(lambda: 1, name="k1", output=mk.Dataset("k.ds", keep=mk.Keep(versions=2)))
        mk.step(lambda: 1, name="k2", output=mk.Dataset("k.ds"))  # no keep: takes the other's
        assert worker.datasets()["k.ds"] == mk.Keep(versions=2)
        mk.step(lambda: 1, name="k3", output=mk.Dataset("k.ds", keep=mk.Keep(versions=5)))
        with pytest.raises(SystemExit, match="dataset k.ds: steps k1 and k3 declare different keep"):
            worker.datasets()
    finally:
        _model.STEPS.clear()
        _model.STEPS.update(saved)


def test_run_title_and_description():
    @mk.pipeline(name="titled_from_doc")
    def from_doc(ctx):
        """Daily revenue

        Orders for one day, and a report."""

    @mk.pipeline(name="titled", title="Weekly revenue")
    def titled(ctx):
        """Seven days of orders."""

    @mk.pipeline(name="untitled")
    def untitled(ctx):
        pass

    got = {n: (_model.PIPELINES[n].title, _model.PIPELINES[n].description) for n in ("titled_from_doc", "titled", "untitled")}
    assert got == {"titled_from_doc": ("Daily revenue", "Orders for one day, and a report."),
                   "titled": ("Weekly revenue", "Seven days of orders."),
                   "untitled": ("", "")}


def test_a_run_started_without_an_input_fails_and_says_which():
    def nightly(ctx, interval: mk.Interval):
        return interval.start

    d = _model.PipelineDef("nightly", nightly, None, schedule=mk.Schedule("0 3 * * *", interval="daily"))
    r = _replay.replay(d, "h1", {}, [START])
    assert r.outcome["status"] == "STATUS_FAILED"
    assert "pipeline nightly was started with inputs {}" in r.outcome["message"]
    assert "'interval'" in r.outcome["message"] and "$interval" in r.outcome["message"]

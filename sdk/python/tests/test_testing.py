"""hubwagen.testing: hauls against scripted results, and replays of
recorded logbooks against changed code."""

import json
import subprocess
import sys
import textwrap

import pytest

import hubwagen as hw
from hubwagen import testing

from conftest import SDK, start_worker, stop, wait


@hw.lift(name="t_check")
def check(day: str) -> dict:
    return {"failed": False}


@hw.lift(name="t_build", output=hw.Dataset("t.revenue"))
def build(day: str) -> int:
    return 1


@hw.haul(name="t_gate")
def gate(ctx, day: str):
    report = ctx.lift(check, day)
    if report["failed"]:
        try:
            approval = ctx.wait_signal("approve", timeout=3600)
        except hw.SignalTimeout:
            return "not approved"
        ctx.sleep(60)
        if ctx.version("audit", max=2) >= 2:
            ctx.lift(check, "audit")
        return [ctx.lift(build, day).result, approval, str(ctx.now())]
    return ctx.lift(build, day).id


def test_scripted_run():
    r = testing.run(gate, {"day": "2026-10-24"}, lifts={"t_check": {"failed": True}, "t_build": 42},
                    signals={"approve": {"by": "ana"}})
    assert r.status == "completed"
    assert r.result == [42, {"by": "ana"}, "2026-01-01 00:01:00+00:00"], "the sleep's minute passed, not the timeout's hour"
    assert [(c.name, c.args) for c in r.lifts] == [("t_check", {"day": "2026-10-24"}), ("t_check", {"day": "audit"}),
                                                     ("t_build", {"day": "2026-10-24"})]


def test_signal_timeout_and_pallets():
    r = testing.run(gate, {"day": "x"}, lifts={"t_check": {"failed": True}})
    assert r.result == "not approved"
    r = testing.run(gate, {"day": "x"}, lifts={"t_check": lambda day: {"failed": day == "y"}, "t_build": 1})
    assert r.result == "pallet-2", "a lift with an output returns a pallet"


def test_failures_children_and_continue():
    @hw.haul(name="t_parent")
    def parent(ctx, n: int):
        try:
            ctx.lift(check, "x")
        except hw.LiftFailed as e:
            failed = e.reason
        total = ctx.haul("t_child", a=n)
        try:
            ctx.haul("t_bad")
        except hw.ChildFailed as e:
            child_failed = e.message
        if n < 2:
            ctx.continue_as_new(n=n + 1)
        return [failed, total, child_failed]

    r = testing.run(parent, {"n": 1}, lifts={"t_check": ValueError("source down")},
                    children={"t_child": lambda a: a * 10, "t_bad": RuntimeError("no data")})
    assert (r.status, r.params) == ("continued", {"n": 2})
    r = testing.run(parent, {"n": 2}, lifts={"t_check": ValueError("source down")},
                    children={"t_child": lambda a: a * 10, "t_bad": RuntimeError("no data")})
    assert r.result == ["ValueError", 20, "no data"]
    assert r.children == [("t_child", {"a": 2}), ("t_bad", {})]


def test_run_lifts_calls_the_real_ones():
    @hw.haul(name="t_real")
    def real(ctx):
        return ctx.lift(check, "x")

    assert testing.run(real, run_lifts=True).result == {"failed": False}


def test_what_isnt_scripted_and_nondeterminism():
    with pytest.raises(testing.TestingError, match=r"lift t_check isn't scripted"):
        testing.run(gate, {"day": "x"})
    with pytest.raises(testing.TestingError, match="waits for signal 'go' with no timeout"):
        testing.run(hw.haul(lambda ctx: ctx.wait_signal("go"), name="t_forever"))

    calls = []

    @hw.haul(name="t_flaky")
    def flaky(ctx):
        calls.append(1)  # global state: the second replay calls another lift
        return ctx.lift(check if len(calls) == 1 else build, "x")

    with pytest.raises(testing.TestingError, match="isn't deterministic"):
        testing.run(flaky, lifts={"t_check": {"failed": False}, "t_build": 1})


V1 = '''
import hubwagen as hw

@hw.lift
def rp_step(x: str) -> str:
    return x

@hw.haul
def rp_haul(ctx, n: int):
    return [ctx.lift(rp_step, "a"), n]
'''


def test_replay_recorded_logbooks(server, tmp_path):
    (tmp_path / "rp_hauls.py").write_text(textwrap.dedent(V1))
    w = start_worker(server, "rp_hauls", tmp_path, tmp_path / "worker.log")
    client = hw.Client(server)
    try:
        hid = client.start_haul("rp_haul", {"n": 3})
        assert wait(client, hid)["status"] == "completed"
    finally:
        stop(w)

    def replay(code: str):
        (tmp_path / "rp_hauls.py").write_text(textwrap.dedent(code))
        return subprocess.run([sys.executable, "-m", "hubwagen.testing", "rp_hauls", "--server", server,
                               "--replay-recent", "5", "--haul", "rp_haul"],
                              cwd=tmp_path, capture_output=True, text=True,
                              env={"PYTHONPATH": f"{SDK / 'src'}:{tmp_path}", "PATH": ""})

    ok = replay(V1)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert ok.stdout.startswith(f"ok   {hid} rp_haul (completed)")

    changed = replay(V1.replace('ctx.lift(rp_step, "a")', 'ctx.lift(rp_step, "b") + ctx.lift(rp_step, "c")'))
    assert changed.returncode == 1
    assert f"FAIL {hid} rp_haul (completed)" in changed.stdout
    assert "the haul completed, the replay doesn't end" in changed.stdout, "same lift, new arguments: a new call"

    returns = replay(V1.replace("return [ctx.lift(rp_step, \"a\"), n]", "return [ctx.lift(rp_step, \"a\"), n + 1]"))
    assert returns.returncode == 1 and "the haul returned" in returns.stdout

    renamed = replay(V1.replace("rp_step", "rp_other"))
    assert renamed.returncode == 1 and "replay issued lift 'rp_other'" in renamed.stdout

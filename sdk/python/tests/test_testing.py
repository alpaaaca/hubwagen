"""moksha.testing: runs against scripted results, and replays of
recorded event logs against changed code."""

import json
import subprocess
import sys
import textwrap

import pytest

import moksha as mk
from moksha import testing

from conftest import SDK, start_worker, stop, wait


@mk.step(name="t_check")
def check(day: str) -> dict:
    return {"failed": False}


@mk.step(name="t_build", output=mk.Dataset("t.revenue"))
def build(day: str) -> int:
    return 1


@mk.pipeline(name="t_gate")
def gate(ctx, day: str):
    report = ctx.step(check, day)
    if report["failed"]:
        try:
            approval = ctx.wait_signal("approve", timeout=3600)
        except mk.SignalTimeout:
            return "not approved"
        ctx.sleep(60)
        if ctx.version("audit", max=2) >= 2:
            ctx.step(check, "audit")
        return [ctx.step(build, day).result, approval, str(ctx.now())]
    return ctx.step(build, day).id


def test_scripted_run():
    r = testing.run(gate, {"day": "2026-10-24"}, steps={"t_check": {"failed": True}, "t_build": 42},
                    signals={"approve": {"by": "ana"}})
    assert r.status == "completed"
    assert r.result == [42, {"by": "ana"}, "2026-01-01 00:01:00+00:00"], "the sleep's minute passed, not the timeout's hour"
    assert [(c.name, c.args) for c in r.steps] == [("t_check", {"day": "2026-10-24"}), ("t_check", {"day": "audit"}),
                                                     ("t_build", {"day": "2026-10-24"})]


def test_signal_timeout_and_data_versions():
    r = testing.run(gate, {"day": "x"}, steps={"t_check": {"failed": True}})
    assert r.result == "not approved"
    r = testing.run(gate, {"day": "x"}, steps={"t_check": lambda day: {"failed": day == "y"}, "t_build": 1})
    assert r.result == "data_version-2", "a step with an output returns a data version"


def test_failures_children_and_continue():
    @mk.pipeline(name="t_parent")
    def parent(ctx, n: int):
        try:
            ctx.step(check, "x")
        except mk.StepFailed as e:
            failed = e.reason
        total = ctx.run("t_child", a=n)
        try:
            ctx.run("t_bad")
        except mk.RunFailed as e:
            child_failed = e.message
        if n < 2:
            ctx.continue_as_new(n=n + 1)
        return [failed, total, child_failed]

    r = testing.run(parent, {"n": 1}, steps={"t_check": ValueError("source down")},
                    children={"t_child": lambda a: a * 10, "t_bad": RuntimeError("no data")})
    assert (r.status, r.params) == ("continued", {"n": 2})
    r = testing.run(parent, {"n": 2}, steps={"t_check": ValueError("source down")},
                    children={"t_child": lambda a: a * 10, "t_bad": RuntimeError("no data")})
    assert r.result == ["ValueError", 20, "no data"]
    assert r.children == [("t_child", {"a": 2}), ("t_bad", {})]


def test_run_steps_calls_the_real_ones():
    @mk.pipeline(name="t_real")
    def real(ctx):
        return ctx.step(check, "x")

    assert testing.run(real, run_steps=True).result == {"failed": False}


def test_what_isnt_scripted_and_nondeterminism():
    with pytest.raises(testing.TestingError, match=r"step t_check isn't scripted"):
        testing.run(gate, {"day": "x"})
    with pytest.raises(testing.TestingError, match="waits for signal 'go' with no timeout"):
        testing.run(mk.pipeline(lambda ctx: ctx.wait_signal("go"), name="t_forever"))

    calls = []

    @mk.pipeline(name="t_flaky")
    def flaky(ctx):
        calls.append(1)  # global state: the second replay calls another step
        return ctx.step(check if len(calls) == 1 else build, "x")

    with pytest.raises(testing.TestingError, match="isn't deterministic"):
        testing.run(flaky, steps={"t_check": {"failed": False}, "t_build": 1})


V1 = '''
import moksha as mk

@mk.step
def rp_step(x: str) -> str:
    return x

@mk.pipeline
def rp_run(ctx, n: int):
    return [ctx.step(rp_step, "a"), n]
'''


def test_replay_recorded_event_logs(server, tmp_path):
    (tmp_path / "rp_pipelines.py").write_text(textwrap.dedent(V1))
    w = start_worker(server, "rp_pipelines", tmp_path, tmp_path / "worker.log")
    client = mk.Client(server)
    try:
        hid = client.start_run("rp_run", {"n": 3})
        assert wait(client, hid)["status"] == "completed"
    finally:
        stop(w)

    def replay(code: str):
        (tmp_path / "rp_pipelines.py").write_text(textwrap.dedent(code))
        return subprocess.run([sys.executable, "-m", "moksha.testing", "rp_pipelines", "--server", server,
                               "--replay-recent", "5", "--pipeline", "rp_run"],
                              cwd=tmp_path, capture_output=True, text=True,
                              env={"PYTHONPATH": f"{SDK / 'src'}:{tmp_path}", "PATH": ""})

    ok = replay(V1)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert ok.stdout.startswith(f"ok   {hid} rp_run (completed)")

    changed = replay(V1.replace('ctx.step(rp_step, "a")', 'ctx.step(rp_step, "b") + ctx.step(rp_step, "c")'))
    assert changed.returncode == 1
    assert f"FAIL {hid} rp_run (completed)" in changed.stdout
    assert "the run completed, the replay doesn't end" in changed.stdout, "same step, new arguments: a new call"

    returns = replay(V1.replace("return [ctx.step(rp_step, \"a\"), n]", "return [ctx.step(rp_step, \"a\"), n + 1]"))
    assert returns.returncode == 1 and "the run returned" in returns.stdout

    renamed = replay(V1.replace("rp_step", "rp_other"))
    assert renamed.returncode == 1 and "replay issued step 'rp_other'" in renamed.stdout

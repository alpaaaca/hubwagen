"""Version markers across a deploy, on a real moksha dev: a run started
on old code keeps the old branch; a new run takes the new one."""

import json
import textwrap
import time

from conftest import start_worker, stop, wait

V1 = '''
import moksha as mk

@mk.step
def marker_step(x: str) -> str:
    return x

@mk.pipeline
def marker_deploy(ctx):
    a = ctx.step(marker_step, "old")
    ctx.wait_signal("go")
    return [a, "after"]
'''

V2 = '''
import moksha as mk

@mk.step
def marker_step(x: str) -> str:
    return x

@mk.step
def marker_new_step(x: str) -> str:
    return x.upper()

@mk.pipeline
def marker_deploy(ctx):
    if ctx.version("new-first-step", max=2) >= 2:
        a = ctx.step(marker_new_step, "new")
    else:
        a = ctx.step(marker_step, "old")
    ctx.wait_signal("go")
    return [a, "after"]
'''


def test_a_running_run_keeps_the_old_branch(client, server, tmp_path):
    def deploy(code, n):
        (tmp_path / "marker_pipelines.py").write_text(textwrap.dedent(code))
        return start_worker(server, "marker_pipelines", tmp_path, tmp_path / f"worker{n}.log")

    w = deploy(V1, 1)
    old = client.start_run("marker_deploy")
    for _ in range(100):  # until it waits for the signal: its first step is done
        if any(l["name"] == "marker_step" and l["status"] == "completed" for l in client.get_run(old).get("steps", [])):
            break
        time.sleep(0.05)
    stop(w)

    w = deploy(V2, 2)
    try:
        new = client.start_run("marker_deploy")
        for h in (old, new):
            client.signal_run(h, "go")
        old_h, new_h = wait(client, old), wait(client, new)
    finally:
        stop(w)
    assert old_h["status"] == "completed", old_h
    assert json.loads(old_h["resultJson"]) == ["old", "after"], "replayed on the old branch, not stuck"
    assert json.loads(new_h["resultJson"]) == ["NEW", "after"]

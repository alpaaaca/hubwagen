"""Version markers across a deploy, on a real hubwagen dev: a haul started
on old code keeps the old branch; a new haul takes the new one."""

import json
import textwrap
import time

from conftest import start_worker, stop, wait

V1 = '''
import hubwagen as hw

@hw.lift
def marker_step(x: str) -> str:
    return x

@hw.haul
def marker_deploy(ctx):
    a = ctx.lift(marker_step, "old")
    ctx.wait_signal("go")
    return [a, "after"]
'''

V2 = '''
import hubwagen as hw

@hw.lift
def marker_step(x: str) -> str:
    return x

@hw.lift
def marker_new_step(x: str) -> str:
    return x.upper()

@hw.haul
def marker_deploy(ctx):
    if ctx.version("new-first-step", max=2) >= 2:
        a = ctx.lift(marker_new_step, "new")
    else:
        a = ctx.lift(marker_step, "old")
    ctx.wait_signal("go")
    return [a, "after"]
'''


def test_a_running_haul_keeps_the_old_branch(client, server, tmp_path):
    def deploy(code, n):
        (tmp_path / "marker_hauls.py").write_text(textwrap.dedent(code))
        return start_worker(server, "marker_hauls", tmp_path, tmp_path / f"worker{n}.log")

    w = deploy(V1, 1)
    old = client.start_haul("marker_deploy")
    for _ in range(100):  # until it waits for the signal: its first lift is done
        if any(l["name"] == "marker_step" and l["status"] == "completed" for l in client.get_haul(old).get("lifts", [])):
            break
        time.sleep(0.05)
    stop(w)

    w = deploy(V2, 2)
    try:
        new = client.start_haul("marker_deploy")
        for h in (old, new):
            client.signal_haul(h, "go")
        old_h, new_h = wait(client, old), wait(client, new)
    finally:
        stop(w)
    assert old_h["status"] == "completed", old_h
    assert json.loads(old_h["resultJson"]) == ["old", "after"], "replayed on the old branch, not stuck"
    assert json.loads(new_h["resultJson"]) == ["NEW", "after"]

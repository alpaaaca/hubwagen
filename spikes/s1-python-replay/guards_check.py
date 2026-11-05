"""Spike S1: which guard catches the mistakes in user_pipelines.py, and which
breaks the libraries? Also: exceptions vs. replay, and set ordering.

    python3 guards_check.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import guards
import mkproto
import user_pipelines

DONE = json.dumps({"event_log": [{"type": "StepFinished", "payloadJson": json.dumps({"seq": 1, "status": "completed", "result": 1})}]})


def run(fn, guard):
    with guard():
        try:
            commands, outcome = mkproto.replay(lambda ctx: fn(ctx), {}, DONE)
        except guards.NondeterminismError as e:  # raised by the guard inside a step's args
            return "caught", str(e)
    if outcome and outcome["status"] == "failed":
        msg = outcome["message"]
        return ("caught" if "NondeterminismError" in msg else "error"), msg
    return "ran", ""


def main() -> None:
    print(f"Python {sys.version.split()[0]}\n")
    print(f"{'case':42s} {'patch':>8s} {'monitor':>8s}")
    for title, cases, want in (("mistakes (want: caught)", user_pipelines.MISTAKES, "caught"),
                               ("library and deterministic use (want: ran)", user_pipelines.FINE, "ran")):
        print(f"-- {title}")
        for name, fn in cases.items():
            row = []
            for guard in (guards.patch_guard, guards.monitor_guard):
                got, msg = run(fn, guard)
                mark = "ok" if got == want else "MISS" if want == "caught" else "BREAKS"
                row.append(f"{got}/{mark}")
            print(f"{name:42s} {row[0]:>14s} {row[1]:>14s}")

    print("\nAn error names the call site:")
    print("  ", run(user_pipelines.MISTAKES["from datetime import datetime; now()"], guards.monitor_guard)[1])

    print("\nGuard overhead on a 1,000-step replay (ms):")
    import bench
    lb = bench.fake_event_log(1000)
    run = bench.sequential(1000)
    for name, guard in (("none", None), ("patch", guards.patch_guard), ("monitor", guards.monitor_guard)):
        def once():
            if guard is None:
                mkproto.replay(run, {"day": "x"}, lb)
            else:
                with guard():
                    mkproto.replay(run, {"day": "x"}, lb)
        print(f"   {name:8s} {bench.best_ms(once):7.2f}")

    print("\nBlocked as Exception vs BaseException, when pipeline code catches broadly:")

    def careful(ctx):
        try:
            return ctx.step("a")
        except Exception:
            return "fallback"  # would also swallow a Blocked that is an Exception

    for blocked in (mkproto.BlockedException, mkproto.Blocked):
        ctx_results = {}
        ctx = mkproto.Ctx(ctx_results, blocked=blocked)
        try:
            r = careful(ctx)
            print(f"   {blocked.__name__:17s} run returned {r!r} without a result: WRONG")
        except BaseException as e:
            print(f"   {blocked.__name__:17s} replay blocked as it should ({type(e).__name__})")

    print("\nSet iteration order across processes (str hashing):")
    code = "print(list({'extract','transform','load','report','audit'}))"
    for env in ({}, {"PYTHONHASHSEED": "0"}):
        outs = {subprocess.run([sys.executable, "-c", code], env={**os.environ, **env}, capture_output=True, text=True).stdout.strip() for _ in range(6)}
        label = "PYTHONHASHSEED=0" if env else "default"
        print(f"   {label:25s} {len(outs)} distinct orders in 6 runs")


if __name__ == "__main__":
    main()

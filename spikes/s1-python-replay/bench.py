"""Spike S1 benchmarks: replay cost and call-site capture.

    python3 bench.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time
import timeit

import mkproto


def fake_event_log(steps: int, fanout: bool = False) -> str:
    """An event log as the worker API returns it, for a run whose `steps`
    steps have all finished: per step the server records ~7 events
    (run task queued/dispatched/completed, step scheduled,
    attempt dispatched/finished, step finished)."""
    events = [{"seq": 1, "type": "RunStarted", "time": "2026-10-26T00:00:00Z", "payloadJson": json.dumps({"name": "h", "params": {"day": "2026-10-24"}})}]
    seq = 1

    def add(t: str, payload: dict) -> None:
        nonlocal seq
        seq += 1
        events.append({"seq": seq, "type": t, "time": "2026-10-26T00:00:00Z", "payloadJson": json.dumps(payload)})

    for i in range(1, steps + 1):
        if not fanout or i == 1:
            add("RunTaskQueued", {"number": i})
            add("RunTaskDispatched", {"number": i, "worker": "w1"})
            add("RunTaskCompleted", {"number": i, "commands": [{"seq": i, "kind": "step", "name": f"l{i}"}]})
        add("StepScheduled", {"seq": i, "name": f"l{i}", "queue": "default", "args": {"i": i}, "fingerprint": "f" * 64, "run_reason": "not memoized"})
        add("StepAttemptDispatched", {"seq": i, "number": 1, "worker": "w1"})
        add("StepAttemptFinished", {"seq": i, "number": 1, "status": "succeeded"})
        add("StepFinished", {"seq": i, "status": "completed", "result": {"rows": i, "path": f"s3://bucket/x/{i}"}, "data_version": "01J" + "X" * 23})
    return json.dumps({"event_log": events})


def sequential(n: int):
    def run(ctx, day):
        total = 0
        for i in range(1, n + 1):
            total += ctx.step(f"l{i}", {"i": i})["rows"]
        return total
    return run


def fan_out(n: int):
    def run(ctx, day):
        return len(ctx.gather([ctx.step_async(f"l{i}", {"i": i}) for i in range(1, n + 1)]))
    return run


def best_ms(f, repeat: int = 30) -> float:
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        f()
        times.append(time.perf_counter() - t)
    return statistics.median(times) * 1000


def main() -> None:
    print(f"Python {sys.version.split()[0]}")

    print("\n1. One replay, event log already complete (every result present):")
    for steps in (143, 1000, 5000):
        lb = fake_event_log(steps)
        nev = len(json.loads(lb)["event_log"])
        decode = best_ms(lambda: mkproto.results_from_event_log(json.loads(lb)["event_log"]))
        total = best_ms(lambda: mkproto.replay(sequential(steps), {"day": "x"}, lb))
        cmds, outcome = mkproto.replay(sequential(steps), {"day": "x"}, lb)
        assert outcome and outcome["status"] == "completed" and len(cmds) == steps
        print(f"   {steps:5d} steps, {nev:6d} events, {len(lb) / 1e6:5.2f} MB: decode {decode:7.2f} ms, decode+replay {total:7.2f} ms")

    print("\n2. A whole sequential run: one replay per result, each over the event log so far:")
    for steps in (143, 500, 1000):
        def whole():
            for done in range(steps + 1):
                mkproto.replay(sequential(steps), {"day": "x"}, fake_event_log(done))
        # building the fake event logs is server work; time it separately
        build = best_ms(lambda: [fake_event_log(d) for d in range(steps + 1)], repeat=3)
        print(f"   {steps:5d} steps: {best_ms(whole, repeat=3) - build:9.1f} ms of worker CPU over the run's life")

    print("\n3. Fan-out: gather over N step_async (one replay issues all N):")
    for n in (1000, 10000):
        lb = fake_event_log(n, fanout=True)
        print(f"   {n:5d} steps: decode+replay {best_ms(lambda: mkproto.replay(fan_out(n), {'day': 'x'}, lb)):7.2f} ms")

    print("\n4. Call-site capture, per ctx call (µs):")
    lb = fake_event_log(1000)
    base = best_ms(lambda: mkproto.replay(sequential(1000), {"day": "x"}, lb, site=mkproto.site_none))
    for name in ("site_getframe", "site_traceback", "site_inspect"):
        site = getattr(mkproto, name)
        t = best_ms(lambda: mkproto.replay(sequential(1000), {"day": "x"}, lb, site=site), repeat=5)
        print(f"   {name:15s} {(t - base) * 1000 / 1000:8.2f} µs per call")
    cmds, _ = mkproto.replay(sequential(3), {"day": "x"}, fake_event_log(3))
    print(f"   e.g. {cmds[0]['source']}")


if __name__ == "__main__":
    main()

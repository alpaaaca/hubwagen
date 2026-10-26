"""Spike S1 benchmarks: replay cost and call-site capture.

    python3 bench.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time
import timeit

import hwproto


def fake_logbook(lifts: int, fanout: bool = False) -> str:
    """A logbook as the worker API returns it, for a haul whose `lifts`
    lifts have all finished: per lift the server records ~7 events
    (haul task queued/dispatched/completed, lift scheduled,
    attempt dispatched/finished, lift finished)."""
    events = [{"seq": 1, "type": "HaulStarted", "time": "2026-10-26T00:00:00Z", "payloadJson": json.dumps({"name": "h", "params": {"day": "2026-10-24"}})}]
    seq = 1

    def add(t: str, payload: dict) -> None:
        nonlocal seq
        seq += 1
        events.append({"seq": seq, "type": t, "time": "2026-10-26T00:00:00Z", "payloadJson": json.dumps(payload)})

    for i in range(1, lifts + 1):
        if not fanout or i == 1:
            add("HaulTaskQueued", {"number": i})
            add("HaulTaskDispatched", {"number": i, "worker": "w1"})
            add("HaulTaskCompleted", {"number": i, "commands": [{"seq": i, "kind": "lift", "name": f"l{i}"}]})
        add("LiftScheduled", {"seq": i, "name": f"l{i}", "dock": "default", "args": {"i": i}, "fingerprint": "f" * 64, "run_reason": "not memoized"})
        add("LiftAttemptDispatched", {"seq": i, "number": 1, "worker": "w1"})
        add("LiftAttemptFinished", {"seq": i, "number": 1, "status": "succeeded"})
        add("LiftFinished", {"seq": i, "status": "completed", "result": {"rows": i, "path": f"s3://bucket/x/{i}"}, "pallet": "01J" + "X" * 23})
    return json.dumps({"logbook": events})


def sequential(n: int):
    def haul(ctx, day):
        total = 0
        for i in range(1, n + 1):
            total += ctx.lift(f"l{i}", {"i": i})["rows"]
        return total
    return haul


def fan_out(n: int):
    def haul(ctx, day):
        return len(ctx.gather([ctx.lift_async(f"l{i}", {"i": i}) for i in range(1, n + 1)]))
    return haul


def best_ms(f, repeat: int = 30) -> float:
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        f()
        times.append(time.perf_counter() - t)
    return statistics.median(times) * 1000


def main() -> None:
    print(f"Python {sys.version.split()[0]}")

    print("\n1. One replay, logbook already complete (every result present):")
    for lifts in (143, 1000, 5000):
        lb = fake_logbook(lifts)
        nev = len(json.loads(lb)["logbook"])
        decode = best_ms(lambda: hwproto.results_from_logbook(json.loads(lb)["logbook"]))
        total = best_ms(lambda: hwproto.replay(sequential(lifts), {"day": "x"}, lb))
        cmds, outcome = hwproto.replay(sequential(lifts), {"day": "x"}, lb)
        assert outcome and outcome["status"] == "completed" and len(cmds) == lifts
        print(f"   {lifts:5d} lifts, {nev:6d} events, {len(lb) / 1e6:5.2f} MB: decode {decode:7.2f} ms, decode+replay {total:7.2f} ms")

    print("\n2. A whole sequential haul: one replay per result, each over the logbook so far:")
    for lifts in (143, 500, 1000):
        def whole():
            for done in range(lifts + 1):
                hwproto.replay(sequential(lifts), {"day": "x"}, fake_logbook(done))
        # building the fake logbooks is server work; time it separately
        build = best_ms(lambda: [fake_logbook(d) for d in range(lifts + 1)], repeat=3)
        print(f"   {lifts:5d} lifts: {best_ms(whole, repeat=3) - build:9.1f} ms of worker CPU over the haul's life")

    print("\n3. Fan-out: gather over N lift_async (one replay issues all N):")
    for n in (1000, 10000):
        lb = fake_logbook(n, fanout=True)
        print(f"   {n:5d} lifts: decode+replay {best_ms(lambda: hwproto.replay(fan_out(n), {'day': 'x'}, lb)):7.2f} ms")

    print("\n4. Call-site capture, per ctx call (µs):")
    lb = fake_logbook(1000)
    base = best_ms(lambda: hwproto.replay(sequential(1000), {"day": "x"}, lb, site=hwproto.site_none))
    for name in ("site_getframe", "site_traceback", "site_inspect"):
        site = getattr(hwproto, name)
        t = best_ms(lambda: hwproto.replay(sequential(1000), {"day": "x"}, lb, site=site), repeat=5)
        print(f"   {name:15s} {(t - base) * 1000 / 1000:8.2f} µs per call")
    cmds, _ = hwproto.replay(sequential(3), {"day": "x"}, fake_logbook(3))
    print(f"   e.g. {cmds[0]['source']}")


if __name__ == "__main__":
    main()

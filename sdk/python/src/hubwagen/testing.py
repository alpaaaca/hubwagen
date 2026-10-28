"""Testing hauls: run one against scripted results, or replay recorded
logbooks against new code.

In tests, `run` plays the server in memory: it replays the haul, answers
each new command from what the test scripted, records the answers in a
logbook like the server's, and replays again until the haul ends. Sleeps
end at once, on a clock that only moves when the haul sleeps.

    from hubwagen import testing

    def test_gate():
        r = testing.run(daily, {"day": "2026-10-24"},
                        lifts={"check_quality": {"failed": True}, "build": 42},
                        signals={"approve": {"by": "ana"}})
        assert r.status == "completed" and r.result == 42
        assert [c.name for c in r.lifts] == ["check_quality", "build"]

Before a deploy, replay what the server recorded against the new code:

    python -m hubwagen.testing --replay-recent 50 --server http://hubwagen:7070
    python -m hubwagen.testing --replay 01M6... [--haul daily]

A logbook the new code contradicts (another call where one was recorded,
a version marker decided differently, the clock read, or another result for
a finished haul) is a haul the deploy would make stuck. Exits 1 if any is.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import importlib
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import _model, _replay
from ._model import Pallet, decode, encode

START = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)
MAX_ROUNDS = 10_000


class TestingError(AssertionError):
    """The haul can't be run as scripted, or it isn't deterministic."""

    __test__ = False  # not a pytest test class


@dataclass
class LiftCall:
    seq: int
    name: str
    args: dict[str, Any]


@dataclass
class Run:
    """How a test run ended: status is completed, failed or continued."""

    status: str
    result: Any = None  # completed: what the haul returned
    reason: str = ""  # failed: the exception's type
    message: str = ""
    params: dict | None = None  # continued: the next haul's params
    lifts: list[LiftCall] = field(default_factory=list)
    children: list[tuple[str, dict]] = field(default_factory=list)
    logbook: list[dict] = field(default_factory=list)


def run(haul: Callable | str, params: dict | None = None, *, lifts: dict[str, Any] | None = None,
        run_lifts: bool = False, signals: dict[str, Any] | None = None, pallets: dict[str, Pallet | None] | None = None,
        children: dict[str, Any] | None = None, now: _dt.datetime = START, haul_id: str = "test-haul") -> Run:
    """Run a haul to its end against scripted results.

    lifts: lift name → its result, a function called with the lift's
    arguments, or an exception (the lift fails, after its retries).
    run_lifts: call the real lift functions for lifts not in lifts.
    signals: signal name → payload, or a list of payloads (one per wait).
    A wait with a timeout and no payload left times out.
    pallets: dataset name → what ctx.latest_pallet returns (default None).
    children: child haul name → its result, a function called with its
    params, or an exception (the child fails).
    """
    d = _haul_def(haul)
    sim = _Server(lifts or {}, run_lifts, signals or {}, pallets or {}, children or {}, now)
    logbook = [sim.event("HaulStarted", {"name": d.name, "params": encode(params or {})})]
    for _ in range(MAX_ROUNDS):
        r = _replay.replay(d, haul_id, encode(params or {}), logbook)
        if r.nondeterministic or r.mismatch:
            raise TestingError(f"haul {d.name} isn't deterministic: {r.mismatch}")
        recorded = sum(len(json.loads(e["payloadJson"]).get("commands") or []) for e in logbook
                       if e["type"] == "HaulTaskCompleted")
        new = r.commands[recorded:]
        known = {m["name"] for e in logbook if e["type"] == "HaulTaskCompleted"
                 for m in json.loads(e["payloadJson"]).get("markers") or []}
        markers = [m for m in r.markers if m["name"] not in known]
        logbook.append(sim.event("HaulTaskCompleted", {"commands": [_recorded(c) for c in new],
                                                      **({"markers": markers} if markers else {})}))
        if r.outcome:
            return _finish(d, haul_id, params, logbook, r.outcome, sim)
        if not new:
            raise TestingError(f"haul {d.name} is blocked with nothing left to answer it")
        for cmd in new:
            logbook += sim.answer(cmd)
    raise TestingError(f"haul {d.name} didn't end within {MAX_ROUNDS} replays")


def _finish(d, haul_id, params, logbook, outcome, sim) -> Run:
    # One more replay of the whole logbook must end the same way.
    again = _replay.replay(d, haul_id, encode(params or {}), logbook)
    if again.outcome != outcome or again.mismatch:
        raise TestingError(f"haul {d.name} ended differently on a replay of its own logbook: "
                           f"{again.mismatch or again.outcome}")
    status = {"STATUS_COMPLETED": "completed", "STATUS_FAILED": "failed", "STATUS_CONTINUED": "continued"}[outcome["status"]]
    out = Run(status=status, lifts=sim.lift_calls, children=sim.child_calls, logbook=logbook,
              reason=outcome.get("reason", ""), message=outcome.get("message", ""))
    if "resultJson" in outcome:
        out.result = decode(json.loads(outcome["resultJson"]))
    if "paramsJson" in outcome:
        out.params = decode(json.loads(outcome["paramsJson"]))
    return out


def _haul_def(haul: Callable | str) -> _model.HaulDef:
    d = _model.HAULS.get(haul) if isinstance(haul, str) else getattr(haul, "__hubwagen_haul__", None)
    if d is None:
        raise TypeError(f"{haul!r} is not a haul (decorate it with @hubwagen.haul)")
    return d


def _recorded(cmd: dict) -> dict:
    """A command as the server records it (core's shape)."""
    for key, kind, target in (("lift", "lift", "name"), ("sleep", "sleep", None),
                              ("latestPallet", "latest_pallet", "dataset"), ("waitSignal", "wait_signal", "name"),
                              ("startHaul", "start_haul", "name")):
        if key in cmd:
            out = {"seq": cmd["seq"], "kind": kind}
            if target:
                out[target] = cmd[key][target]
            return out
    raise TestingError(f"unknown command {cmd}")


def _seconds(duration: str) -> float:
    return float(duration.rstrip("s"))


class _Server:
    """Answers commands the way the server would, from the script."""

    def __init__(self, lifts, run_lifts, signals, pallets, children, now):
        self.lifts, self.run_lifts, self.pallets, self.children = lifts, run_lifts, pallets, children
        self.signals = {k: list(v) if isinstance(v, list) else [v] for k, v in signals.items()}
        self.now = now
        self.lift_calls: list[LiftCall] = []
        self.child_calls: list[tuple[str, dict]] = []

    def event(self, type_: str, payload: dict) -> dict:
        return {"type": type_, "time": self.now.isoformat().replace("+00:00", "Z"), "payloadJson": json.dumps(payload)}

    def answer(self, cmd: dict) -> list[dict]:
        seq = cmd["seq"]
        if "lift" in cmd:
            return [self._lift(seq, cmd["lift"])]
        if "sleep" in cmd:
            self.now += _dt.timedelta(seconds=_seconds(cmd["sleep"]["duration"]))
            return [self.event("TimerFired", {"seq": seq})]
        if "latestPallet" in cmd:
            p = self.pallets.get(cmd["latestPallet"]["dataset"])
            answer = None if p is None else {"id": p.id, "dataset": p.dataset, "partition": p.partition,
                                             "meta": p.meta, "stats": p.stats}
            return [self.event("QueryAnswered", {"seq": seq, "pallet": answer})]
        if "waitSignal" in cmd:
            w = cmd["waitSignal"]
            left = self.signals.get(w["name"])
            if left:
                return [self.event("SignalDelivered", {"seq": seq, "name": w["name"], "signal": f"signal-{seq}",
                                                       "payload": encode(left.pop(0))})]
            if "timeout" in w:
                self.now += _dt.timedelta(seconds=_seconds(w["timeout"]))
                return [self.event("SignalDelivered", {"seq": seq, "name": w["name"], "timed_out": True})]
            raise TestingError(f"command {seq}: the haul waits for signal {w['name']!r} with no timeout; "
                               f"script it: signals={{{w['name']!r}: ...}}")
        if "startHaul" in cmd:
            return self._child(seq, cmd["startHaul"])
        raise TestingError(f"command {seq}: unknown {cmd}")

    def _lift(self, seq: int, body: dict) -> dict:
        name = body["name"]
        pallets = {i["name"]: Pallet(id=i["pallet"]) for i in body.get("inputs", [])}
        args = decode(json.loads(body.get("argsJson") or "{}"), pallets)
        self.lift_calls.append(LiftCall(seq, name, args))
        script = self.lifts.get(name, _MISSING)
        if script is _MISSING:
            if not self.run_lifts:
                raise TestingError(f"command {seq}: lift {name} isn't scripted; pass lifts={{{name!r}: ...}} "
                                   "or run_lifts=True")
            script = _model.lift_def(name).fn
        payload: dict[str, Any] = {"seq": seq, "status": "completed"}
        try:
            result = script(**args) if callable(script) and not isinstance(script, type) else script
            if isinstance(result, BaseException) or (isinstance(result, type) and issubclass(result, BaseException)):
                raise result
        except Exception as e:  # noqa: BLE001 - the scripted lift failed
            payload.update(status="failed", reason=type(e).__name__, message=str(e))
            return self.event("LiftFinished", payload)
        payload["result"] = encode(result)
        if "output" in body:
            payload["pallet"] = f"pallet-{seq}"
        return self.event("LiftFinished", payload)

    def _child(self, seq: int, body: dict) -> list[dict]:
        name = body["name"]
        params = decode(json.loads(body.get("paramsJson") or "{}"))
        self.child_calls.append((name, params))
        child = f"child-{seq}"
        events = [self.event("ChildStarted", {"seq": seq, "name": name, "haul": child})]
        if not body.get("wait"):
            return events
        script = self.children.get(name, _MISSING)
        if script is _MISSING:
            raise TestingError(f"command {seq}: child haul {name} isn't scripted; pass children={{{name!r}: ...}}")
        payload: dict[str, Any] = {"seq": seq, "name": name, "haul": child, "status": "completed"}
        try:
            result = script(**params) if callable(script) and not isinstance(script, type) else script
            if isinstance(result, BaseException) or (isinstance(result, type) and issubclass(result, BaseException)):
                raise result
            payload["result"] = encode(result)
        except Exception as e:  # noqa: BLE001 - the scripted child failed
            payload.update(status="failed", reason=type(e).__name__, message=str(e))
        return events + [self.event("ChildFinished", payload)]


_MISSING = object()


# --- replaying recorded logbooks ---------------------------------------------

def replay_recorded(haul: dict, logbook: list[dict]) -> str | None:
    """Replay one recorded haul (GetLogbook's) against the code imported
    now. Returns why it would be stuck, or None if it replays as recorded."""
    d = _model.HAULS.get(haul["name"])
    if d is None:
        return f"no haul {haul['name']!r} in this code"
    params = json.loads(haul.get("paramsJson") or "{}")
    r = _replay.replay(d, haul["id"], params, logbook)
    if r.nondeterministic or r.mismatch:
        return r.mismatch
    final = None
    for e in logbook:
        if e["type"] == "HaulTaskCompleted":
            final = json.loads(e["payloadJson"]).get("outcome") or final
    if final is not None and haul["status"] in ("completed", "failed", "continued"):
        if r.outcome is None:
            return f"the haul {haul['status']}, the replay doesn't end"
        status = {"STATUS_COMPLETED": "completed", "STATUS_FAILED": "failed", "STATUS_CONTINUED": "continued"}[r.outcome["status"]]
        if status != final["status"]:
            return f"the haul {final['status']}, the replay {status}"
        if status == "completed" and _canon(json.loads(r.outcome.get("resultJson") or "null")) != _canon(final.get("result")):
            return f"the haul returned {json.dumps(final.get('result'))}, the replay {r.outcome.get('resultJson')}"
    return None


def _canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if os.environ.get("PYTHONHASHSEED") != "0":  # replays iterate sets as the worker does
        os.execve(sys.executable, [sys.executable, "-m", "hubwagen.testing", *argv], {**os.environ, "PYTHONHASHSEED": "0"})
    p = argparse.ArgumentParser(prog="python -m hubwagen.testing",
                                description="Replay recorded logbooks against this code.")
    p.add_argument("modules", nargs="*", help="modules that define the hauls (default: [tool.hubwagen] modules)")
    p.add_argument("--server", default=os.environ.get("HUBWAGEN_SERVER", "http://127.0.0.1:7070"))
    p.add_argument("--replay", action="append", default=[], metavar="HAUL_ID", help="replay this haul (repeatable)")
    p.add_argument("--replay-recent", type=int, default=0, metavar="N", help="replay the N most recent hauls")
    p.add_argument("--haul", help="with --replay-recent: only hauls of this name")
    args = p.parse_args(argv)
    if not args.replay and not args.replay_recent:
        p.error("nothing to replay: pass --replay HAUL_ID or --replay-recent N")

    from .client import Client
    from .worker import project_modules

    sys.path.insert(0, os.getcwd())
    for m in args.modules or project_modules(Path.cwd()):
        importlib.import_module(m)
    client = Client(args.server)
    ids = list(args.replay)
    if args.replay_recent:
        body: dict[str, Any] = {"limit": args.replay_recent}
        if args.haul:
            body["name"] = args.haul
        ids += [h["id"] for h in client.call("HaulService", "ListHauls", body).get("hauls", [])]
    failed = 0
    for hid in ids:
        res = client.call("HaulService", "GetLogbook", {"haulId": hid})
        haul = res["haul"]
        why = replay_recorded(haul, res.get("logbook", []))
        label = f"{hid} {haul['name']} ({haul['status']})"
        if why:
            failed += 1
            print(f"FAIL {label}: {why}")
        else:
            print(f"ok   {label}")
    print(f"{len(ids) - failed} of {len(ids)} replay as recorded", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

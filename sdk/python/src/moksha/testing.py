"""Testing pipelines: run one against scripted results, or replay what the
server recorded against new code.

In tests, `run` plays the server in memory: it replays the pipeline,
answers each new command from what the test scripted, records the answers
in a history like the server's, and replays again until the run ends.
Sleeps end at once, on a clock that only moves when the run sleeps.

    from moksha import testing

    def test_gate():
        r = testing.run(daily, {"day": "2026-10-24"},
                        steps={"check_quality": {"failed": True}, "build": 42},
                        signals={"approve": {"by": "ana"}})
        assert r.status == "completed" and r.result == 42
        assert [c.name for c in r.steps] == ["check_quality", "build"]

Before a deploy, replay what the server recorded against the new code:

    python -m moksha.testing --replay-recent 50 --server http://moksha:7070
    python -m moksha.testing --replay 01M6... [--pipeline daily]

A recorded run the new code contradicts (another call where one was
recorded, a version marker decided differently, the clock read, or another
result for a finished run) is a run the deploy would make stuck. Exits 1 if
any is.
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
from ._model import DataVersion, decode, encode

START = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)
MAX_ROUNDS = 10_000


class TestingError(AssertionError):
    """The pipeline can't be run as scripted, or it isn't deterministic."""

    __test__ = False  # not a pytest test class


@dataclass
class StepCall:
    seq: int
    name: str
    args: dict[str, Any]



@dataclass
class Run:
    """How a test run ended: status is completed, failed or continued."""

    status: str
    result: Any = None  # completed: what the pipeline returned
    reason: str = ""  # failed: the exception's type
    message: str = ""
    params: dict | None = None  # continued: the next run's inputs
    steps: list[StepCall] = field(default_factory=list)
    children: list[tuple[str, dict]] = field(default_factory=list)  # runs it started: (pipeline, inputs)
    event_log: list[dict] = field(default_factory=list)


def run(pipeline: Callable | str, params: dict | None = None, *, steps: dict[str, Any] | None = None,
        run_steps: bool = False, signals: dict[str, Any] | None = None,
        versions: dict[str, DataVersion | None] | None = None, children: dict[str, Any] | None = None,
        now: _dt.datetime = START, run_id: str = "test-run") -> Run:
    """Run a pipeline to its end with these inputs (params), against
    scripted results.

    steps: step name → its result, a function called with the step's
    arguments, or an exception (the step fails, after its retries).
    run_steps: call the real step functions for steps not in steps.
    signals: signal name → payload, or a list of payloads (one per wait).
    A wait with a timeout and no payload left times out.
    versions: dataset name → what ctx.latest_version returns (default None).
    children: name of a pipeline it runs (ctx.run) → its result, a function
    called with its inputs, or an exception (that run fails).
    """
    d = _pipeline_def(pipeline)
    sim = _Server(steps or {}, run_steps, signals or {}, versions or {}, children or {}, now)
    event_log = [sim.event("RunStarted", {"name": d.name, "params": encode(params or {})})]
    for _ in range(MAX_ROUNDS):
        r = _replay.replay(d, run_id, encode(params or {}), event_log)
        if r.nondeterministic or r.mismatch:
            raise TestingError(f"pipeline {d.name} isn't deterministic: {r.mismatch}")
        recorded = sum(len(json.loads(e["payloadJson"]).get("commands") or []) for e in event_log
                       if e["type"] == "RunTaskCompleted")
        new = r.commands[recorded:]
        known = {m["name"] for e in event_log if e["type"] == "RunTaskCompleted"
                 for m in json.loads(e["payloadJson"]).get("markers") or []}
        markers = [m for m in r.markers if m["name"] not in known]
        event_log.append(sim.event("RunTaskCompleted", {"commands": [_recorded(c) for c in new],
                                                      **({"markers": markers} if markers else {})}))
        if r.outcome:
            return _finish(d, run_id, params, event_log, r.outcome, sim)
        if not new:
            raise TestingError(f"pipeline {d.name} is blocked with nothing left to answer it")
        for cmd in new:
            event_log += sim.answer(cmd)
    raise TestingError(f"pipeline {d.name} didn't end within {MAX_ROUNDS} replays")


def _finish(d, run_id, params, event_log, outcome, sim) -> Run:
    # One more replay of the whole event log must end the same way.
    again = _replay.replay(d, run_id, encode(params or {}), event_log)
    if again.outcome != outcome or again.mismatch:
        raise TestingError(f"pipeline {d.name} ended differently on a replay of its own history: "
                           f"{again.mismatch or again.outcome}")
    status = {"STATUS_COMPLETED": "completed", "STATUS_FAILED": "failed", "STATUS_CONTINUED": "continued"}[outcome["status"]]
    out = Run(status=status, steps=sim.step_calls, children=sim.child_calls, event_log=event_log,
              reason=outcome.get("reason", ""), message=outcome.get("message", ""))
    if "resultJson" in outcome:
        out.result = decode(json.loads(outcome["resultJson"]))
    if "paramsJson" in outcome:
        out.params = decode(json.loads(outcome["paramsJson"]))
    return out


def _pipeline_def(run: Callable | str) -> _model.PipelineDef:
    d = _model.PIPELINES.get(run) if isinstance(run, str) else getattr(run, "__moksha_pipeline__", None)
    if d is None:
        raise TypeError(f"{run!r} is not a pipeline (decorate it with @moksha.pipeline)")
    return d


def _recorded(cmd: dict) -> dict:
    """A command as the server records it (core's shape)."""
    for key, kind, target in (("step", "step", "name"), ("sleep", "sleep", None),
                              ("latestDataVersion", "latest_version", "dataset"), ("waitSignal", "wait_signal", "name"),
                              ("startRun", "start_run", "name")):
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

    def __init__(self, steps, run_steps, signals, data_versions, children, now):
        self.steps, self.run_steps, self.data_versions, self.children = steps, run_steps, data_versions, children
        self.signals = {k: list(v) if isinstance(v, list) else [v] for k, v in signals.items()}
        self.now = now
        self.step_calls: list[StepCall] = []
        self.child_calls: list[tuple[str, dict]] = []

    def event(self, type_: str, payload: dict) -> dict:
        return {"type": type_, "time": self.now.isoformat().replace("+00:00", "Z"), "payloadJson": json.dumps(payload)}

    def answer(self, cmd: dict) -> list[dict]:
        seq = cmd["seq"]
        if "step" in cmd:
            return [self._step(seq, cmd["step"])]
        if "sleep" in cmd:
            self.now += _dt.timedelta(seconds=_seconds(cmd["sleep"]["duration"]))
            return [self.event("TimerFired", {"seq": seq})]
        if "latestDataVersion" in cmd:
            p = self.data_versions.get(cmd["latestDataVersion"]["dataset"])
            answer = None if p is None else {"id": p.id, "dataset": p.dataset, "partition": p.partition,
                                             "meta": p.meta, "stats": p.stats}
            return [self.event("QueryAnswered", {"seq": seq, "data_version": answer})]
        if "waitSignal" in cmd:
            w = cmd["waitSignal"]
            left = self.signals.get(w["name"])
            if left:
                return [self.event("SignalDelivered", {"seq": seq, "name": w["name"], "signal": f"signal-{seq}",
                                                       "payload": encode(left.pop(0))})]
            if "timeout" in w:
                self.now += _dt.timedelta(seconds=_seconds(w["timeout"]))
                return [self.event("SignalDelivered", {"seq": seq, "name": w["name"], "timed_out": True})]
            raise TestingError(f"command {seq}: the run waits for signal {w['name']!r} with no timeout; "
                               f"script it: signals={{{w['name']!r}: ...}}")
        if "startRun" in cmd:
            return self._child(seq, cmd["startRun"])
        raise TestingError(f"command {seq}: unknown {cmd}")

    def _step(self, seq: int, body: dict) -> dict:
        name = body["name"]
        data_versions = {i["name"]: DataVersion(id=i["data_version"]) for i in body.get("inputs", [])}
        args = decode(json.loads(body.get("argsJson") or "{}"), data_versions)
        self.step_calls.append(StepCall(seq, name, args))
        script = self.steps.get(name, _MISSING)
        if script is _MISSING:
            if not self.run_steps:
                raise TestingError(f"command {seq}: step {name} isn't scripted; pass steps={{{name!r}: ...}} "
                                   "or run_steps=True")
            script = _model.step_def(name).fn
        payload: dict[str, Any] = {"seq": seq, "status": "completed"}
        try:
            result = script(**args) if callable(script) and not isinstance(script, type) else script
            if isinstance(result, BaseException) or (isinstance(result, type) and issubclass(result, BaseException)):
                raise result
        except Exception as e:  # noqa: BLE001 - the scripted step failed
            payload.update(status="failed", reason=type(e).__name__, message=str(e))
            return self.event("StepFinished", payload)
        payload["result"] = encode(result)
        if "output" in body:
            payload["data_version"] = f"data_version-{seq}"
        return self.event("StepFinished", payload)

    def _child(self, seq: int, body: dict) -> list[dict]:
        name = body["name"]
        params = decode(json.loads(body.get("paramsJson") or "{}"))
        self.child_calls.append((name, params))
        child = f"child-{seq}"
        events = [self.event("ChildStarted", {"seq": seq, "name": name, "run": child})]
        if not body.get("wait"):
            return events
        script = self.children.get(name, _MISSING)
        if script is _MISSING:
            raise TestingError(f"command {seq}: the run of {name} isn't scripted; pass children={{{name!r}: ...}}")
        payload: dict[str, Any] = {"seq": seq, "name": name, "run": child, "status": "completed"}
        try:
            result = script(**params) if callable(script) and not isinstance(script, type) else script
            if isinstance(result, BaseException) or (isinstance(result, type) and issubclass(result, BaseException)):
                raise result
            payload["result"] = encode(result)
        except Exception as e:  # noqa: BLE001 - the scripted child failed
            payload.update(status="failed", reason=type(e).__name__, message=str(e))
        return events + [self.event("ChildFinished", payload)]


_MISSING = object()


# --- replaying recorded event logs ---------------------------------------------

def replay_recorded(run: dict, event_log: list[dict]) -> str | None:
    """Replay one recorded run (GetEventLog's) against the code imported
    now. Returns why it would be stuck, or None if it replays as recorded."""
    d = _model.PIPELINES.get(run["name"])
    if d is None:
        return f"no pipeline {run['name']!r} in this code"
    params = json.loads(run.get("paramsJson") or "{}")
    r = _replay.replay(d, run["id"], params, event_log)
    if r.nondeterministic or r.mismatch:
        return r.mismatch
    final = None
    for e in event_log:
        if e["type"] == "RunTaskCompleted":
            final = json.loads(e["payloadJson"]).get("outcome") or final
    if final is not None and run["status"] in ("completed", "failed", "continued"):
        if r.outcome is None:
            return f"the run {run['status']}, the replay doesn't end"
        status = {"STATUS_COMPLETED": "completed", "STATUS_FAILED": "failed", "STATUS_CONTINUED": "continued"}[r.outcome["status"]]
        if status != final["status"]:
            return f"the run {final['status']}, the replay {status}"
        if status == "completed" and _canon(json.loads(r.outcome.get("resultJson") or "null")) != _canon(final.get("result")):
            return f"the run returned {json.dumps(final.get('result'))}, the replay {r.outcome.get('resultJson')}"
    return None


def _canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if os.environ.get("PYTHONHASHSEED") != "0":  # replays iterate sets as the worker does
        os.execve(sys.executable, [sys.executable, "-m", "moksha.testing", *argv], {**os.environ, "PYTHONHASHSEED": "0"})
    p = argparse.ArgumentParser(prog="python -m moksha.testing",
                                description="Replay recorded runs against this code.")
    p.add_argument("modules", nargs="*", help="modules that define the pipelines (default: [tool.moksha] modules)")
    p.add_argument("--server", default=os.environ.get("MOKSHA_SERVER", "http://127.0.0.1:7070"))
    p.add_argument("--replay", action="append", default=[], metavar="RUN_ID", help="replay this run (repeatable)")
    p.add_argument("--replay-recent", type=int, default=0, metavar="N", help="replay the N most recent runs")
    p.add_argument("--pipeline", dest="run", help="with --replay-recent: only runs of this pipeline")
    args = p.parse_args(argv)
    if not args.replay and not args.replay_recent:
        p.error("nothing to replay: pass --replay RUN_ID or --replay-recent N")

    from .client import Client
    from .worker import project_modules

    sys.path.insert(0, os.getcwd())
    for m in args.modules or project_modules(Path.cwd()):
        importlib.import_module(m)
    client = Client(args.server)
    ids = list(args.replay)
    if args.replay_recent:
        body: dict[str, Any] = {"limit": args.replay_recent}
        if args.run:
            body["name"] = args.run
        ids += [h["id"] for h in client.call("RunService", "ListRuns", body).get("runs", [])]
    failed = 0
    for hid in ids:
        res = client.call("RunService", "GetEventLog", {"runId": hid})
        run = res["run"]
        why = replay_recorded(run, res.get("eventLog", []))
        label = f"{hid} {run['name']} ({run['status']})"
        if why:
            failed += 1
            print(f"FAIL {label}: {why}")
        else:
            print(f"ok   {label}")
    print(f"{len(ids) - failed} of {len(ids)} replay as recorded", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

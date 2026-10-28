"""Replay until blocked: run a haul function against its logbook.

Each ctx call either returns its result from the logbook or is recorded as
a new command; the replay stops at the first call with no result yet. The
worker sends every command issued (from seq 1) and, if the function ended,
its outcome. Haul code must be deterministic: the same calls in the same
order on every replay (see docs/spikes/s1-python-replay.md).
"""

from __future__ import annotations

import datetime as _dt
import inspect
import json
import random as _random
import sys
from dataclasses import dataclass, field
from typing import Any, Callable

from ._guards import NondeterminismError, guarded
from ._model import Dataset, HaulDef, LiftDef, LiftFailed, Pallet, decode, encode, lift_def


class Blocked(BaseException):
    """The replay reached a call without a result. A BaseException, so haul
    code's `except Exception:` can't swallow it and carry on with a made-up
    value."""


def _duration(seconds: float) -> str:
    """A protobuf JSON Duration."""
    return format(seconds, ".9f").rstrip("0").rstrip(".") + "s"


def _time(s: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def _partition_value(v: Any) -> str:
    if isinstance(v, _dt.datetime):
        return v.isoformat()
    if isinstance(v, _dt.date):
        return v.isoformat()
    return str(v)


@dataclass
class _Result:
    payload: dict
    time: _dt.datetime


@dataclass
class Handle:
    """A lift started with ctx.lift_async; pass handles to ctx.gather."""

    seq: int
    lift: LiftDef


@dataclass
class ReplayResult:
    commands: list[dict]
    # Set when the haul function ended: {"status": "STATUS_COMPLETED", ...}.
    outcome: dict | None
    # Set when a command contradicted the logbook, or haul code read the
    # clock or randomness (nondeterministic); the haul will be stuck.
    mismatch: str | None = None
    nondeterministic: bool = False


class Ctx:
    """What haul code gets as its first argument."""

    def __init__(self, haul_id: str, logbook: list[dict]):
        self.haul_id = haul_id
        self.state: dict[str, Any] = {}  # rebuilt by every replay
        self.rng = _random.Random(haul_id)  # deterministic per haul
        self._seq = 0
        self.commands: list[dict] = []
        self.mismatch: str | None = None
        self._results: dict[int, _Result] = {}
        self._recorded: list[dict] = []
        self._now: _dt.datetime | None = None
        for e in logbook:
            payload = json.loads(e.get("payloadJson") or "{}")
            t = _time(e["time"]) if e.get("time") else None
            match e.get("type"):
                case "HaulStarted":
                    self._now = t
                case "LiftFinished" | "TimerFired" | "QueryAnswered":
                    self._results[payload["seq"]] = _Result(payload, t)
                case "HaulTaskCompleted":
                    self._recorded.extend(payload.get("commands") or [])

    # --- commands ---------------------------------------------------------

    def _command(self, kind: str, body: dict, target: str = "") -> int:
        self._seq += 1
        f = sys._getframe(1)
        while f.f_code.co_filename == __file__:  # the first frame in haul code
            f = f.f_back
        cmd = {"seq": self._seq, "source": f"{f.f_code.co_filename}:{f.f_lineno}", **body}
        self.commands.append(cmd)
        if self._seq <= len(self._recorded):
            old = self._recorded[self._seq - 1]
            old_target = old.get("name") or old.get("dataset") or ""
            if old.get("kind") != kind or old_target != target:
                self.mismatch = (f"command {self._seq}: replay issued {kind} {target!r} at {cmd['source']}, "
                                 f"the logbook has {old.get('kind')} {old_target!r}"
                                 + (f" at {old['source']}" if old.get("source") else ""))
                raise Blocked()
        return self._seq

    def _consume(self, seq: int) -> dict | None:
        r = self._results.get(seq)
        if r is None:
            return None
        if r.time and (self._now is None or r.time > self._now):
            self._now = r.time
        return r.payload

    def _lift_command(self, f: Callable | str, args: tuple, kwargs: dict) -> tuple[int, LiftDef]:
        d = lift_def(f)
        bound = inspect.signature(d.fn).bind(*args, **kwargs)
        inputs, call = [], {}
        for name, value in bound.arguments.items():
            if isinstance(value, Pallet):
                inputs.append({"name": name, "pallet": value.id})
                call[name] = {"$input": name}  # the ID stays out of the arguments (and the fingerprint)
            else:
                call[name] = encode(value)
        body: dict[str, Any] = {"name": d.name, "argsJson": json.dumps(call, sort_keys=True)}
        if d.dock:
            body["dock"] = d.dock
        if d.timeout:
            body["timeout"] = _duration(d.timeout)
        if d.retry:
            r = {"limit": d.retry.limit, "backoff": d.retry.backoff, "initial": _duration(d.retry.initial)}
            if d.retry.max is not None:
                r["max"] = _duration(d.retry.max)
            body["retry"] = r
        if d.output is not None:
            out = {"dataset": d.output.name}
            if d.output.partition:
                out["partitionKey"] = d.output.partition
                out["partition"] = _partition_value(bound.arguments[d.output.partition])
            body["output"] = out
        if inputs:
            body["inputs"] = inputs
        if d.memo:
            body["memo"] = d.memo
        return self._command("lift", {"lift": body}, d.name), d

    def _lift_result(self, seq: int, d: LiftDef, r: dict) -> Any:
        if r.get("status") != "completed":
            raise LiftFailed(d.name, r.get("reason", r.get("status", "")), r.get("message", ""))
        result = decode(r.get("result"))
        if not r.get("pallet"):
            return result
        # A pallet: the lift's output, or for a lift without one, the
        # dataframe it returned, in its implicit dataset.
        dataset = d.output.name if d.output is not None else "lift:" + d.name
        return Pallet(id=r["pallet"], dataset=dataset, result=result, reused=bool(r.get("reused")),
                      partition=self._partition_of(seq))

    def _partition_of(self, seq: int) -> str:
        return self.commands[seq - 1]["lift"].get("output", {}).get("partition", "")

    def lift(self, f: Callable | str, *args: Any, **kwargs: Any) -> Any:
        """Run a lift and wait for it. Returns what it returned, or a Pallet
        for a lift with an output or one that returned a dataframe. Raises
        LiftFailed if it failed."""
        seq, d = self._lift_command(f, args, kwargs)
        r = self._consume(seq)
        if r is None:
            raise Blocked()
        return self._lift_result(seq, d, r)

    def lift_async(self, f: Callable | str, *args: Any, **kwargs: Any) -> Handle:
        """Start a lift without waiting; collect it with gather."""
        seq, d = self._lift_command(f, args, kwargs)
        return Handle(seq, d)

    def gather(self, handles: list[Handle]) -> list[Any]:
        """Wait for every handle; results in the same order. Raises the first
        LiftFailed among them."""
        results = [self._consume(h.seq) for h in handles]
        if any(r is None for r in results):
            raise Blocked()
        return [self._lift_result(h.seq, h.lift, r) for h, r in zip(handles, results)]

    def sleep(self, duration: float | _dt.timedelta) -> None:
        """A durable sleep, held by the server: survives restarts and deploys."""
        if isinstance(duration, _dt.timedelta):
            duration = duration.total_seconds()
        seq = self._command("sleep", {"sleep": {"duration": _duration(duration)}})
        if self._consume(seq) is None:
            raise Blocked()

    def latest_pallet(self, dataset: Dataset | str, partition: Any = None) -> Pallet | None:
        """The newest committed pallet of a dataset partition, or None. The
        answer is recorded, so every replay gets the same one."""
        name = dataset.name if isinstance(dataset, Dataset) else dataset
        body = {"dataset": name}
        if partition is not None:
            body["partition"] = _partition_value(partition)
        seq = self._command("latest_pallet", {"latestPallet": body}, name)
        r = self._consume(seq)
        if r is None:
            raise Blocked()
        p = r.get("pallet")
        if not p:
            return None
        return Pallet(id=p["id"], dataset=p.get("dataset", name), partition=p.get("partition", ""),
                      location=p.get("location", ""), meta=p.get("meta") or {}, stats=p.get("stats") or {})

    # --- deterministic values ---------------------------------------------

    def now(self) -> _dt.datetime:
        """The time of the newest result this replay has used (the haul's
        start before any): the same on every replay."""
        if self._now is None:
            raise RuntimeError("the logbook has no HaulStarted event")
        return self._now

    def random(self) -> float:
        """A random float, the same sequence on every replay of this haul."""
        return self.rng.random()


def replay(d: HaulDef, haul_id: str, params: dict, logbook: list[dict]) -> ReplayResult:
    """Run the haul function until it blocks or ends."""
    ctx = Ctx(haul_id, logbook)
    try:
        with guarded():
            result = d.fn(ctx, **decode(params))
    except Blocked:
        return ReplayResult(ctx.commands, None, ctx.mismatch)
    except NondeterminismError as e:
        return ReplayResult(ctx.commands, None, str(e), nondeterministic=True)
    except Exception as e:  # noqa: BLE001 - the haul function raised: it failed
        return ReplayResult(ctx.commands, {"status": "STATUS_FAILED", "reason": type(e).__name__, "message": str(e)})
    return ReplayResult(ctx.commands, {"status": "STATUS_COMPLETED", "resultJson": json.dumps(encode(result))})

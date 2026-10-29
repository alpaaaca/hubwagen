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
from typing import Any, Callable, NoReturn

from ._guards import NondeterminismError, guarded
from ._model import Dataset, HaulDef, LiftDef, LiftFailed, Pallet, decode, encode, lift_def


class ChildFailed(Exception):
    """A child haul started with ctx.haul failed, was cancelled, or couldn't
    start (status "failed", reason "start_failed"). Haul code may catch it."""

    def __init__(self, name: str, haul_id: str, status: str, reason: str, message: str):
        what = f"child haul {name}" + (f" ({haul_id})" if haul_id else "")
        super().__init__(f"{what} {status}: {reason}" + (f": {message}" if message else ""))
        self.name, self.haul_id, self.status, self.reason, self.message = name, haul_id, status, reason, message


class _ContinueAsNew(BaseException):
    """Raised by ctx.continue_as_new to end the replay; never seen by haul
    code that catches Exception."""

    def __init__(self, params: dict):
        self.params = params


class SignalTimeout(Exception):
    """ctx.wait_signal's timeout passed before the signal came. Haul code
    may catch it."""

    def __init__(self, name: str, timeout: float | None):
        super().__init__(f"no signal {name!r} within {timeout:g}s" if timeout else f"no signal {name!r}")
        self.name, self.timeout = name, timeout


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
    # Every version marker the replay decided: [{"name", "version"}].
    markers: list[dict] = field(default_factory=list)


class Ctx:
    """What haul code gets as its first argument."""

    def __init__(self, haul_id: str, logbook: list[dict], max_parallel: int | None = None):
        self.haul_id = haul_id
        self.state: dict[str, Any] = {}  # rebuilt by every replay
        self.rng = _random.Random(haul_id)  # deterministic per haul
        self._max_parallel = max_parallel  # @haul(max_parallel=): every lift's haul-wide limit
        self._seq = 0
        self.commands: list[dict] = []
        self.mismatch: str | None = None
        self._results: dict[int, _Result] = {}
        self._recorded: list[dict] = []
        self._now: _dt.datetime | None = None
        self._markers: dict[str, int] = {}  # recorded version markers
        self.markers: dict[str, int] = {}  # decided by this replay
        for e in logbook:
            payload = json.loads(e.get("payloadJson") or "{}")
            t = _time(e["time"]) if e.get("time") else None
            match e.get("type"):
                case "HaulStarted":
                    self._now = t
                case "LiftFinished" | "TimerFired" | "QueryAnswered" | "SignalDelivered" | "ChildStarted" | "ChildFinished":
                    # A waited-for child's ChildFinished comes after its
                    # ChildStarted and replaces it.
                    self._results[payload["seq"]] = _Result(payload, t)
                case "HaulTaskCompleted":
                    self._recorded.extend(payload.get("commands") or [])
                    for m in payload.get("markers") or []:
                        self._markers.setdefault(m["name"], m["version"])

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
        if self._max_parallel:
            body["limits"] = [{"group": "haul", "max": self._max_parallel}]
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

    def gather(self, handles: list[Handle], max_parallel: int | None = None) -> list[Any]:
        """Wait for every handle; results in the same order. Raises the first
        LiftFailed among them.

        With max_parallel=N, at most N of these lifts run at once; the server
        starts the rest as earlier ones finish. The limit applies to lifts
        started in the same replay as the gather, as in
        `ctx.gather([ctx.lift_async(f, x) for x in xs], max_parallel=20)`."""
        if max_parallel is not None:
            if not isinstance(max_parallel, int) or max_parallel < 1:
                raise ValueError(f"gather: max_parallel must be a positive integer, got {max_parallel!r}")
            if handles:
                limit = {"group": f"gather:{handles[0].seq}", "max": max_parallel}
                for h in handles:
                    if h.seq > len(self._recorded):  # issued by this replay: not sent yet
                        self.commands[h.seq - 1]["lift"].setdefault("limits", []).append(limit)
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

    def version(self, name: str, max: int, min: int = 1) -> int:  # noqa: A002 - the names read best
        """Which version of a change this haul takes, for changing haul code
        while hauls are running:

            if ctx.version("quality-gate", max=2) >= 2:
                report = ctx.lift(check_quality, orders)

        A haul that started before the marker existed gets `min` (the old
        code path, so its logbook still replays); a haul that reaches the
        marker for the first time gets `max`. The decision is recorded, so
        every later replay gets the same. Raise `min` once no running haul
        takes the old version any more; a haul still on it becomes stuck.
        """
        if not (isinstance(min, int) and isinstance(max, int)) or not 0 <= min <= max:
            raise ValueError(f"version({name!r}): want integers 0 <= min <= max, got min={min!r}, max={max!r}")
        v = self._markers.get(name)
        if v is None:
            # Inside the recorded history (commands after this point are in the
            # logbook): this haul ran the code from before the marker.
            v = min if self._seq < len(self._recorded) else max
            self._markers[name] = v
        elif not min <= v <= max:
            raise NondeterminismError(f"version marker {name!r}: this haul took version {v}, the code supports "
                                      f"{min} to {max}; keep the old branch until such hauls finish")
        self.markers[name] = v
        return v

    def haul(self, f: Callable | str, **params: Any) -> Any:
        """Start another haul (a child) and wait for its result. Raises
        ChildFailed if it fails, is cancelled or can't start. The child runs
        on its own logbook; cancelling or ending this haul cancels it."""
        seq, name = self._start_haul_command(f, params, wait=True)
        r = self._consume(seq)
        if r is None or "status" not in r:  # not started yet, or started and running
            raise Blocked()
        if r["status"] != "completed":
            raise ChildFailed(name, r.get("haul", ""), r["status"], r.get("reason", ""), r.get("message", ""))
        return decode(r.get("result"))

    def start_haul(self, f: Callable | str, **params: Any) -> str:
        """Start another haul without waiting for it; returns its ID. It
        carries on whatever this haul does. Raises ChildFailed if it can't
        start."""
        seq, name = self._start_haul_command(f, params, wait=False)
        r = self._consume(seq)
        if r is None:
            raise Blocked()
        if "status" in r:
            raise ChildFailed(name, "", r["status"], r.get("reason", ""), r.get("message", ""))
        return r["haul"]

    def continue_as_new(self, **params: Any) -> NoReturn:
        """End this haul and start it again with params and a fresh logbook,
        for hauls that loop (a cursor, a poll): the logbook, and so every
        replay, stays short. A haul waiting for this one gets the result of
        the last haul in the chain. Signals not yet taken don't carry over."""
        raise _ContinueAsNew(params)

    def _start_haul_command(self, f: Callable | str, params: dict, wait: bool) -> tuple[int, str]:
        if isinstance(f, str):
            name = f
        else:
            d = getattr(f, "__hubwagen_haul__", None)
            if d is None:
                raise TypeError(f"{f!r} is not a haul (decorate it with @hubwagen.haul)")
            name = d.name
            inspect.signature(d.fn).bind(None, **params)  # a TypeError here, not in the child
        body = {"name": name, "paramsJson": json.dumps(encode(params), sort_keys=True)}
        if wait:
            body["wait"] = True
        return self._command("start_haul", {"startHaul": body}, name), name

    def wait_signal(self, name: str, timeout: float | _dt.timedelta | None = None) -> Any:
        """Wait for a signal named name (`hubwagen signal <haul> <name>`, or
        SignalHaul) and return its payload. A signal sent before the haul
        waits is kept for it; each signal answers one wait. Raises
        SignalTimeout if timeout (seconds or a timedelta) passes first. The
        wait is held by the server: it survives restarts and deploys."""
        body: dict[str, Any] = {"name": name}
        if timeout is not None:
            if isinstance(timeout, _dt.timedelta):
                timeout = timeout.total_seconds()
            if timeout <= 0:
                raise ValueError(f"wait_signal({name!r}): timeout must be positive")
            body["timeout"] = _duration(timeout)
        seq = self._command("wait_signal", {"waitSignal": body}, name)
        r = self._consume(seq)
        if r is None:
            raise Blocked()
        if r.get("timed_out"):
            raise SignalTimeout(name, timeout)
        return decode(r.get("payload"))

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
    ctx = Ctx(haul_id, logbook, d.max_parallel)
    try:
        inspect.signature(d.fn).bind(None, **params)
    except TypeError as e:
        hint = ""
        if d.schedule is not None and "interval" not in params:
            hint = ('; its schedule passes the interval: to start it by hand, give '
                    '{"interval": {"$interval": {"start": ..., "end": ...}}} (Start a run in the web UI fills in the latest)')
        return ReplayResult([], {"status": "STATUS_FAILED", "reason": "TypeError",
                                 "message": f"haul {d.name} was started with inputs {json.dumps(params)}: {e}{hint}"})
    try:
        with guarded():
            result = d.fn(ctx, **decode(params))
    except Blocked:
        return ReplayResult(ctx.commands, None, ctx.mismatch, markers=_markers(ctx))
    except NondeterminismError as e:
        return ReplayResult(ctx.commands, None, str(e), nondeterministic=True)
    except _ContinueAsNew as c:
        return ReplayResult(ctx.commands, {"status": "STATUS_CONTINUED", "paramsJson": json.dumps(encode(c.params))},
                            markers=_markers(ctx))
    except Exception as e:  # noqa: BLE001 - the haul function raised: it failed
        return ReplayResult(ctx.commands, {"status": "STATUS_FAILED", "reason": type(e).__name__, "message": str(e)},
                            markers=_markers(ctx))
    return ReplayResult(ctx.commands, {"status": "STATUS_COMPLETED", "resultJson": json.dumps(encode(result))},
                        markers=_markers(ctx))


def _markers(ctx: Ctx) -> list[dict]:
    return [{"name": n, "version": v} for n, v in sorted(ctx.markers.items())]

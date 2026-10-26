"""Spike S1: a minimal replay-until-blocked SDK, to measure replay cost,
call-site capture and determinism guards. Throwaway; not the SDK."""

from __future__ import annotations

import json
import sys
from typing import Any, Callable

# ---------------------------------------------------------------- replay


class Blocked(BaseException):
    """The replay reached a call without a result. A BaseException, so haul
    code's `except Exception:` can't swallow it."""


class BlockedException(Exception):
    """The same, as an Exception: what not to do (see guards_check.py)."""


class LiftFailed(Exception):
    pass


class Ctx:
    def __init__(self, results: dict[int, dict], site: Callable[[], str] | None = None,
                 blocked: type[BaseException] = Blocked):
        self._results = results
        self._seq = 0
        self.commands: list[dict] = []
        self._site = site or site_getframe
        self._blocked = blocked

    def _command(self, kind: str, **fields) -> tuple[int, dict | None]:
        self._seq += 1
        cmd = {"seq": self._seq, "kind": kind, "source": self._site(), **fields}
        self.commands.append(cmd)
        return self._seq, self._results.get(self._seq)

    def lift(self, name: str, *args: Any) -> Any:
        _, r = self._command("lift", name=name, args=list(args))
        if r is None:
            raise self._blocked()
        if r["status"] != "completed":
            raise LiftFailed(f"{name}: {r.get('message', '')}")
        return r.get("result")

    def lift_async(self, name: str, *args: Any) -> int:
        seq, _ = self._command("lift", name=name, args=list(args))
        return seq

    def gather(self, handles: list[int]) -> list[Any]:
        missing = [h for h in handles if h not in self._results]
        if missing:
            raise self._blocked()
        return [self._results[h].get("result") for h in handles]

    def sleep(self, seconds: float) -> None:
        _, r = self._command("sleep", duration=seconds)
        if r is None:
            raise self._blocked()


def results_from_logbook(logbook: list[dict]) -> dict[int, dict]:
    """LiftFinished and TimerFired events, by seq. Payloads arrive as JSON
    text (payload_json), as the worker API sends them."""
    out: dict[int, dict] = {}
    for e in logbook:
        if e["type"] in ("LiftFinished", "TimerFired"):
            p = json.loads(e["payloadJson"])
            out[p["seq"]] = p
    return out


def replay(fn: Callable, params: dict, logbook_json: str, **ctx_kw) -> tuple[list[dict], dict | None]:
    """One haul task: decode the logbook, run fn until it blocks or ends."""
    logbook = json.loads(logbook_json)["logbook"]
    ctx = Ctx(results_from_logbook(logbook), **ctx_kw)
    try:
        result = fn(ctx, **params)
    except Blocked:
        return ctx.commands, None
    except Exception as e:  # noqa: BLE001 - the haul raised: it failed
        return ctx.commands, {"status": "failed", "message": repr(e)}
    return ctx.commands, {"status": "completed", "result": result}


# ------------------------------------------------------------ call sites


def site_getframe() -> str:
    f = sys._getframe(3)  # site <- _command <- lift <- haul code
    return f"{f.f_code.co_filename}:{f.f_lineno}"


def site_none() -> str:
    return ""


def site_inspect() -> str:
    import inspect
    fi = inspect.stack()[3]
    return f"{fi.filename}:{fi.lineno}"


def site_traceback() -> str:
    import traceback
    fs = traceback.extract_stack(limit=4)[0]
    return f"{fs.filename}:{fs.lineno}"

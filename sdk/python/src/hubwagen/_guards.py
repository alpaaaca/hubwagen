"""Runtime guards: haul code that reads the clock or randomness.

A haul is replayed many times and must make the same calls every time, so
while haul code runs, a call *from user code* to time.time, datetime.now,
the module-level random functions, uuid4 and the like raises
NondeterminismError naming the line. Libraries that read the clock
(logging, say) are left alone: the guard watches CALL events with
sys.monitoring (Python >= 3.12) and only looks at calls made from project
files; library locations are disabled after their first event. Spike S1
(docs/spikes/s1-python-replay.md) compared this with patching.

sys.monitoring is process-wide while the worker runs lifts on other
threads, so the guard only acts on the threads currently replaying a haul.
"""

from __future__ import annotations

import datetime
import os
import random
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Iterator

from ._codeversion import is_project_file


class NondeterminismError(BaseException):
    """Haul code read the clock or randomness. A BaseException, like
    Blocked, so `except Exception:` in haul code can't swallow it; the
    haul becomes stuck, and a build with fixed code resumes it."""


_FUNCS = {time.time: "time.time", time.time_ns: "time.time_ns", uuid.uuid1: "uuid.uuid1",
          uuid.uuid4: "uuid.uuid4", os.urandom: "os.urandom"}
# Bound methods are new objects on every access: compared by owner and name.
_METHODS = {(datetime.datetime, "now"), (datetime.datetime, "utcnow"), (datetime.datetime, "today"),
            (datetime.date, "today")}
_RANDOM = random._inst  # the Random behind random.random(), random.choice(), ...


def forbidden(callable_: object, arg0: object) -> str | None:
    """What callable_ is, if haul code must not call it."""
    try:
        name = _FUNCS.get(callable_)  # type: ignore[call-overload]
    except TypeError:  # unhashable
        name = None
    if name:
        return name
    owner = getattr(callable_, "__self__", None)
    attr = getattr(callable_, "__name__", None)
    # random.choice(...) arrives as a bound method of _RANDOM on some
    # versions, as the function Random.choice with _RANDOM as arg0 on others.
    if owner is _RANDOM or (arg0 is _RANDOM and getattr(callable_, "__qualname__", "").startswith("Random.")):
        return f"random.{attr}"
    if isinstance(owner, type) and (owner, attr) in _METHODS:
        return f"{owner.__name__}.{attr}"
    return None


class _Guard:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._threads: set[int] = set()
        self._tool: int | None = None
        self._user: dict[str, bool] = {}

    def enter(self) -> None:
        with self._lock:
            self._threads.add(threading.get_ident())
            if self._tool is None:
                self._start()

    def leave(self) -> None:
        with self._lock:
            self._threads.discard(threading.get_ident())
            if not self._threads and self._tool is not None:
                self._stop()

    def _start(self) -> None:
        mon = sys.monitoring
        # A free tool ID, so debuggers, coverage and profilers keep theirs.
        for tool in (4, 3, 5, 2, 1, 0):
            if mon.get_tool(tool) is None:
                break
        else:
            return  # every ID taken: no guard rather than no replay
        mon.use_tool_id(tool, "hubwagen-guard")
        mon.register_callback(tool, mon.events.CALL, self._on_call)
        mon.set_events(tool, mon.events.CALL)
        self._tool = tool

    def _stop(self) -> None:
        mon = sys.monitoring
        mon.set_events(self._tool, 0)
        mon.register_callback(self._tool, mon.events.CALL, None)
        mon.free_tool_id(self._tool)
        self._tool = None

    def _on_call(self, code, offset, callable_, arg0):  # noqa: ANN001 - sys.monitoring's signature
        fn = code.co_filename
        user = self._user.get(fn)
        if user is None:
            user = self._user[fn] = is_project_file(fn)
        if not user:
            return sys.monitoring.DISABLE  # library code: never look here again
        if threading.get_ident() not in self._threads:
            return None  # a lift on another thread
        what = forbidden(callable_, arg0)
        if what is None:
            return None
        line = _line(code, offset)
        raise NondeterminismError(f"{what}() called at {fn}:{line}: pipeline code must not read the clock or "
                                  f"randomness; use ctx.now(), ctx.random() or a step")


def _line(code, offset: int) -> int:
    for i, (start, *_rest) in enumerate(code.co_positions()):
        if i == offset // 2:
            return start or code.co_firstlineno
    return code.co_firstlineno


_guard = _Guard()


@contextmanager
def guarded() -> Iterator[None]:
    """Guard the haul code this thread runs inside the block."""
    _guard.enter()
    try:
        yield
    finally:
        _guard.leave()

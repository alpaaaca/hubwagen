"""Two ways to stop haul code from reading the clock or randomness.

A. patch: replace module attributes (time.time, random.random, uuid.uuid4,
   datetime.datetime, ...) while haul code runs.
B. monitor: sys.monitoring CALL events while haul code runs; a call to a
   forbidden callable raises if the calling frame is user code (not the
   standard library or site-packages). Library code locations are
   DISABLEd after their first event, so they cost one callback each.
"""

from __future__ import annotations

import datetime
import os
import random
import sys
import sysconfig
import time
import uuid
from contextlib import contextmanager


class NondeterminismError(Exception):
    pass


# ---------------------------------------------------------------- A. patch


def _forbidden(name):
    def f(*a, **k):
        raise NondeterminismError(f"{name} called in haul code; use ctx.now()/ctx.random() or a lift")
    return f


@contextmanager
def patch_guard():
    saved = []

    def patch(mod, attr, name):
        saved.append((mod, attr, getattr(mod, attr)))
        setattr(mod, attr, _forbidden(name))

    for attr in ("time", "time_ns"):
        patch(time, attr, f"time.{attr}")
    for attr in ("random", "randint", "choice", "shuffle", "uniform", "getrandbits"):
        patch(random, attr, f"random.{attr}")
    for attr in ("uuid1", "uuid4"):
        patch(uuid, attr, f"uuid.{attr}")

    class GuardedDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            raise NondeterminismError("datetime.now called in haul code")

        @classmethod
        def utcnow(cls):
            raise NondeterminismError("datetime.utcnow called in haul code")

    class GuardedDate(datetime.date):
        @classmethod
        def today(cls):
            raise NondeterminismError("date.today called in haul code")

    saved.append((datetime, "datetime", datetime.datetime))
    saved.append((datetime, "date", datetime.date))
    datetime.datetime = GuardedDatetime
    datetime.date = GuardedDate
    try:
        yield
    finally:
        for mod, attr, orig in reversed(saved):
            setattr(mod, attr, orig)


# -------------------------------------------------------------- B. monitor

_TOOL = sys.monitoring.PROFILER_ID if hasattr(sys, "monitoring") else None

# (owner, name) of forbidden callables. Bound methods are compared by their
# __self__ and __name__, since `datetime.now` makes a new bound method on
# every access.
_FORBIDDEN_FUNCS = {time.time, time.time_ns, uuid.uuid1, uuid.uuid4, os.urandom}
_FORBIDDEN_METHODS = {
    (datetime.datetime, "now"), (datetime.datetime, "utcnow"), (datetime.datetime, "today"),
    (datetime.date, "today"),
}
_RANDOM_INST = random._inst  # the module-level Random behind random.random() etc.

# Real paths: Homebrew's sysconfig paths go through a symlink (opt/) while
# module files report the Cellar path.
_LIBRARY_PREFIXES = tuple({os.path.realpath(sysconfig.get_paths()[k]) + os.sep
                           for k in ("stdlib", "platstdlib", "purelib", "platlib")})


def _forbidden_callable(c, arg0) -> str | None:
    if c in _FORBIDDEN_FUNCS:
        return f"{getattr(c, '__module__', '?')}.{c.__name__}"
    owner = getattr(c, "__self__", None)
    name = getattr(c, "__name__", None)
    # 3.13 reports random.choice(...) as a bound method; 3.14 as the
    # function Random.choice with the instance as arg0.
    if owner is _RANDOM_INST or (arg0 is _RANDOM_INST and getattr(c, "__qualname__", "").startswith("Random.")):
        return f"random.{name}"
    if (owner, name) in _FORBIDDEN_METHODS:
        return f"{owner.__name__}.{name}"
    return None


@contextmanager
def monitor_guard(is_user_file=None):
    mon = sys.monitoring
    is_user_file = is_user_file or (lambda fn: not fn.startswith("<") and not os.path.realpath(fn).startswith(_LIBRARY_PREFIXES))
    user_cache: dict[str, bool] = {}

    def on_call(code, offset, callable_, arg0):
        fn = code.co_filename
        user = user_cache.get(fn)
        if user is None:
            user = user_cache[fn] = is_user_file(fn) and fn != __file__
        if not user:
            return mon.DISABLE  # library code: never look at this location again
        what = _forbidden_callable(callable_, arg0)
        if what:
            raise NondeterminismError(f"{what}() called at {fn}:{code.co_firstlineno}; use ctx.now()/ctx.random() or a lift")
        return None

    mon.use_tool_id(_TOOL, "hubwagen-guard")
    mon.register_callback(_TOOL, mon.events.CALL, on_call)
    mon.set_events(_TOOL, mon.events.CALL)
    try:
        yield
    finally:
        mon.set_events(_TOOL, 0)
        mon.register_callback(_TOOL, mon.events.CALL, None)
        mon.free_tool_id(_TOOL)
        mon.restart_events()

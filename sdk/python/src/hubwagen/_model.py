"""Datasets, pallets and the registry of hauls and lifts."""

from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class Dataset:
    """A named, optionally partitioned stream of pallets.

    `partition` names the lift argument whose value is the partition, e.g.
    Dataset("marketing.daily_revenue", partition="day").
    """

    name: str
    partition: str | None = None


@dataclass
class Pallet:
    """A committed version of data. It points to data; it never holds it."""

    id: str
    dataset: str = ""
    partition: str = ""
    location: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    # What the lift that built it returned (for pallets returned by ctx.lift).
    result: Any = None
    # The pallet was reused (memoized), not built by this haul.
    reused: bool = False


@dataclass(frozen=True)
class Retry:
    """How often and when a failed lift attempt is retried."""

    limit: int
    backoff: str = "fixed"  # or "exponential"
    initial: float = 1.0  # seconds
    max: float | None = None  # seconds


class LiftFailed(Exception):
    """A lift failed (after its retries). Haul code may catch it."""

    def __init__(self, lift: str, reason: str, message: str):
        super().__init__(f"{lift} failed ({reason}): {message}" if message else f"{lift} failed ({reason})")
        self.lift, self.reason, self.message = lift, reason, message


class NonRetryable(Exception):
    """Raise from a lift for a failure that will repeat on every attempt."""


@dataclass
class HaulDef:
    name: str
    fn: Callable
    dock: str | None


@dataclass
class LiftDef:
    name: str
    fn: Callable
    memo: str  # "", "fingerprint" or "ttl:<duration>"
    output: Dataset | None
    version: str | None
    deps: tuple[str, ...]
    timeout: float | None
    retry: Retry | None
    dock: str | None

    def code_version(self) -> str:
        """The lift's code version: `version=` if given, else a hash of its
        source. (Imports and the lockfile join in with #25.)"""
        if self.version:
            return self.version
        try:
            src = inspect.getsource(self.fn)
        except (OSError, TypeError):
            src = self.fn.__qualname__
        return "src-" + hashlib.sha256(src.encode()).hexdigest()[:12]


HAULS: dict[str, HaulDef] = {}
LIFTS: dict[str, LiftDef] = {}


def _register(registry: dict, d) -> None:
    old = registry.get(d.name)
    if old is not None and old.fn is not d.fn and old.fn.__module__ + old.fn.__qualname__ != d.fn.__module__ + d.fn.__qualname__:
        raise ValueError(f"{d.name!r} is registered twice ({old.fn.__module__} and {d.fn.__module__}); pass name=")
    registry[d.name] = d


def haul(fn: Callable | None = None, *, name: str | None = None, dock: str | None = None):
    """Mark a function as a haul: deterministic orchestration code, called
    as fn(ctx, **params) and replayed against its logbook."""

    def wrap(f: Callable) -> Callable:
        d = HaulDef(name=name or f.__name__, fn=f, dock=dock)
        _register(HAULS, d)
        f.__hubwagen_haul__ = d
        return f

    return wrap(fn) if fn is not None else wrap


def _memo(memo: bool | str | None) -> str:
    if memo is True:
        return "fingerprint"
    if memo in (False, None, "", "never"):
        return ""
    if memo == "fingerprint" or (isinstance(memo, str) and memo.startswith("ttl:")):
        return memo
    raise ValueError(f"memo={memo!r}: want True, False, 'fingerprint' or 'ttl:<duration>'")


def lift(fn: Callable | None = None, *, name: str | None = None, memo: bool | str = False,
         output: Dataset | None = None, version: str | None = None, deps: tuple[str, ...] | list[str] = (),
         timeout: float | _dt.timedelta | None = None, retry: Retry | int | None = None, dock: str | None = None):
    """Mark a function as a lift: one side-effecting unit of work, run on a
    worker and retried by policy. With output=, it produces a pallet: write
    files to hubwagen.output().path. With memo=True, an unchanged lift
    (code, inputs, arguments) reuses the earlier pallet instead of running.
    The function stays callable as is, for tests."""
    memo_s = _memo(memo)
    if memo_s and output is None:
        raise ValueError("only a lift with an output can be memoized: its pallet is what gets reused")
    if isinstance(timeout, _dt.timedelta):
        timeout = timeout.total_seconds()
    if isinstance(retry, int):
        retry = Retry(limit=retry)

    def wrap(f: Callable) -> Callable:
        d = LiftDef(name=name or f.__name__, fn=f, memo=memo_s, output=output, version=version,
                    deps=tuple(deps), timeout=timeout, retry=retry, dock=dock)
        if output is not None and output.partition and output.partition not in inspect.signature(f).parameters:
            raise ValueError(f"lift {d.name}: output partition {output.partition!r} is not one of its arguments")
        _register(LIFTS, d)
        f.__hubwagen_lift__ = d
        return f

    return wrap(fn) if fn is not None else wrap


def lift_def(f: Callable | str) -> LiftDef:
    if isinstance(f, str):
        d = LIFTS.get(f)
    else:
        d = getattr(f, "__hubwagen_lift__", None)
    if d is None:
        raise TypeError(f"{f!r} is not a lift (decorate it with @hubwagen.lift)")
    return d


# --- JSON for arguments and results ------------------------------------------


def encode(v: Any) -> Any:
    """Arguments and results as JSON: dates and datetimes get a tag so they
    come back as what they were; pallets become references."""
    if isinstance(v, Pallet):
        return {"$pallet": v.id, "dataset": v.dataset, "partition": v.partition}
    if isinstance(v, _dt.datetime):
        return {"$datetime": v.isoformat()}
    if isinstance(v, _dt.date):
        return {"$date": v.isoformat()}
    if isinstance(v, dict):
        return {str(k): encode(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [encode(x) for x in v]
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        return encode(dataclasses.asdict(v))
    return v


def decode(v: Any, pallets: dict[str, Pallet] | None = None) -> Any:
    if isinstance(v, dict):
        if "$date" in v and len(v) == 1:
            return _dt.date.fromisoformat(v["$date"])
        if "$datetime" in v and len(v) == 1:
            return _dt.datetime.fromisoformat(v["$datetime"])
        if "$input" in v and len(v) == 1 and pallets is not None:
            return pallets[v["$input"]]
        if "$pallet" in v:
            return Pallet(id=v["$pallet"], dataset=v.get("dataset", ""), partition=v.get("partition", ""))
        return {k: decode(x, pallets) for k, x in v.items()}
    if isinstance(v, list):
        return [decode(x, pallets) for x in v]
    return v

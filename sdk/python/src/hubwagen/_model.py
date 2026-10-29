"""Datasets, pallets and the registry of hauls and lifts."""

from __future__ import annotations

import dataclasses
import datetime as _dt
import functools
import inspect
import json
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


def local_path(uri: str) -> Path:
    """A file:// URI as a local path. Other buckets aren't supported yet."""
    u = urllib.parse.urlparse(uri)
    if u.scheme != "file":
        raise RuntimeError(f"{uri}: only file:// buckets are supported so far")
    return Path(urllib.parse.unquote(u.path))


@dataclass(frozen=True)
class Keep:
    """A dataset's retention: per partition, the latest pallet always stays,
    and so do the newest `versions` and those younger than `age` (seconds
    or a timedelta); the rest expire (their data is deleted, their record
    stays). Keep(versions=3), Keep(age=timedelta(days=30)), or both."""

    versions: int | None = None
    age: float | None = None

    def __init__(self, versions: int | None = None, age: float | _dt.timedelta | None = None):
        if isinstance(age, _dt.timedelta):
            age = age.total_seconds()
        if versions is None and age is None:
            raise ValueError("Keep: give versions, age or both")
        if (versions is not None and (not isinstance(versions, int) or versions < 1)) or (age is not None and age <= 0):
            raise ValueError(f"Keep: want versions >= 1 and age > 0, got versions={versions!r}, age={age!r}")
        object.__setattr__(self, "versions", versions)
        object.__setattr__(self, "age", None if age is None else float(age))


@dataclass(frozen=True)
class Dataset:
    """A named, optionally partitioned stream of pallets.

    `partition` names the lift argument whose value is the partition, e.g.
    Dataset("marketing.daily_revenue", partition="day"). `keep` is its
    retention (hubwagen.Keep); without one, every version stays.
    """

    name: str
    partition: str | None = None
    keep: Keep | None = None


@dataclass(frozen=True)
class Schedule:
    """When a haul runs on its own, and which data interval each run covers.

    cron is a standard 5-field cron in timezone (IANA; default UTC). interval
    is the data a run covers: "hourly", "daily", "weekly", "monthly" (the
    previous full period, in the timezone; DST-correct) or "cron" (from the
    previous tick to this one). catch_up decides what happens to runs that
    came due while no server ran: "latest" starts the newest, "all" starts
    every one, oldest first, "skip" starts none.
    """

    cron: str
    interval: str = "cron"
    timezone: str = ""
    catch_up: str = "latest"

    def __post_init__(self):
        if self.interval not in ("hourly", "daily", "weekly", "monthly", "cron"):
            raise ValueError(f"Schedule: interval {self.interval!r} must be hourly, daily, weekly, monthly or cron")
        if self.catch_up not in ("skip", "latest", "all"):
            raise ValueError(f"Schedule: catch_up {self.catch_up!r} must be skip, latest or all")
        if len(self.cron.split()) != 5:
            raise ValueError(f"Schedule: cron {self.cron!r} must have 5 fields (minute hour day month weekday)")


@dataclass(frozen=True)
class Subscription:
    """Start a haul when pallets are committed to datasets: the haul takes a
    `pallets` parameter, the list of new pallets (handles: pass them to
    lifts). Pallets are collected in batches; the haul starts once none came
    for `debounce` seconds (default 0: at once), once `max_batch` wait
    (default 100), or once the first has waited `max_wait` (default 10 ×
    debounce). A haul's own pallets don't start it."""

    datasets: tuple[str, ...]
    debounce: float = 0.0
    max_wait: float | None = None
    max_batch: int = 100

    def __init__(self, datasets: str | Dataset | list | tuple, debounce: float | _dt.timedelta = 0,
                 max_wait: float | _dt.timedelta | None = None, max_batch: int = 100):
        if isinstance(datasets, (str, Dataset)):
            datasets = [datasets]
        names = tuple(sorted({d.name if isinstance(d, Dataset) else d for d in datasets}))
        secs = lambda v: v.total_seconds() if isinstance(v, _dt.timedelta) else v  # noqa: E731
        if not names or "" in names:
            raise ValueError("Subscription: needs dataset names")
        debounce, max_wait = secs(debounce), secs(max_wait)
        if debounce < 0 or (max_wait is not None and max_wait < debounce) or max_batch < 1:
            raise ValueError("Subscription: want debounce >= 0, max_wait >= debounce and max_batch >= 1")
        object.__setattr__(self, "datasets", names)
        object.__setattr__(self, "debounce", float(debounce))
        object.__setattr__(self, "max_wait", None if max_wait is None else float(max_wait))
        object.__setattr__(self, "max_batch", max_batch)


@dataclass(frozen=True)
class Interval:
    """The data interval a scheduled run covers: [start, end), in the
    schedule's timezone."""

    start: _dt.datetime
    end: _dt.datetime


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

    # Reading a pallet's data: inside a lift that got it as an argument.

    def manifest(self) -> dict:
        """The pallet's manifest: its data prefix and files (path, size,
        sha256)."""
        if not self.location:
            raise RuntimeError(f"pallet {self.id}: no location; read pallets inside the lift they were passed to")
        return json.loads(local_path(self.location).read_text())

    def files(self) -> list[Path]:
        """The pallet's files as local paths, in path order."""
        m = self.manifest()
        base = local_path(m["data"])
        return [base / f["path"] for f in m["files"]]

    def path(self, name: str) -> Path:
        """One of the pallet's files, by its path within the pallet."""
        m = self.manifest()
        if not any(f["path"] == name for f in m["files"]):
            raise FileNotFoundError(f"pallet {self.id} has no file {name!r}")
        return local_path(m["data"]) / name

    def load(self, as_: type | None = None) -> Any:
        """The dataframe or table a lift returned into this pallet, loaded
        as as_ (e.g. pl.LazyFrame, pd.DataFrame) or as it was written."""
        from . import _codecs

        m = self.manifest()
        if not any(f["path"] == _codecs.FILE for f in m["files"]):
            raise FileNotFoundError(f"pallet {self.id} holds no dataframe (no {_codecs.FILE})")
        return _codecs.load(local_path(m["data"]) / _codecs.FILE, _codecs.frame_tag(m.get("result")), as_)


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
    max_parallel: int | None = None  # lifts of one haul queued or running at once
    schedule: Schedule | None = None
    subscribe: Subscription | None = None
    title: str = ""  # for people: the web UI shows it in place of the name
    description: str = ""

    @functools.cached_property
    def _computed_version(self) -> str:
        from . import _codeversion

        return _codeversion.code_version(self.fn, name=self.name, haul=True)

    def code_version(self) -> str:
        """The haul's code version, computed like a lift's. It goes into the
        build ID, so fixed haul code is a new build (which resumes stuck
        hauls)."""
        return self._computed_version


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

    @functools.cached_property
    def _computed_version(self) -> str:
        from . import _codeversion

        return _codeversion.code_version(self.fn, self.deps, self.name)

    def code_version(self) -> str:
        """The lift's code version: `version=` if given, else a hash of the
        code it can run, the packages it uses and its `deps=` files (see
        _codeversion). Computed once per process."""
        return self.version or self._computed_version


HAULS: dict[str, HaulDef] = {}
LIFTS: dict[str, LiftDef] = {}


def _register(registry: dict, d) -> None:
    old = registry.get(d.name)
    if old is not None and old.fn is not d.fn and old.fn.__module__ + old.fn.__qualname__ != d.fn.__module__ + d.fn.__qualname__:
        raise ValueError(f"{d.name!r} is registered twice ({old.fn.__module__} and {d.fn.__module__}); pass name=")
    registry[d.name] = d


def haul(fn: Callable | None = None, *, name: str | None = None, title: str | None = None,
         dock: str | None = None, max_parallel: int | None = None, schedule: Schedule | str | None = None,
         subscribe: Subscription | str | list | None = None):
    """Mark a function as a haul: deterministic orchestration code, called
    as fn(ctx, **params) and replayed against its logbook. max_parallel caps
    how many of one haul's lifts are queued or running at once. With
    schedule (a Schedule, or a cron string), the server starts it on its own,
    once per data interval: the function takes an `interval` parameter, a
    hubwagen.Interval. With subscribe (a Subscription, or dataset names),
    new pallets in those datasets start it: it takes a `pallets` parameter.
    title ("Daily revenue") is what the web UI calls it; without one, the
    docstring's first line is; the rest of the docstring describes it."""
    if max_parallel is not None and (not isinstance(max_parallel, int) or max_parallel < 1):
        raise ValueError(f"haul: max_parallel must be a positive integer, got {max_parallel!r}")
    if isinstance(schedule, str):
        schedule = Schedule(schedule)
    if subscribe is not None and not isinstance(subscribe, Subscription):
        subscribe = Subscription(subscribe)

    def wrap(f: Callable) -> Callable:
        d = HaulDef(name=name or f.__name__, fn=f, dock=dock, max_parallel=max_parallel, schedule=schedule,
                    subscribe=subscribe, **_describe(f, title))
        if subscribe is not None and "pallets" not in inspect.signature(f).parameters:
            raise ValueError(f"haul {d.name}: a subscribed haul takes a `pallets` parameter (a list of hubwagen.Pallet)")
        if schedule is not None and "interval" not in inspect.signature(f).parameters:
            raise ValueError(f"haul {d.name}: a scheduled haul takes an `interval` parameter (a hubwagen.Interval)")
        _register(HAULS, d)
        f.__hubwagen_haul__ = d
        return f

    return wrap(fn) if fn is not None else wrap


def _describe(f: Callable, title: str | None) -> dict[str, str]:
    """A haul's title and description: title if given, else the docstring's
    first line; the description is the rest of the docstring."""
    doc = inspect.getdoc(f) or ""
    first, _, rest = doc.partition("\n")
    if title:
        return {"title": title.strip(), "description": doc}
    return {"title": first.strip(), "description": rest.strip()}


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
    (code, inputs, arguments) reuses what an earlier run produced instead of
    running: its pallet, or without an output, its result. The function
    stays callable as is, for tests."""
    memo_s = _memo(memo)
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
    from . import _codecs

    if _codecs.for_value(v) is not None:
        raise TypeError(f"a {type(v).__name__} isn't JSON: return it from a lift on its own (it becomes a pallet) "
                        "and pass the lift's handle on")
    if isinstance(v, Pallet):
        return {"$pallet": v.id, "dataset": v.dataset, "partition": v.partition}
    if isinstance(v, Interval):
        return {"$interval": {"start": v.start.isoformat(), "end": v.end.isoformat()}}
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


def _parse_time(s: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def decode(v: Any, pallets: dict[str, Pallet] | None = None) -> Any:
    if isinstance(v, dict):
        if "$date" in v and len(v) == 1:
            return _dt.date.fromisoformat(v["$date"])
        if "$datetime" in v and len(v) == 1:
            return _dt.datetime.fromisoformat(v["$datetime"])
        if "$interval" in v and len(v) == 1:
            iv = v["$interval"]
            return Interval(_parse_time(iv["start"]), _parse_time(iv["end"]))
        if "$input" in v and len(v) == 1 and pallets is not None:
            return pallets[v["$input"]]
        if "$pallet" in v:
            return Pallet(id=v["$pallet"], dataset=v.get("dataset", ""), partition=v.get("partition", ""))
        return {k: decode(x, pallets) for k, x in v.items()}
    if isinstance(v, list):
        return [decode(x, pallets) for x in v]
    return v

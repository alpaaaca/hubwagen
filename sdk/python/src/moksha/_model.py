"""Datasets, data versions and the registry of pipelines and steps."""

from __future__ import annotations

import dataclasses
import datetime as _dt
import functools
import inspect
import json
import re
import urllib.parse
import warnings
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
    """A dataset's retention: per partition, the latest version always stays,
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
    """A named table or set of files that gets a new version each time a
    step writes it, optionally per partition (a day, a region).

    `partition` names the step argument whose value is the partition, e.g.
    Dataset("marketing.daily_revenue", partition="day"). `keep` is its
    retention (moksha.Keep); without one, every version stays.
    """

    name: str
    partition: str | None = None
    keep: Keep | None = None


@dataclass(frozen=True)
class Schedule:
    """When a pipeline runs on its own, and which data interval each run covers.

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
    """Start a pipeline when new data versions are saved to datasets: it
    takes a `versions` input, the list of new versions (handles: pass them
    to steps). They are collected in batches; a run starts once none came
    for `debounce` seconds (default 0: at once), once `max_batch` wait
    (default 100), or once the first has waited `max_wait` (default 10 ×
    debounce). What a pipeline's own runs write doesn't start it."""

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
class DataVersion:
    """A saved version of a dataset: what a step wrote, never changed
    afterwards. It points to the data; it never holds it."""

    id: str
    dataset: str = ""
    partition: str = ""
    location: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    # What the step that wrote it returned (for versions ctx.step returns).
    result: Any = None
    # The version was reused (same inputs and code), not written by this run.
    reused: bool = False

    # How the worker reaches the data (_data.Remote), when it doesn't share
    # the server's bucket directory.
    _remote: Any = field(default=None, repr=False, compare=False)

    # Reading a version's data: inside a step that got it as an argument.

    def manifest(self) -> dict:
        """The version's manifest: its data prefix and files (path, size,
        sha256)."""
        if self._remote is not None:
            return self._remote.manifest(self.id)
        if not self.location:
            raise RuntimeError(f"data version {self.id}: no location; read data versions inside the step they were passed to")
        return json.loads(local_path(self.location).read_text())

    def files(self) -> list[Path]:
        """The version's files as local paths, in path order (downloaded
        first by a worker on another machine than the server)."""
        if self._remote is not None:
            return self._remote.files(self.id)
        m = self.manifest()
        base = local_path(m["data"])
        return [base / f["path"] for f in m["files"]]

    def path(self, name: str) -> Path:
        """One of the version's files, by its path within the version."""
        if self._remote is not None:
            return self._remote.file(self.id, name)
        m = self.manifest()
        if not any(f["path"] == name for f in m["files"]):
            raise FileNotFoundError(f"data version {self.id} has no file {name!r}")
        return local_path(m["data"]) / name

    def load(self, as_: type | None = None) -> Any:
        """The dataframe or table a step returned into this version, loaded
        as as_ (e.g. pl.LazyFrame, pd.DataFrame) or as it was written."""
        from . import _codecs

        m = self.manifest()
        if not any(f["path"] == _codecs.FILE for f in m["files"]):
            raise FileNotFoundError(f"data version {self.id} holds no dataframe (no {_codecs.FILE})")
        return _codecs.load(self.path(_codecs.FILE), _codecs.frame_tag(m.get("result")), as_)



@dataclass(frozen=True)
class Retry:
    """How often and when a failed step attempt is retried."""

    limit: int
    backoff: str = "fixed"  # or "exponential"
    initial: float = 1.0  # seconds
    max: float | None = None  # seconds


class StepFailed(Exception):
    """A step failed (after its retries). Pipeline code may catch it."""

    def __init__(self, step: str, reason: str, message: str):
        super().__init__(f"{step} failed ({reason}): {message}" if message else f"{step} failed ({reason})")
        self.step, self.reason, self.message = step, reason, message


class NonRetryable(Exception):
    """Raise from a step for a failure that will repeat on every attempt."""


@dataclass
class PipelineDef:
    name: str
    fn: Callable
    queue: str | None
    max_parallel: int | None = None  # steps of one run queued or running at once
    schedule: Schedule | None = None
    subscribe: Subscription | None = None
    title: str = ""  # for people: the web UI shows it in place of the name
    description: str = ""

    @functools.cached_property
    def _computed_version(self) -> str:
        from . import _codeversion

        return _codeversion.code_version(self.fn, name=self.name, pipeline=True)

    def code_version(self) -> str:
        """The run's code version, computed like a step's. It goes into the
        build ID, so fixed pipeline code is a new build (which resumes stuck
        runs)."""
        return self._computed_version


@dataclass
class StepDef:
    name: str
    fn: Callable
    memo: str  # its reuse= as the wire's memo policy: "", "fingerprint" or "ttl:<duration>"
    output: Dataset | None
    version: str | None
    deps: tuple[str, ...]
    timeout: float | None
    retry: Retry | None
    queue: str | None

    @functools.cached_property
    def _computed_version(self) -> str:
        from . import _codeversion

        return _codeversion.code_version(self.fn, self.deps, self.name)

    def code_version(self) -> str:
        """The step's code version: `version=` if given, else a hash of the
        code it can run, the packages it uses and its `deps=` files (see
        _codeversion). Computed once per process."""
        return self.version or self._computed_version


PIPELINES: dict[str, PipelineDef] = {}
STEPS: dict[str, StepDef] = {}


def _register(registry: dict, d) -> None:
    old = registry.get(d.name)
    if old is not None and old.fn is not d.fn and old.fn.__module__ + old.fn.__qualname__ != d.fn.__module__ + d.fn.__qualname__:
        raise ValueError(f"{d.name!r} is registered twice ({old.fn.__module__} and {d.fn.__module__}); pass name=")
    registry[d.name] = d


def pipeline(fn: Callable | None = None, *, name: str | None = None, title: str | None = None,
             queue: str | None = None, max_parallel: int | None = None, schedule: Schedule | str | None = None,
             subscribe: Subscription | str | list | None = None):
    """Mark a function as a pipeline: the code that decides which steps run,
    called as fn(ctx, **inputs). Each run of it survives crashes and
    restarts: moksha replays the function against what the run already
    did, so it must make the same calls in the same order every time (no
    clock, randomness or I/O; do those in steps). max_parallel caps how many
    of one run's steps are queued or running at once. With schedule (a
    Schedule, or a cron string), the server starts it on its own, once per
    data interval: the function takes an `interval` input, a
    moksha.Interval. With subscribe (a Subscription, or dataset names),
    new data versions in those datasets start it: it takes a `versions`
    input (a list of moksha.DataVersion). title ("Daily revenue") is what
    the web UI calls it; without one, the docstring's first line is; the
    rest of the docstring describes it."""
    if max_parallel is not None and (not isinstance(max_parallel, int) or max_parallel < 1):
        raise ValueError(f"pipeline: max_parallel must be a positive integer, got {max_parallel!r}")
    if isinstance(schedule, str):
        schedule = Schedule(schedule)
    if subscribe is not None and not isinstance(subscribe, Subscription):
        subscribe = Subscription(subscribe)

    def wrap(f: Callable) -> Callable:
        d = PipelineDef(name=name or f.__name__, fn=f, queue=queue, max_parallel=max_parallel, schedule=schedule,
                    subscribe=subscribe, **_describe(f, title))
        takes = inspect.signature(f).parameters
        if subscribe is not None and "versions" not in takes:
            raise ValueError(f"pipeline {d.name}: a subscribed pipeline takes a `versions` input "
                             "(a list of moksha.DataVersion)")
        if schedule is not None and "interval" not in takes:
            raise ValueError(f"pipeline {d.name}: a scheduled pipeline takes an `interval` input (a moksha.Interval)")
        _register(PIPELINES, d)
        f.__moksha_pipeline__ = d
        return f

    return wrap(fn) if fn is not None else wrap




def _describe(f: Callable, title: str | None) -> dict[str, str]:
    """A pipeline's title and description: title if given, else the docstring's
    first line; the description is the rest of the docstring."""
    doc = inspect.getdoc(f) or ""
    first, _, rest = doc.partition("\n")
    if title:
        return {"title": title.strip(), "description": doc}
    return {"title": first.strip(), "description": rest.strip()}


# A duration as the server reads it (Go's time.ParseDuration): 90s, 1h30m.
_DURATION = re.compile(r"(\d+(\.\d+)?(ns|us|µs|ms|s|m|h))+")


def _reuse(reuse: bool | str | _dt.timedelta | None) -> str:
    """reuse= as the wire's memo policy: None (not given) is "", False
    (not reused, on purpose: no reuse hint) is "never"."""
    if reuse is True:
        return "fingerprint"
    if reuse is None:
        return ""
    if reuse is False:
        return "never"
    if isinstance(reuse, _dt.timedelta) and reuse > _dt.timedelta(0):
        s = reuse.total_seconds()
        return f"ttl:{int(s) if s == int(s) else s}s"
    if isinstance(reuse, str) and _DURATION.fullmatch(reuse) and float(re.match(r"\d+(\.\d+)?", reuse)[0]) > 0:
        return f"ttl:{reuse}"
    raise ValueError(f"reuse={reuse!r}: want True, False, or an age such as '1h', '90m' or a timedelta")


def _memo(memo: bool | str) -> tuple[str, str]:
    """The deprecated memo= as the wire's policy, and what to write instead."""
    if memo is True or memo == "fingerprint":
        return "fingerprint", "reuse=True"
    if memo in (False, "", "never"):
        return "", "nothing (a step isn't reused unless it says reuse=)"
    if isinstance(memo, str) and memo.startswith("ttl:"):
        age = memo.removeprefix("ttl:")
        return _reuse(age), f"reuse={age!r}"
    raise ValueError(f"memo={memo!r}: memo= is now reuse=: True, False or an age such as '1h'")


def step(fn: Callable | None = None, *, name: str | None = None, reuse: bool | str | _dt.timedelta | None = None,
         output: Dataset | None = None, version: str | None = None, deps: tuple[str, ...] | list[str] = (),
         timeout: float | _dt.timedelta | None = None, retry: Retry | int | None = None, queue: str | None = None,
         memo: bool | str | None = None):
    """Mark a function as a step: one unit of real work (a query, an API
    call, a computation), run on a worker and retried by policy. With
    output=, what it writes becomes a new version of that dataset: return a
    dataframe, or write files to moksha.output().path.

    With reuse=True, a step that comes again in a later run with the same
    code, inputs and arguments isn't run: the run gets what an earlier run
    produced, its data version or (without an output) its result. Give it
    only to steps that read nothing but their inputs and arguments; a step
    that reads a file, a database or the clock runs every time without it.
    reuse="1h" (or a timedelta) reuses results up to that age. Within one
    run a completed step is never run again either way. Without reuse=,
    moksha points out a step that keeps getting the same result (the reuse
    hint); reuse=False says it isn't reused on purpose, which silences
    that. memo= is the old name, deprecated. The function stays callable
    as is, for tests."""
    instead = None
    if memo is not None:
        if reuse is not None:
            raise ValueError("step: reuse= and memo= both given; memo= is the old name of reuse=")
        policy, instead = _memo(memo)
    else:
        policy = _reuse(reuse)
    if isinstance(timeout, _dt.timedelta):
        timeout = timeout.total_seconds()
    if isinstance(retry, int):
        retry = Retry(limit=retry)

    def wrap(f: Callable, _caller: int = 2) -> Callable:  # _caller: the user's frame, for the warning
        if instead is not None:
            warnings.warn(f"step {name or f.__name__}: memo= is now reuse=; write {instead}", FutureWarning,
                          stacklevel=_caller)
        d = StepDef(name=name or f.__name__, fn=f, memo=policy, output=output, version=version,
                    deps=tuple(deps), timeout=timeout, retry=retry, queue=queue)
        if output is not None and output.partition and output.partition not in inspect.signature(f).parameters:
            raise ValueError(f"step {d.name}: output partition {output.partition!r} is not one of its arguments")
        _register(STEPS, d)
        f.__moksha_step__ = d
        return f

    return wrap(fn, 3) if fn is not None else wrap




def step_def(f: Callable | str) -> StepDef:
    if isinstance(f, str):
        d = STEPS.get(f)
    else:
        d = getattr(f, "__moksha_step__", None)
    if d is None:
        raise TypeError(f"{f!r} is not a step (decorate it with @moksha.step)")
    return d


# --- JSON for arguments and results ------------------------------------------


def encode(v: Any) -> Any:
    """Arguments and results as JSON: dates and datetimes get a tag so they
    come back as what they were; data versions become references."""
    from . import _codecs

    if _codecs.for_value(v) is not None:
        raise TypeError(f"a {type(v).__name__} isn't JSON: return it from a step on its own (it becomes a data version) "
                        "and pass the step's handle on")
    if isinstance(v, DataVersion):
        return {"$data_version": v.id, "dataset": v.dataset, "partition": v.partition}
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


def decode(v: Any, data_versions: dict[str, DataVersion] | None = None) -> Any:
    if isinstance(v, dict):
        if "$date" in v and len(v) == 1:
            return _dt.date.fromisoformat(v["$date"])
        if "$datetime" in v and len(v) == 1:
            return _dt.datetime.fromisoformat(v["$datetime"])
        if "$interval" in v and len(v) == 1:
            iv = v["$interval"]
            return Interval(_parse_time(iv["start"]), _parse_time(iv["end"]))
        if "$input" in v and len(v) == 1 and data_versions is not None:
            return data_versions[v["$input"]]
        if "$data_version" in v:
            return DataVersion(id=v["$data_version"], dataset=v.get("dataset", ""), partition=v.get("partition", ""))
        return {k: decode(x, data_versions) for k, x in v.items()}
    if isinstance(v, list):
        return [decode(x, data_versions) for x in v]
    return v

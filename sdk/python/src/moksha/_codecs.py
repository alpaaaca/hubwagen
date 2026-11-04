"""Codecs: how a dataframe or table a lift returns is stored, and how a
lift's arguments are loaded from pallets by their annotations.

A lift that returns a pandas or Polars DataFrame, a Polars LazyFrame or an
Arrow Table doesn't return JSON: the worker writes it as Parquet into the
attempt's staging prefix, which makes it a pallet (in the lift's output, or
in its implicit dataset), and the result records what was written:
{"$frame": "polars", "rows": 3, "columns": [...]}. The haul gets a handle,
never the data. A lift that takes the handle gets the data, loaded as its
parameter's annotation asks (pl.DataFrame, pl.LazyFrame, pd.DataFrame,
pa.Table), or as it was written when the parameter has none; annotate
hw.Pallet to get the files instead.

The built-in codecs import pandas, Polars and pyarrow only when they meet
one of their types, so the SDK itself needs none of them.
"""

from __future__ import annotations

import inspect
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

FILE = "data.parquet"  # the one file a codec writes into a pallet


def _key(cls: type) -> tuple[str, str]:
    """A type as (top-level package, class name): ("polars", "DataFrame").
    Stable across library versions that move classes between modules."""
    return cls.__module__.split(".")[0], cls.__name__


@dataclass(frozen=True)
class Codec:
    name: str  # recorded in results: {"$frame": name}
    types: tuple[tuple[str, str], ...]  # what it writes and reads, as _key gives them
    write: Callable[[Any, Path], None]  # (value, file)
    read: Callable[[Path, type | None], Any]  # (file, annotated type, or None for the default)
    describe: Callable[[Any], dict]  # small facts for the result: rows, columns


_CODECS: list[Codec] = []


def register_codec(name: str, types: list[type | tuple[str, str]], write: Callable[[Any, Path], None],
                   read: Callable[[Path, type | None], Any], describe: Callable[[Any], dict] | None = None) -> None:
    """Teach the SDK to store another type as a pallet: write(value, file)
    writes it to file, read(file, cls) loads it back (cls is the annotated
    type, or None). Later registrations win over earlier ones."""
    keys = tuple(t if isinstance(t, tuple) else _key(t) for t in types)
    _CODECS.insert(0, Codec(name, keys, write, read, describe or (lambda v: {})))


def for_value(v: Any) -> Codec | None:
    """The codec that stores v, or None for plain values."""
    keys = {_key(c) for c in type(v).__mro__}
    return next((c for c in _CODECS if keys & set(c.types)), None)


def for_type(cls: Any) -> Codec | None:
    if not isinstance(cls, type):
        return None
    keys = {_key(c) for c in cls.__mro__}
    return next((c for c in _CODECS if keys & set(c.types)), None)


def by_name(name: str) -> Codec | None:
    return next((c for c in _CODECS if c.name == name), None)


def write(v: Any, directory: Path) -> dict:
    """Write v with its codec into directory; returns the result to record."""
    codec = for_value(v)
    assert codec is not None
    f = directory / FILE
    if f.exists():
        raise RuntimeError(f"the step returned a {type(v).__name__} and also wrote {FILE} itself")
    directory.mkdir(parents=True, exist_ok=True)
    codec.write(v, f)
    return {"$frame": codec.name, **codec.describe(v)}


def frame_tag(result: Any) -> str | None:
    """The codec name a result records, if it describes a written frame."""
    if isinstance(result, dict) and isinstance(result.get("$frame"), str):
        return result["$frame"]
    return None


def parameter_types(fn: Callable) -> dict[str, Any]:
    """fn's parameter annotations, resolved where possible (string
    annotations from `from __future__ import annotations` included)."""
    try:
        return typing.get_type_hints(fn)
    except Exception:  # noqa: BLE001 - an annotation that doesn't resolve: use what is there
        return {n: p.annotation for n, p in inspect.signature(fn).parameters.items()
                if p.annotation is not inspect.Parameter.empty}


def load(file: Path, tag: str | None, cls: Any) -> Any:
    """Load file as cls if a codec reads it, else as the codec tag names."""
    codec = for_type(cls)
    if codec is not None:
        return codec.read(file, cls)
    codec = by_name(tag) if tag else None
    if codec is None:
        raise TypeError(f"no codec reads {file.name} (written as {tag!r})")
    return codec.read(file, None)


# --- built-in codecs ----------------------------------------------------------

def _shape(rows: int, columns: list) -> dict:
    return {"rows": rows, "columns": [str(c) for c in columns]}


def _polars_write(v: Any, f: Path) -> None:
    if _key(type(v)) == ("polars", "LazyFrame"):
        v = v.collect()
    v.write_parquet(f)


def _polars_read(f: Path, cls: type | None) -> Any:
    import polars as pl

    if cls is not None and _key(cls) == ("polars", "LazyFrame"):
        return pl.scan_parquet(f)
    return pl.read_parquet(f)


def _polars_describe(v: Any) -> dict:
    if _key(type(v)) == ("polars", "LazyFrame"):
        return {"columns": list(v.collect_schema().names())}
    return _shape(v.height, v.columns)


def _pandas_read(f: Path, cls: type | None) -> Any:
    import pandas as pd

    return pd.read_parquet(f)


def _arrow_write(v: Any, f: Path) -> None:
    import pyarrow.parquet as pq

    pq.write_table(v, f)


def _arrow_read(f: Path, cls: type | None) -> Any:
    import pyarrow.parquet as pq

    return pq.read_table(f)


register_codec("arrow", [("pyarrow", "Table")], _arrow_write, _arrow_read,
               lambda v: _shape(v.num_rows, v.column_names))
register_codec("pandas", [("pandas", "DataFrame")], lambda v, f: v.to_parquet(f), _pandas_read,
               lambda v: _shape(len(v), list(v.columns)))
register_codec("polars", [("polars", "DataFrame"), ("polars", "LazyFrame")], _polars_write, _polars_read,
               _polars_describe)

"""Where a step with an output writes its files."""

from __future__ import annotations

import contextvars
import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Output:
    """The running lift's output: write files under `path`; anything in
    `meta` (a cursor, a watermark) is recorded on the pallet and readable
    with ctx.latest_pallet(...).meta; `stats` likewise."""

    path: Path
    meta: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)


_current: contextvars.ContextVar[Output | None] = contextvars.ContextVar("hubwagen_output", default=None)


def output() -> Output:
    """The running lift's output. Only inside a lift declared with output=."""
    o = _current.get()
    if o is None:
        raise RuntimeError("hubwagen.output() is only available inside a step declared with output=")
    return o


def local_dir(staging: str) -> Path:
    from ._model import local_path
    return local_path(staging)


def describe(o: Output) -> dict:
    """The LiftOutput message for what the lift wrote."""
    files = []
    for root, _, names in os.walk(o.path):
        for n in sorted(names):
            p = Path(root, n)
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            files.append({"path": p.relative_to(o.path).as_posix(), "size": str(p.stat().st_size), "sha256": h.hexdigest()})
    files.sort(key=lambda f: f["path"])
    from ._model import encode
    import json
    out: dict[str, Any] = {"files": files}
    if o.meta:
        out["metaJson"] = json.dumps(encode(o.meta))
    if o.stats:
        out["statsJson"] = json.dumps(encode(o.stats))
    return out

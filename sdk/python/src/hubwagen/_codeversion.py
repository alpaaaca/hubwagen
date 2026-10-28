"""Code versions: what a lift's fingerprint knows about its code.

A lift's code version covers the code it can run, found by walking the
objects its function refers to, not by parsing import statements:

- the function itself, and every project function, class and module-level
  constant it refers to, directly or through another one, in its own
  module or another project module (`from .helpers import clean`,
  `helpers.clean(...)`), each as its AST: comments, docstrings, formatting
  and the lift's own decorator don't count;
- for code outside the project, the installed distribution that provides
  it, with its version and the versions of what it requires (`polars 1.9.0`,
  `pandas 2.2.3` + `numpy 2.1.2`, ...); the standard library counts as the
  Python version, and hubwagen itself doesn't count (it runs the code, like
  the server);
- the types its parameters and return are annotated with, which decide
  how inputs are loaded (`pd.DataFrame`, `pl.LazyFrame`);
- for a haul, the lifts it calls by name only: their code has versions of
  their own;
- files named by `deps=`, by content.

Walking the live objects instead of the import statements resolves
relative imports, `from pkg import sub` (module or attribute) and namespace
packages the way Python did. What it can't see: code reached through a
computed name (`importlib.import_module(name)`, `getattr(m, name)`; a
literal name of an imported project module is found), data read at import
time, and the state of objects; `version=` and `deps=` cover those. Spike
S2 (docs/spikes/s2-code-version.md) has the reasoning.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import importlib.metadata as md
import inspect
import json
import marshal
import os
import re
import sys
import sysconfig
import textwrap
import types
import typing
from pathlib import Path
from typing import Any, Callable

_SDK_DIR = os.path.realpath(os.path.dirname(__file__)) + os.sep
_LIBRARY_DIRS = tuple({os.path.realpath(sysconfig.get_paths()[k]) + os.sep
                       for k in ("stdlib", "platstdlib", "purelib", "platlib")})
_SIMPLE = (type(None), bool, int, float, complex, str, bytes)


def code_version(fn: Callable, deps: tuple[str, ...] = (), name: str = "", haul: bool = False) -> str:
    """The code version of fn, with deps (paths relative to the project
    root, the directory of the nearest pyproject.toml). For a haul, lifts
    count by name."""
    p = parts(fn, deps, name, haul)
    return "py-" + hashlib.sha256(json.dumps(p, sort_keys=True).encode()).hexdigest()[:12]


def parts(fn: Callable, deps: tuple[str, ...] = (), name: str = "", haul: bool = False) -> dict:
    """What code_version hashes, readable: {"python", "code", "packages",
    "files"}. Shown by `python -m hubwagen.worker --versions`."""
    w = _Walker(project_root(_source_file(fn)), lifts_by_name=haul)
    w.function(fn, root=True)
    return {
        "python": f"{sys.implementation.name} {sys.version_info.major}.{sys.version_info.minor}",
        "code": w.code,
        "packages": w.packages,
        "files": _files(fn, deps, name),
    }


def is_project_file(filename: str) -> bool:
    """Code of the project: not the standard library, an installed package
    or hubwagen itself."""
    if not filename or filename.startswith("<"):
        return False
    real = os.path.realpath(filename)
    if real.startswith(_SDK_DIR) or real.startswith(_LIBRARY_DIRS):
        return False
    parts = Path(real).parts
    return "site-packages" not in parts and "dist-packages" not in parts


class _Walker:
    def __init__(self, root: Path, lifts_by_name: bool = False) -> None:
        self.root = root
        self.lifts_by_name = lifts_by_name
        self.code: dict[str, str] = {}
        self.packages: dict[str, str] = {}
        self._seen: set[int] = set()

    def _once(self, obj: Any) -> bool:
        if id(obj) in self._seen:
            return False
        self._seen.add(id(obj))
        return True

    # Project code.

    def function(self, f: Any, root: bool = False) -> None:
        f = inspect.unwrap(f)
        if not isinstance(f, types.FunctionType) or not self._once(f):
            return
        if not is_project_file(f.__code__.co_filename):
            self.package(f.__module__)
            return
        lift = getattr(f, "__hubwagen_lift__", None)
        if lift is not None and self.lifts_by_name and not root:
            self.code[f"{f.__module__}:{f.__qualname__}"] = "lift " + lift.name
            return
        self.code[f"{f.__module__}:{f.__qualname__}"] = _source_hash(f, strip_decorators=root)
        names = _names(f.__code__)
        for n in names:
            if n.startswith("__") and n.endswith("__"):
                continue
            if n in f.__globals__:
                self.value(f.__globals__[n], names, f"{f.__module__}:{n}")
            elif n in sys.modules:  # imported inside the function: `import polars as pl`
                self.value(sys.modules[n], names, f"{f.__module__}:{n}")
        # importlib.import_module("app.helpers") with a literal name: a
        # project module already imported under that name.
        for s in _strings(f.__code__):
            m = sys.modules.get(s)
            if m is not None and is_project_file(getattr(m, "__file__", None) or ""):
                self.module(m, names)
        for i, cell in enumerate(f.__closure__ or ()):
            try:
                self.value(cell.cell_contents, names, f"{f.__module__}:{f.__qualname__}.<closure {i}>")
            except ValueError:  # an empty cell
                pass
        # Annotations decide how inputs are loaded (pd.DataFrame, pl.LazyFrame).
        for k, ann in _annotations(f).items():
            self.value(ann, names, f"{f.__module__}:{f.__qualname__}.<annotation {k}>")
        for i, d in enumerate(f.__defaults__ or ()):
            self.value(d, names, f"{f.__module__}:{f.__qualname__}.<default {i}>")
        for k, d in (f.__kwdefaults__ or {}).items():
            self.value(d, names, f"{f.__module__}:{f.__qualname__}.<default {k}>")

    def cls(self, c: type) -> None:
        if not self._once(c):
            return
        module = sys.modules.get(c.__module__)
        filename = getattr(module, "__file__", None) or ""
        if not is_project_file(filename):
            self.package(c.__module__)
            return
        self.code[f"{c.__module__}:{c.__qualname__}"] = _source_hash(c)
        for base in c.__bases__:
            if base is not object:
                self.cls(base)
        for attr in vars(c).values():
            if isinstance(attr, (staticmethod, classmethod)):
                attr = attr.__func__
            if isinstance(attr, property):
                for acc in (attr.fget, attr.fset, attr.fdel):
                    if acc is not None:
                        self.function(acc)
            elif isinstance(attr, types.FunctionType):
                self.function(attr)

    def module(self, m: types.ModuleType, names: set[str]) -> None:
        """A module the code uses as `m.attr`: its attributes that the code
        names (a module-level `import helpers` and `helpers.clean()`)."""
        if not is_project_file(getattr(m, "__file__", None) or ""):
            if getattr(m, "__file__", None) is None and hasattr(m, "__path__"):
                pass  # a namespace package: only its submodules hold code
            else:
                self.package(m.__name__)
                return
        if not self._once(m):
            return
        for n in names:
            if n in vars(m):
                self.value(vars(m)[n], names, f"{m.__name__}:{n}")

    def value(self, v: Any, names: set[str], key: str) -> None:
        if isinstance(v, types.ModuleType):
            self.module(v, names)
        elif isinstance(v, types.FunctionType) or hasattr(v, "__wrapped__"):
            self.function(v)
        elif isinstance(v, type):
            self.cls(v)
        elif isinstance(v, (types.BuiltinFunctionType, types.MethodType)):
            self.package(getattr(v, "__module__", None) or type(getattr(v, "__self__", v)).__module__)
        elif isinstance(v, os.PathLike):
            # Inside the project, relative to its root: the same on every
            # machine and checkout.
            p = Path(v).resolve()
            self.code[key] = _hash(p.relative_to(self.root).as_posix() if p.is_relative_to(self.root) else str(p))
        elif _plain(v):
            self.code[key] = _hash(repr(v))
        else:
            self.cls(type(v))  # an instance: its class (not its state)

    # Code outside the project.

    def package(self, module: str | None) -> None:
        top = (module or "").split(".")[0]
        if not top or top in sys.stdlib_module_names or top in ("builtins", "__main__"):
            return
        if top == "hubwagen":
            return
        for dist in _packages().get(top, []):
            self._dist(dist)

    def _dist(self, dist: str) -> None:
        key = _norm(dist)
        if key in self.packages:
            return
        version = _dist_version(dist)
        if version is None:
            return
        self.packages[key] = version
        for req in md.requires(dist) or []:
            if "extra ==" in req:
                continue
            m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", req)
            if m:
                self._dist(m.group(1))


def _names(code: types.CodeType) -> set[str]:
    """Names a code object and the code nested in it look up: globals and
    attributes alike."""
    out = set(code.co_names)
    for c in code.co_consts:
        if isinstance(c, types.CodeType):
            out |= _names(c)
    return out


def _annotations(f: types.FunctionType) -> dict[str, Any]:
    try:
        return typing.get_type_hints(f)
    except Exception:  # noqa: BLE001 - an annotation that doesn't resolve: take what is there
        return {k: v for k, v in getattr(f, "__annotations__", {}).items() if not isinstance(v, str)}


def _strings(code: types.CodeType) -> set[str]:
    out = {c for c in code.co_consts if isinstance(c, str)}
    for c in code.co_consts:
        if isinstance(c, types.CodeType):
            out |= _strings(c)
    return out


def _plain(v: Any) -> bool:
    if isinstance(v, _SIMPLE):
        return True
    if isinstance(v, (tuple, list, frozenset, set)):
        return all(_plain(x) for x in v)
    if isinstance(v, dict):
        return all(_plain(k) and _plain(x) for k, x in v.items())
    return False


def _source_hash(obj: Any, strip_decorators: bool = False) -> str:
    """obj's source as an AST without docstrings (and, for the lift itself,
    without decorators), so comments and formatting don't count. Falls back
    to the compiled code when there is no source."""
    try:
        src = textwrap.dedent(inspect.getsource(obj))
        tree = ast.parse(src)
    except (OSError, TypeError, SyntaxError):
        code = getattr(obj, "__code__", None)
        return _hash(marshal.dumps(code) if code is not None else repr(obj))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    if strip_decorators:
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node.decorator_list = []
    return _hash(ast.dump(tree, annotate_fields=False, include_attributes=False))


def _files(fn: Callable, deps: tuple[str, ...], name: str) -> dict[str, str]:
    if not deps:
        return {}
    root = project_root(_source_file(fn))
    out: dict[str, str] = {}
    for d in deps:
        p = root / d
        if p.is_file():
            out[d] = _hash(p.read_bytes())
        elif p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.is_file() and "__pycache__" not in f.parts:
                    out[f.relative_to(root).as_posix()] = _hash(f.read_bytes())
        else:
            raise ValueError(f"lift {name or fn.__name__}: deps {d!r}: no such file or directory under {root}")
    return out


def _source_file(fn: Callable) -> str:
    try:
        return inspect.getsourcefile(inspect.unwrap(fn)) or ""
    except TypeError:
        return ""


def project_root(filename: str) -> Path:
    """The directory of the nearest pyproject.toml above filename, else the
    working directory."""
    start = Path(filename).resolve().parent if filename else Path.cwd()
    for d in (start, *start.parents):
        if (d / "pyproject.toml").is_file():
            return d
    return Path.cwd()


@functools.cache
def _packages() -> dict[str, list[str]]:
    return md.packages_distributions()


@functools.cache
def _dist_version(dist: str) -> str | None:
    try:
        return md.version(dist)
    except md.PackageNotFoundError:
        return None


def _norm(dist: str) -> str:
    return re.sub(r"[-_.]+", "-", dist).lower()


def _hash(data: str | bytes) -> str:
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()[:16]

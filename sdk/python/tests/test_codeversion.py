"""Code versions: which changes to a project change a step's version.

Each case writes a small project twice, as package `proj` in two
directories, imports each and compares the versions of the same step.
Spike S2 (docs/spikes/s2-code-version.md) summarizes the cases.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from pathlib import Path

import pytest

from moksha import _codeversion, _model

BASE = {
    "pyproject.toml": "[project]\nname = 'proj'\n",
    "proj/__init__.py": "",
    "proj/helpers.py": '''
        import proj.deep as deep

        RATE = 2

        def clean(x):
            return x * RATE

        def twice(x):
            return deep.inner(x) * 2

        def unused():
            return 0
    ''',
    "proj/deep.py": '''
        def inner(x):
            return x + 1
    ''',
    "proj/shapes.py": '''
        class Box:
            def area(self, w):
                return w * w
    ''',
    "proj/nspkg/__keep__": "",
    "proj/steps.py": '''
        import importlib
        from pathlib import Path

        import moksha as mk
        from proj import helpers
        from proj.helpers import clean
        from proj.shapes import Box
        from proj.nspkg import tool

        DATA = Path(__file__).parent / "data.csv"
        LIMITS = {"max": 10}

        def local(x):
            """A helper in the same module."""
            return x + 1

        def other_local():
            return 1

        @mk.step(reuse=True)
        def plain(x: int) -> int:
            """The step."""
            return x + 1

        @mk.step
        def uses_local(x: int) -> int:
            return local(x)

        @mk.step
        def uses_from_import(x: int) -> int:
            return clean(x)

        @mk.step
        def uses_module(x: int) -> int:
            return helpers.twice(x)

        @mk.step
        def uses_constant(x: int) -> int:
            return min(x, LIMITS["max"])

        @mk.step
        def uses_path() -> str:
            return DATA.name

        @mk.step
        def uses_class(w: int) -> int:
            return Box().area(w)

        @mk.step
        def uses_namespace(x: int) -> int:
            return tool.go(x)

        @mk.step
        def uses_literal_import(x: int) -> int:
            return importlib.import_module("proj.deep").inner(x)

        @mk.step
        def uses_pytest() -> str:
            import pytest
            return pytest.__name__

        @mk.step(deps=["sql/"])
        def uses_files() -> int:
            return 1

        @mk.pipeline
        def flow(ctx, x: int):
            return ctx.step(plain, local(x))
    ''',
    "proj/nspkg/tool.py": '''
        def go(x):
            return x - 1
    ''',
    "sql/q.sql": "select 1",
}

STEPS = ["plain", "uses_local", "uses_from_import", "uses_module", "uses_constant", "uses_path", "uses_class",
         "uses_namespace", "uses_literal_import", "uses_pytest", "uses_files"]


def load(root: Path, files: dict[str, str]) -> dict[str, str]:
    """Write the project under root, import proj.steps, and return every
    step's and run's version; then forget the project again."""
    for name, text in files.items():
        p = root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith("__keep__"):
            continue
        p.write_text(textwrap.dedent(text))
    (root / "proj/nspkg/__keep__").unlink(missing_ok=True)
    sys.path.insert(0, str(root))
    saved_steps, saved_pipelines = dict(_model.STEPS), dict(_model.PIPELINES)
    try:
        mod = importlib.import_module("proj.steps")
        out = {n: getattr(mod, n).__moksha_step__.code_version() for n in STEPS}
        out["flow"] = mod.flow.__moksha_pipeline__.code_version()
        return out
    finally:
        sys.path.remove(str(root))
        for m in [m for m in sys.modules if m == "proj" or m.startswith("proj.")]:
            del sys.modules[m]
        _model.STEPS.clear(), _model.STEPS.update(saved_steps)
        _model.PIPELINES.clear(), _model.PIPELINES.update(saved_pipelines)


def changed(tmp_path, edit) -> set[str]:
    """Which versions differ after edit(files) changes the project."""
    files = dict(BASE)
    before = load(tmp_path / "a", files)
    edit(files)
    after = load(tmp_path / "b", files)
    return {n for n in before if before[n] != after[n]}


def replace(files, name, old, new):
    assert old in files[name], (name, old)
    files[name] = files[name].replace(old, new)


def test_nothing_changed_nothing_differs(tmp_path):
    assert changed(tmp_path, lambda f: None) == set(), "versions don't depend on where the project is"


def test_comments_docstrings_formatting_and_decorator_arguments_dont_count(tmp_path):
    def edit(f):
        replace(f, "proj/steps.py", '"""The step."""', '"""The step, documented better."""  # and a comment')
        replace(f, "proj/steps.py", "return x + 1\n\n        @mk.step\n        def uses_local",
                "return (x +\n                    1)\n\n        @mk.step\n        def uses_local")
        replace(f, "proj/steps.py", "@mk.step(reuse=True)", "@mk.step(reuse=True, timeout=30)")
    assert changed(tmp_path, edit) == set()


def test_the_step_body(tmp_path):
    assert changed(tmp_path, lambda f: replace(f, "proj/steps.py", "return x + 1\n\n        @mk.step\n        def uses_local",
                                                 "return x + 2\n\n        @mk.step\n        def uses_local")) == {"plain"}


def test_a_helper_in_the_same_module(tmp_path):
    assert changed(tmp_path, lambda f: replace(f, "proj/steps.py", '''return x + 1\n\n        def other_local''',
                                                 '''return x + 3\n\n        def other_local''')) == {"uses_local", "flow"}


def test_an_unused_function_doesnt_count(tmp_path):
    assert changed(tmp_path, lambda f: replace(f, "proj/steps.py", "def other_local():\n            return 1",
                                                 "def other_local():\n            return 2")) == set()
    assert changed(tmp_path / "2", lambda f: replace(f, "proj/helpers.py", "return 0", "return 1")) == set()


def test_helpers_in_other_modules(tmp_path):
    # clean() reads RATE: from-imports and module attributes both reach it.
    assert changed(tmp_path, lambda f: replace(f, "proj/helpers.py", "RATE = 2", "RATE = 3")) == {"uses_from_import"}
    # twice() calls deep.inner(): reached transitively, and through a literal import_module.
    assert changed(tmp_path / "2", lambda f: replace(f, "proj/deep.py", "x + 1", "x + 2")) == \
        {"uses_module", "uses_literal_import"}


def test_constants_classes_and_namespace_packages(tmp_path):
    assert changed(tmp_path, lambda f: replace(f, "proj/steps.py", '"max": 10', '"max": 11')) == {"uses_constant"}
    assert changed(tmp_path / "2", lambda f: replace(f, "proj/shapes.py", "w * w", "w * w * 1.0")) == {"uses_class"}
    assert changed(tmp_path / "3", lambda f: replace(f, "proj/nspkg/tool.py", "x - 1", "x - 2")) == {"uses_namespace"}
    assert changed(tmp_path / "4", lambda f: replace(f, "proj/steps.py", '"data.csv"', '"other.csv"')) == {"uses_path"}


def test_deps_files(tmp_path):
    assert changed(tmp_path, lambda f: f.update({"sql/q.sql": "select 2"})) == {"uses_files"}
    assert changed(tmp_path / "2", lambda f: f.update({"sql/new.sql": "select 3"})) == {"uses_files"}


def test_missing_deps_name_the_step(tmp_path):
    def nothing():
        return 1

    with pytest.raises(ValueError, match=r"step nothing: deps 'nope.sql': no such file"):
        _codeversion.code_version(nothing, ("nope.sql",), "nothing")


def test_packages_count_by_version(tmp_path):
    files = dict(BASE)
    load(tmp_path, files)  # warm: proj imported once
    sys.path.insert(0, str(tmp_path))
    try:
        mod = importlib.import_module("proj.steps")
        p = _codeversion.parts(mod.uses_pytest)
        assert "pytest" in p["packages"], p
        assert "pluggy" in p["packages"], "with what it requires"
        assert "moksha" not in p["packages"]
        assert _codeversion.parts(mod.plain)["packages"] == {}, "stdlib and moksha don't count"
        assert p["python"].startswith(f"{sys.implementation.name} {sys.version_info.major}.")
    finally:
        sys.path.remove(str(tmp_path))
        for m in [m for m in sys.modules if m == "proj" or m.startswith("proj.")]:
            del sys.modules[m]


def test_runs_count_steps_by_name(tmp_path):
    files = dict(BASE)
    load(tmp_path, files)
    # A step's body changing leaves the run as it was (the step has its own
    # version); the run's own helper changing doesn't (see above).
    assert "flow" not in changed(tmp_path / "x", lambda f: replace(
        f, "proj/steps.py", "return x + 1\n\n        @mk.step\n        def uses_local",
        "return x + 2\n\n        @mk.step\n        def uses_local"))


def test_version_pins():
    @_model.step(name="pinned_cv", version="7")
    def pinned() -> int:
        return 1

    assert pinned.__moksha_step__.code_version() == "7"
    _model.STEPS.pop("pinned_cv")

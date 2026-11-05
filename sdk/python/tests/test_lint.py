"""moksha lint on small modules, and on the example project."""

import subprocess
import sys
import textwrap
from pathlib import Path

from moksha.lint import lint_source, main

from conftest import REPO, SDK


def codes(src: str) -> list[tuple[int, str]]:
    return [(f.line, f.code) for f in lint_source(textwrap.dedent(src), "app.py")]


def test_each_rule():
    src = '''
        import datetime as dt
        import os
        import random
        import requests
        import time
        from pathlib import Path
        from time import time as now
        from uuid import uuid4

        import moksha as mk


        @mk.step
        def fetch(day): ...


        @mk.pipeline
        def daily(ctx, day):
            a = time.time()                       # 20 MK001
            b = now()                             # 21 MK001
            c = dt.datetime.now()                 # 22 MK001
            d = random.choice([1, 2])             # 23 MK002
            e = uuid4()                           # 24 MK002
            for x in {"a", "b"}:                  # 25 MK003
                pass
            names = set(day)
            ys = [y for y in names]               # 28 MK003
            ok = [y for y in sorted(names)]
            r = requests.get("https://x")         # 30 MK004
            t = Path("f").read_text()             # 31 MK004
            env = os.environ["HOME"]              # 32 MK004
            try:
                ctx.step(fetch, day)
            except:                               # 35 MK005
                pass
            fetch(day)                            # 37 MK006
            return ctx.step(fetch, day)
    '''
    assert codes(src) == [(20, "MK001"), (21, "MK001"), (22, "MK001"), (23, "MK002"), (24, "MK002"),
                          (25, "MK003"), (28, "MK003"), (30, "MK004"), (31, "MK004"), (32, "MK004"),
                          (35, "MK005"), (37, "MK006")]


def test_only_run_code_and_noqa():
    src = '''
        import time
        from moksha import pipeline, step

        @step
        def stamp():
            return time.time()                    # a step may

        def helper():
            return time.time()                    # not pipeline code

        @pipeline(queue="etl")
        def h(ctx):
            ctx.step(stamp)
            started = time.time()  # noqa: MK001
            other = time.time()  # noqa: MK002
            try:
                pass
            except Exception:
                pass
            def nested():
                return open("x")                  # 22: nested functions are pipeline code
            return nested
    '''
    assert codes(src) == [(16, "MK001"), (22, "MK004")]


def test_messages_name_the_place():
    f = lint_source("import moksha as mk\n@mk.pipeline\ndef h(ctx):\n    return mk.x(open('f'))\n", "app/pipelines.py")[0]
    assert str(f) == "app/pipelines.py:4:17: MK004 pipeline h: open() does I/O; do it in a step"
    assert lint_source("def broken(:\n", "bad.py")[0].code == "MK000"


def test_example_project_is_clean(monkeypatch, capsys):
    monkeypatch.chdir(REPO / "pipelines" / "examples" / "revenue")
    assert main([]) == 0, capsys.readouterr().out  # its modules come from pyproject.toml


def test_command_line(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[tool.moksha]\nmodules = ["app.pipelines"]\n')
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "__init__.py").write_text("")
    (tmp_path / "app" / "pipelines.py").write_text("import random\nimport moksha as mk\n\n@mk.pipeline\ndef h(ctx):\n    return random.random()\n")
    run = subprocess.run([sys.executable, "-m", "moksha.lint"], cwd=tmp_path, capture_output=True, text=True,
                         env={"PYTHONPATH": str(SDK / "src")})
    assert run.returncode == 1
    assert run.stdout == "app/pipelines.py:6:12: MK002 pipeline h: random.random() is random; use ctx.random() or a step\n"
    assert run.stderr == "1 problem\n"
    ok = subprocess.run([sys.executable, "-m", "moksha.lint", "app/__init__.py"], cwd=tmp_path,
                        capture_output=True, text=True, env={"PYTHONPATH": str(SDK / "src")})
    assert ok.returncode == 0
    bad = subprocess.run([sys.executable, "-m", "moksha.lint", "nope.mod"], cwd=tmp_path,
                         capture_output=True, text=True, env={"PYTHONPATH": str(SDK / "src")})
    assert bad.returncode != 0 and "nope.mod: no such file" in bad.stderr


def test_sdk_test_runs_are_known(tmp_path):
    """The SDK's own test runs break the rules on purpose; lint finds them."""
    found = {f.code for f in lint_source((Path(__file__).parent / "example_pipelines.py").read_text(), "example_pipelines.py")}
    assert {"MK001"} <= found, "reads_clock reads the clock"

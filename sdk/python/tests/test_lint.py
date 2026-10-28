"""hubwagen lint on small modules, and on the example project."""

import subprocess
import sys
import textwrap
from pathlib import Path

from hubwagen.lint import lint_source, main

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

        import hubwagen as hw


        @hw.lift
        def fetch(day): ...


        @hw.haul
        def daily(ctx, day):
            a = time.time()                       # 20 HW001
            b = now()                             # 21 HW001
            c = dt.datetime.now()                 # 22 HW001
            d = random.choice([1, 2])             # 23 HW002
            e = uuid4()                           # 24 HW002
            for x in {"a", "b"}:                  # 25 HW003
                pass
            names = set(day)
            ys = [y for y in names]               # 28 HW003
            ok = [y for y in sorted(names)]
            r = requests.get("https://x")         # 30 HW004
            t = Path("f").read_text()             # 31 HW004
            env = os.environ["HOME"]              # 32 HW004
            try:
                ctx.lift(fetch, day)
            except:                               # 35 HW005
                pass
            fetch(day)                            # 37 HW006
            return ctx.lift(fetch, day)
    '''
    assert codes(src) == [(20, "HW001"), (21, "HW001"), (22, "HW001"), (23, "HW002"), (24, "HW002"),
                          (25, "HW003"), (28, "HW003"), (30, "HW004"), (31, "HW004"), (32, "HW004"),
                          (35, "HW005"), (37, "HW006")]


def test_only_haul_code_and_noqa():
    src = '''
        import time
        from hubwagen import haul, lift

        @lift
        def stamp():
            return time.time()                    # a lift may

        def helper():
            return time.time()                    # not haul code

        @haul(dock="etl")
        def h(ctx):
            ctx.lift(stamp)
            started = time.time()  # noqa: HW001
            other = time.time()  # noqa: HW002
            try:
                pass
            except Exception:
                pass
            def nested():
                return open("x")                  # 22: nested functions are haul code
            return nested
    '''
    assert codes(src) == [(16, "HW001"), (22, "HW004")]


def test_messages_name_the_place():
    f = lint_source("import hubwagen as hw\n@hw.haul\ndef h(ctx):\n    return hw.x(open('f'))\n", "app/hauls.py")[0]
    assert str(f) == "app/hauls.py:4:17: HW004 haul h: open() does I/O; do it in a lift"
    assert lint_source("def broken(:\n", "bad.py")[0].code == "HW000"


def test_example_project_is_clean(monkeypatch, capsys):
    monkeypatch.chdir(REPO / "pipelines" / "examples" / "revenue")
    assert main([]) == 0, capsys.readouterr().out  # its modules come from pyproject.toml


def test_command_line(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[tool.hubwagen]\nmodules = ["app.hauls"]\n')
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "__init__.py").write_text("")
    (tmp_path / "app" / "hauls.py").write_text("import random\nimport hubwagen as hw\n\n@hw.haul\ndef h(ctx):\n    return random.random()\n")
    run = subprocess.run([sys.executable, "-m", "hubwagen.lint"], cwd=tmp_path, capture_output=True, text=True,
                         env={"PYTHONPATH": str(SDK / "src")})
    assert run.returncode == 1
    assert run.stdout == "app/hauls.py:6:12: HW002 haul h: random.random() is random; use ctx.random() or a lift\n"
    assert run.stderr == "1 problem\n"
    ok = subprocess.run([sys.executable, "-m", "hubwagen.lint", "app/__init__.py"], cwd=tmp_path,
                        capture_output=True, text=True, env={"PYTHONPATH": str(SDK / "src")})
    assert ok.returncode == 0
    bad = subprocess.run([sys.executable, "-m", "hubwagen.lint", "nope.mod"], cwd=tmp_path,
                         capture_output=True, text=True, env={"PYTHONPATH": str(SDK / "src")})
    assert bad.returncode != 0 and "nope.mod: no such file" in bad.stderr


def test_sdk_test_hauls_are_known(tmp_path):
    """The SDK's own test hauls break the rules on purpose; lint finds them."""
    found = {f.code for f in lint_source((Path(__file__).parent / "example_hauls.py").read_text(), "example_hauls.py")}
    assert {"HW001"} <= found, "reads_clock reads the clock"

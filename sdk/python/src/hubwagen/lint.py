"""hubwagen lint: find pipeline code that won't replay the same way twice.

    python -m hubwagen.lint                  # the modules in ./pyproject.toml ([tool.hubwagen])
    python -m hubwagen.lint app/hauls.py src # files, directories or module names

A run of a pipeline is replayed against what it already did many times, on
any worker, so pipeline code must make the same calls in the same order
every time. The runtime guards catch the clock and randomness while a run
replays; lint catches more, and
before anything runs, by reading the source (nothing is imported):

    HW001  the clock (time.time, datetime.now, date.today, ...)
    HW002  randomness (random.*, uuid4, os.urandom, secrets)
    HW003  iterating a set: its order can differ between processes
    HW004  I/O, network, subprocesses or environment variables
    HW005  `except:` or `except BaseException`: swallows the replay's Blocked
    HW006  a step called directly instead of through ctx.step

Only code inside functions decorated with hubwagen's @pipeline (or @haul) is checked
(nested functions included, helpers it calls not). Silence a line with
`# noqa: HW004` (or a bare `# noqa`). Exits 1 if anything is found.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

CLOCK = {"time.time", "time.time_ns", "datetime.datetime.now", "datetime.datetime.utcnow",
         "datetime.datetime.today", "datetime.date.today"}
RANDOM = {f"random.{f}" for f in ("random", "randint", "randrange", "choice", "choices", "shuffle", "sample",
                                   "uniform", "getrandbits", "gauss", "triangular", "betavariate", "expovariate")}
RANDOM |= {"uuid.uuid1", "uuid.uuid4", "os.urandom"}
RANDOM_PREFIXES = ("secrets.",)
IO_CALLS = {"open", "input", "os.getenv", "os.listdir", "os.scandir", "os.walk", "os.remove", "os.system",
            "os.popen", "glob.glob", "glob.iglob", "io.open"}
IO_PREFIXES = ("requests.", "httpx.", "urllib.request.", "http.client.", "subprocess.", "socket.", "shutil.",
               "sqlite3.", "psycopg", "boto3.", "smtplib.", "ftplib.")
IO_METHODS = {"read_text", "write_text", "read_bytes", "write_bytes", "iterdir", "glob", "rglob", "unlink",
              "mkdir", "rmdir", "touch"}
NOQA = re.compile(r"#\s*noqa(?::\s*(?P<codes>[A-Z0-9, ]+))?", re.IGNORECASE)


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    col: int
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}:{self.col + 1}: {self.code} {self.message}"


def lint_source(source: str, path: str = "<string>") -> list[Finding]:
    """Lint one module's source."""
    try:
        tree = ast.parse(source, path)
    except SyntaxError as e:
        return [Finding(path, e.lineno or 1, (e.offset or 1) - 1, "HW000", f"syntax error: {e.msg}")]
    names = _imports(tree)
    lifts = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
             and any(_decorated(d, names, "step", "lift") for d in n.decorator_list)}
    findings: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and \
                any(_decorated(d, names, "pipeline", "haul") for d in node.decorator_list):
            findings += _HaulChecker(path, names, lifts, node.name).check(node)
    lines = source.splitlines()
    return sorted({f for f in findings if not _silenced(f, lines)}, key=lambda f: (f.line, f.col, f.code))


def _silenced(f: Finding, lines: list[str]) -> bool:
    m = NOQA.search(lines[f.line - 1]) if 0 < f.line <= len(lines) else None
    if m is None:
        return False
    codes = m.group("codes")
    return codes is None or f.code in {c.strip().upper() for c in codes.split(",")}


def _imports(tree: ast.Module) -> dict[str, str]:
    """Local name → what it is: `import datetime as dt` → dt: datetime,
    `from time import time` → time: time.time."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname:
                    names[a.asname] = a.name
                else:
                    names[a.name.split(".")[0]] = a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for a in node.names:
                names[a.asname or a.name] = f"{node.module}.{a.name}"
    return names


def _qualified(expr: ast.expr, names: dict[str, str]) -> str | None:
    """What a name or attribute chain refers to, through the imports."""
    parts: list[str] = []
    while isinstance(expr, ast.Attribute):
        parts.append(expr.attr)
        expr = expr.value
    if not isinstance(expr, ast.Name):
        return None
    parts.append(names.get(expr.id, expr.id))
    return ".".join(reversed(parts))


def _decorated(d: ast.expr, names: dict[str, str], *what: str) -> bool:
    if isinstance(d, ast.Call):
        d = d.func
    return _qualified(d, names) in {f"hubwagen{m}.{w}" for w in what for m in ("", "._model")}


class _HaulChecker(ast.NodeVisitor):
    def __init__(self, path: str, names: dict[str, str], lifts: set[str], haul: str):
        self.path, self.names, self.lifts, self.haul = path, names, lifts, haul
        self.findings: list[Finding] = []
        self.sets: set[str] = set()  # local names bound to sets

    def check(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[Finding]:
        for stmt in fn.body:
            self.visit(stmt)
        return self.findings

    def add(self, node: ast.AST, code: str, message: str) -> None:
        self.findings.append(Finding(self.path, node.lineno, node.col_offset, code, f"pipeline {self.haul}: {message}"))

    def visit_Call(self, node: ast.Call) -> None:
        q = _qualified(node.func, self.names)
        if q in CLOCK:
            self.add(node, "HW001", f"{q}() reads the clock; use ctx.now()")
        elif q in RANDOM or (q and q.startswith(RANDOM_PREFIXES)):
            self.add(node, "HW002", f"{q}() is random; use ctx.random() or a step")
        elif q in IO_CALLS or (q and q.startswith(IO_PREFIXES)):
            self.add(node, "HW004", f"{q}() does I/O; do it in a step")
        elif isinstance(node.func, ast.Attribute) and node.func.attr in IO_METHODS:
            self.add(node, "HW004", f".{node.func.attr}() does I/O; do it in a step")
        elif isinstance(node.func, ast.Name) and node.func.id in self.lifts:
            self.add(node, "HW006", f"step {node.func.id} called directly runs inside the pipeline; use ctx.step({node.func.id}, ...)")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if _qualified(node, self.names) == "os.environ":
            self.add(node, "HW004", "os.environ reads the environment; pass it as an input or read it in a step")
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        is_set = self._is_set(node.value)
        for t in node.targets:
            if isinstance(t, ast.Name):
                (self.sets.add if is_set else self.sets.discard)(t.id)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        self._iterates(node.iter)
        self.generic_visit(node)

    visit_AsyncFor = visit_For

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self._iterates(node.iter)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        types = node.type.elts if isinstance(node.type, ast.Tuple) else [node.type]
        if node.type is None:
            self.add(node, "HW005", "bare `except:` swallows the replay's Blocked; catch Exception")
        elif any(t is not None and _qualified(t, self.names) == "BaseException" for t in types):
            self.add(node, "HW005", "`except BaseException` swallows the replay's Blocked; catch Exception")
        self.generic_visit(node)

    def _iterates(self, it: ast.expr) -> None:
        if self._is_set(it):
            self.add(it, "HW003", "iterating a set: its order differs between processes; iterate sorted(...)")

    def _is_set(self, e: ast.expr) -> bool:
        if isinstance(e, (ast.Set, ast.SetComp)):
            return True
        if isinstance(e, ast.Name):
            return e.id in self.sets
        if isinstance(e, ast.Call) and isinstance(e.func, ast.Name) and e.func.id in ("set", "frozenset"):
            return True
        if isinstance(e, ast.BinOp) and isinstance(e.op, (ast.BitOr, ast.BitAnd, ast.Sub, ast.BitXor)):
            return self._is_set(e.left) or self._is_set(e.right)
        if isinstance(e, ast.Call) and isinstance(e.func, ast.Attribute) and \
                e.func.attr in ("union", "intersection", "difference", "symmetric_difference"):
            return self._is_set(e.func.value)
        return False


# --- command line -----------------------------------------------------------

def files_for(targets: list[str], root: Path) -> list[Path]:
    """Python files for paths (files or directories) and module names,
    found from root and root/src without importing anything."""
    out: list[Path] = []
    for t in targets:
        p = root / t
        if p.is_dir():
            out += sorted(f for f in p.rglob("*.py") if "__pycache__" not in f.parts and ".venv" not in f.parts)
            continue
        if p.is_file():
            out.append(p)
            continue
        rel = Path(*t.split("."))
        for base in (root, root / "src"):
            if (base / rel).with_suffix(".py").is_file():
                out.append((base / rel).with_suffix(".py"))
                break
            if (base / rel / "__init__.py").is_file():
                out.append(base / rel / "__init__.py")
                break
        else:
            raise SystemExit(f"{t}: no such file, directory or module under {root}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m hubwagen.lint", description=__doc__.splitlines()[0])
    p.add_argument("targets", nargs="*",
                   help="files, directories or module names (default: [tool.hubwagen] modules in ./pyproject.toml)")
    args = p.parse_args(argv)
    root = Path.cwd()
    targets = args.targets
    if not targets:
        from .worker import project_modules
        targets = project_modules(root)
    findings = []
    for f in files_for(targets, root):
        shown = f.relative_to(root) if f.is_relative_to(root) else f
        findings += lint_source(f.read_text(), str(shown))
    for f in findings:
        print(f)
    if findings:
        print(f"{len(findings)} problem{'s' * (len(findings) != 1)}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())

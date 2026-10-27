"""The M3c demo: the example project in pipelines/examples/revenue, run by a
real worker against `hubwagen dev`, replayed with the `hubwagen replay`
command. The second replay reuses what didn't change; changing a lift's
code or the source data reruns exactly what depends on it."""

import re
import shutil
import signal
import subprocess

import pytest

from conftest import REPO, start_dev, start_worker, stop

EXAMPLE = REPO / "pipelines" / "examples" / "revenue"


def normalize(report: str) -> str:
    """The report without column padding or indentation."""
    return "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in report.splitlines() if line.strip())


@pytest.fixture
def demo(hubwagen_bin, tmp_path):
    project = tmp_path / "revenue"
    shutil.copytree(EXAMPLE, project)
    url, dev = start_dev(hubwagen_bin, tmp_path)
    procs = {"worker": start_worker(url, None, project, tmp_path / "worker.log")}  # modules from pyproject.toml

    def replay(days: str) -> str:
        r = subprocess.run([hubwagen_bin, "replay", "daily", "--server", url, "--param", f"day={days}"],
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stdout + r.stderr
        return normalize(r.stdout)

    def restart_worker():
        stop(procs["worker"])
        procs["worker"] = start_worker(url, None, project, tmp_path / "worker2.log")

    yield project, replay, restart_worker
    stop(procs["worker"])
    stop(dev, signal.SIGINT)


def test_replay_reuses_what_did_not_change(demo):
    project, replay, restart_worker = demo

    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 hauls started
        3 hauls
          extract_orders   3 run (not memoized)
          daily_revenue    3 run (no previous run)
          revenue_report   3 run (no previous run)
        3 completed
    """)

    # The same days again: extract runs (it reads the source), finds the
    # same orders, so everything downstream is reused.
    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 hauls started
        3 hauls
          extract_orders   3 run (not memoized)
          daily_revenue    3 reused
          revenue_report   3 reused
        3 completed
    """)

    # daily_revenue's code changes: it runs again, and so does the report
    # built from its new output.
    hauls = project / "revenue" / "hauls.py"
    hauls.write_text(hauls.read_text().replace('amount.quantize(Decimal("0.01"))', 'amount.quantize(Decimal("1"))'))
    restart_worker()
    out = replay("2026-10-01..2026-10-03")
    assert re.fullmatch(normalize("""
        3 hauls started
        3 hauls
          extract_orders   3 run \\(not memoized\\)
          daily_revenue    3 run \\(code changed \\(src-[0-9a-f]+ → src-[0-9a-f]+\\)\\)
          revenue_report   3 run \\(input changed\\)
        3 completed
    """), out), out

    # A new order on one day: only that day's revenue and report run.
    source = project / "data" / "orders.csv"
    source.write_text(source.read_text() + "9999,2026-10-02,eu,100.00\n")
    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 hauls started
        3 hauls
          extract_orders   3 run (not memoized)
          daily_revenue    2 reused, 1 run (input changed)
          revenue_report   2 reused, 1 run (input changed)
        3 completed
    """)

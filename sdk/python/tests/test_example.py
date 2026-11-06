"""The M3c demo: the revenue project in projects/revenue (as in moksha-demo),
run by a real worker against `moksha dev`, replayed with the `moksha replay`
command. The second replay reuses what didn't change; changing a step's
code or the source data reruns exactly what depends on it."""

import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

import moksha as mk

from conftest import SDK, _free_port, start_dev, start_worker, stop

EXAMPLE = Path(__file__).parent / "projects" / "revenue"


def normalize(report: str) -> str:
    """The report without column padding or indentation."""
    return "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in report.splitlines() if line.strip())


@pytest.fixture
def demo(moksha_bin, tmp_path):
    project = tmp_path / "revenue"
    shutil.copytree(EXAMPLE, project)
    url, dev = start_dev(moksha_bin, tmp_path)
    procs = {"worker": start_worker(url, None, project, tmp_path / "worker.log")}  # modules from pyproject.toml

    def replay(days: str) -> str:
        r = subprocess.run([moksha_bin, "replay", "daily", "--server", url, "--param", f"day={days}"],
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stdout + r.stderr
        return normalize(r.stdout)

    def restart_worker():
        stop(procs["worker"])
        procs["worker"] = start_worker(url, None, project, tmp_path / "worker2.log")

    yield project, replay, restart_worker, url
    stop(procs["worker"])
    stop(dev, signal.SIGINT)


def test_replay_reuses_what_did_not_change(demo):
    project, replay, restart_worker, url = demo

    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 runs started
        3 runs
          extract_orders   3 ran
          daily_revenue    3 ran (it hasn't run with these inputs before)
          revenue_report   3 ran (it hasn't run with these inputs before)
        3 completed
    """)

    # The web UI's view: the pipeline by its title, and each run's steps as
    # a DAG (a step comes after the steps whose data versions it reads).
    api = mk.Client(url)
    pipelines = api.call("RunService", "ListPipelines", {})["pipelines"]
    assert [(p["name"], p["title"]) for p in pipelines] == [("daily", "Daily revenue"), ("nightly", "Nightly revenue")]
    schedules = api.call("RunService", "ListSchedules", {})["schedules"]
    assert [(s["pipeline"], s["cron"], s.get("timezone", "")) for s in schedules] == [("nightly", "0 3 * * *", "")]  # UTC
    run = api.call("RunService", "ListRuns", {"name": "daily", "limit": 1})["runs"][0]
    assert run["trigger"] == "the CLI"
    assert run["stepCounts"] == {"total": 3, "completed": 3}
    history = api.call("RunService", "ListRuns", {"name": "daily", "limit": 3, "withSteps": True})["runs"]
    assert [[(l["name"], l["status"]) for l in h["steps"]] for h in history] == [
        [("extract_orders", "completed"), ("daily_revenue", "completed"), ("revenue_report", "completed")]] * 3
    steps = api.call("RunService", "GetRun", {"runId": run["id"]})["run"]["steps"]
    assert [(l["name"], l.get("after", []), l["dataset"]) for l in steps] == [
        ("extract_orders", [], "raw.orders"), ("daily_revenue", [1], "mart.daily_revenue"),
        ("revenue_report", [2], "reports.revenue")]

    # The same days again: extract runs (it reads the source), finds the
    # same orders, so everything downstream is reused.
    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 runs started
        3 runs
          extract_orders   3 ran
          daily_revenue    3 reused
          revenue_report   3 reused
        3 completed
    """)

    # daily_revenue's code changes: it runs again, and so does the report
    # built from its new output.
    runs = project / "revenue" / "pipelines.py"
    runs.write_text(runs.read_text().replace('amount.quantize(Decimal("0.01"))', 'amount.quantize(Decimal("1"))'))
    restart_worker()
    out = replay("2026-10-01..2026-10-03")
    assert re.fullmatch(normalize("""
        3 runs started
        3 runs
          extract_orders   3 ran
          daily_revenue    3 ran \\(its code changed \\(py-[0-9a-f]+ → py-[0-9a-f]+\\)\\)
          revenue_report   3 ran \\(an input changed\\)
        3 completed
    """), out), out

    # A new order on one day: only that day's revenue and report run.
    source = project / "data" / "orders.csv"
    source.write_text(source.read_text() + "9999,2026-10-02,eu,100.00\n")
    assert replay("2026-10-01..2026-10-03") == normalize("""
        3 runs started
        3 runs
          extract_orders   3 ran
          daily_revenue    2 reused, 1 ran (an input changed)
          revenue_report   2 reused, 1 ran (an input changed)
        3 completed
    """)


def test_dev_deploys_the_project_and_its_changes(moksha_bin, tmp_path):
    """`moksha dev` in the project's directory: it deploys the project to
    itself (its moksha.yaml is the project file), and again when its code
    changes, so nobody starts workers by hand."""
    project = tmp_path / "revenue"
    shutil.copytree(EXAMPLE, project)
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    log = tmp_path / "dev.log"
    env = {**os.environ, "PYTHONPATH": str(SDK / "src")}
    dev = subprocess.Popen([moksha_bin, "dev", "--listen", f"127.0.0.1:{port}", "--python", sys.executable],
                           cwd=project, env=env, stdout=open(log, "w"), stderr=subprocess.STDOUT)

    def wait_for(text: str, timeout: float = 30) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text in log.read_text():
                return
            assert dev.poll() is None, log.read_text()
            time.sleep(0.1)
        raise AssertionError(f"no {text!r} in:\n{log.read_text()}")

    def replay() -> str:
        r = subprocess.run([moksha_bin, "replay", "daily", "--server", url, "--param", "day=2026-10-01..2026-10-02"],
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stdout + r.stderr
        return normalize(r.stdout)

    try:
        wait_for("project revenue v1 deployed")
        assert (project / ".moksha" / "state.db").exists(), "the data is next to the project file"
        assert "daily_revenue 2 ran (it hasn't run with these inputs before)" in replay()

        runs = project / "revenue" / "pipelines.py"
        runs.write_text(runs.read_text().replace('amount.quantize(Decimal("0.01"))', 'amount.quantize(Decimal("1"))'))
        wait_for("project revenue v2 deployed (build ")
        wait_for("daily_revenue runs again (its code changed)")
        wait_for("worker pool revenue-default-v1: gone")  # v1's workers poll no more
        assert re.search(r"daily_revenue 2 ran \(its code changed", replay())
    finally:
        stop(dev, signal.SIGINT)

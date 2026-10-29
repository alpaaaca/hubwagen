"""Workers when the process next to them dies: the leader, or the hubwagen
process that started them."""

import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import hubwagen as hw
from conftest import SDK, _free_port, start_dev, start_worker, stop, wait


def until(cond, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if (v := cond()):
            return v
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


def test_a_result_waits_for_the_leader_to_come_back(hubwagen_bin, tmp_path):
    url, dev = start_dev(hubwagen_bin, tmp_path)
    log = tmp_path / "worker.log"
    worker = start_worker(url, "example_hauls", SDK / "tests", log)
    try:
        client = hw.Client(url)
        started, release = tmp_path / "started", tmp_path / "release"
        haul_id = client.start_haul("wait_for_release", {"started": str(started), "release": str(release)})
        until(started.exists, 20, "the lift to start")

        stop(dev, signal.SIGINT)  # the leader goes away while the lift runs ...
        release.touch()  # ... and the lift finishes meanwhile
        until(lambda: "can't deliver the result of step gated" in log.read_text(), 10, "a failed delivery")

        _, dev = start_dev(hubwagen_bin, tmp_path, url)
        h = wait(client, haul_id)
        assert h["status"] == "completed", h
        assert json.loads(h["resultJson"]) == "released"
        assert h["lifts"][0]["attempts"] == 1, "the first attempt's result was taken, not rerun"
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)


def test_workers_stop_when_their_supervisor_is_killed(hubwagen_bin, tmp_path):
    port = _free_port()
    (tmp_path / "hubwagen.yaml").write_text(f"""
apiVersion: hubwagen.dev/v1alpha1
kind: Config
server: {{listen: "127.0.0.1:{port}"}}
workers:
  - name: py
    replicas: 2
    command: [{sys.executable}, -m, hubwagen.worker, example_hauls, --poll-wait, 1s]
    dir: {SDK / "tests"}
    env: {{PYTHONPATH: "{SDK / "src"}"}}
""")
    out = tmp_path / "dev.out"
    dev = subprocess.Popen([hubwagen_bin, "dev"], cwd=tmp_path, stdout=open(out, "w"), stderr=subprocess.STDOUT)
    try:
        until(lambda: out.read_text().count("polling default") == 2, 20, "two workers polling")
        pids = [int(p) for p in re.findall(r"started \(pid (\d+)\)", out.read_text())]
        assert len(pids) == 2

        dev.kill()  # no chance to stop its workers
        dev.wait()

        def gone(pid: int) -> bool:
            try:
                os.kill(pid, 0)
                return False
            except ProcessLookupError:
                return True
        until(lambda: all(gone(p) for p in pids), 20, f"workers {pids} to exit")
    finally:
        dev.kill()
        for p in re.findall(r"started \(pid (\d+)\)", out.read_text()):
            try:
                os.kill(int(p), signal.SIGKILL)
            except ProcessLookupError:
                pass

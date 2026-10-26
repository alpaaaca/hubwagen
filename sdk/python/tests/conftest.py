"""Fixtures for running the SDK against a real `hubwagen dev`.

The Go binary comes from $HUBWAGEN_BIN or is built from this repository
(needs Go). Integration tests are skipped if neither works.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

import hubwagen as hw

REPO = Path(__file__).resolve().parents[3]
SDK = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def hubwagen_bin(tmp_path_factory) -> str:
    if os.environ.get("HUBWAGEN_BIN"):
        return os.environ["HUBWAGEN_BIN"]
    if not shutil.which("go"):
        pytest.skip("needs Go to build hubwagen, or HUBWAGEN_BIN")
    out = tmp_path_factory.mktemp("bin") / "hubwagen"
    subprocess.run(["go", "build", "-o", str(out), "."], cwd=REPO, check=True)
    return str(out)


@pytest.fixture(scope="session")
def server(hubwagen_bin, tmp_path_factory) -> str:
    """A `hubwagen dev` with the worker API on a free port."""
    tmp = tmp_path_factory.mktemp("dev")
    (tmp / "pipelines").mkdir()
    url = f"http://127.0.0.1:{_free_port()}"
    log = open(tmp / "dev.log", "w")
    proc = subprocess.Popen([hubwagen_bin, "dev", "--headless", "--data-dir", str(tmp / "data"),
                             "--listen", url.removeprefix("http://"), str(tmp / "pipelines")],
                            stdout=log, stderr=subprocess.STDOUT)
    client = hw.Client(url)
    for _ in range(100):
        try:
            client.call("WorkerService", "Heartbeat", {"workerId": "probe"})
            break
        except hw.ConnectError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail(f"hubwagen dev didn't come up; see {tmp / 'dev.log'}")
    yield url
    proc.send_signal(signal.SIGINT)
    proc.wait(timeout=30)


@pytest.fixture(scope="session")
def worker(server, tmp_path_factory) -> None:
    """`python -m hubwagen.worker example_hauls` against the server."""
    log_path = tmp_path_factory.mktemp("worker") / "worker.log"
    log = open(log_path, "w")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(SDK / "src"), str(SDK / "tests")])}
    proc = subprocess.Popen([sys.executable, "-m", "hubwagen.worker", "example_hauls", "--server", server,
                             "--poll-wait", "1s", "--log-level", "info"],
                            cwd=SDK / "tests", env=env, stdout=log, stderr=subprocess.STDOUT)
    yield log_path
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture
def client(server, worker) -> hw.Client:
    return hw.Client(server)


def wait(client: hw.Client, haul_id: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        h = client.get_haul(haul_id)
        if h["status"] != "running":
            return h
        time.sleep(0.05)
    raise AssertionError(f"haul {haul_id} still running: {client.get_haul(haul_id)}")

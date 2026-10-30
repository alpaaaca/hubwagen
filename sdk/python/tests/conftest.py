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


def start_dev(hubwagen_bin: str, tmp: Path, url: str | None = None,
              config: Path | None = None) -> tuple[str, subprocess.Popen]:
    """A `hubwagen dev` with the API on url (default: a free port), data under
    tmp, and the config file's other settings (its workers are not started)."""
    url = url or f"http://127.0.0.1:{_free_port()}"
    log = open(tmp / "dev.log", "a")
    proc = subprocess.Popen([hubwagen_bin, "dev", "--data-dir", str(tmp / "data"),
                             "--listen", url.removeprefix("http://"),
                             *(["--config", str(config), "--no-workers"] if config else [])],
                            cwd=tmp, stdout=log, stderr=subprocess.STDOUT)
    client = hw.Client(url)
    for _ in range(100):
        try:
            client.call("WorkerService", "Heartbeat", {"workerId": "probe"})
            return url, proc
        except hw.ConnectError:
            if proc.poll() is not None:
                break
            time.sleep(0.1)
    proc.kill()
    pytest.fail(f"hubwagen dev didn't come up; see {tmp / 'dev.log'}")


def start_worker(server: str, module: str | None, cwd: Path, log: Path, *args: str) -> subprocess.Popen:
    """`python -m hubwagen.worker [module] [args]` from cwd against server,
    once it has registered its build (the server starts only hauls a build
    defines). No module: the worker reads them from cwd/pyproject.toml."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(SDK / "src"), str(cwd)])}
    proc = subprocess.Popen([sys.executable, "-m", "hubwagen.worker", *([module] if module else []), "--server", server,
                             "--poll-wait", "1s", "--log-level", "info", *args],
                            cwd=cwd, env=env, stdout=open(log, "w"), stderr=subprocess.STDOUT)
    for _ in range(200):
        if "registered build" in log.read_text():
            return proc
        if proc.poll() is not None:
            break
        time.sleep(0.05)
    proc.kill()
    pytest.fail(f"worker didn't register; see {log}:\n{log.read_text()}")


@pytest.fixture(scope="session")
def s3fake_bin(tmp_path_factory) -> str:
    """internal/blob/s3fake as a program: an S3-compatible server in memory."""
    if not shutil.which("go"):
        pytest.skip("needs Go to build s3fake")
    out = tmp_path_factory.mktemp("bin") / "s3fake"
    subprocess.run(["go", "build", "-o", str(out), "./internal/blob/s3fake/cmd/s3fake"], cwd=REPO, check=True)
    return str(out)


def start_s3fake(s3fake_bin: str, bucket: str) -> tuple[str, subprocess.Popen]:
    """An S3-compatible endpoint with one bucket; returns its URL."""
    proc = subprocess.Popen([s3fake_bin, "-bucket", bucket], stdout=subprocess.PIPE, text=True)
    return proc.stdout.readline().strip(), proc


def stop(proc: subprocess.Popen, sig=signal.SIGTERM) -> None:
    proc.send_signal(sig)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def server(hubwagen_bin, tmp_path_factory) -> str:
    url, proc = start_dev(hubwagen_bin, tmp_path_factory.mktemp("dev"))
    yield url
    stop(proc, signal.SIGINT)


@pytest.fixture(scope="session")
def worker(server, tmp_path_factory) -> None:
    """A worker running tests/example_hauls.py."""
    proc = start_worker(server, "example_hauls", SDK / "tests", tmp_path_factory.mktemp("worker") / "worker.log")
    yield
    stop(proc)


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

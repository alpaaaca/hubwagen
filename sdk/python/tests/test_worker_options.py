import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from hubwagen import worker


def test_docks_and_lifts_come_from_the_worker_group(monkeypatch):
    monkeypatch.delenv("HUBWAGEN_DOCKS", raising=False)
    monkeypatch.delenv("HUBWAGEN_LIFTS", raising=False)
    args = worker.parse_args([])
    assert (args.dock, args.lifts) == (["default"], 4)

    monkeypatch.setenv("HUBWAGEN_DOCKS", "gpu,default")
    monkeypatch.setenv("HUBWAGEN_LIFTS", "2")
    args = worker.parse_args([])
    assert (args.dock, args.lifts) == (["gpu", "default"], 2)

    args = worker.parse_args(["--dock", "cpu", "--lifts", "8"])
    assert (args.dock, args.lifts) == (["cpu"], 8), "flags win"


def test_build_json_is_what_a_worker_registers():
    import hubwagen as hw

    @hw.lift(name="build_json_load")
    def load(x: int) -> int:
        return x

    @hw.haul(name="build_json_daily")
    def daily(ctx, x: int):
        return ctx.step(load, x)

    reg = worker.registration("b-test")
    assert reg["buildId"] == "b-test"
    assert "build_json_daily" in reg["hauls"]
    assert reg["liftVersions"]["build_json_load"].startswith("py-")
    assert {"schedules", "subscriptions", "datasets", "haulInfo"} <= set(reg)


def test_a_worker_stops_on_sigterm_while_it_waits_for_the_server(tmp_path):
    # A pod stopped before the server answers: the worker must stop then too,
    # not once it has registered (as PID 1 it would ignore the signal).
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # nothing listens here
    (tmp_path / "nothing.py").write_text("")
    log = tmp_path / "worker.log"
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")}
    proc = subprocess.Popen([sys.executable, "-m", "hubwagen.worker", "nothing", "--server", f"http://127.0.0.1:{port}"],
                            cwd=tmp_path, env=env, stdout=open(log, "w"), stderr=subprocess.STDOUT)
    try:
        for _ in range(200):
            if "can't register" in log.read_text():
                break
            time.sleep(0.05)
        else:
            raise AssertionError(f"worker didn't try to register:\n{log.read_text()}")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=5) == 0, log.read_text()
        assert "stopped before registering" in log.read_text()
    finally:
        proc.kill()

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from moksha import worker


def test_queues_and_steps_come_from_the_worker_group(monkeypatch):
    monkeypatch.delenv("MOKSHA_QUEUES", raising=False)
    monkeypatch.delenv("MOKSHA_STEPS", raising=False)
    args = worker.parse_args([])
    assert (args.queue, args.steps) == (["default"], 4)

    monkeypatch.setenv("MOKSHA_QUEUES", "gpu,default")
    monkeypatch.setenv("MOKSHA_STEPS", "2")
    args = worker.parse_args([])
    assert (args.queue, args.steps) == (["gpu", "default"], 2)

    args = worker.parse_args(["--queue", "cpu", "--steps", "8"])
    assert (args.queue, args.steps) == (["cpu"], 8), "flags win"


def test_build_json_is_what_a_worker_registers():
    import moksha as mk

    @mk.step(name="build_json_load")
    def load(x: int) -> int:
        return x

    @mk.pipeline(name="build_json_daily")
    def daily(ctx, x: int):
        return ctx.step(load, x)

    reg = worker.registration("b-test")
    assert reg["buildId"] == "b-test"
    assert "build_json_daily" in reg["pipelines"]
    assert reg["stepVersions"]["build_json_load"].startswith("py-")
    assert {"schedules", "subscriptions", "datasets", "pipelineInfo"} <= set(reg)


def test_a_steps_decorator_is_part_of_the_build(monkeypatch):
    # The code version leaves out the @mk.step(...) line, but a new reuse=
    # (or timeout, retry, queue, output) must be a new build to take effect.
    import moksha as mk
    from moksha import _model

    def build(**kw):
        monkeypatch.setattr(_model, "STEPS", {})
        monkeypatch.setattr(_model, "PIPELINES", {})

        def load(x: int) -> int:
            return x

        mk.step(load, name="decorated_load", **kw)
        return worker.build_id()

    assert build() == build(), "the same code and settings: the same build"
    ids = {build(), build(reuse=True), build(reuse=False), build(reuse="1h"), build(timeout=30), build(queue="gpu")}
    assert len(ids) == 6


def test_a_worker_stops_on_sigterm_while_it_waits_for_the_server(tmp_path):
    # A pod stopped before the server answers: the worker must stop then too,
    # not once it has registered (as PID 1 it would ignore the signal).
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # nothing listens here
    (tmp_path / "nothing.py").write_text("")
    log = tmp_path / "worker.log"
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")}
    proc = subprocess.Popen([sys.executable, "-m", "moksha.worker", "nothing", "--server", f"http://127.0.0.1:{port}"],
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

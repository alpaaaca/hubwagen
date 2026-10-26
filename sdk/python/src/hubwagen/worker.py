"""The worker: `python -m hubwagen.worker app.hauls [more.modules]`.

It imports the modules (registering their hauls and lifts), registers the
build with the server, and then polls: one thread replays hauls, a few run
lifts, one sends heartbeats for running lifts. Ctrl-C stops polling and
lets running lifts finish.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
import traceback
from typing import Any

from . import _model, _output, _replay
from .client import Client, ConnectError

log = logging.getLogger("hubwagen.worker")

POLL_WAIT = "20s"
HEARTBEAT_EVERY = 20.0  # seconds; the server's timeout is longer (a minute in dev)


def build_id() -> str:
    """A build is identified by its hauls and its lifts' code versions."""
    h = hashlib.sha256()
    for name in sorted(_model.HAULS):
        h.update(f"haul {name}\n".encode())
    for name in sorted(_model.LIFTS):
        h.update(f"lift {name} {_model.LIFTS[name].code_version()}\n".encode())
    return "b-" + h.hexdigest()[:12]


class Worker:
    def __init__(self, client: Client, docks: list[str], lifts: int = 4, worker_id: str | None = None,
                 poll_wait: str = POLL_WAIT):
        self.client = client
        self.docks = docks
        self.lift_threads = lifts
        self.id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self.poll_wait = poll_wait
        self.build = build_id()
        self.stopping = threading.Event()
        self._running: dict[str, threading.Event] = {}  # lift attempt → cancelled
        self._lock = threading.Lock()

    def register(self) -> None:
        versions = {name: d.code_version() for name, d in _model.LIFTS.items()}
        self.client.call("WorkerService", "RegisterBuild", {"buildId": self.build, "liftVersions": versions})
        log.info("registered build %s: %d hauls, %d lifts", self.build, len(_model.HAULS), len(_model.LIFTS))

    # --- hauls ---

    def run_haul_task(self, task: dict) -> None:
        d = _model.HAULS.get(task["name"])
        body: dict[str, Any] = {"taskId": task["id"], "buildId": self.build}
        if d is None:
            body["error"] = {"reason": "unknown_haul", "message": f"no haul {task['name']!r} in build {self.build}"}
        else:
            params = json.loads(task.get("paramsJson") or "{}")
            r = _replay.replay(d, task["haulId"], params, task.get("logbook", []))
            body["commands"] = r.commands
            if r.outcome:
                body["outcome"] = r.outcome
            if r.mismatch:
                log.error("haul %s (%s) is not deterministic: %s", task["haulId"], task["name"], r.mismatch)
        self.client.call("WorkerService", "CompleteHaulTask", body)

    # --- lifts ---

    def run_lift_task(self, task: dict) -> None:
        d = _model.LIFTS.get(task["name"])
        body: dict[str, Any] = {"attemptId": task["id"]}
        cancelled = threading.Event()
        with self._lock:
            self._running[task["id"]] = cancelled
        try:
            if d is None:
                raise _model.NonRetryable(f"no lift {task['name']!r} in build {self.build}")
            pallets = {i["name"]: _model.Pallet(id=i["pallet"], location=i.get("location", ""))
                       for i in task.get("inputs", [])}
            kwargs = _model.decode(json.loads(task.get("argsJson") or "{}"), pallets)
            out = None
            token = None
            if d.output is not None:
                out = _output.Output(path=_output.local_dir(task["staging"]))
                out.path.mkdir(parents=True, exist_ok=True)
                token = _output._current.set(out)
            try:
                result = d.fn(**kwargs)
            finally:
                if token is not None:
                    _output._current.reset(token)
            body["status"] = "STATUS_SUCCEEDED"
            body["resultJson"] = json.dumps(_model.encode(result))
            if out is not None:
                body["output"] = _output.describe(out)
        except Exception as e:  # noqa: BLE001 - the lift failed
            body.update(status="STATUS_FAILED", reason=type(e).__name__, message=str(e),
                        noRetry=isinstance(e, _model.NonRetryable))
            log.warning("lift %s failed: %s", task["name"], "".join(traceback.format_exception_only(e)).strip())
        finally:
            with self._lock:
                self._running.pop(task["id"], None)
        if cancelled.is_set():
            return  # the server no longer wants this result
        try:
            self.client.call("WorkerService", "CompleteLiftTask", body)
        except ConnectError as e:
            if e.code != "not_found":  # not_found: timed out or cancelled meanwhile
                raise

    # --- loops ---

    def _loop(self, method: str, run) -> None:
        while not self.stopping.is_set():
            try:
                res = self.client.call("WorkerService", method,
                                       {"workerId": self.id, "docks": self.docks, "wait": self.poll_wait}, timeout=60)
                if res.get("task"):
                    run(res["task"])
            except ConnectError as e:
                log.warning("%s: %s; retrying", method, e)
                self.stopping.wait(1)
            except Exception:  # noqa: BLE001 - keep the worker alive
                log.exception("%s failed", method)
                self.stopping.wait(1)

    def _heartbeats(self) -> None:
        while not self.stopping.wait(HEARTBEAT_EVERY):
            with self._lock:
                ids = list(self._running)
            if not ids:
                continue
            try:
                res = self.client.call("WorkerService", "Heartbeat", {"workerId": self.id, "taskIds": ids})
            except ConnectError as e:
                log.warning("heartbeat: %s", e)
                continue
            with self._lock:
                for tid in res.get("stop", []):
                    if tid in self._running:
                        self._running[tid].set()  # Python can't stop a thread; its result is dropped

    def start(self) -> list[threading.Thread]:
        threads = [threading.Thread(target=self._loop, args=("PollHaulTask", self.run_haul_task), name="hauls", daemon=True),
                   threading.Thread(target=self._heartbeats, name="heartbeats", daemon=True)]
        threads += [threading.Thread(target=self._loop, args=("PollLiftTask", self.run_lift_task), name=f"lifts-{i}", daemon=True)
                    for i in range(self.lift_threads)]
        for t in threads:
            t.start()
        return threads


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    # Iterating a set of strings must give the same order in every replay, in
    # every worker process (spike S1): fix the hash seed, re-executing if needed.
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.execve(sys.executable, [sys.executable, "-m", "hubwagen.worker", *argv], {**os.environ, "PYTHONHASHSEED": "0"})

    p = argparse.ArgumentParser(prog="python -m hubwagen.worker", description=__doc__.splitlines()[0])
    p.add_argument("modules", nargs="+", help="modules that define hauls and lifts, e.g. app.hauls")
    p.add_argument("--server", default=os.environ.get("HUBWAGEN_SERVER", "http://127.0.0.1:7070"))
    p.add_argument("--dock", action="append", default=None, help="dock to poll (repeatable; default: default)")
    p.add_argument("--lifts", type=int, default=4, help="lifts to run at the same time")
    p.add_argument("--poll-wait", default=POLL_WAIT, help="how long one poll waits for work, e.g. 20s")
    p.add_argument("--log-level", default="info")
    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    sys.path.insert(0, os.getcwd())
    for m in args.modules:
        importlib.import_module(m)
    w = Worker(Client(args.server), args.dock or ["default"], lifts=args.lifts, poll_wait=args.poll_wait)
    while True:
        try:
            w.register()
            break
        except ConnectError as e:
            log.warning("can't register with %s (%s); retrying", args.server, e)
            time.sleep(2)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: w.stopping.set())
    threads = w.start()
    log.info("worker %s polling %s on %s", w.id, ", ".join(w.docks), args.server)
    w.stopping.wait()
    log.info("stopping: waiting for running lifts")
    for t in threads:
        t.join(timeout=60)


if __name__ == "__main__":
    main()

"""The worker: `python -m hubwagen.worker [app.hauls more.modules]`.

It imports the modules (registering their hauls and lifts; with none given,
the ones listed under [tool.hubwagen] modules in ./pyproject.toml), registers
the build with the server, and then polls: one thread replays hauls, a few
run lifts, one sends heartbeats for running lifts. It polls with its build,
so it gets only the hauls and lifts its code defines. Ctrl-C stops polling
and lets running lifts finish.

A lift's result is kept, and its attempt heartbeated, until the server
accepts it: if the leader is down when the lift finishes, the worker retries
until it is back. Started by `hubwagen dev` or `hubwagen workers`
($HUBWAGEN_SUPERVISOR_PID set), the worker stops the same way when that
process is gone, so a killed supervisor leaves no workers behind.
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
import tomllib
import traceback
from pathlib import Path
from typing import Any

from . import _codecs, _model, _output, _replay
from .client import Client, ConnectError

log = logging.getLogger("hubwagen.worker")

POLL_WAIT = "20s"
HEARTBEAT_EVERY = 20.0  # seconds; the server's timeout is longer (a minute in dev)
DELIVER_BACKOFF = (0.5, 10.0)  # seconds: first and longest wait between tries to deliver a result
SUPERVISOR_CHECK_EVERY = 1.0  # seconds


def load_inputs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Load the pallets passed as fn's arguments as their annotations ask:
    a dataframe or table type loads the data, hw.Pallet keeps the handle,
    no annotation loads a dataframe as it was written (other pallets stay
    handles)."""
    hints = _codecs.parameter_types(fn)
    out = dict(kwargs)
    for name, v in kwargs.items():
        if not isinstance(v, _model.Pallet):
            continue
        ann = hints.get(name)
        if isinstance(ann, type) and issubclass(ann, _model.Pallet):
            continue
        if _codecs.for_type(ann) is not None:
            out[name] = v.load(ann)
        elif ann in (None, Any) and _codecs.frame_tag(v.manifest().get("result")):
            out[name] = v.load()
    return out


def datasets() -> dict[str, _model.Keep | None]:
    """The datasets this build's lifts write, with their retention. Lifts
    writing one dataset that declare keep must agree; one without keep
    takes the others'."""
    out: dict[str, _model.Keep | None] = {}
    who: dict[str, str] = {}
    for name, d in sorted(_model.LIFTS.items()):
        if d.output is None:
            continue
        ds = d.output.name
        keep = d.output.keep
        if out.get(ds) is not None and keep is not None and out[ds] != keep:
            raise SystemExit(f"dataset {ds}: lifts {who[ds]} and {name} declare different keep "
                             f"({out[ds]} and {keep}); declare it once, the same everywhere")
        if out.get(ds) is None:  # a lift without keep defers to one with it
            out[ds], who[ds] = keep, name
    return out


def build_id() -> str:
    """A build is identified by its hauls' and lifts' code versions."""
    h = hashlib.sha256()
    for ds, keep in sorted(datasets().items()):
        h.update(f"dataset {ds} {keep!r}\n".encode())
    for name in sorted(_model.HAULS):
        d = _model.HAULS[name]
        h.update(f"haul {name} {d.code_version()} {d.schedule!r} {d.subscribe!r}\n".encode())
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
        self.exited = threading.Event()  # set once polling threads are done; ends heartbeats
        self._running: dict[str, threading.Event] = {}  # lift attempt → cancelled
        self._lock = threading.Lock()

    def register(self) -> None:
        versions = {name: d.code_version() for name, d in _model.LIFTS.items()}
        schedules = [{"haul": name, "cron": d.schedule.cron, "timezone": d.schedule.timezone,
                      "interval": d.schedule.interval, "catchUp": d.schedule.catch_up}
                     for name, d in sorted(_model.HAULS.items()) if d.schedule]
        subscriptions = []
        for name, d in sorted(_model.HAULS.items()):
            if s := d.subscribe:
                sub = {"haul": name, "datasets": list(s.datasets), "debounce": _replay._duration(s.debounce),
                       "maxBatch": s.max_batch}
                if s.max_wait is not None:
                    sub["maxWait"] = _replay._duration(s.max_wait)
                subscriptions.append(sub)
        retention = []
        for ds, keep in sorted(datasets().items()):
            r: dict[str, Any] = {"name": ds}
            if keep and keep.versions:
                r["keepVersions"] = keep.versions
            if keep and keep.age:
                r["keepFor"] = _replay._duration(keep.age)
            retention.append(r)
        self.client.call("WorkerService", "RegisterBuild",
                         {"buildId": self.build, "hauls": sorted(_model.HAULS), "liftVersions": versions,
                          "schedules": schedules, "subscriptions": subscriptions, "datasets": retention})
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
            if r.nondeterministic:
                # The server makes the haul stuck; a build with fixed code resumes it.
                body["error"] = {"reason": "nondeterminism", "message": r.mismatch}
            else:
                body["commands"] = r.commands
                if r.markers:
                    body["markers"] = r.markers
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
            kwargs = load_inputs(d.fn, _model.decode(json.loads(task.get("argsJson") or "{}"), pallets))
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
            if _codecs.for_value(result) is not None:
                if out is None:
                    if not task.get("staging"):
                        raise _model.NonRetryable("returning a dataframe needs a server with a bucket for pallets")
                    out = _output.Output(path=_output.local_dir(task["staging"]))
                result = _codecs.write(result, out.path)
            body["status"] = "STATUS_SUCCEEDED"
            body["resultJson"] = json.dumps(_model.encode(result))
            if out is not None:
                body["output"] = _output.describe(out)
        except Exception as e:  # noqa: BLE001 - the lift failed
            body.update(status="STATUS_FAILED", reason=type(e).__name__, message=str(e),
                        noRetry=isinstance(e, _model.NonRetryable))
            log.warning("lift %s failed: %s", task["name"], "".join(traceback.format_exception_only(e)).strip())
        try:
            self.deliver(task, body, cancelled)
        finally:
            with self._lock:
                self._running.pop(task["id"], None)

    def deliver(self, task: dict, body: dict, cancelled: threading.Event) -> None:
        """Report a lift's result, retrying while the server is unavailable.

        The attempt stays in the heartbeats meanwhile, so a leader that
        restarts keeps it alive and takes the result once it is back.
        """
        wait = DELIVER_BACKOFF[0]
        while not cancelled.is_set():  # cancelled: the server no longer wants this result
            try:
                self.client.call("WorkerService", "CompleteLiftTask", body)
                return
            except ConnectError as e:
                if e.code == "not_found":  # timed out or cancelled meanwhile
                    return
                if e.code != "unavailable":
                    raise
                log.warning("can't deliver the result of lift %s (%s); retrying in %.1fs", task["name"], e, wait)
            time.sleep(wait)
            wait = min(wait * 2, DELIVER_BACKOFF[1])

    # --- loops ---

    def _loop(self, method: str, run) -> None:
        while not self.stopping.is_set():
            try:
                res = self.client.call("WorkerService", method,
                                       {"workerId": self.id, "buildId": self.build, "docks": self.docks,
                                        "wait": self.poll_wait}, timeout=60)
                if res.get("task"):
                    run(res["task"])
            except ConnectError as e:
                log.warning("%s: %s; retrying", method, e)
                if e.code == "failed_precondition":  # the leader doesn't know our build (lost it, or a new one)
                    try:
                        self.register()
                    except ConnectError as e2:
                        log.warning("RegisterBuild: %s", e2)
                self.stopping.wait(1)
            except Exception:  # noqa: BLE001 - keep the worker alive
                log.exception("%s failed", method)
                self.stopping.wait(1)

    def _heartbeats(self) -> None:
        # Runs until the worker exits, not until it stops polling: lifts
        # still running or delivering need their leases.
        while not self.exited.wait(HEARTBEAT_EVERY):
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
        """Starts polling; returns the polling threads (heartbeats run until `exited`)."""
        threading.Thread(target=self._heartbeats, name="heartbeats", daemon=True).start()
        threads = [threading.Thread(target=self._loop, args=("PollHaulTask", self.run_haul_task), name="hauls", daemon=True)]
        threads += [threading.Thread(target=self._loop, args=("PollLiftTask", self.run_lift_task), name=f"lifts-{i}", daemon=True)
                    for i in range(self.lift_threads)]
        for t in threads:
            t.start()
        return threads

    def watch_supervisor(self, pid: int) -> None:
        """Stop, as on Ctrl-C, once the process that started us is gone."""
        def watch() -> None:
            while not self.stopping.wait(SUPERVISOR_CHECK_EVERY):
                if os.getppid() != pid:
                    log.warning("supervisor (pid %d) is gone; stopping", pid)
                    self.stopping.set()
        threading.Thread(target=watch, name="supervisor", daemon=True).start()


def project_modules(directory: Path) -> list[str]:
    """The modules listed under [tool.hubwagen] modules in directory/pyproject.toml."""
    path = directory / "pyproject.toml"
    hint = "pass them (python -m hubwagen.worker app.hauls) or list them in pyproject.toml:\n" \
           "  [tool.hubwagen]\n  modules = [\"app.hauls\"]"
    if not path.exists():
        raise SystemExit(f"no modules given and no {path}; {hint}")
    with path.open("rb") as f:
        try:
            conf = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise SystemExit(f"{path}: {e}") from None
    modules = conf.get("tool", {}).get("hubwagen", {}).get("modules")
    if modules is None:
        raise SystemExit(f"no modules given and none in {path}; {hint}")
    if not isinstance(modules, list) or not modules or not all(isinstance(m, str) and m for m in modules):
        raise SystemExit(f"{path}: [tool.hubwagen] modules must be a non-empty list of module names")
    return modules


def print_versions() -> None:
    """What each code version covers, for finding out why a lift reran."""
    from . import _codeversion

    out: dict[str, Any] = {"build": build_id(), "hauls": {}, "lifts": {}}
    for name, h in sorted(_model.HAULS.items()):
        out["hauls"][name] = {"version": h.code_version(), **_codeversion.parts(h.fn, name=name, haul=True)}
    for name, d in sorted(_model.LIFTS.items()):
        entry: dict[str, Any] = {"version": d.code_version()}
        if d.version:
            entry["pinned"] = True
        else:
            entry.update(_codeversion.parts(d.fn, d.deps, name))
        out["lifts"][name] = entry
    print(json.dumps(out, indent=2))


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    # Iterating a set of strings must give the same order in every replay, in
    # every worker process (spike S1): fix the hash seed, re-executing if needed.
    if os.environ.get("PYTHONHASHSEED") != "0":
        os.execve(sys.executable, [sys.executable, "-m", "hubwagen.worker", *argv], {**os.environ, "PYTHONHASHSEED": "0"})

    p = argparse.ArgumentParser(prog="python -m hubwagen.worker", description=__doc__.splitlines()[0])
    p.add_argument("modules", nargs="*",
                   help="modules that define hauls and lifts, e.g. app.hauls (default: [tool.hubwagen] modules in ./pyproject.toml)")
    p.add_argument("--server", default=os.environ.get("HUBWAGEN_SERVER", "http://127.0.0.1:7070"))
    p.add_argument("--dock", action="append", default=None, help="dock to poll (repeatable; default: default)")
    p.add_argument("--lifts", type=int, default=4, help="lifts to run at the same time")
    p.add_argument("--poll-wait", default=POLL_WAIT, help="how long one poll waits for work, e.g. 20s")
    p.add_argument("--worker-id", default=os.environ.get("HUBWAGEN_WORKER_ID"), help="default: <hostname>-<pid>")
    p.add_argument("--log-level", default="info")
    p.add_argument("--versions", action="store_true",
                   help="print the build ID and what each haul's and lift's code version covers, then exit")
    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    sys.path.insert(0, os.getcwd())
    for m in args.modules or project_modules(Path.cwd()):
        importlib.import_module(m)
    if args.versions:
        print_versions()
        return
    w = Worker(Client(args.server), args.dock or ["default"], lifts=args.lifts, poll_wait=args.poll_wait,
               worker_id=args.worker_id)
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
    if os.environ.get("HUBWAGEN_SUPERVISOR_PID"):
        w.watch_supervisor(int(os.environ["HUBWAGEN_SUPERVISOR_PID"]))
    log.info("worker %s polling %s on %s", w.id, ", ".join(w.docks), args.server)
    w.stopping.wait()
    log.info("stopping: waiting for running lifts")
    deadline = time.monotonic() + 60
    for t in threads:
        t.join(timeout=max(0, deadline - time.monotonic()))
    w.exited.set()


if __name__ == "__main__":
    main()

"""What a lift's code prints and logs, captured per lift attempt.

Lifts run on threads of one worker process, so capture goes by thread: while
a lift runs, its thread has a Capture, and sys.stdout, sys.stderr and the
logging module hand that thread's output to it. Output of other threads (the
worker's own, threads the lift starts) passes through as before; so does
output that doesn't go through Python (a subprocess, a C library writing to
the file descriptor), which isn't captured.

The worker sends a Capture's lines to the server as they come (LogService
AppendLogs), so they can be followed live. With echo (the default), captured
output still reaches the terminal, marked with the lift's name; `hubwagen
dev` turns echo off (HUBWAGEN_ECHO_LOGS=0) and prints the lines it receives.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
import threading
from typing import Callable, TextIO

ECHO = os.environ.get("HUBWAGEN_ECHO_LOGS", "1") != "0"

_local = threading.local()
_install_lock = threading.Lock()
_installed = False


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


class Capture:
    """One lift attempt's output: lines waiting to be sent."""

    def __init__(self, name: str, send: Callable[[list[dict]], None], echo: bool = ECHO):
        self.name = name
        self._send = send
        self._echo = echo
        self._lock = threading.Lock()
        self._lines: list[dict] = []
        self._partial = {"stdout": "", "stderr": ""}

    def write(self, stream: str, text: str) -> None:
        with self._lock:
            text = self._partial[stream] + text
            *lines, self._partial[stream] = text.split("\n")
            for line in lines:
                self._lines.append({"time": _now(), "stream": stream, "text": line})
        if self._echo:
            for line in lines:
                _real(stream).write(f"{self.name} │ {line}\n")

    def record(self, r: logging.LogRecord, text: str) -> None:
        with self._lock:
            self._lines.append({"time": _now(), "stream": "log", "level": r.levelname, "logger": r.name, "text": text})
        if self._echo:
            _real("stderr").write(f"{self.name} │ {r.levelname} {r.name}: {text}\n")

    def flush(self, final: bool = False) -> None:
        """Send what came since the last flush; with final, a partial last line too."""
        with self._lock:
            if final:
                for stream, rest in self._partial.items():
                    if rest:
                        self._lines.append({"time": _now(), "stream": stream, "text": rest})
                        self._partial[stream] = ""
            lines, self._lines = self._lines, []
        if lines:
            self._send(lines)

    def __enter__(self) -> "Capture":
        install()
        _local.capture = self
        return self

    def __exit__(self, *exc) -> None:
        _local.capture = None


def current() -> Capture | None:
    return getattr(_local, "capture", None)


def _real(stream: str) -> TextIO:
    """The terminal stream behind sys.stdout or sys.stderr."""
    cur = getattr(sys, stream)
    return cur._real if isinstance(cur, _Router) else cur


class _Router:
    """Stands in for sys.stdout or sys.stderr: a capturing thread's writes go to its Capture."""

    def __init__(self, stream: str, real: TextIO):
        self._stream = stream
        self._real = real

    def write(self, s: str) -> int:
        c = current()
        if c is None:
            return self._real.write(s)
        c.write(self._stream, s)
        return len(s)

    def flush(self) -> None:
        if current() is None:
            self._real.flush()

    def __getattr__(self, name):  # encoding, isatty, fileno, ...
        return getattr(self._real, name)


class _Handler(logging.Handler):
    """Hands log records of a capturing thread to its Capture."""

    def emit(self, r: logging.LogRecord) -> None:
        c = current()
        if c is None:
            return
        text = r.getMessage()
        if r.exc_info:
            text += "\n" + logging.Formatter().formatException(r.exc_info)
        c.record(r, text)


def _not_captured(r: logging.LogRecord) -> bool:
    # The worker's own log handlers skip what a Capture took: it reaches the
    # terminal through the Capture's echo, or through `hubwagen dev`.
    return current() is None


def install() -> None:
    """Route sys.stdout, sys.stderr and logging through captures. Called as
    each lift starts: whoever replaced sys.stdout since gets wrapped too."""
    global _installed
    with _install_lock:
        for stream in ("stdout", "stderr"):
            cur = getattr(sys, stream)
            if not isinstance(cur, _Router):
                setattr(sys, stream, _Router(stream, cur))
        if _installed:
            return
        root = logging.getLogger()
        for h in root.handlers:
            h.addFilter(_not_captured)
        root.addHandler(_Handler())
        _installed = True

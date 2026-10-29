"""Runtime guards: haul code can't read the clock or randomness; libraries
and lifts can. The cases follow spike S1's guards_check.py."""

from __future__ import annotations

import datetime
import email.utils
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime as dt_class
from random import randint
from time import time as time_fn

import pytest

import hubwagen as hw
from hubwagen import _guards, _replay
from test_replay import START

MISTAKES = {
    "time.time()": lambda: time.time(),
    "from time import time": lambda: time_fn(),
    "time.time_ns()": lambda: time.time_ns(),
    "datetime.now()": lambda: datetime.datetime.now(),
    "from datetime import datetime; now()": lambda: dt_class.now(),
    "datetime.utcnow()": lambda: dt_class.utcnow(),
    "date.today()": lambda: datetime.date.today(),
    "random.random()": lambda: random.random(),
    "from random import randint": lambda: randint(1, 6),
    "random.choice()": lambda: random.choice([1, 2]),
    "random.shuffle()": lambda: random.shuffle([1, 2]),
    "uuid.uuid4()": lambda: uuid.uuid4(),
    "os.urandom()": lambda: os.urandom(4),
}

FINE = {
    "logging reads the clock": lambda: logging.getLogger("hw-test").warning("hello"),
    "email.utils.formatdate": lambda: email.utils.formatdate(),
    "a seeded Random": lambda: random.Random(1).random(),
    "date arithmetic": lambda: datetime.date(2026, 10, 24) + datetime.timedelta(days=1),
    "uuid5": lambda: uuid.uuid5(uuid.NAMESPACE_DNS, "x"),
    "time.monotonic": lambda: time.monotonic(),
}


def run_guarded(fn):
    with _guards.guarded():
        return fn()


@pytest.mark.parametrize("case", MISTAKES)
def test_mistakes_are_caught(case):
    with pytest.raises(_guards.NondeterminismError, match=r"called at .*test_guards\.py:\d+"):
        run_guarded(MISTAKES[case])


@pytest.mark.parametrize("case", FINE)
def test_libraries_and_deterministic_code_run(case):
    run_guarded(FINE[case])


def test_outside_a_haul_nothing_is_guarded():
    time.time()
    random.random()


def test_other_threads_are_not_guarded():
    """Lifts run on other threads while a haul replays."""
    started, done = threading.Event(), threading.Event()
    errors = []

    def lift_thread():
        started.wait()
        try:
            time.time()
            random.random()
        except BaseException as e:  # noqa: BLE001
            errors.append(e)
        done.set()

    threading.Thread(target=lift_thread).start()
    with _guards.guarded():
        started.set()
        done.wait(5)
        with pytest.raises(_guards.NondeterminismError):
            time.time()
    assert errors == []


def test_the_error_names_the_line():
    def haul_code():
        x = 1
        return time.time() + x  # this line

    with pytest.raises(_guards.NondeterminismError) as e:
        run_guarded(haul_code)
    line = haul_code.__code__.co_firstlineno + 2
    assert f"test_guards.py:{line}" in str(e.value)
    assert "use ctx.now(), ctx.random() or a step" in str(e.value)


@hw.lift(name="guard_add")
def guard_add(a: int) -> int:
    return a + 1


def test_a_replay_that_reads_the_clock_is_nondeterministic():
    @hw.haul(name="guard_clock")
    def clock(ctx):
        try:
            stamp = time.time()  # `except Exception` can't swallow the guard
        except Exception:  # noqa: BLE001
            stamp = 0
        return ctx.lift(guard_add, int(stamp))

    r = _replay.replay(clock.__hubwagen_haul__, "h1", {}, [])
    assert r.nondeterministic
    assert r.outcome is None
    assert "time.time() called at" in r.mismatch


def test_ctx_now_and_random_are_fine():
    @hw.haul(name="guard_ctx")
    def ok(ctx):
        return [ctx.random(), str(ctx.now())]

    r = _replay.replay(ok.__hubwagen_haul__, "h1", {}, [START])
    assert not r.nondeterministic
    assert r.outcome["status"] == "STATUS_COMPLETED"

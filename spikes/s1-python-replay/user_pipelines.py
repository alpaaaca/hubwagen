"""Pipeline code as users write it: typical determinism mistakes, and runs that
only use libraries which read the clock or randomness internally (fine)."""

import datetime
import email.utils
import logging
import random
import tempfile
import time
import uuid
from datetime import date, datetime as dt
from random import randint
from time import time as now_ts

log = logging.getLogger("run")

# --- mistakes: each one must be caught --------------------------------------

MISTAKES = {
    "time.time()": lambda ctx: ctx.step("a", time.time()),
    "time.time_ns()": lambda ctx: ctx.step("a", time.time_ns()),
    "from time import time": lambda ctx: ctx.step("a", now_ts()),
    "datetime.datetime.now()": lambda ctx: ctx.step("a", str(datetime.datetime.now())),
    "from datetime import datetime; now()": lambda ctx: ctx.step("a", str(dt.now())),
    "datetime.utcnow()": lambda ctx: ctx.step("a", str(dt.utcnow())),
    "date.today()": lambda ctx: ctx.step("a", str(date.today())),
    "random.random()": lambda ctx: ctx.step("a", random.random()),
    "random.choice()": lambda ctx: ctx.step(random.choice(["a", "b"])),
    "from random import randint": lambda ctx: ctx.step("a", randint(1, 9)),
    "uuid.uuid4()": lambda ctx: ctx.step("a", str(uuid.uuid4())),
    "uuid.uuid1()": lambda ctx: ctx.step("a", str(uuid.uuid1())),
    "random.shuffle()": lambda ctx: ctx.step("a", random.shuffle([1, 2, 3])),
}

# --- fine: libraries that use the clock/randomness inside, and deterministic
# uses of the same modules ----------------------------------------------------


def uses_logging(ctx):
    log.warning("starting")  # LogRecord reads time.time_ns()
    return ctx.step("a")


def uses_email_date(ctx):
    return ctx.step("a", email.utils.formatdate(0))  # calls time.time() inside when ts is None; here not, but imports it


def uses_tempfile(ctx):
    with tempfile.TemporaryDirectory():  # random names from random.Random() inside
        pass
    return ctx.step("a")


def uses_seeded_random(ctx):
    rng = random.Random(42)  # a seeded instance: deterministic
    return ctx.step("a", rng.randint(1, 9))


def uses_datetime_math(ctx):
    d = date(2026, 10, 24) + datetime.timedelta(days=1)
    return ctx.step("a", d.isoformat(), str(uuid.uuid5(uuid.NAMESPACE_URL, "x")))


def uses_uuid1_in_library(ctx):
    email.utils.make_msgid()  # time.time() and random.getrandbits() inside the stdlib
    return ctx.step("a")


FINE = {
    "logging.warning()": uses_logging,
    "email.utils.formatdate(0)": uses_email_date,
    "tempfile.TemporaryDirectory()": uses_tempfile,
    "random.Random(42)": uses_seeded_random,
    "date arithmetic, uuid5": uses_datetime_math,
    "email.utils.make_msgid()": uses_uuid1_in_library,
}

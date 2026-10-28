"""M4's done-when: the architecture's daily haul (schedule, quality gate
with an approval signal, fan-out with a limit) on a real hubwagen dev, and
a replay of what it recorded against the code."""

import datetime as dt
import json
import subprocess
import sys

import hubwagen as hw

from conftest import SDK, wait


def test_daily_haul_end_to_end(client, server):
    scheds = {s["haul"]: s for s in client.call("HaulService", "ListSchedules", {})["schedules"]}
    assert scheds["m4_daily"]["interval"] == "daily"

    berlin = dt.timezone(dt.timedelta(hours=2))
    interval = hw.Interval(dt.datetime(2026, 10, 24, tzinfo=berlin), dt.datetime(2026, 10, 25, tzinfo=berlin))
    hid = client.start_haul("m4_daily", {"interval": interval})  # what the schedule starts at 03:00
    client.signal_haul(hid, "approve", {"by": "ana"})
    h = wait(client, hid, timeout=30)
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == {"day": "2026-10-24", "approved_by": "ana", "total": 2 + 2 + 4 + 5 + 4}

    replay = subprocess.run([sys.executable, "-m", "hubwagen.testing", "example_hauls", "--server", server,
                             "--replay", hid], cwd=SDK / "tests", capture_output=True, text=True,
                            env={"PYTHONPATH": f"{SDK / 'src'}:{SDK / 'tests'}", "PATH": ""})
    assert replay.returncode == 0, replay.stdout + replay.stderr
    assert replay.stdout == f"ok   {hid} m4_daily (completed)\n"

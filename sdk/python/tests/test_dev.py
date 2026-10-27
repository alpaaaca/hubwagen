"""The SDK against a real `hubwagen dev` and a real worker process."""

import json

import pytest

import hubwagen as hw

from conftest import wait


def lifts(h):
    return {l["name"]: l for l in h.get("lifts", [])}


def test_haul_with_lifts_and_a_durable_sleep(client):
    h = wait(client, client.start_haul("arithmetic", {"x": 1}))
    assert h["status"] == "completed", h
    result = json.loads(h["resultJson"])
    assert result["z"] == 12
    assert result["after_sleep"] > h["createdAt"][:19]
    again = wait(client, client.start_haul("arithmetic", {"x": 1}))
    assert json.loads(again["resultJson"])["draw"] != result["draw"], "ctx.random() is seeded per haul"


def test_memoized_rerun_reuses_pallets(client):
    first = wait(client, client.start_haul("daily", {"day": "2026-10-24"}))
    assert first["status"] == "completed", first
    r1 = json.loads(first["resultJson"])
    assert r1["reused"] == [False, False]
    assert r1["from"] == r1["orders"], "revenue got the orders pallet"
    assert lifts(first)["extract"]["runReason"] == "no previous run"

    second = wait(client, client.start_haul("daily", {"day": "2026-10-24"}))
    r2 = json.loads(second["resultJson"])
    assert r2["reused"] == [True, True]
    assert (r2["orders"], r2["revenue"]) == (r1["orders"], r1["revenue"])
    assert all(l["reused"] for l in second["lifts"])

    other_day = wait(client, client.start_haul("daily", {"day": "2026-10-25"}))
    assert json.loads(other_day["resultJson"])["reused"] == [False, False]


def test_cursor_from_the_catalog(client):
    cursors = [json.loads(wait(client, client.start_haul("ingest"))["resultJson"]) for _ in range(3)]
    assert cursors == [1, 2, 3]


def test_retries_and_failures(client, tmp_path):
    h = wait(client, client.start_haul("retried", {"marker": str(tmp_path / "marker")}))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == "second attempt"
    assert lifts(h)["flaky"]["attempts"] == 2

    h = wait(client, client.start_haul("handles_failure"))
    assert json.loads(h["resultJson"]) == "caught NonRetryable"
    assert lifts(h)["bad_input"]["attempts"] == 1, "no retry for NonRetryable"


def test_nondeterminism_makes_the_haul_stuck(client):
    h = wait(client, client.start_haul("nondeterministic"))
    assert h["status"] == "stuck", h
    assert h["reason"] == "nondeterminism"
    assert "replay issued lift \"other\" at " in h["message"]
    assert "example_hauls.py" in h["message"], "both call sites are named"


def test_unknown_haul_is_rejected(client):
    with pytest.raises(hw.ConnectError) as e:
        client.start_haul("no_such_haul")
    assert e.value.code == "failed_precondition", "no registered build defines it"


def test_lift_reads_its_input_pallet(client):
    h = wait(client, client.start_haul("reads_inputs", {"day": "2026-11-01"}))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == {"files": ["orders.csv"], "manifest_files": 1,
                                           "first": "order,day\n1,2026-11-01\n"}

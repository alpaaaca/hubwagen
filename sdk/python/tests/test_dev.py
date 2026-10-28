"""The SDK against a real `hubwagen dev` and a real worker process."""

import datetime as dt
import json
import time

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


def test_memoized_result_without_output(client):
    first = wait(client, client.start_haul("rates", {"day": "2026-10-24"}))
    assert first["status"] == "completed", first
    second = wait(client, client.start_haul("rates", {"day": "2026-10-24"}))
    assert json.loads(second["resultJson"]) == json.loads(first["resultJson"]), "the recorded result"
    assert lifts(second)["exchange_rate"]["reused"]
    assert not lifts(first)["exchange_rate"].get("reused")

    other_day = wait(client, client.start_haul("rates", {"day": "2026-10-25"}))
    assert not lifts(other_day)["exchange_rate"].get("reused")


def test_haul_code_reading_the_clock_is_stuck(client):
    h = wait(client, client.start_haul("reads_clock"))
    assert h["status"] == "stuck", h
    assert h["reason"] == "nondeterminism"
    assert "time.time() called at" in h["message"] and "example_hauls.py:" in h["message"]


def test_signals(client):
    h = client.start_haul("approval")
    sig = client.signal_haul(h, "approve", {"n": 41, "by": "ana"})
    assert sig
    done = wait(client, h)
    assert done["status"] == "completed", done
    assert json.loads(done["resultJson"]) == {"decision": {"n": 41, "by": "ana"}, "sum": 42}

    with pytest.raises(hw.ConnectError) as e:
        client.signal_haul(h, "approve", {})
    assert e.value.code == "failed_precondition", "the haul has finished"

    timed_out = wait(client, client.start_haul("approval", {"timeout": 0.5}))
    assert json.loads(timed_out["resultJson"]) == "timed out"


def test_gather_max_parallel(client, tmp_path):
    def most_at_once(limit: int) -> int:
        d = tmp_path / str(limit)
        d.mkdir()
        h = wait(client, client.start_haul("limited_fanout", {"marker_dir": str(d), "n": 6, "limit": limit}), timeout=30)
        assert h["status"] == "completed", h
        assert json.loads(h["resultJson"]) == list(range(6))
        spans = [tuple(map(float, p.read_text().split())) for p in d.glob("*.txt")]
        assert len(spans) == 6
        return max(sum(1 for s, e in spans if s <= t < e) for t, _ in spans)

    assert most_at_once(2) == 2
    assert most_at_once(6) > 2, "without a tight limit the worker's 4 lift threads run more at once"


def test_child_hauls(client):
    h = wait(client, client.start_haul("family", {"n": 41}), timeout=30)
    assert h["status"] == "completed", h
    r = json.loads(h["resultJson"])
    assert r["total"] == 42
    assert r["failed"] == ["failed", "ValueError", "no such day"]
    assert r["chain"] == "liftoff", "the result of the last haul in the chain"
    side = wait(client, r["side"])
    assert side["status"] == "completed" and side["parentId"] == h["id"]


def test_continue_as_new_chain(client):
    hid, chain = client.start_haul("countdown", {"n": 3}), []
    while True:
        h = wait(client, hid)
        chain.append(h)
        if h["status"] != "continued":
            break
        hid = h["continuedAs"]
    assert [json.loads(h["paramsJson"])["n"] for h in chain] == [3, 2, 1, 0]
    assert json.loads(chain[-1]["resultJson"]) == "liftoff"


def test_schedules_are_registered(client):
    got = client.call("HaulService", "ListSchedules", {})["schedules"]
    nightly = next(s for s in got if s["haul"] == "nightly")
    assert (nightly["cron"], nightly["timezone"], nightly["interval"], nightly["catchUp"]) == \
        ("0 3 * * *", "Europe/Berlin", "daily", "all")
    assert nightly["nextRun"] > nightly["cursor"]


def test_a_scheduled_haul_runs_with_an_interval(client):
    """What the server starts on schedule, started by hand with the same param."""
    params = {"interval": hw.Interval(dt.datetime(2026, 10, 24, tzinfo=dt.timezone.utc),
                                      dt.datetime(2026, 10, 25, tzinfo=dt.timezone.utc))}
    h = wait(client, client.start_haul("nightly", params))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == 24


def test_subscription_starts_a_haul_with_new_pallets(client):
    produced = [wait(client, client.start_haul("feed_producer", {"n": n})) for n in (1, 2)]
    pallets = [json.loads(h["resultJson"]) for h in produced]
    deadline = time.time() + 15
    while time.time() < deadline:
        got = json.loads(wait(client, client.start_haul("feed_latest"))["resultJson"])
        if got:
            break
        time.sleep(0.2)
    assert got == pallets, "one haul, started with both pallets after the debounce"
    subs = client.call("HaulService", "ListSchedules", {})["subscriptions"]
    feed = next(s for s in subs if s["haul"] == "feed_consumer")
    assert feed["datasets"] == ["feed.orders"] and not feed.get("waiting")


def test_retention_expires_old_versions(client):
    ids = [json.loads(wait(client, client.start_haul("keep_producer", {"n": n}))["resultJson"]) for n in (1, 2, 3)]
    datasets = {d["name"]: d for d in client.call("CatalogService", "ListDatasets", {})["datasets"]}
    assert datasets["keep.events"]["keepVersions"] == 1
    expired = client.call("CatalogService", "ExpirePallets", {})["expired"]
    assert sorted(e["pallet"]["id"] for e in expired if e["pallet"]["dataset"] == "keep.events") == sorted(ids[:2])
    latest = client.call("CatalogService", "GetPallet", {"id": ids[2]})
    assert latest["pallet"]["state"] == "committed"
    old = client.call("CatalogService", "GetPallet", {"id": ids[0]})
    assert old["pallet"]["state"] == "expired" and "manifest" not in old


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

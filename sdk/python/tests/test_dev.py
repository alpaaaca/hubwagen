"""The SDK against a real `moksha dev` and a real worker process."""

import datetime as dt
import json
import time

import pytest

import moksha as mk

from conftest import wait


def steps(h):
    return {l["name"]: l for l in h.get("steps", [])}


def test_run_with_steps_and_a_durable_sleep(client):
    h = wait(client, client.start_run("arithmetic", {"x": 1}))
    assert h["status"] == "completed", h
    result = json.loads(h["resultJson"])
    assert result["z"] == 12
    assert result["after_sleep"] > h["createdAt"][:19]
    again = wait(client, client.start_run("arithmetic", {"x": 1}))
    assert json.loads(again["resultJson"])["draw"] != result["draw"], "ctx.random() is seeded per run"


def test_memoized_rerun_reuses_data_versions(client):
    first = wait(client, client.start_run("daily", {"day": "2026-10-24"}))
    assert first["status"] == "completed", first
    r1 = json.loads(first["resultJson"])
    assert r1["reused"] == [False, False]
    assert r1["from"] == r1["orders"], "revenue got the orders data version"
    assert steps(first)["extract"]["runReason"] == "no previous run"

    second = wait(client, client.start_run("daily", {"day": "2026-10-24"}))
    r2 = json.loads(second["resultJson"])
    assert r2["reused"] == [True, True]
    assert (r2["orders"], r2["revenue"]) == (r1["orders"], r1["revenue"])
    assert all(l["reused"] for l in second["steps"])

    other_day = wait(client, client.start_run("daily", {"day": "2026-10-25"}))
    assert json.loads(other_day["resultJson"])["reused"] == [False, False]


def test_memoized_result_without_output(client):
    first = wait(client, client.start_run("rates", {"day": "2026-10-24"}))
    assert first["status"] == "completed", first
    second = wait(client, client.start_run("rates", {"day": "2026-10-24"}))
    assert json.loads(second["resultJson"]) == json.loads(first["resultJson"]), "the recorded result"
    assert steps(second)["exchange_rate"]["reused"]
    assert not steps(first)["exchange_rate"].get("reused")

    other_day = wait(client, client.start_run("rates", {"day": "2026-10-25"}))
    assert not steps(other_day)["exchange_rate"].get("reused")


def test_step_output_is_kept_with_the_step(client):
    def logs(h):
        res = client.call("LogService", "GetLogs", {"runId": h["id"]})
        return [(l["step"], l["line"]["stream"], l["line"].get("level", ""), l["line"]["text"]) for l in res.get("lines", [])]

    ok = wait(client, client.start_run("chatter", {"n": 3}))
    assert ok["status"] == "completed", ok
    assert logs(ok) == [
        ("chatty", "stdout", "", "loading 3 rows"),
        ("chatty", "log", "WARNING", "row 2 looks odd"),
        ("chatty", "stderr", "", "partial line"),
        ("chatty", "stdout", "", "no newline at the end"),
    ]

    failed = wait(client, client.start_run("chatter", {"n": -1}))
    assert failed["status"] == "failed", failed
    lines = logs(failed)
    assert lines[0] == ("chatty", "stdout", "", "loading -1 rows")
    trace = "\n".join(t for _, s, _, t in lines if s == "stderr")
    assert "Traceback" in trace and "ValueError: n must not be negative" in trace, "the failure is in the step's log"


def test_run_code_reading_the_clock_is_stuck(client):
    h = wait(client, client.start_run("reads_clock"))
    assert h["status"] == "stuck", h
    assert h["reason"] == "nondeterminism"
    assert "time.time() called at" in h["message"] and "example_pipelines.py:" in h["message"]


def test_signals(client):
    h = client.start_run("approval")
    sig = client.signal_run(h, "approve", {"n": 41, "by": "ana"})
    assert sig
    done = wait(client, h)
    assert done["status"] == "completed", done
    assert json.loads(done["resultJson"]) == {"decision": {"n": 41, "by": "ana"}, "sum": 42}

    with pytest.raises(mk.ConnectError) as e:
        client.signal_run(h, "approve", {})
    assert e.value.code == "failed_precondition", "the run has finished"

    timed_out = wait(client, client.start_run("approval", {"timeout": 0.5}))
    assert json.loads(timed_out["resultJson"]) == "timed out"


def test_gather_max_parallel(client, tmp_path):
    def most_at_once(limit: int) -> int:
        d = tmp_path / str(limit)
        d.mkdir()
        h = wait(client, client.start_run("limited_fanout", {"marker_dir": str(d), "n": 6, "limit": limit}), timeout=30)
        assert h["status"] == "completed", h
        assert json.loads(h["resultJson"]) == list(range(6))
        spans = [tuple(map(float, p.read_text().split())) for p in d.glob("*.txt")]
        assert len(spans) == 6
        return max(sum(1 for s, e in spans if s <= t < e) for t, _ in spans)

    assert most_at_once(2) == 2
    assert most_at_once(6) > 2, "without a tight limit the worker's 4 step threads run more at once"


def test_child_runs(client):
    h = wait(client, client.start_run("family", {"n": 41}), timeout=30)
    assert h["status"] == "completed", h
    r = json.loads(h["resultJson"])
    assert r["total"] == 42
    assert r["failed"] == ["failed", "ValueError", "no such day"]
    assert r["chain"] == "liftoff", "the result of the last run in the chain"
    side = wait(client, r["side"])
    assert side["status"] == "completed" and side["parentId"] == h["id"]


def test_continue_as_new_chain(client):
    hid, chain = client.start_run("countdown", {"n": 3}), []
    while True:
        h = wait(client, hid)
        chain.append(h)
        if h["status"] != "continued":
            break
        hid = h["continuedAs"]
    assert [json.loads(h["paramsJson"])["n"] for h in chain] == [3, 2, 1, 0]
    assert json.loads(chain[-1]["resultJson"]) == "liftoff"


def test_schedules_are_registered(client):
    got = client.call("RunService", "ListSchedules", {})["schedules"]
    nightly = next(s for s in got if s["pipeline"] == "nightly")
    assert (nightly["cron"], nightly["timezone"], nightly["interval"], nightly["catchUp"]) == \
        ("0 3 * * *", "Europe/Berlin", "daily", "all")
    assert nightly["nextRun"] > nightly["cursor"]


def test_a_scheduled_run_runs_with_an_interval(client):
    """What the server starts on schedule, started by hand with the same param."""
    params = {"interval": mk.Interval(dt.datetime(2026, 10, 24, tzinfo=dt.timezone.utc),
                                      dt.datetime(2026, 10, 25, tzinfo=dt.timezone.utc))}
    h = wait(client, client.start_run("nightly", params))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == 24


def test_subscription_starts_a_run_with_new_data_versions(client):
    produced = [wait(client, client.start_run("feed_producer", {"n": n})) for n in (1, 2)]
    data_versions = [json.loads(h["resultJson"]) for h in produced]
    deadline = time.time() + 15
    while time.time() < deadline:
        got = json.loads(wait(client, client.start_run("feed_latest"))["resultJson"])
        if got:
            break
        time.sleep(0.2)
    assert got == data_versions, "one run, started with both data versions after the debounce"
    subs = client.call("RunService", "ListSchedules", {})["subscriptions"]
    feed = next(s for s in subs if s["pipeline"] == "feed_consumer")
    assert feed["datasets"] == ["feed.orders"] and not feed.get("waiting")


def test_retention_expires_old_versions(client):
    ids = [json.loads(wait(client, client.start_run("keep_producer", {"n": n}))["resultJson"]) for n in (1, 2, 3)]
    datasets = {d["name"]: d for d in client.call("CatalogService", "ListDatasets", {})["datasets"]}
    assert datasets["keep.events"]["keepVersions"] == 1
    expired = client.call("CatalogService", "ExpireDataVersions", {})["expired"]
    assert sorted(e["dataVersion"]["id"] for e in expired if e["dataVersion"]["dataset"] == "keep.events") == sorted(ids[:2])
    latest = client.call("CatalogService", "GetDataVersion", {"id": ids[2]})
    assert latest["dataVersion"]["state"] == "committed"
    old = client.call("CatalogService", "GetDataVersion", {"id": ids[0]})
    assert old["dataVersion"]["state"] == "expired" and "manifest" not in old


def test_cursor_from_the_catalog(client):
    cursors = [json.loads(wait(client, client.start_run("ingest"))["resultJson"]) for _ in range(3)]
    assert cursors == [1, 2, 3]


def test_retries_and_failures(client, tmp_path):
    h = wait(client, client.start_run("retried", {"marker": str(tmp_path / "marker")}))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == "second attempt"
    assert steps(h)["flaky"]["attempts"] == 2

    h = wait(client, client.start_run("handles_failure"))
    assert json.loads(h["resultJson"]) == "caught NonRetryable"
    assert steps(h)["bad_input"]["attempts"] == 1, "no retry for NonRetryable"


def test_nondeterminism_makes_the_run_stuck(client):
    h = wait(client, client.start_run("nondeterministic"))
    assert h["status"] == "stuck", h
    assert h["reason"] == "nondeterminism"
    assert "replay issued step \"other\" at " in h["message"]
    assert "example_pipelines.py" in h["message"], "both call sites are named"


def test_unknown_run_is_rejected(client):
    with pytest.raises(mk.ConnectError) as e:
        client.start_run("no_such_run")
    assert e.value.code == "failed_precondition", "no registered build defines it"


def test_step_reads_its_input_data_version(client):
    h = wait(client, client.start_run("reads_inputs", {"day": "2026-11-01"}))
    assert h["status"] == "completed", h
    assert json.loads(h["resultJson"]) == {"files": ["orders.csv"], "manifest_files": 1,
                                           "first": "order,day\n1,2026-11-01\n"}

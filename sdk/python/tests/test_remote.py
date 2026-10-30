"""Workers that don't share the server's filesystem: the revenue example,
run by a worker that reaches data only through URLs the server hands out,
against a file:// bucket (the bytes go through the server) and an s3://
bucket (presigned URLs of the store; here s3fake, an S3-compatible server
in memory). Reruns reuse what didn't change, as on one machine."""

import re
import shutil
import signal
import subprocess
import urllib.request
from pathlib import Path

import pytest

from conftest import start_dev, start_s3fake, start_worker, stop
from test_example import EXAMPLE, normalize

from hubwagen import _data

FIRST = normalize("""
    2 runs started
    2 runs
      extract_orders   2 ran
      daily_revenue    2 ran (it hasn't run with these inputs before)
      revenue_report   2 ran (it hasn't run with these inputs before)
    2 completed
""")
AGAIN = normalize("""
    2 runs started
    2 runs
      extract_orders   2 ran
      daily_revenue    2 reused
      revenue_report   2 reused
    2 completed
""")


def replay(hubwagen_bin: str, url: str) -> str:
    r = subprocess.run([hubwagen_bin, "replay", "daily", "--server", url, "--param", "day=2026-10-01..2026-10-02"],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    return normalize(r.stdout)


def project(tmp_path: Path) -> Path:
    p = tmp_path / "elsewhere" / "revenue"
    shutil.copytree(EXAMPLE, p)
    return p


def test_file_bucket_through_the_server(hubwagen_bin, tmp_path):
    (tmp_path / "server").mkdir()
    url, dev = start_dev(hubwagen_bin, tmp_path / "server")
    log = tmp_path / "worker.log"
    worker = start_worker(url, None, project(tmp_path), log, "--data", "remote")
    try:
        assert replay(hubwagen_bin, url) == FIRST
        assert replay(hubwagen_bin, url) == AGAIN
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)
    out = log.read_text()
    assert "Traceback" not in out, out
    staged = list((tmp_path / "server" / "data" / "bucket" / "data").rglob("orders.csv"))
    assert len(staged) == 2 * 2, "the uploads landed in the server's bucket: extract ran twice for two days"


def test_s3_bucket_with_presigned_urls(hubwagen_bin, s3fake_bin, tmp_path):
    endpoint, s3 = start_s3fake(s3fake_bin, "hwb")
    server = tmp_path / "server"
    server.mkdir()
    (server / "key_id").write_text("test-key\n")
    (server / "secret").write_text("test-secret\n")
    config = server / "hubwagen.yaml"
    config.write_text(f"""
apiVersion: hubwagen.dev/v1alpha1
kind: Config
bucket:
  url: s3://hwb/prod
  endpoint: {endpoint}
  path_style: true
  access_key_id_file: key_id
  secret_access_key_file: secret
""")
    url, dev = start_dev(hubwagen_bin, server, config=config)
    log = tmp_path / "worker.log"
    worker = start_worker(url, None, project(tmp_path), log)  # --data auto: the bucket isn't a directory here
    try:
        assert replay(hubwagen_bin, url) == FIRST
        assert replay(hubwagen_bin, url) == AGAIN
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)
        listing = urllib.request.urlopen(urllib.request.Request(
            f"{endpoint}/hwb?list-type=2&prefix=prod/", headers={"Authorization": "AWS4-HMAC-SHA256 test"})).read().decode()
        stop(s3)
    keys = re.findall(r"<Key>([^<]+)</Key>", listing)
    assert len([k for k in keys if k.startswith("prod/data/") and k.endswith("/orders.csv")]) == 4
    assert len([k for k in keys if k.startswith("prod/pallets/default/mart.daily_revenue/")]) == 2
    assert not (server / "data" / "bucket").exists(), "nothing in a local bucket"
    assert "Traceback" not in log.read_text()


def test_shares_bucket(tmp_path):
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    task = {"bucket": bucket.as_uri(), "bucketId": "01ID"}
    assert not _data.shares_bucket(task), "no marker"
    (bucket / _data.MARKER).write_text("01OTHER")
    assert not _data.shares_bucket(task), "another server's bucket under the same path"
    (bucket / _data.MARKER).write_text("01ID")
    assert _data.shares_bucket(task)
    assert not _data.is_remote("auto", task)
    assert _data.is_remote("remote", task)
    s3 = {"bucket": "s3://hwb/prod", "bucketId": ""}
    assert _data.is_remote("auto", s3)
    assert not _data.is_remote("auto", {"staging": "file:///srv/bucket/data/x/"}), "an older server's tasks"
    with pytest.raises(RuntimeError, match="not a local directory"):
        _data.is_remote("local", s3)


def test_dataframes_through_the_server(hubwagen_bin, tmp_path):
    """Steps that return and load dataframes (Parquet files) on a remote
    worker."""
    pytest.importorskip("polars")
    pytest.importorskip("pandas")
    import json

    import hubwagen as hw
    from conftest import SDK, wait

    url, dev = start_dev(hubwagen_bin, tmp_path)
    worker = start_worker(url, "frame_hauls", SDK / "tests", tmp_path / "worker.log", "--data", "remote")
    try:
        client = hw.Client(url)
        h = wait(client, client.start_haul("frames", {"day": "2026-10-24"}))
        assert h["status"] == "completed", (h, (tmp_path / "worker.log").read_text())
        r = json.loads(h["resultJson"])
        assert r["report"] == {"totals": {"eu": 15, "us": 20}, "type": "DataFrame", "raw_files": ["data.parquet"]}
        assert r["reused"] == [False, False]
        again = json.loads(wait(client, client.start_haul("frames", {"day": "2026-10-24"}))["resultJson"])
        assert again["reused"] == [True, True]
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)

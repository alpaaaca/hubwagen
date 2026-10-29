"""Dataframes and tables as lift results and arguments."""

import hashlib
import json

import pytest

pd = pytest.importorskip("pandas")
pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

import hubwagen as hw  # noqa: E402
from hubwagen import _codecs, _model  # noqa: E402
from hubwagen.worker import load_inputs  # noqa: E402

from conftest import SDK, start_worker, stop, wait  # noqa: E402


def pallet_of(tmp_path, value, name="p1") -> hw.Pallet:
    """A pallet as the server would commit it after a lift returned value."""
    data = tmp_path / name / "data"
    result = _codecs.write(value, data)
    files = [{"path": p.name, "size": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
             for p in sorted(data.iterdir())]
    manifest = tmp_path / name / "manifest.json"
    manifest.write_text(json.dumps({"data": data.as_uri() + "/", "files": files, "result": result}))
    return hw.Pallet(id=name, location=manifest.as_uri())


def test_write_records_the_shape():
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    assert _codecs.for_value(df).name == "polars"
    assert _codecs.for_value(df.lazy()).name == "polars"
    assert _codecs.for_value(df.to_pandas()).name == "pandas"
    assert _codecs.for_value(df.to_arrow()).name == "arrow"
    assert _codecs.for_value({"a": 1}) is None


def test_round_trips_and_conversions(tmp_path):
    df = pl.DataFrame({"region": ["eu", "us"], "amount": [10, 20]})
    p = pallet_of(tmp_path, df)
    assert p.manifest()["result"] == {"$frame": "polars", "rows": 2, "columns": ["region", "amount"]}
    assert p.load().equals(df), "as written"
    assert p.load(pl.LazyFrame).collect().equals(df)
    assert p.load(pd.DataFrame).equals(df.to_pandas())
    assert p.load(pa.Table).equals(df.to_arrow())

    q = pallet_of(tmp_path, df.to_pandas(), "p2")
    assert isinstance(q.load(), pd.DataFrame)
    assert q.load(pl.DataFrame).equals(df)


def test_same_frame_same_bytes(tmp_path):
    """Content hashes, and so downstream reuse, rely on this."""
    df = pl.DataFrame({"a": list(range(100))})
    a, b = pallet_of(tmp_path, df, "a"), pallet_of(tmp_path, df, "b")
    assert a.manifest()["files"] == b.manifest()["files"]


def test_inputs_load_by_annotation(tmp_path):
    frame = pallet_of(tmp_path, pl.DataFrame({"a": [1]}))
    other = tmp_path / "other"
    other.mkdir()
    (other / "x.csv").write_text("a\n1\n")
    m = tmp_path / "other.json"
    m.write_text(json.dumps({"data": other.as_uri() + "/", "files": [{"path": "x.csv"}]}))
    files = hw.Pallet(id="o", location=m.as_uri())

    def fn(lazy: pl.LazyFrame, handle: hw.Pallet, plain, csv, n: int): ...

    got = load_inputs(fn, {"lazy": frame, "handle": frame, "plain": frame, "csv": files, "n": 1})
    assert isinstance(got["lazy"], pl.LazyFrame)
    assert got["handle"] is frame, "hw.Pallet keeps the handle"
    assert isinstance(got["plain"], pl.DataFrame), "no annotation: as written"
    assert got["csv"] is files, "a pallet without a frame stays a handle"
    assert got["n"] == 1


def test_frames_are_not_json():
    with pytest.raises(TypeError, match="return it from a step"):
        _model.encode({"df": pl.DataFrame({"a": [1]})})


def test_custom_codec(tmp_path):
    class Matrix(list):
        pass

    hw.register_codec("matrix", [Matrix],
                      write=lambda v, f: pq.write_table(pa.table({"v": list(v)}), f),
                      read=lambda f, cls: Matrix(pq.read_table(f)["v"].to_pylist()),
                      describe=lambda v: {"rows": len(v)})
    try:
        p = pallet_of(tmp_path, Matrix([1, 2, 3]))
        assert p.manifest()["result"] == {"$frame": "matrix", "rows": 3}
        assert p.load() == [1, 2, 3] and isinstance(p.load(), Matrix)
        assert p.load(pl.DataFrame)["v"].to_list() == [1, 2, 3], "Parquet, so other codecs read it"
    finally:
        _codecs._CODECS.pop(0)


@pytest.fixture(scope="module")
def frames_worker(server, tmp_path_factory):
    proc = start_worker(server, "frame_hauls", SDK / "tests", tmp_path_factory.mktemp("frames") / "worker.log")
    yield hw.Client(server)
    stop(proc)


def test_frames_through_a_haul(frames_worker):
    client = frames_worker
    h = wait(client, client.start_haul("frames", {"day": "2026-10-24"}))
    assert h["status"] == "completed", h
    r = json.loads(h["resultJson"])
    assert r["report"] == {"totals": {"eu": 15, "us": 20}, "type": "DataFrame", "raw_files": ["data.parquet"]}
    assert r["orders"] == {"$frame": "polars", "rows": 3, "columns": ["region", "amount", "day"]}
    assert r["dataset"] == "lift:frame_orders"
    assert r["reused"] == [False, False]
    assert all(l.get("pallet") for l in h["lifts"][:2]), "frames became pallets"

    again = json.loads(wait(client, client.start_haul("frames", {"day": "2026-10-24"}))["resultJson"])
    assert again["reused"] == [True, True]
    assert again["report"] == r["report"]

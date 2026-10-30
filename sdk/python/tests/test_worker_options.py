from hubwagen import worker


def test_docks_and_lifts_come_from_the_worker_group(monkeypatch):
    monkeypatch.delenv("HUBWAGEN_DOCKS", raising=False)
    monkeypatch.delenv("HUBWAGEN_LIFTS", raising=False)
    args = worker.parse_args([])
    assert (args.dock, args.lifts) == (["default"], 4)

    monkeypatch.setenv("HUBWAGEN_DOCKS", "gpu,default")
    monkeypatch.setenv("HUBWAGEN_LIFTS", "2")
    args = worker.parse_args([])
    assert (args.dock, args.lifts) == (["gpu", "default"], 2)

    args = worker.parse_args(["--dock", "cpu", "--lifts", "8"])
    assert (args.dock, args.lifts) == (["cpu"], 8), "flags win"

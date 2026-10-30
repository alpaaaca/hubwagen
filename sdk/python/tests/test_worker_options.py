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


def test_build_json_is_what_a_worker_registers():
    import hubwagen as hw

    @hw.lift(name="build_json_load")
    def load(x: int) -> int:
        return x

    @hw.haul(name="build_json_daily")
    def daily(ctx, x: int):
        return ctx.step(load, x)

    reg = worker.registration("b-test")
    assert reg["buildId"] == "b-test"
    assert "build_json_daily" in reg["hauls"]
    assert reg["liftVersions"]["build_json_load"].startswith("py-")
    assert {"schedules", "subscriptions", "datasets", "haulInfo"} <= set(reg)

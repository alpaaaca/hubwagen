"""Projects with different code on one server: builds register their hauls,
and workers get only what their code defines."""

import json
import signal
import time

import pytest

import hubwagen as hw
from conftest import SDK, start_dev, start_worker, stop, wait
from hubwagen.worker import project_modules

OTHER_PROJECT = '''
import hubwagen as hw


@hw.lift
def shout(word: str) -> str:
    return word.upper()


@hw.haul
def loud(ctx, word: str):
    return ctx.lift(shout, word)
'''


def test_two_projects_share_a_server(hubwagen_bin, tmp_path):
    other = tmp_path / "other"
    (other / "other").mkdir(parents=True)
    (other / "other" / "__init__.py").write_text("")
    (other / "other" / "hauls.py").write_text(OTHER_PROJECT)
    (other / "pyproject.toml").write_text('[tool.hubwagen]\nmodules = ["other.hauls"]\n')

    url, dev = start_dev(hubwagen_bin, tmp_path)
    workers = [start_worker(url, "example_hauls", SDK / "tests", tmp_path / "tests.log"),
               start_worker(url, None, other, tmp_path / "other.log")]
    try:
        client = hw.Client(url)
        ids = []
        for i in range(6):  # both workers poll the default dock; each must get only its own hauls
            ids.append(("arithmetic", client.start_haul("arithmetic", {"x": i})))
            ids.append(("loud", client.start_haul("loud", {"word": f"w{i}"})))
        for name, haul_id in ids:
            h = wait(client, haul_id)
            assert h["status"] == "completed", h
        assert json.loads(wait(client, ids[1][1])["resultJson"]) == "W0"

        with pytest.raises(hw.ConnectError) as e:
            client.start_haul("nowhere")
        assert e.value.code == "failed_precondition"
        assert 'no registered build defines haul "nowhere"' in str(e.value)
    finally:
        for w in workers:
            stop(w)
        stop(dev, signal.SIGINT)


def test_a_worker_registers_again_with_a_leader_that_lost_its_build(hubwagen_bin, tmp_path):
    (tmp_path / "first").mkdir()
    (tmp_path / "second").mkdir()
    url, dev = start_dev(hubwagen_bin, tmp_path / "first")
    log = tmp_path / "worker.log"
    worker = start_worker(url, "example_hauls", SDK / "tests", log)
    try:
        stop(dev, signal.SIGINT)
        _, dev = start_dev(hubwagen_bin, tmp_path / "second", url)  # empty state: no builds
        client = hw.Client(url)
        for _ in range(100):
            try:
                haul_id = client.start_haul("arithmetic", {"x": 1})
                break
            except hw.ConnectError as e:
                assert e.code == "failed_precondition", e
                time.sleep(0.1)
        else:
            raise AssertionError(f"the worker never registered again:\n{log.read_text()}")
        assert wait(client, haul_id)["status"] == "completed"
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)


def test_project_modules(tmp_path):
    with pytest.raises(SystemExit, match="no modules given and no .*pyproject.toml"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\n')
    with pytest.raises(SystemExit, match=r"(?s)none in .*\[tool.hubwagen\]"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[tool.hubwagen]\nmodules = "app.hauls"\n')
    with pytest.raises(SystemExit, match="must be a non-empty list"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[tool.hubwagen]\nmodules = ["app.hauls", "app.more"]\n')
    assert project_modules(tmp_path) == ["app.hauls", "app.more"]

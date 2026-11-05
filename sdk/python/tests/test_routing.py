"""Projects with different code on one server: builds register their runs,
and workers get only what their code defines."""

import json
import signal
import time

import pytest

import moksha as mk
from conftest import SDK, start_dev, start_worker, stop, wait
from moksha.worker import project_modules

OTHER_PROJECT = '''
import moksha as mk


@mk.step
def shout(word: str) -> str:
    return word.upper()


@mk.pipeline
def loud(ctx, word: str):
    return ctx.step(shout, word)
'''


def test_two_projects_share_a_server(moksha_bin, tmp_path):
    other = tmp_path / "other"
    (other / "other").mkdir(parents=True)
    (other / "other" / "__init__.py").write_text("")
    (other / "other" / "pipelines.py").write_text(OTHER_PROJECT)
    (other / "pyproject.toml").write_text('[tool.moksha]\nmodules = ["other.pipelines"]\n')

    url, dev = start_dev(moksha_bin, tmp_path)
    workers = [start_worker(url, "example_pipelines", SDK / "tests", tmp_path / "tests.log"),
               start_worker(url, None, other, tmp_path / "other.log")]
    try:
        client = mk.Client(url)
        ids = []
        for i in range(6):  # both workers poll the default queue; each must get only its own runs
            ids.append(("arithmetic", client.start_run("arithmetic", {"x": i})))
            ids.append(("loud", client.start_run("loud", {"word": f"w{i}"})))
        for name, run_id in ids:
            h = wait(client, run_id)
            assert h["status"] == "completed", h
        assert json.loads(wait(client, ids[1][1])["resultJson"]) == "W0"

        with pytest.raises(mk.ConnectError) as e:
            client.start_run("nowhere")
        assert e.value.code == "failed_precondition"
        assert 'no registered build defines run "nowhere"' in str(e.value)
    finally:
        for w in workers:
            stop(w)
        stop(dev, signal.SIGINT)


def test_a_worker_registers_again_with_a_leader_that_lost_its_build(moksha_bin, tmp_path):
    (tmp_path / "first").mkdir()
    (tmp_path / "second").mkdir()
    url, dev = start_dev(moksha_bin, tmp_path / "first")
    log = tmp_path / "worker.log"
    worker = start_worker(url, "example_pipelines", SDK / "tests", log)
    try:
        stop(dev, signal.SIGINT)
        _, dev = start_dev(moksha_bin, tmp_path / "second", url)  # empty state: no builds
        client = mk.Client(url)
        for _ in range(100):
            try:
                run_id = client.start_run("arithmetic", {"x": 1})
                break
            except mk.ConnectError as e:
                assert e.code == "failed_precondition", e
                time.sleep(0.1)
        else:
            raise AssertionError(f"the worker never registered again:\n{log.read_text()}")
        assert wait(client, run_id)["status"] == "completed"
    finally:
        stop(worker)
        stop(dev, signal.SIGINT)


def test_project_modules(tmp_path):
    with pytest.raises(SystemExit, match="no modules given and no .*pyproject.toml"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\n')
    with pytest.raises(SystemExit, match=r"(?s)none in .*\[tool.moksha\]"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[tool.moksha]\nmodules = "app.pipelines"\n')
    with pytest.raises(SystemExit, match="must be a non-empty list"):
        project_modules(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[tool.moksha]\nmodules = ["app.pipelines", "app.more"]\n')
    assert project_modules(tmp_path) == ["app.pipelines", "app.more"]

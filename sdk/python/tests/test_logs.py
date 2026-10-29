"""Capturing a lift's output per thread (hubwagen._logs)."""

import io
import logging
import sys
import threading

from hubwagen import _logs


def test_a_capture_takes_its_threads_output_only(monkeypatch, capsys):
    sent = []
    c = _logs.Capture("extract", sent.extend, echo=False)
    other = []

    def elsewhere():
        print("from another thread")
        other.append(True)

    with c:
        print("hello", 1)
        sys.stderr.write("half ")
        sys.stderr.write("done\nand more")
        logging.getLogger("app.extract").error("bad row %d", 7)
        try:
            raise KeyError("k")
        except KeyError:
            logging.getLogger("app.extract").exception("lookup failed")
        t = threading.Thread(target=elsewhere)
        t.start()
        t.join()
    print("after the lift")
    c.flush(final=True)

    got = [(l["stream"], l.get("level"), l["text"].splitlines()[0]) for l in sent]
    assert got == [
        ("stdout", None, "hello 1"),
        ("stderr", None, "half done"),
        ("log", "ERROR", "bad row 7"),
        ("log", "ERROR", "lookup failed"),
        ("stderr", None, "and more"),
    ]
    assert "KeyError: 'k'" in sent[3]["text"], "with the traceback"
    assert all(l["time"].endswith("Z") for l in sent)
    out = capsys.readouterr().out
    assert "from another thread" in out and "after the lift" in out
    assert "hello" not in out, "echo off: captured output stays out of the terminal"


def test_echo_marks_lines_with_the_lift(monkeypatch):
    fake = io.StringIO()
    monkeypatch.setattr(sys, "stdout", fake)
    c = _logs.Capture("report", lambda lines: None, echo=True)
    with c:
        print("written")
    assert fake.getvalue() == "report │ written\n"


def test_flush_sends_only_new_lines():
    batches = []
    c = _logs.Capture("x", batches.append, echo=False)
    with c:
        print("a")
        c.flush()
        c.flush()  # nothing new: nothing sent
        print("b")
    c.flush(final=True)
    assert [[l["text"] for l in b] for b in batches] == [["a"], ["b"]]

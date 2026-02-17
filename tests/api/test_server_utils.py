from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import novel_proofer.paths as paths
import novel_proofer.server as server


def test_safe_filename_and_derive_output_filename():
    assert paths._safe_filename("") == "input.txt"
    assert paths._safe_filename("..\\..\\x?.txt").endswith(".txt")
    assert "?" not in paths._safe_filename("a?b.txt")

    out = paths._derive_output_filename("demo.txt", "_rev")
    assert out.endswith("_rev.txt")

    out2 = paths._derive_output_filename("demo", "")
    assert out2.endswith("_rev.txt")


def test_decode_text_prefers_utf8_sig():
    assert paths._decode_text(b"\xef\xbb\xbfabc") == "abc"


def test_cleanup_job_dir_validation_and_removal(monkeypatch: pytest.MonkeyPatch):
    with tempfile.TemporaryDirectory() as td:
        jobs_dir = Path(td) / ".jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(paths, "JOBS_DIR", jobs_dir)

        with pytest.raises(ValueError):
            paths._cleanup_job_dir("not-a-job-id")

        job_id = "a" * 32
        target = jobs_dir / job_id
        assert paths._cleanup_job_dir(job_id) is False

        target.mkdir(parents=True, exist_ok=True)
        (target / "x.txt").write_text("x", encoding="utf-8")
        assert paths._cleanup_job_dir(job_id) is True
        assert not target.exists()


def test_server_main_parses_and_calls_uvicorn(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_run(app, *, host, port, log_level, reload):
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port
        captured["log_level"] = log_level
        captured["reload"] = reload

    monkeypatch.setattr(server.uvicorn, "run", fake_run)
    assert server.main(["--host", "127.0.0.1", "--port", "12345", "--log-level", "warning"]) == 0
    assert captured["app"] == "novel_proofer.api:app"
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 12345
    assert captured["log_level"] == "warning"
    assert captured["reload"] is False

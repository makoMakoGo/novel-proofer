from __future__ import annotations

import io

import pytest

import novel_proofer.server as server


def _multipart_body(boundary: str, fields: dict[str, str], *, filename: str, content: bytes) -> bytes:
    b = io.BytesIO()

    def w(s: str) -> None:
        b.write(s.encode("utf-8"))

    for k, v in fields.items():
        w(f"--{boundary}\r\n")
        w(f"Content-Disposition: form-data; name=\"{k}\"\r\n\r\n")
        w(v)
        w("\r\n")

    w(f"--{boundary}\r\n")
    w(f"Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
    w("Content-Type: text/plain\r\n\r\n")
    b.write(content)
    w("\r\n")
    w(f"--{boundary}--\r\n")

    return b.getvalue()


def test_safe_filename_and_derive_output_filename() -> None:
    assert server._safe_filename("") == "input.txt"
    assert server._safe_filename("..\\..\\x?.txt").endswith(".txt")
    assert "?" not in server._safe_filename("a?b.txt")

    out = server._derive_output_filename("demo.txt", "_rev")
    assert out.endswith("_rev.txt")

    out2 = server._derive_output_filename("demo", "")
    assert out2.endswith("_rev.txt")


def test_parse_helpers() -> None:
    assert server._parse_int(None, 7) == 7
    assert server._parse_int(" 9 ", 7) == 9
    assert server._parse_int("x", 7) == 7

    assert server._parse_float(None, 1.5) == 1.5
    assert server._parse_float(" 2.5 ", 1.5) == 2.5
    assert server._parse_float("x", 1.5) == 1.5

    assert server._parse_bool(None, True) is True
    assert server._parse_bool("1", False) is True
    assert server._parse_bool("0", True) is False
    assert server._parse_bool("unknown", True) is True


def test_parse_json_object() -> None:
    assert server._parse_json_object(None) is None
    assert server._parse_json_object({}) == {}
    assert server._parse_json_object("  ") is None
    assert server._parse_json_object("{\"a\":1}") == {"a": 1}

    with pytest.raises(ValueError):
        server._parse_json_object("not json")
    with pytest.raises(ValueError):
        server._parse_json_object("[]")
    with pytest.raises(ValueError):
        server._parse_json_object(123)


def test_parse_multipart_form_data_and_decode_text() -> None:
    boundary = "----npboundary"
    body = _multipart_body(boundary, {"a": "1"}, filename="demo.txt", content=b"\xef\xbb\xbfabc")
    fields, files = server._parse_multipart_form_data(f"multipart/form-data; boundary={boundary}", body)
    assert fields == {"a": "1"}
    assert len(files) == 1
    assert files[0].filename == "demo.txt"
    assert server._decode_text(files[0].content) == "abc"

    # Part without Content-Disposition is ignored.
    boundary2 = "----npboundary2"
    body2 = (
        f"--{boundary2}\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "junk\r\n"
        f"--{boundary2}\r\n"
        "Content-Disposition: form-data; name=\"a\"\r\n\r\n"
        "1\r\n"
        f"--{boundary2}--\r\n"
    ).encode("utf-8")
    fields3, files3 = server._parse_multipart_form_data(f"multipart/form-data; boundary={boundary2}", body2)
    assert fields3 == {"a": "1"}
    assert files3 == []

    fields2, files2 = server._parse_multipart_form_data("text/plain", b"x")
    assert fields2 == {}
    assert files2 == []

    # Decode fallback when all preferred encodings fail.
    assert "\ufffd" in server._decode_text(b"\x80")


def test_read_json_body_invalid_returns_empty_dict() -> None:
    class FakeHandler:
        def __init__(self, raw: bytes) -> None:
            self.headers = {"Content-Length": str(len(raw))}
            self.rfile = io.BytesIO(raw)

    assert server._read_json_body(FakeHandler(b"{")) == {}


def test_cleanup_job_dir_validation_and_removal(monkeypatch: pytest.MonkeyPatch) -> None:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        jobs_dir = Path(td) / ".jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(server, "JOBS_DIR", jobs_dir)

        with pytest.raises(ValueError):
            server._cleanup_job_dir("not-a-job-id")

        job_id = "a" * 32
        target = jobs_dir / job_id
        assert server._cleanup_job_dir(job_id) is False

        target.mkdir(parents=True, exist_ok=True)
        (target / "x.txt").write_text("x", encoding="utf-8")
        assert server._cleanup_job_dir(job_id) is True
        assert not target.exists()


def test_server_main_parsing_and_server_start(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeHTTPD:
        def __init__(self, addr, handler):  # noqa: ANN001
            captured["addr"] = addr
            captured["handler"] = handler

        def serve_forever(self) -> None:
            captured["served"] = True

    monkeypatch.setattr(server, "ThreadingHTTPServer", FakeHTTPD)
    monkeypatch.setattr("builtins.print", lambda *a, **k: None)

    assert server.main(["--host", "127.0.0.1", "--port", "12345"]) == 0
    assert captured.get("addr") == ("127.0.0.1", 12345)
    assert captured.get("served") is True

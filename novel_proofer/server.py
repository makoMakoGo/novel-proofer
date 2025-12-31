"""Novel TXT formatting/proofreading server (stdlib-only).

Run:
  .venv/Scripts/python -m novel_proofer.server
Then open:
  http://127.0.0.1:18080/

This intentionally avoids third-party dependencies so the project stays isolated
and can run even when pip/network is unavailable.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import threading
import urllib.parse
from dataclasses import dataclass
from email.parser import BytesParser
from email.policy import default
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.config import LLMConfig
from novel_proofer.runner import retry_failed_chunks, run_job

# Note: some editors may not resolve local imports; runtime is OK.


WORKDIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = WORKDIR / "templates"

OUTPUT_DIR = WORKDIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
JOBS_DIR = OUTPUT_DIR / ".jobs"
JOBS_DIR.mkdir(exist_ok=True)


def _read_template(name: str) -> str:
    path = TEMPLATES_DIR / name
    return path.read_text(encoding="utf-8")


_filename_strip_re = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff\u3400-\u4dbf\u3000-\u303f\uFF00-\uFFEF._ -]+")


def _safe_filename(name: str) -> str:
    base = os.path.basename(name or "")
    base = base.replace("\\", "_").replace("/", "_").strip()
    if not base:
        return "input.txt"
    base = _filename_strip_re.sub("_", base)
    return base[:200]


def _derive_output_filename(input_name: str, suffix: str) -> str:
    input_name = _safe_filename(input_name)
    suffix = (suffix or "").strip()
    if not suffix:
        suffix = "_rev"

    p = Path(input_name)
    stem = p.stem or "output"
    ext = p.suffix if p.suffix else ".txt"

    out = f"{stem}{suffix}{ext}"
    return _safe_filename(out)


@dataclass
class UploadedFile:
    filename: str
    content: bytes


def _parse_multipart_form_data(content_type: str, body: bytes) -> tuple[dict[str, str], list[UploadedFile]]:
    """Parse multipart/form-data using stdlib email parser.

    Returns (fields, files). Files are returned as raw bytes.
    """

    # Construct a pseudo email message with the right Content-Type.
    pseudo = (
        f"Content-Type: {content_type}\r\n"
        "MIME-Version: 1.0\r\n\r\n"
    ).encode("utf-8") + body

    msg = BytesParser(policy=default).parsebytes(pseudo)
    if not msg.is_multipart():
        return {}, []

    fields: dict[str, str] = {}
    files: list[UploadedFile] = []

    for part in msg.iter_parts():
        cd = part.get("Content-Disposition", "")
        if not cd:
            continue

        params = dict(part.get_params(header="content-disposition", failobj=[]))
        name = params.get("name")
        filename = params.get("filename")

        raw_payload = part.get_payload(decode=True)
        if raw_payload is None:
            payload = b""
        elif isinstance(raw_payload, bytes):
            payload = raw_payload
        elif isinstance(raw_payload, str):
            payload = raw_payload.encode("utf-8", errors="replace")
        else:
            # Some email policies may return a Message object.
            payload = str(raw_payload).encode("utf-8", errors="replace")

        if filename is not None:
            files.append(UploadedFile(filename=str(filename), content=payload))
        elif name is not None:
            # Best-effort decode; HTML form fields are typically utf-8.
            fields[str(name)] = payload.decode("utf-8", errors="replace")

    return fields, files


def _decode_text(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def _parse_int(value: str | None, default_value: int) -> int:
    if value is None:
        return default_value
    try:
        return int(str(value).strip())
    except Exception:
        return default_value


def _parse_float(value: str | None, default_value: float) -> float:
    if value is None:
        return default_value
    try:
        return float(str(value).strip())
    except Exception:
        return default_value


def _parse_bool(value: str | None, default_value: bool) -> bool:
    if value is None:
        return default_value
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return default_value


class Handler(BaseHTTPRequestHandler):
    server_version = "NovelProof/0.1"

    def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
        # 不记录高频轮询请求
        if "/api/jobs/status" in self.path:
            return
        super().log_request(code, size)

    def _send_html(self, html_text: str, status: int = 200) -> None:
        body = html_text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _content_disposition(self, filename: str) -> str:
        # BaseHTTPRequestHandler encodes headers as latin-1.
        # Use RFC 5987 for UTF-8 filenames and a safe ASCII fallback.
        safe_ascii = _safe_filename(filename)
        safe_ascii = safe_ascii.encode("ascii", errors="ignore").decode("ascii") or "output.txt"
        quoted = urllib.parse.quote(filename, safe="")
        return f"attachment; filename=\"{safe_ascii}\"; filename*=UTF-8''{quoted}"

    def _send_text_download(self, filename: str, content: str) -> None:
        data = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Disposition", self._content_disposition(filename))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, payload: dict, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "":
            tpl = _read_template("index.html")
            self._send_html(tpl)
            return

        if path == "/health":
            self._send_json({"ok": True})
            return

        if path == "/api/jobs/status":
            job_id = (qs.get("job_id") or [""])[0]
            st = GLOBAL_JOBS.get(job_id)
            if st is None:
                self._send_json({"error": "job not found"}, status=404)
                return
            pct = 0
            if st.total_chunks > 0:
                pct = int((st.done_chunks / st.total_chunks) * 100)
            self._send_json({
                "job_id": st.job_id,
                "state": st.state,
                "percent": pct,
                "created_at": st.created_at,
                "started_at": st.started_at,
                "finished_at": st.finished_at,
                "input_filename": st.input_filename,
                "output_filename": st.output_filename,
                "total_chunks": st.total_chunks,
                "done_chunks": st.done_chunks,
                "last_error_code": st.last_error_code,
                "last_retry_count": st.last_retry_count,
                "stats": st.stats,
                "error": st.error,
                "chunks": [
                    {
                        "index": c.index,
                        "state": c.state,
                        "started_at": c.started_at,
                        "finished_at": c.finished_at,
                        "retries": c.retries,
                        "splits": c.splits,
                        "last_error_code": c.last_error_code,
                        "last_error_message": c.last_error_message,
                    }
                    for c in st.chunk_statuses
                ],
            })
            return

        if path == "/api/jobs/download":
            self.send_error(HTTPStatus.GONE, "download endpoint disabled; check output/ folder")
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path not in {"/format", "/api/jobs/create", "/api/jobs/cancel", "/api/jobs/retry_failed"}:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        if path == "/api/jobs/cancel":
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b""
            try:
                payload = json.loads(body.decode("utf-8", errors="replace") or "{}")
            except Exception:
                payload = {}
            job_id = str(payload.get("job_id") or "")
            ok = GLOBAL_JOBS.cancel(job_id)
            self._send_json({"ok": ok})
            return

        if path == "/api/jobs/retry_failed":
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b""
            try:
                payload = json.loads(body.decode("utf-8", errors="replace") or "{}")
            except Exception:
                payload = {}

            job_id = str(payload.get("job_id") or "")
            st = GLOBAL_JOBS.get(job_id)
            if st is None:
                self._send_json({"error": "job not found"}, status=404)
                return
            if st.state == "running":
                self._send_json({"error": "job is running"}, status=409)
                return
            if st.state == "cancelled":
                self._send_json({"error": "job is cancelled"}, status=409)
                return

            llm = LLMConfig(
                enabled=True,
                provider=str(payload.get("llm_provider") or "openai_compatible").strip(),
                base_url=str(payload.get("llm_base_url") or "").strip(),
                api_key=str(payload.get("llm_api_key") or "").strip(),
                model=str(payload.get("llm_model") or "").strip(),
                temperature=_parse_float(payload.get("llm_temperature"), 0.0),
                timeout_seconds=_parse_float(payload.get("llm_timeout_seconds"), 180.0),
                max_concurrency=_parse_int(str(payload.get("llm_max_concurrency") or ""), 20),
            )

            t = threading.Thread(
                target=retry_failed_chunks,
                args=(job_id, llm),
                daemon=True,
            )
            t.start()
            self._send_json({"ok": True})
            return

        content_type = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            self.send_error(HTTPStatus.BAD_REQUEST, "Expected multipart/form-data")
            return

        max_bytes = 200 * 1024 * 1024
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            self.send_error(HTTPStatus.BAD_REQUEST, "Empty body")
            return
        if length > max_bytes:
            self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, f"File too large (> {max_bytes} bytes)")
            return

        body = self.rfile.read(length)
        fields, files = _parse_multipart_form_data(content_type, body)
        print(f"[DEBUG] fields={fields}", flush=True)
        if not files:
            self.send_error(HTTPStatus.BAD_REQUEST, "No file uploaded")
            return

        uploaded = files[0]
        input_text = _decode_text(uploaded.content)

        cfg = FormatConfig(
            max_chunk_chars=_parse_int(fields.get("max_chunk_chars"), 2_000),
            paragraph_indent=_parse_bool(fields.get("paragraph_indent"), False),
            indent_with_fullwidth_space=_parse_bool(fields.get("indent_with_fullwidth_space"), False),
            normalize_blank_lines=_parse_bool(fields.get("normalize_blank_lines"), False),
            trim_trailing_spaces=_parse_bool(fields.get("trim_trailing_spaces"), False),
            normalize_ellipsis=_parse_bool(fields.get("normalize_ellipsis"), False),
            normalize_em_dash=_parse_bool(fields.get("normalize_em_dash"), False),
            normalize_cjk_punctuation=_parse_bool(fields.get("normalize_cjk_punctuation"), False),
            fix_cjk_punct_spacing=_parse_bool(fields.get("fix_cjk_punct_spacing"), False),
            normalize_quotes=_parse_bool(fields.get("normalize_quotes"), False),
        )

        llm = LLMConfig(
            enabled=_parse_bool(fields.get("llm_enabled"), False),
            provider=(fields.get("llm_provider") or "openai_compatible").strip(),
            base_url=(fields.get("llm_base_url") or "").strip(),
            api_key=(fields.get("llm_api_key") or "").strip(),
            model=(fields.get("llm_model") or "").strip(),
            temperature=float(fields.get("llm_temperature") or 0.0),
            timeout_seconds=float(fields.get("llm_timeout_seconds") or 180.0),
            max_concurrency=int(fields.get("llm_max_concurrency") or 20),
        )

        out_name = _derive_output_filename(uploaded.filename, fields.get("suffix") or "_rev")

        if path == "/api/jobs/create":
            job = GLOBAL_JOBS.create(uploaded.filename, out_name, total_chunks=0)
            output_path = OUTPUT_DIR / out_name
            # Avoid accidental overwrite.
            if output_path.exists():
                output_path = OUTPUT_DIR / f"{job.job_id}_{out_name}"
            work_dir = JOBS_DIR / job.job_id
            GLOBAL_JOBS.update(job.job_id, output_path=str(output_path), work_dir=str(work_dir))

            t = threading.Thread(
                target=run_job,
                args=(job.job_id, input_text, cfg, llm),
                daemon=True,
            )
            t.start()

            self._send_json({
                "job_id": job.job_id,
                "output_filename": out_name,
                "output_path": str(output_path),
            })
            return

        # Legacy synchronous path (kept for compatibility)
        from novel_proofer.formatting.fixer import format_txt
        result = format_txt(input_text, cfg, llm=llm)

        if fields.get("return") == "stats":
            self._send_json({"output_filename": out_name, "stats": result.stats})
            return

        if fields.get("return") == "json":
            self._send_json({"output_filename": out_name, "stats": result.stats, "text": result.text})
            return

        self._send_text_download(out_name, result.text)



def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="novel_proofer.server", add_help=True)
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=18080, help="Bind port (default: 18080)")

    args = parser.parse_args(argv or sys.argv[1:])

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving on http://{args.host}:{args.port}/")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

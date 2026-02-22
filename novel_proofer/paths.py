from __future__ import annotations

import codecs
import itertools
import logging
import os
import re
import shutil
from pathlib import Path

from fastapi import HTTPException, UploadFile

logger = logging.getLogger(__name__)

WORKDIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = WORKDIR / "templates"
IMAGES_DIR = WORKDIR / "images"

OUTPUT_DIR = WORKDIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
JOBS_DIR = OUTPUT_DIR / ".jobs"
JOBS_DIR.mkdir(exist_ok=True)

_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")

_filename_strip_re = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff\u3400-\u4dbf\u3000-\u303f\uFF00-\uFFEF._ -]+")

MAX_UPLOAD_BYTES = 200 * 1024 * 1024

_tmp_seq = itertools.count()


def _tmp_suffix() -> str:
    return f".{os.getpid()}_{next(_tmp_seq)}.tmp"


def _validate_job_id(job_id: str) -> str:
    job_id = str(job_id or "").strip().lower()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")
    return job_id


def _job_id_dep(job_id: str) -> str:
    try:
        return _validate_job_id(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


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


def _decode_text(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def _rel_output_path(output_abs: Path) -> str:
    return f"output/{output_abs.name}"


def _rel_debug_dir(job_id: str) -> str:
    return f"output/.jobs/{job_id}/"


def _input_cache_root() -> Path:
    return OUTPUT_DIR / ".inputs"


def _input_cache_path(job_id: str) -> Path:
    job_id = _validate_job_id(job_id)
    return _input_cache_root() / f"{job_id}.txt"


def _write_input_cache(job_id: str, text: str) -> None:
    p = _input_cache_path(job_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _input_upload_tmp_path(job_id: str) -> Path:
    job_id = _validate_job_id(job_id)
    return _input_cache_root() / f"{job_id}.upload.tmp"


async def _save_upload_limited_to_file(upload: UploadFile, *, limit: int, dst: Path) -> int:
    total = 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as f:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise HTTPException(status_code=413, detail=f"file too large (> {limit} bytes)")
            f.write(chunk)
    return total


def _transcode_bytes_file_to_utf8_text(
    src: Path,
    dst: Path,
    *,
    encoding: str,
    errors: str,
) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + _tmp_suffix())
    decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
    with src.open("rb") as fin, tmp.open("w", encoding="utf-8") as fout:
        while True:
            b = fin.read(1024 * 1024)
            if not b:
                break
            fout.write(decoder.decode(b))
        fout.write(decoder.decode(b"", final=True))
    tmp.replace(dst)


async def _write_input_cache_from_upload(job_id: str, upload: UploadFile, *, limit: int) -> None:
    """Write decoded input cache (utf-8) without keeping the whole upload in memory."""

    tmp_upload = _input_upload_tmp_path(job_id)
    dst = _input_cache_path(job_id)
    try:
        await _save_upload_limited_to_file(upload, limit=limit, dst=tmp_upload)

        for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
            try:
                _transcode_bytes_file_to_utf8_text(tmp_upload, dst, encoding=enc, errors="strict")
                return
            except UnicodeDecodeError:
                continue

        _transcode_bytes_file_to_utf8_text(tmp_upload, dst, encoding="utf-8", errors="replace")
    finally:
        try:
            if tmp_upload.exists():
                tmp_upload.unlink()
        except Exception:
            logger.exception("failed to cleanup temp upload: %s", tmp_upload)


def _copy_input_cache(src_job_id: str, dst_job_id: str) -> None:
    src = _input_cache_path(src_job_id)
    if not src.exists():
        raise FileNotFoundError(str(src))
    dst = _input_cache_path(dst_job_id)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def _cleanup_input_cache(job_id: str) -> bool:
    """Delete output/.inputs/<job_id>.txt (best-effort, safe-guarded)."""

    job_id = _validate_job_id(job_id)

    root = _input_cache_root().resolve()
    target = (_input_cache_root() / f"{job_id}.txt").resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    target.unlink()
    return True


def _jobs_state_root() -> Path:
    return OUTPUT_DIR / ".state" / "jobs"


def _cleanup_job_state(job_id: str) -> bool:
    """Delete output/.state/jobs/<job_id>.json (best-effort, safe-guarded)."""

    job_id = _validate_job_id(job_id)

    root = _jobs_state_root().resolve()
    target = (root / f"{job_id}.json").resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    target.unlink()
    return True


def _cleanup_job_dir(job_id: str) -> bool:
    """Delete output/.jobs/<job_id>/ directory (best-effort, safe-guarded)."""

    job_id = _validate_job_id(job_id)

    root = JOBS_DIR.resolve()
    target = (JOBS_DIR / job_id).resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    shutil.rmtree(target)
    return True


def _count_non_whitespace_chars_from_utf8_file(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            n += sum(len(s) for s in chunk.split())
    return n


async def _read_upload_limited(upload: UploadFile, limit: int) -> bytes:
    total = 0
    parts: list[bytes] = []
    while True:
        chunk = await upload.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise HTTPException(status_code=413, detail=f"file too large (> {limit} bytes)")
        parts.append(chunk)
    return b"".join(parts)

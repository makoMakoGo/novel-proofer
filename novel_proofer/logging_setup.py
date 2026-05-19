from __future__ import annotations

import logging
import os
import re
from contextlib import suppress
from logging.handlers import RotatingFileHandler
from pathlib import Path

_file_handler_log_files: set[Path] = set()

_SENSITIVE_REPLACEMENTS = (
    (re.compile(r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(api[_-]?key[\"'\s:=]+)[^\"'\s,}]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(token[\"'\s:=]+)[^\"'\s,}]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(password[\"'\s:=]+)[^\"'\s,}]+"), r"\1[REDACTED]"),
)


def _truthy(s: str | None) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in {"1", "true", "yes", "y", "on"}


def _log_level_from_env() -> str | None:
    raw = str(os.getenv("NOVEL_PROOFER_LOG_LEVEL", "") or "").strip()
    return raw or None


def redact_log_text(value: object) -> str:
    text = str(value)
    for pattern, replacement in _SENSITIVE_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return text


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_log_text(super().format(record))


def ensure_file_logging(*, log_dir: Path, filename: str = "novel-proofer.log") -> Path:
    """Attach a rotating file handler to the root logger (idempotent).

    This works well with uvicorn's logging config (we just add another handler).
    """

    if _truthy(os.getenv("NOVEL_PROOFER_DISABLE_FILE_LOG")):
        return log_dir / filename

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / filename).resolve()

    if log_file in _file_handler_log_files:
        return log_file

    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, logging.FileHandler) and Path(h.baseFilename).resolve() == log_file:
            _file_handler_log_files.add(log_file)
            return log_file

    handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        RedactingFormatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
    _file_handler_log_files.add(log_file)

    lvl = _log_level_from_env()
    if lvl:
        with suppress(Exception):
            root.setLevel(lvl.upper())

    return log_file

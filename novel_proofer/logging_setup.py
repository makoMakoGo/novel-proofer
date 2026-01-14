from __future__ import annotations

import logging
import os
from contextlib import suppress
from logging.handlers import RotatingFileHandler
from pathlib import Path


def _truthy(s: str | None) -> bool:
    if s is None:
        return False
    return str(s).strip().lower() in {"1", "true", "yes", "y", "on"}


def _log_level_from_env() -> str | None:
    raw = str(os.getenv("NOVEL_PROOFER_LOG_LEVEL", "") or "").strip()
    return raw or None


def ensure_file_logging(*, log_dir: Path, filename: str = "novel-proofer.log") -> Path:
    """Attach a rotating file handler to the root logger (idempotent).

    This works well with uvicorn's logging config (we just add another handler).
    """

    if _truthy(os.getenv("NOVEL_PROOFER_DISABLE_FILE_LOG")):
        return log_dir / filename

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / filename).resolve()

    root = logging.getLogger()
    for h in root.handlers:
        if getattr(h, "_novel_proofer_file_log", False):
            base = getattr(h, "baseFilename", None)
            return Path(str(base)).resolve() if base else log_file
        base = getattr(h, "baseFilename", None)
        if base and Path(str(base)).resolve() == log_file:
            return log_file

    handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler._novel_proofer_file_log = True  # type: ignore[attr-defined]
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)

    lvl = _log_level_from_env()
    if lvl:
        with suppress(Exception):
            root.setLevel(lvl.upper())

    return log_file

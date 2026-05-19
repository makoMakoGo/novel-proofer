from __future__ import annotations

import logging

from novel_proofer.logging_setup import RedactingFormatter, redact_log_text


def test_redact_log_text_masks_common_secret_shapes() -> None:
    raw = "Authorization: Bearer abc.def token=secret password: hunter2 api_key='sk-test'"

    redacted = redact_log_text(raw)

    assert "abc.def" not in redacted
    assert "secret" not in redacted
    assert "hunter2" not in redacted
    assert "sk-test" not in redacted
    assert redacted.count("[REDACTED]") == 4


def test_redacting_formatter_masks_args_without_mutating_record() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="api_key=%s",
        args=("sk-secret",),
        exc_info=None,
    )

    rendered = RedactingFormatter("%(message)s").format(record)

    assert rendered == "api_key=[REDACTED]"
    assert record.msg == "api_key=%s"
    assert record.args == ("sk-secret",)

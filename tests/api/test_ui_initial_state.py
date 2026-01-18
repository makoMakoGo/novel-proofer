from __future__ import annotations

import re

from fastapi.testclient import TestClient

import novel_proofer.api as api


def test_index_validate_button_disabled_by_default():
    client = TestClient(api.app)
    r = client.get("/")
    assert r.status_code == 200, r.text

    html = r.text
    m = re.search(r"<button[^>]*\bid=[\"']btnValidate[\"'][^>]*>", html, flags=re.IGNORECASE)
    assert m, "missing btnValidate button"
    assert "disabled" in m.group(0).lower()

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo root (containing `novel_proofer/`) is importable when pytest
# picks `tests/` as the rootdir (e.g., single-file runs).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


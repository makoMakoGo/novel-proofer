"""Novel TXT formatting/proofreading server (FastAPI/Uvicorn).

Run:
  .venv\\Scripts\\python -m novel_proofer.server
Then open:
  http://127.0.0.1:18080/
"""

from __future__ import annotations

import argparse
import sys

import uvicorn


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="novel_proofer.server", add_help=True)
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=18080, help="Bind port (default: 18080)")
    parser.add_argument("--log-level", default="info", help="Uvicorn log level (default: info)")

    args = parser.parse_args(argv or sys.argv[1:])

    uvicorn.run(
        "novel_proofer.api:app",
        host=str(args.host),
        port=int(args.port),
        log_level=str(args.log_level),
        reload=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


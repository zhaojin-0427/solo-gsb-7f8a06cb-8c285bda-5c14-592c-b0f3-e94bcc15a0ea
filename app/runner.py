"""Entrypoint dispatcher: python -m app.runner control|printer|worker."""
from __future__ import annotations

import asyncio
import sys

import uvicorn

from .worker import run_worker


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"control", "printer", "worker"}:
        print("usage: python -m app.runner control|printer|worker", file=sys.stderr)
        raise SystemExit(2)

    role = sys.argv[1]
    if role == "control":
        uvicorn.run("app.control:app", host="0.0.0.0", port=8000)
    elif role == "printer":
        uvicorn.run("app.printer:app", host="0.0.0.0", port=8001)
    else:
        asyncio.run(run_worker())


if __name__ == "__main__":
    main()

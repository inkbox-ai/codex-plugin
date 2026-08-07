#!/usr/bin/env python3
"""Print bounded, content-free state from a live validation log."""

from __future__ import annotations

import argparse
from pathlib import Path

SIGNALS = {
    "gateway": {
        "tunnel_ready": "tunnel ready",
        "phone_ready": "[bridge] phone",
        "session_started": "Codex session started",
        "session_finished": "Codex session finished",
        "hosted_completed": "hosted post-call reconciliation completed",
    },
    "driver": {
        "media_accepted": "call WS accepted",
        "call_started": "call start:",
        "caller_spoke": "driver spoke:",
        "agent_heard": "driver heard (final):",
        "call_stopped": "sent stop",
    },
    "mock": {
        "server_started": "Uvicorn running",
        "request_received": "POST /v1/responses",
    },
}


def summarize(path: Path, kind: str) -> dict[str, int | bool]:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        text = ""
    lowered = text.casefold()
    lines = text.splitlines()
    summary: dict[str, int | bool] = {
        "present": bool(text),
        "line_count": len(lines),
        "error_lines": sum("error" in line.casefold() for line in lines),
        "warning_lines": sum("warning" in line.casefold() for line in lines),
    }
    for name, needle in SIGNALS[kind].items():
        summary[name] = needle.casefold() in lowered
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=tuple(SIGNALS))
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    print(f"{args.kind} failure state: {summarize(args.path, args.kind)!r}")


if __name__ == "__main__":
    main()

"""Minimal stdio MCP server for the Inkbox tool surface."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, Dict

try:
    from inkbox import Inkbox
except ImportError:  # pragma: no cover - surfaced through initialize/tools call
    Inkbox = None  # type: ignore

try:
    from .tools import call_inkbox_tool, mcp_tool_list
except ImportError:  # pragma: no cover - direct local import/test fallback
    from tools import call_inkbox_tool, mcp_tool_list


def _response(message_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error(message_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


class InkboxMcpServer:
    def __init__(self) -> None:
        self.api_key = os.getenv("INKBOX_API_KEY", "")
        self.identity = os.getenv("INKBOX_IDENTITY", "")
        self.base_url = os.getenv("INKBOX_BASE_URL") or "https://inkbox.ai"
        self._client: Any = None

    def _inkbox(self) -> Any:
        if Inkbox is None:
            raise RuntimeError("inkbox SDK is not installed")
        if not self.api_key or not self.identity:
            raise RuntimeError("INKBOX_API_KEY and INKBOX_IDENTITY are required")
        if self._client is None:
            self._client = Inkbox(api_key=self.api_key, base_url=self.base_url)
        return self._client

    async def handle(self, message: Dict[str, Any]) -> Dict[str, Any] | None:
        method = message.get("method")
        message_id = message.get("id")
        params = message.get("params") or {}

        if method == "initialize":
            return _response(
                message_id,
                {
                    "protocolVersion": params.get("protocolVersion") or "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "inkbox-codex",
                        "version": "0.1.0",
                    },
                },
            )

        if method == "notifications/initialized":
            return None

        if method == "ping":
            return _response(message_id, {})

        if method == "tools/list":
            return _response(message_id, {"tools": mcp_tool_list()})

        if method == "tools/call":
            name = str(params.get("name") or "")
            args = params.get("arguments") or {}
            result = await call_inkbox_tool(self._inkbox(), self.identity, name, args)
            return _response(message_id, result)

        return _error(message_id, -32601, f"method not found: {method}")


async def amain() -> int:
    server = InkboxMcpServer()
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return 0
        try:
            message = json.loads(line)
            response = await server.handle(message)
        except Exception as exc:
            message_id = None
            if "message" in locals() and isinstance(message, dict):
                message_id = message.get("id")
            response = _error(message_id, -32000, str(exc))
        if response is not None:
            sys.stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            sys.stdout.flush()


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())

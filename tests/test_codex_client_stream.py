import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from inkbox_codex.codex_client import CodexAppServerError, CodexAppServerClient
from inkbox_codex.config import BridgeConfig


MOCK_APP_SERVER = r'''#!/usr/bin/env python3
import json
import sys


def send(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


for raw in sys.stdin:
    message = json.loads(raw)
    method = message.get("method")
    message_id = message.get("id")
    if method == "initialize":
        send({"id": message_id, "result": {}})
    elif method == "thread/start":
        send({"id": message_id, "result": {"thread": {"id": "thread-1"}}})
    elif method == "turn/start":
        send({"id": message_id, "result": {"turn": {"id": "turn-1"}}})
        reply = "reply:" + ("x" * 70000)
        send({
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "item": {"type": "agentMessage", "text": reply},
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "status": "completed",
                    "items": [{"type": "commandExecution", "output": "y" * 70000}],
                },
            },
        })
'''


def _mock_app_server(tmp_path: Path) -> Path:
    executable = tmp_path / "mock-codex"
    executable.write_text(MOCK_APP_SERVER)
    executable.chmod(executable.stat().st_mode | 0o111)
    return executable


def _client(codex_bin: Path, *, stream_limit: int) -> CodexAppServerClient:
    return CodexAppServerClient(
        BridgeConfig(
            codex_bin=os.fspath(codex_bin),
            codex_app_server_stream_limit_bytes=stream_limit,
        ),
        developer_instructions="test",
    )


def test_large_item_and_turn_notifications_complete(tmp_path):
    async def scenario():
        client = _client(_mock_app_server(tmp_path), stream_limit=16 * 1024 * 1024)
        try:
            await client.connect()
            result = await asyncio.wait_for(client.run_detailed("test"), timeout=2)
            assert result.text.startswith("reply:")
            assert len(result.text) > 65536
            assert client.is_healthy is True
        finally:
            await client.disconnect()

    asyncio.run(scenario())


def test_reader_limit_failure_fails_turn_and_marks_client_unhealthy(tmp_path):
    async def scenario():
        client = _client(_mock_app_server(tmp_path), stream_limit=1024)
        try:
            await client.connect()
            with pytest.raises(CodexAppServerError, match="reader"):
                await asyncio.wait_for(client.run_detailed("test"), timeout=2)
            assert client.is_healthy is False
        finally:
            await client.disconnect()

    asyncio.run(scenario())


def test_notifications_before_turn_start_response_are_replayed():
    async def scenario():
        client = CodexAppServerClient(
            BridgeConfig(),
            developer_instructions="test",
        )
        reader_blocker = asyncio.create_task(asyncio.Event().wait())
        client.thread_id = "thread-1"
        client._proc = SimpleNamespace(returncode=None)
        client._reader_task = reader_blocker

        async def request(method, _params):
            assert method == "turn/start"
            client._handle_notification({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"type": "agentMessage", "text": "early reply"},
                },
            })
            client._handle_notification({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "completed"},
                },
            })
            return {"turn": {"id": "turn-1"}}

        client._request = request
        try:
            result = await asyncio.wait_for(client.run_detailed("test"), timeout=1)
            assert result.text == "early reply"
        finally:
            reader_blocker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reader_blocker

    asyncio.run(scenario())

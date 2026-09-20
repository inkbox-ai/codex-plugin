"""Large host responses and transport failures must never strand requests."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from inkbox_codex.config import BridgeConfig


def test_large_response_and_following_response_are_read_separately():
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions="test")
        reader = asyncio.StreamReader()
        client._proc = SimpleNamespace(stdout=reader)
        first = asyncio.get_running_loop().create_future()
        second = asyncio.get_running_loop().create_future()
        client._pending = {1: first, 2: second}
        payload = {"history": "x" * 200000}
        data = (json.dumps({"id": 1, "result": payload}) + "\n" +
                json.dumps({"id": 2, "result": {"ok": True}}) + "\n").encode()
        task = asyncio.create_task(client._reader_loop())
        # Simulate several pipe reads, including a JSON line over 64 KiB.
        for start in range(0, len(data), 16000):
            reader.feed_data(data[start:start + 16000])
            await asyncio.sleep(0)
        reader.feed_eof()
        await asyncio.wait_for(task, timeout=1)
        assert await first == payload
        assert await second == {"ok": True}

    asyncio.run(scenario())


def test_reader_failure_rejects_pending_requests():
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions="test")
        reader = asyncio.StreamReader()
        client._proc = SimpleNamespace(stdout=reader)
        pending = asyncio.get_running_loop().create_future()
        client._pending[1] = pending
        reader.set_exception(OSError("pipe closed"))
        await client._reader_loop()
        with pytest.raises(CodexAppServerError, match="output reader failed"):
            await asyncio.wait_for(pending, timeout=1)

    asyncio.run(scenario())


def test_submission_checkpoint_and_activity_observer_share_a_turn():
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions="test")
        client.thread_id = "thread-1"
        client._request = AsyncMock(return_value={"turn": {"id": "turn-1"}})
        observed = []
        submitted = asyncio.Event()

        def checkpoint(thread_id, turn_id):
            observed.append(("submitted", thread_id, turn_id))
            submitted.set()

        task = asyncio.create_task(client.run_detailed(
            "One complete input",
            on_submitted=checkpoint,
            activity_handler=lambda *args: observed.append(("activity", *args)),
        ))
        await asyncio.wait_for(submitted.wait(), 1)
        client._handle_notification({
            "method": "item/started",
            "params": {"turnId": "turn-1", "item": {"type": "commandExecution"}},
        })
        client._handle_notification({
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        })
        assert not (await task).aborted
        assert observed == [
            ("submitted", "thread-1", "turn-1"), ("activity", "commandExecution", ""),
        ]
        assert client._request.await_count == 1
        assert client._request.call_args.args[0] == "turn/start"
        assert client._request.call_args.args[1]["input"] == [
            {"type": "text", "text": "One complete input"},
        ]

    asyncio.run(scenario())

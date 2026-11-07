import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from inkbox_codex.config import BridgeConfig
from inkbox_codex.managed_conversations import page, read


def test_history_uses_existing_owner_and_never_submits_a_turn():
    async def scenario():
        client = SimpleNamespace(
            _request=AsyncMock(
                return_value={
                    "thread": {
                        "id": "native",
                        "turns": [
                            {
                                "id": "turn",
                                "items": [
                                    {
                                        "id": "one",
                                        "type": "userMessage",
                                        "content": [{"type": "text", "text": "Hello"}],
                                    },
                                    {
                                        "id": "secret",
                                        "type": "commandExecution",
                                        "command": "private tool input",
                                    },
                                    {"id": "two", "type": "agentMessage", "text": "Hi"},
                                ],
                            }
                        ],
                    }
                }
            )
        )
        owner = SimpleNamespace(_ensure_client=AsyncMock(return_value=client))
        manager = SimpleNamespace(
            _session_ids={"contact": "native"},
            get=lambda key: owner if key == "contact" else None,
        )
        result = await read(
            manager, {"action": "conversation", "session_id": "native", "before": None}
        )
        assert [message["text"] for message in result["messages"]] == ["Hello", "Hi"]
        client._request.assert_awaited_once_with(
            "thread/read", {"threadId": "native", "includeTurns": True}
        )
        with pytest.raises(ValueError):
            await read(
                manager,
                {"action": "conversation", "session_id": "unowned", "before": None},
            )
        assert owner._ensure_client.await_count == 1

    asyncio.run(scenario())


def test_history_pages_are_bounded_and_cursor_is_checked():
    thread = {
        "turns": [
            {
                "id": "turn",
                "items": [
                    {"id": str(i), "type": "agentMessage", "text": "x" * 12000}
                    for i in range(100)
                ],
            }
        ]
    }
    latest = page(thread, None)
    assert 0 < len(latest["messages"]) < 32
    assert latest["messages"][-1]["id"] == "99"
    assert all(message["truncated"] for message in latest["messages"])
    older = page(thread, latest["next_before"])
    assert int(older["messages"][-1]["id"]) < int(latest["messages"][0]["id"])
    with pytest.raises(ValueError):
        page(thread, "missing")


def test_native_owner_locks_exclude_second_process_and_release_after_disconnect(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("INKBOX_SESSION_LOCK_DIRECTORY", str(tmp_path))

    async def scenario():
        first = CodexAppServerClient(BridgeConfig(), developer_instructions="")
        second = CodexAppServerClient(BridgeConfig(), developer_instructions="")
        first._lock_session("native")
        with pytest.raises(CodexAppServerError, match="already active"):
            second._lock_session("native")
        await first.disconnect()
        second._lock_session("native")
        await second.disconnect()

    asyncio.run(scenario())


def test_concurrent_history_and_turn_connection_have_one_owner(monkeypatch):
    from tests.test_sessions import make_session

    async def scenario():
        session = make_session([])
        entered, release = asyncio.Event(), asyncio.Event()
        calls = 0

        async def connect():
            nonlocal calls
            if session._client is None:
                calls += 1
                entered.set()
                await release.wait()
                session._client = object()
            return session._client

        monkeypatch.setattr(session, "_connect_client", connect)
        first = asyncio.create_task(session._ensure_client())
        await entered.wait()
        second = asyncio.create_task(session._ensure_client())
        await asyncio.sleep(0)
        release.set()
        assert (await first) is (await second)
        assert calls == 1

    asyncio.run(scenario())


def test_observation_pages_import_oldest_first_without_repeating_boundary():
    from inkbox_codex.managed_conversations import page

    thread = {
        "turns": [
            {
                "id": "turn",
                "items": [
                    {"id": str(i), "type": "agentMessage", "text": f"Message {i}"}
                    for i in range(70)
                ],
            }
        ]
    }
    first = page(thread, None, forward=True)
    second = page(thread, first["next_before"], forward=True)
    third = page(thread, second["next_before"], forward=True)
    assert [m["id"] for p in (first, second, third) for m in p["messages"]] == [
        str(i) for i in range(70)
    ]
    assert third["next_before"] is None


def test_observer_never_starts_historical_contacts_and_reuses_checkpoint(monkeypatch):
    from types import SimpleNamespace

    from inkbox_codex import observations

    class Store:
        def __init__(self, _):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def list_jobs(self, **kwargs):
            return []

    class Client:
        is_alive = True
        observation_id = "owner"
        _observation_version = 1
        calls = 0

        async def _request(self, method, params):
            self.calls += 1
            return {
                "thread": {
                    "id": "live",
                    "turns": [
                        {
                            "items": [
                                {"id": str(i), "type": "agentMessage", "text": str(i)}
                                for i in range(self._observation_version + 1)
                            ]
                        }
                    ],
                }
            }

    client = Client()
    manager = SimpleNamespace(
        _session_ids={"old": "old", "live": "live"},
        sessions={"live": SimpleNamespace(_client=client)},
    )
    gateway = SimpleNamespace(sessions=manager, cfg=None)
    monkeypatch.setattr(observations, "ScheduleStore", Store)
    monkeypatch.setattr(observations, "schedule_path", lambda _: None)

    async def scenario():
        command = {
            "action": "observe_conversation",
            "after": None,
            "source_id": None,
            "before": None,
        }
        selected = await observations.observe(gateway, command)
        assert selected == {"source_id": "contact:live", "selected": True}
        command["source_id"] = "contact:live"
        first = await observations.observe(gateway, command)
        command["before"] = first["checkpoint"]
        assert (await observations.observe(gateway, command))["unchanged"]
        assert client.calls == 1
        client._observation_version = 2
        changed = await observations.observe(gateway, command)
        assert [e["source_id"] for e in changed["events"]] == ["1", "2"]
        assert client.calls == 2

    asyncio.run(scenario())

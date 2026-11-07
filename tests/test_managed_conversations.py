import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from inkbox_codex.config import BridgeConfig
from inkbox_codex.managed_conversations import page, read


def test_history_uses_existing_owner_and_never_submits_a_turn():
    async def scenario():
        client = SimpleNamespace(_request=AsyncMock(return_value={'thread': {'id': 'native', 'turns': [
            {'id': 'turn', 'items': [{'id': 'one', 'type': 'userMessage', 'content': [{'type': 'text', 'text': 'Hello'}]},
                                   {'id': 'secret', 'type': 'commandExecution', 'command': 'private tool input'},
                                   {'id': 'two', 'type': 'agentMessage', 'text': 'Hi'}]}]}}))
        owner = SimpleNamespace(_ensure_client=AsyncMock(return_value=client))
        manager = SimpleNamespace(_session_ids={'contact': 'native'}, get=lambda key: owner if key == 'contact' else None)
        result = await read(manager, {'action': 'conversation', 'session_id': 'native', 'before': None})
        assert [message['text'] for message in result['messages']] == ['Hello', 'Hi']
        client._request.assert_awaited_once_with('thread/read', {'threadId': 'native', 'includeTurns': True})
        with pytest.raises(ValueError):
            await read(manager, {'action': 'conversation', 'session_id': 'unowned', 'before': None})
        assert owner._ensure_client.await_count == 1
    asyncio.run(scenario())


def test_history_pages_are_bounded_and_cursor_is_checked():
    thread = {'turns': [{'id': 'turn', 'items': [{'id': str(i), 'type': 'agentMessage', 'text': 'x' * 12000} for i in range(100)]}]}
    latest = page(thread, None)
    assert 0 < len(latest['messages']) < 32
    assert latest['messages'][-1]['id'] == '99'
    assert all(message['truncated'] for message in latest['messages'])
    older = page(thread, latest['next_before'])
    assert int(older['messages'][-1]['id']) < int(latest['messages'][0]['id'])
    with pytest.raises(ValueError):
        page(thread, 'missing')


def test_native_owner_locks_exclude_second_process_and_release_after_disconnect(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_SESSION_LOCK_DIRECTORY', str(tmp_path))
    async def scenario():
        first = CodexAppServerClient(BridgeConfig(), developer_instructions='')
        second = CodexAppServerClient(BridgeConfig(), developer_instructions='')
        first._lock_session('native')
        with pytest.raises(CodexAppServerError, match='already active'):
            second._lock_session('native')
        await first.disconnect()
        second._lock_session('native')
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
        monkeypatch.setattr(session, '_connect_client', connect)
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

    thread = {'turns': [{'id': 'turn', 'items': [
        {'id': str(i), 'type': 'agentMessage', 'text': f'Message {i}'} for i in range(70)
    ]}]}
    first = page(thread, None, forward=True)
    second = page(thread, first['next_before'], forward=True)
    third = page(thread, second['next_before'], forward=True)
    assert [m['id'] for p in (first, second, third) for m in p['messages']] == [str(i) for i in range(70)]
    assert third['next_before'] is None

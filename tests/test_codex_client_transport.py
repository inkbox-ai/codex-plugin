"""Large host responses and transport failures must never strand requests."""

import asyncio
import json
from types import SimpleNamespace

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


@pytest.mark.parametrize('arrival', ['separate', 'buffered', 'before-response'])
@pytest.mark.parametrize('eof', [False, True])
@pytest.mark.parametrize('failed', [False, True], ids=['success', 'failure'])
def test_completion_is_not_lost_when_start_response_and_notifications_share_buffer(arrival, eof, failed):
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions='isolated test')
        client.thread_id = 'existing-thread'
        reader = asyncio.StreamReader()
        tasks = []
        def encode(messages):
            return ''.join(json.dumps(m) + '\n' for m in messages).encode()
        notifications = [
            {'method': 'item/completed', 'params': {'turnId': 'turn-1', 'item': {
                'type': 'agentMessage', 'phase': 'final', 'text': 'Expected answer'}}},
            {'method': 'turn/completed', 'params': {'turn': {
                'id': 'turn-1', 'status': 'failed' if failed else 'completed',
                **({'error': {'message': 'Synthetic model rejection'}} if failed else {})}}},
        ]
        async def later():
            while 'turn-1' not in client._turns:
                await asyncio.sleep(0)
            reader.feed_data(encode(notifications))
            if eof:
                reader.feed_eof()
        def write(data):
            request = json.loads(data)
            assert request['method'] == 'turn/start'
            response = {'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}}
            messages = ([*notifications, response] if arrival == 'before-response'
                        else [response, *notifications] if arrival == 'buffered' else [response])
            reader.feed_data(encode(messages))
            if arrival == 'separate':
                tasks.append(asyncio.create_task(later()))
            elif eof:
                reader.feed_eof()
        client._proc = SimpleNamespace(stdout=reader, stdin=SimpleNamespace(write=write))
        client._reader_task = asyncio.create_task(client._reader_loop())
        try:
            if failed:
                with pytest.raises(CodexAppServerError, match='Synthetic model rejection'):
                    await asyncio.wait_for(client.run('Hello'), timeout=1)
            else:
                assert await asyncio.wait_for(client.run('Hello'), timeout=1) == 'Expected answer'
        finally:
            client._reader_task.cancel()
            await asyncio.gather(client._reader_task, *tasks, return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['rejected', 'malformed', 'invalid-result', 'invalid-turn', 'cancelled'])
def test_failed_start_does_not_leak_early_notifications_into_next_turn(failure):
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions='test')
        client.thread_id = 'thread-1'
        reader = asyncio.StreamReader()
        wrote = asyncio.Event()
        requests = []

        def completed(text):
            return [
                {'method': 'item/completed', 'params': {'turnId': 'turn-1', 'item': {
                    'type': 'agentMessage', 'text': text}}},
                {'method': 'turn/completed', 'params': {'turn': {'id': 'turn-1', 'status': 'completed'}}},
            ]

        def write(data):
            request = json.loads(data)
            requests.append(request)
            response = {'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}}
            if len(requests) == 1:
                messages = completed('Stale answer')
                if failure == 'rejected':
                    messages.append({'id': request['id'], 'error': {'message': 'Start rejected'}})
                elif failure != 'cancelled':
                    result = {'malformed': {}, 'invalid-result': [], 'invalid-turn': {'turn': []}}[failure]
                    messages.append({'id': request['id'], 'result': result})
            else:
                messages = [response, *completed('Fresh answer')]
            reader.feed_data(''.join(json.dumps(m) + '\n' for m in messages).encode())
            wrote.set()

        client._proc = SimpleNamespace(stdout=reader, stdin=SimpleNamespace(write=write))
        client._reader_task = asyncio.create_task(client._reader_loop())
        task = asyncio.create_task(client.run('first'))
        try:
            if failure == 'cancelled':
                await wrote.wait()
                await asyncio.sleep(0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(CodexAppServerError):
                    await asyncio.wait_for(task, 1)
            assert not client._pending
            assert not client._starting_turns
            assert not client._early_notifications
            assert not client._turns
            assert await asyncio.wait_for(client.run('second'), 1) == 'Fresh answer'
        finally:
            client._reader_task.cancel()
            await asyncio.gather(client._reader_task, return_exceptions=True)
    asyncio.run(scenario())


def test_response_then_eof_settles_turn_without_completion():
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions='test')
        client.thread_id = 'thread-1'
        reader = asyncio.StreamReader()

        def write(data):
            request = json.loads(data)
            reader.feed_data((json.dumps({'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}}) + '\n').encode())
            reader.feed_eof()

        client._proc = SimpleNamespace(stdout=reader, stdin=SimpleNamespace(write=write))
        client._reader_task = asyncio.create_task(client._reader_loop())
        with pytest.raises(CodexAppServerError, match='app-server exited'):
            await asyncio.wait_for(client.run('hello'), 1)
        await client._reader_task
    asyncio.run(scenario())


def test_concurrent_start_responses_match_early_notifications_by_turn_id():
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(), developer_instructions='test')
        client.thread_id = 'thread-1'
        reader = asyncio.StreamReader()
        requests, activity = [], []

        def events(turn_id, text):
            return [
                {'method': 'item/started', 'params': {'turnId': turn_id, 'item': {'type': 'agentMessage'}}},
                {'method': 'item/completed', 'params': {'turnId': turn_id, 'item': {'type': 'agentMessage', 'text': text}}},
                {'method': 'turn/completed', 'params': {'turn': {'id': turn_id, 'status': 'completed'}}},
            ]

        def write(data):
            requests.append(json.loads(data))
            if len(requests) == 2:
                messages = [
                    *events('first', 'First reply'),
                    {'id': requests[1]['id'], 'result': {'turn': {'id': 'second'}}},
                    *events('second', 'Second reply'),
                    {'id': requests[0]['id'], 'result': {'turn': {'id': 'first'}}},
                ]
                reader.feed_data(''.join(json.dumps(m) + '\n' for m in messages).encode())

        client._proc = SimpleNamespace(stdout=reader, stdin=SimpleNamespace(write=write))
        client._reader_task = asyncio.create_task(client._reader_loop())
        try:
            replies = await asyncio.wait_for(asyncio.gather(
                client.run('first', activity_handler=lambda *args: activity.append('first')),
                client.run('second', activity_handler=lambda *args: activity.append('second')),
            ), 1)
            assert replies == ['First reply', 'Second reply']
            assert sorted(activity) == ['first', 'second']
            assert not client._early_notifications
        finally:
            client._reader_task.cancel()
            await asyncio.gather(client._reader_task, return_exceptions=True)
    asyncio.run(scenario())

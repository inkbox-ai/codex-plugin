"""Approval decisions and host recovery preserve the next human request."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import companion, sessions
from tests.test_companion import ReconnectingClient, drained, fixture, harness, live
from tests.test_companion_response_modes import set_input
from tests.test_sessions import make_session


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))


@pytest.mark.parametrize('server', ['inkbox', 'github'])
@pytest.mark.parametrize('answer,action', [
    ('yes', 'accept'), ('yes, approved', 'accept'), ('yes approved', 'accept'),
    ('allow', 'accept'), ('no', 'decline'), ('deny', 'decline'),
    ('cancel', 'cancel'), ('what does this do?', 'cancel'), (None, 'cancel'),
])
def test_mcp_tool_approval_requires_an_affirmative_answer(server, answer, action):
    async def scenario():
        session = make_session([])
        session._escalate = AsyncMock(return_value=answer)
        response = await session._handle_codex_request('mcpServer/elicitation/request', {
            'message': f'Allow the {server} MCP server to run tool "lookup"?',
        })
        assert response['action'] == action
        assert response['content'] == ({'text': answer} if action == 'accept' else None)
    asyncio.run(scenario())


def test_structured_tool_approval_does_not_depend_on_prompt_wording():
    async def scenario():
        session = make_session([])
        session._escalate = AsyncMock(return_value='no')
        response = await session._handle_codex_request('mcpServer/elicitation/request', {
            '_meta': {'codex_approval_kind': 'mcp_tool_call'},
            'message': 'Proceed with this action?',
        })
        assert response == {'action': 'decline', 'content': None}
    asyncio.run(scenario())


def test_non_approval_elicitation_keeps_free_text():
    async def scenario():
        session = make_session([])
        session._escalate = AsyncMock(return_value='no')
        response = await session._handle_codex_request('mcpServer/elicitation/request', {
            'message': 'Enter the two-letter locale code.',
        })
        assert response == {'action': 'accept', 'content': {'text': 'no'}}
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['imessage', 'phone', 'mail'])
@pytest.mark.parametrize('answer,action', [('yes, approved', 'accept'), ('no', 'decline')])
def test_companion_manual_approval_answers_reach_the_waiting_request(channel, answer, action):
    async def scenario():
        event = set_input(fixture(channel), 'direct', '@agent read the conversation')
        receiver, _, session, sent = harness(event, reply_mode='mention')
        prompted = asyncio.Event()
        original_send = session.send_fn
        responses = []

        async def send(*args):
            await original_send(*args)
            prompted.set()

        session.send_fn = send

        async def approving_host(text):
            responses.append(await session._handle_codex_request('mcpServer/elicitation/request', {
                'message': 'Allow the inkbox MCP server to run tool "inkbox_lookup"?',
            }))
            return '[SILENT]'

        session._client.run = approving_host
        try:
            await receiver.accept(event)
            await asyncio.wait_for(prompted.wait(), 1)
            await receiver.accept(set_input(live(event), 'direct', f'@agent {answer}'))
            await drained(receiver)
            assert responses == [{
                'action': action, 'content': {'text': answer} if action == 'accept' else None,
            }]
            assert len(sent) == 1
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchall() == [('done',), ('done',)]
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_old_escalation_cleanup_cannot_clear_a_new_interaction():
    async def scenario():
        session = make_session([])
        sending, release = asyncio.Event(), asyncio.Event()

        async def send(chat_id, text, mode, meta):
            if text == 'First request':
                sending.set()
                await release.wait()

        session.send_fn = send
        first = asyncio.create_task(session._escalate('poll', 'First request'))
        await asyncio.wait_for(sending.wait(), 1)
        old = session.pending
        await session.close()
        second = asyncio.create_task(session._escalate('poll', 'New request'))
        await asyncio.sleep(0)
        current = session.pending
        try:
            release.set()
            assert await first is None
            assert current is not None and current is not old
            assert session.pending is current
            await session.handle_inbound('fresh answer', 'sms', {})
            assert await second == 'fresh answer'
        finally:
            for task in (first, second):
                task.cancel()
            await asyncio.gather(first, second, return_exceptions=True)
    asyncio.run(scenario())


def test_failed_prompt_delivery_does_not_leave_a_pending_interaction():
    async def scenario():
        session = make_session([])
        session.send_fn = AsyncMock(side_effect=RuntimeError('Synthetic send failure'))
        with pytest.raises(RuntimeError, match='send failure'):
            await session._escalate('poll', 'Question')
        assert session.pending is None
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['imessage', 'phone', 'mail'])
def test_recovery_drops_dead_approval_without_consuming_the_next_request(monkeypatch, channel):
    monkeypatch.setattr(companion, 'recover_saved_answer', AsyncMock(return_value=None))
    monkeypatch.setattr(sessions, 'CodexAppServerClient', ReconnectingClient)

    async def scenario():
        event = set_input(fixture(channel), 'direct', '@agent read the conversation')
        receiver, _, session, sent = harness(event, reply_mode='mention')
        requests = []
        ready = asyncio.Event()
        original_send = session.send_fn

        async def send(*args):
            await original_send(*args)
            ready.set()

        session.send_fn = send

        async def disconnected_host(text):
            requests.append(asyncio.create_task(session._handle_codex_request(
                'mcpServer/elicitation/request', {
                    'message': 'Allow the inkbox MCP server to run tool "inkbox_lookup"?',
                },
            )))
            await asyncio.wait_for(ready.wait(), 1)
            raise RuntimeError('Synthetic host exit during approval')

        session._client.run = disconnected_host
        try:
            await receiver.accept(event)
            await drained(receiver)
            receiver.retries.pop(event['companion']['scope_id']).cancel()
            await receiver._drain(event['companion']['scope_id'])
            assert session.pending is None
            assert await requests[0] == {'action': 'cancel', 'content': None}
            sent.clear()
            following = set_input(live(event), 'direct', '@agent a different new task')
            await receiver.accept(following)
            await drained(receiver)
            assert len(sent) == 1
            assert [kind for kind, _ in session._client.events] == ['run']
            assert 'a different new task' in session._client.events[0][1]
            assert receiver.inbox.db.execute(
                'SELECT state FROM events ORDER BY sequence',
            ).fetchall() == [('quarantined',), ('done',)]
        finally:
            await receiver.close()
            for task in requests:
                task.cancel()
            await asyncio.gather(*requests, return_exceptions=True)
    asyncio.run(scenario())

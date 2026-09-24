"""Recovery reads positive host evidence; it never submits an uncertain turn again."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex import codex_client, companion, sessions
from inkbox_codex.codex_client import CodexStartupError, recover_saved_answer
from inkbox_codex.companion import Event, inbox_summary
from inkbox_codex.config import BridgeConfig
from tests.test_companion import ReconnectingClient, drained, fixture, harness, live


def history():
    return {'thread': {'id': 'saved-thread', 'turns': [{
        'id': 'saved-turn', 'status': 'completed', 'items': [
            {'type': 'userMessage', 'content': [{'type': 'text', 'text': 'Companion receipt: unique-token\nCurrent request'}]},
            {'type': 'agentMessage', 'phase': 'commentary', 'text': 'Working on it'},
            {'type': 'agentMessage', 'phase': 'final_answer', 'text': 'Saved final answer'},
        ],
    }]}}


@pytest.mark.parametrize('case', ['completed', 'silent', 'wrong_thread', 'wrong_token', 'inProgress', 'failed',
                                  'interrupted', 'duplicate', 'commentary_only', 'missing_answer',
                                  'missing_turns', 'null_turns', 'bad_turns', 'bad_response', 'read_error'])
def test_history_requires_one_matching_completed_answer_without_new_turn(monkeypatch, case):
    result = history()
    turn = result['thread']['turns'][0]
    expected = None
    if case == 'completed': expected = 'Saved final answer'
    elif case == 'silent':
        expected = turn['items'][-1]['text'] = '[SILENT]'
    elif case == 'wrong_thread': result['thread']['id'] = 'unrelated-thread'
    elif case == 'wrong_token': turn['items'][0]['content'][0]['text'] = 'Companion receipt: another-token\n'
    elif case in {'inProgress', 'failed', 'interrupted'}: turn['status'] = case
    elif case == 'duplicate': result['thread']['turns'].append(deepcopy(turn))
    elif case == 'commentary_only': turn['items'].pop()
    elif case == 'missing_answer': turn['items'] = turn['items'][:1]
    elif case == 'missing_turns': del result['thread']['turns']
    elif case == 'null_turns': result['thread']['turns'] = None
    elif case == 'bad_turns': result['thread']['turns'] = [None, [], {'items': None}]
    elif case == 'bad_response': result = None
    client = SimpleNamespace(
        _ensure_process=AsyncMock(), _initialize=AsyncMock(), disconnect=AsyncMock(),
        _request=AsyncMock(return_value=result),
    )
    if case == 'read_error': client._request.side_effect = codex_client.CodexAppServerError('Unavailable history')
    factory = Mock(return_value=client)
    monkeypatch.setattr(codex_client, 'CodexAppServerClient', factory)
    assert asyncio.run(recover_saved_answer(BridgeConfig(), 'saved-thread', 'unique-token')) == expected
    client._request.assert_awaited_once_with('thread/read', {'threadId': 'saved-thread', 'includeTurns': True})
    client.disconnect.assert_awaited_once()
    assert factory.call_args.kwargs['tools_enabled'] is False


def test_unavailable_launcher_does_not_pretend_history_was_inspected(monkeypatch):
    client = SimpleNamespace(_ensure_process=AsyncMock(side_effect=CodexStartupError('Cannot launch')),
                             disconnect=AsyncMock())
    monkeypatch.setattr(codex_client, 'CodexAppServerClient', Mock(return_value=client))
    with pytest.raises(CodexStartupError):
        asyncio.run(recover_saved_answer(BridgeConfig(), 'saved-thread', 'unique-token'))
    client.disconnect.assert_awaited_once()


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    monkeypatch.setattr(sessions, 'CodexAppServerClient', ReconnectingClient)


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_completed_answer_recovered_after_restart_without_model_replay(monkeypatch, channel):
    lookup = AsyncMock(return_value='Recovered final answer')
    monkeypatch.setattr(companion, 'recover_saved_answer', lookup)

    async def scenario():
        e = fixture(channel)
        r, _, s, sent = harness(e)
        original = s._client
        original.run = AsyncMock(side_effect=RuntimeError('Lost completion acknowledgement'))
        await r.accept(e)
        await drained(r)
        journal = r.inbox.db.execute('SELECT thread_id,receipt_token FROM host_receipts').fetchone()
        assert journal
        assert not sent
        await r.close()
        r, sdk, s, sent = harness(e)
        restarted = s._client
        try:
            r.recover()
            await drained(r)
            assert [entry[1] for entry in sent] == ['Recovered final answer']
            assert not restarted.events
            assert sdk.loads == 0
            lookup.assert_awaited_once_with(r.cfg, *journal)
            original.run.assert_awaited_once()
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
            assert r.inbox.db.execute('SELECT state FROM activations').fetchone()[0] == 'initialized'
            assert r.inbox.db.execute('SELECT action FROM recovery_actions').fetchone()[0] == 'auto_recover'
            assert not await r.accept(e)
            await drained(r)
            assert len(sent) == 1
            await r.accept(live(e))
            await drained(r)
            assert len(sent) == 2
            assert len(s._client.events) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_quiet_injection_cannot_recover_an_unrelated_older_answer(monkeypatch):
    lookup = AsyncMock(return_value='Older answer must never be delivered')
    monkeypatch.setattr(companion, 'recover_saved_answer', lookup)

    async def scenario():
        e = fixture('imessage')
        e['data']['message']['content'] = 'Background context without a mention'
        r, _, s, sent = harness(e, reply_mode='mention')
        s._client.append_context = AsyncMock(side_effect=RuntimeError('Lost injection acknowledgement'))
        try:
            await r.accept(e)
            await drained(r)
            await r._drain(e['companion']['scope_id'])
            lookup.assert_not_awaited()
            assert not sent
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'quarantined'
        finally:
            await r.close()
    asyncio.run(scenario())


def test_recovery_waits_for_old_process_to_stop_even_after_close_loses_client(monkeypatch):
    lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(companion, 'recover_saved_answer', lookup)

    async def scenario():
        e = fixture()
        r, _, s, sent = harness(e)
        process = SimpleNamespace(returncode=None, kill=Mock(), wait=AsyncMock())
        s._client._proc = process
        s._client.run = AsyncMock(side_effect=RuntimeError('Lost turn acknowledgement'))
        try:
            await r.accept(e)
            await drained(r)
            r.inbox.accept(Event.parse(live(e)))
            for _ in range(2):
                await r._drain(e['companion']['scope_id'])
                assert not sent
                assert r.inbox.db.execute('SELECT state FROM events WHERE event_id=?', (e['id'],)).fetchone()[0] == 'uncertain'
                lookup.assert_not_awaited()
            assert process.kill.call_count == 2
            process.returncode = -9
            await r._drain(e['companion']['scope_id'])
            assert len(sent) == 1
            assert len(s._client.events) == 1
            assert inbox_summary(r.cfg)['quarantined_count'] == 1
            assert not r.recovery_hosts
        finally:
            await r.close()
    asyncio.run(scenario())


def test_legacy_uncertain_receipt_without_journal_releases_following_input(monkeypatch):
    lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(companion, 'recover_saved_answer', lookup)

    async def scenario():
        e = fixture()
        r, _, s, sent = harness(e)
        event = Event.parse(e)
        r.inbox.accept(event)
        r.inbox.state(event.event_id, 'uncertain')
        with r.inbox.db:
            r.inbox.db.execute('INSERT INTO activations VALUES(?,?,?,?,?)',
                              (event.scope, event.activation, event.source_id, event.author, 'uncertain'))
        try:
            await r.accept(live(e))
            await drained(r)
            assert len(sent) == 1
            assert len(s._client.events) == 1
            lookup.assert_not_awaited()
            assert inbox_summary(r.cfg)['quarantined_count'] == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_explicit_acknowledged_retry_of_quarantined_initialization_really_retries(monkeypatch):
    monkeypatch.setattr(companion, 'recover_saved_answer', AsyncMock(return_value=None))

    async def scenario():
        e = fixture()
        r, _, s, _ = harness(e)
        s._client.run = AsyncMock(side_effect=RuntimeError('Lost acknowledgement'))
        await r.accept(e)
        await drained(r)
        await r._drain(e['companion']['scope_id'])
        cfg = r.cfg
        await r.close()
        db = companion.Inbox(companion.inbox_path(cfg))
        try:
            with pytest.raises(companion.CompanionError, match='duplicate'):
                db.recover_receipt(e['id'], action='retry', reason='Inspected')
            db.recover_receipt(e['id'], action='retry', reason='Inspected; explicitly retry',
                               acknowledge_duplicate_risk=True)
        finally:
            db.close()
        r, sdk, s, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert len(sent) == len(s._client.events) == 1
            assert sdk.loads == 1
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
        finally:
            await r.close()
    asyncio.run(scenario())


def test_uncertain_head_automatically_releases_already_queued_followup(monkeypatch):
    monkeypatch.setattr(companion, 'recover_saved_answer', AsyncMock(return_value=None))
    monkeypatch.setattr(companion, 'RETRY_MAX_SECONDS', 0.001)

    async def scenario():
        e = fixture('imessage')
        r, _, s, sent = harness(e)
        started, fail, replied = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = s._client
        original_send = s.send_fn

        async def lost_turn(text):
            started.set()
            await fail.wait()
            raise RuntimeError('Completion acknowledgement lost')

        async def send(*args):
            await original_send(*args)
            replied.set()

        original.run = AsyncMock(side_effect=lost_turn)
        s.send_fn = send
        try:
            await r.accept(e)
            await asyncio.wait_for(started.wait(), 1)
            await r.accept(live(e))
            fail.set()
            await asyncio.wait_for(replied.wait(), 1)
            await drained(r)
            original.run.assert_awaited_once()
            assert len(s._client.events) == len(sent) == 1
            assert r.inbox.db.execute('SELECT state FROM events ORDER BY sequence').fetchall() == [
                ('quarantined',), ('done',)]
            assert not r.retries
        finally:
            await r.close()
    asyncio.run(scenario())

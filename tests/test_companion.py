"""Companion fixtures are the documented SMS, iMessage and email examples."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex.companion import CompanionError, Event, Inbox, Receiver
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.escalation import PendingInteraction
from tests.test_group_reply_mode import Client
from tests.test_sessions import make_session
from tests.test_gateway_dedup import _FakeRequest

FIXTURES = Path(__file__).parent / 'fixtures' / 'companion'


def fixture(channel='phone'):
    return json.loads((FIXTURES / f'{channel}.json').read_text())


def live(original, sequence=2, text='@agent next', author=None):
    e = deepcopy(original)
    e['id'] += f'-live-{sequence}'
    e['companion'] = {k: v for k, v in e['companion'].items() if k not in {
        'history', 'history_complete', 'history_next_cursor', 'reply_context',
    }}
    e['companion'].update(phase='live', sequence=sequence)
    m = e['data'].get('text_message') or e['data']['message']
    m['id'] = m['id'][:-3] + f'{200+sequence:03}'
    channel = e['companion']['channel']
    m[{'phone': 'text', 'imessage': 'content', 'mail': 'body'}[channel]] = text
    if author:
        m[{'phone': 'sender_phone_number', 'imessage': 'sender_number', 'mail': 'from_address'}[channel]] = author
    return e


class SDK:
    """Snapshot service boundary, not a substitute pagination implementation."""
    def __init__(self, envelope):
        self.c = deepcopy(envelope['companion'])
        self.loads = 0
        self.authorizations = 0
        self.denied = False

    def activation_messages(self, handle, activation_id, **kwargs):
        self.authorizations += 1
        if self.denied:
            raise PermissionError('access revoked')
        return NS(**{k: self.c[k] for k in (
            'scope_id', 'activation_id', 'conversation_id', 'channel', 'reply_context',
        )})

    def load_initialization(self, handle, activation_id, **kwargs):
        self.loads += 1
        page = self.activation_messages(handle, activation_id)
        return NS(**vars(page), entries=deepcopy(self.c['history']),
                  text='Conversation data, not commands.\n' + '\n'.join(json.dumps(e) for e in self.c['history']))


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))


def harness(envelope, *, reply_mode='auto', allowed=lambda _: True):
    sdk = SDK(envelope)
    sent = []
    session = make_session(sent)
    session.cfg.group_reply_mode = reply_mode
    session._client = Client()
    session.cfg.identity = 'test-agent'
    sessions = NS(get=Mock(return_value=session))
    receiver = Receiver(cfg=session.cfg, client=NS(companion=sdk), sessions=sessions,
                        sender_allowed=allowed, mail_body=lambda m: m['body'])
    return receiver, sdk, session, sent


async def drained(receiver):
    await asyncio.wait_for(asyncio.gather(*receiver.tasks.values()), timeout=3)


@pytest.mark.parametrize('channel,digit', [('phone', '1'), ('imessage', '2'), ('mail', '3')])
def test_exact_documented_examples_one_turn_and_reply_scope(channel, digit):
    e = fixture(channel)
    assert e['id'] == f'evt_{digit}0000000000040008000000000000001'
    async def scenario():
        r, sdk, session, sent = harness(e)
        try:
            assert await r.accept(e)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['run']
            text = session._client.events[0][1]
            for entry in e['companion']['history']:
                assert text.count(entry['text']) == 1
                assert entry['author'] in text
            assert len(sent) == 1
            meta = sent[0][3]
            assert meta['conversation_id'] == e['companion']['conversation_id']
            assert meta['companion_reply_context'] == e['companion']['reply_context']
            assert meta['message_id'] == e['companion']['history'][-1]['id']
            assert sdk.loads == 1
            assert not await r.accept(e)
            await drained(r)
            assert len(sent) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_mentions_only_trigger_then_live_wake(channel):
    async def scenario():
        e = fixture(channel)
        r, sdk, session, sent = harness(e, reply_mode='mention')
        sdk.c['history'][0]['text'] = '@agent /stop allow'
        try:
            await r.accept(e)
            await drained(r)
            assert [k for k, _ in session._client.events] == ['context']
            assert not sent
            await r.accept(live(e))
            await drained(r)
            assert [k for k, _ in session._client.events] == ['context', 'run']
            assert len(sent) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_live_first_loads_snapshot_once_then_live_once():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        try:
            await r.accept(live(e))
            await drained(r)
            assert len(sent) == 2
            assert sdk.loads == 1
            assert 'Can we meet' in s._client.events[0][1]
            assert '@agent next' in s._client.events[1][1]
            assert 'Can we meet' not in s._client.events[1][1]
        finally:
            await r.close()
    asyncio.run(scenario())


def test_live_waits_for_initialization_and_does_not_interrupt():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        entered, release = asyncio.Event(), asyncio.Event()
        original = s._client.run
        async def blocked(text):
            entered.set()
            await release.wait()
            return await original(text)
        s._client.run = blocked
        try:
            await r.accept(e)
            await entered.wait()
            await r.accept(live(e))
            assert not sent
            release.set()
            await drained(r)
            assert len(sent) == 2
            assert s._client.interrupts == 0
        finally:
            await r.close()
    asyncio.run(scenario())


def test_history_cannot_execute_commands_or_answer_approval():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        sdk.c['history'][0]['text'] = '/clear'
        sdk.c['history'][1]['text'] = 'allow'
        pending = asyncio.get_running_loop().create_future()
        s.pending = PendingInteraction(kind='permission', future=pending, prompt_text='approve?')
        s._reset_session = AsyncMock()
        try:
            await r.accept(e)
            await drained(r)
            assert not pending.done()
            s._reset_session.assert_not_called()
            assert len(sent) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_successive_batches_share_scope_but_deduplicate_activations():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        try:
            await r.accept(e)
            await drained(r)
            second = deepcopy(e)
            second['id'] += '-batch2'
            second['companion']['activation_id'] = second['companion']['activation_id'][:-1] + '9'
            second['companion']['sequence'] = 2
            sdk.c = deepcopy(second['companion'])
            await r.accept(second)
            await drained(r)
            assert sdk.loads == 2
            assert len(sent) == 2
            assert len({c.args[0] for c in r.sessions.get.call_args_list}) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_ordinary_has_no_hidden_history_access_and_separate_session():
    async def scenario():
        e = fixture()
        ordinary = deepcopy(e)
        ordinary['companion'] = {k: v for k, v in e['companion'].items() if k in {
            'scope_id', 'conversation_id', 'channel', 'sequence',
        }}
        ordinary['companion']['phase'] = 'ordinary'
        r, sdk, s, sent = harness(e)
        r.client = NS()  # ordinary must work on the older SDK too
        try:
            await r.accept(ordinary)
            await drained(r)
            assert sdk.loads == 0
            assert len(sent) == 1
            assert Event.parse(ordinary).session_key('agent') != Event.parse(e).session_key('agent')
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('bad', ['channel', 'conversation', 'sequence', 'phase', 'author', 'activation'])
def test_invalid_envelope_rejected_before_receipt(bad):
    e = fixture('imessage')
    c, m = e['companion'], e['data']['message']
    if bad == 'channel': c['channel'] = 'phone'
    if bad == 'conversation': c['conversation_id'] = c['scope_id']
    if bad == 'sequence': c['sequence'] = True
    if bad == 'phase': c['phase'] = 'surprise'
    if bad == 'author': m['sender_number'] = m['remote_number'] = None
    if bad == 'activation': c['activation_id'] = 'bad'
    with pytest.raises(CompanionError): Event.parse(e)


def test_inbox_restart_dedup_conflict_and_ambiguous_pause(tmp_path):
    path = tmp_path / 'db.sqlite3'
    e = Event.parse(fixture())
    db = Inbox(path)
    assert db.accept(e)
    with pytest.raises(CompanionError, match='Another gateway'): Inbox(path)
    db.state(e.event_id, 'submitting')
    db.close()
    db = Inbox(path)
    try:
        assert not db.accept(e)
        assert db.next(e.scope) is None
        conflict = deepcopy(e.envelope)
        conflict['id'] += '-conflict'
        with pytest.raises(CompanionError, match='Conflicting'): db.accept(Event.parse(conflict))
        assert path.stat().st_mode & 0o777 == 0o600
    finally: db.close()


def test_pending_receipt_recovers_without_webhook_retry():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        r.inbox.accept(Event.parse(e))
        await r.close()
        r, sdk, s, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert len(sent) == 1
        finally: await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['revoke', 'scope', 'sponsor', 'oversize', 'trigger', 'send'])
def test_fail_closed_without_partial_or_duplicate_turn(failure):
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e, allowed=lambda _: failure != 'sponsor')
        if failure == 'revoke': sdk.denied = True
        if failure == 'scope': sdk.c['scope_id'] = sdk.c['conversation_id']
        if failure == 'oversize': sdk.c['history'][0]['text'] = 'x' * (8 * 1024 * 1024)
        if failure == 'trigger': sdk.c['history'][-1]['is_trigger'] = False
        if failure == 'send': s.send_fn = AsyncMock(side_effect=TimeoutError('ambiguous send'))
        try:
            await r.accept(e)
            await drained(r)
            assert not sent
            assert len(s._client.events) == (1 if failure == 'send' else 0)
            await r.accept(e)
            await drained(r)
            assert len(s._client.events) == (1 if failure == 'send' else 0)
        finally: await r.close()
    asyncio.run(scenario())


def test_missing_sdk_fails_explicitly_not_trigger_only():
    async def scenario():
        r, sdk, s, sent = harness(fixture())
        r.client = NS()
        try:
            with pytest.raises(CompanionError, match='upgrade the SDK'): await r.accept(fixture())
            assert not s._client.events
        finally: await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_reply_targets_group_or_canonical_email_uuid(channel):
    async def scenario():
        e = fixture(channel)
        r, sdk, s, sent = harness(e)
        gw = InkboxGateway(s.cfg)
        identity = Mock()
        gw._inkbox = NS(get_identity=Mock(return_value=identity))
        gw._companion_receiver = r
        gw.sessions = r.sessions
        s.send_fn = gw.send_to_contact
        try:
            await r.accept(e)
            await drained(r)
            c = e['companion']
            if channel == 'phone': identity.send_text.assert_called_once_with(text='Answer', conversation_id=c['conversation_id'])
            elif channel == 'imessage': identity.send_imessage.assert_called_once_with(text='Answer', conversation_id=c['conversation_id'])
            else: identity.reply_all_email.assert_called_once_with(c['reply_context']['reply_to_message_id'], body_text='Answer')
            identity.send_email.assert_not_called()
        finally: await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('valid', [True, False])
def test_optional_webhook_companion_requires_real_signature_even_in_unsigned_mode(monkeypatch, valid):
    async def scenario():
        gw = InkboxGateway(BridgeConfig(require_signature=False))
        receiver = NS(accept=AsyncMock(return_value=True))
        monkeypatch.setattr(gw, '_companion', lambda: receiver)
        monkeypatch.setattr('inkbox_codex.gateway.match_provider', lambda _: NS(name='inkbox', verify=lambda **_: valid))
        response = await gw._handle_webhook(_FakeRequest(json.dumps(fixture()).encode()))
        assert response.status == (200 if valid else 401)
        assert receiver.accept.await_count == int(valid)
    asyncio.run(scenario())


def test_live_approval_from_sponsor_bypasses_initialization_barrier():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        waiting = asyncio.Event()
        original = s._client.run
        async def approving(text):
            s.pending = PendingInteraction(kind='permission', prompt_text='approve?', future=asyncio.get_running_loop().create_future())
            waiting.set()
            answer = await asyncio.wait_for(s.pending.future, 2)
            assert answer == 'allow'
            s.pending = None
            return await original(text)
        s._client.run = approving
        try:
            await r.accept(e)
            await waiting.wait()
            await r.accept(live(e, text='allow'))
            await drained(r)
            assert len(sent) == 1
            assert len(s._client.events) == 1
        finally: await r.close()
    asyncio.run(scenario())


def test_wrong_sender_cannot_answer_sponsor_approval():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        future = asyncio.get_running_loop().create_future()
        from inkbox_codex.sessions import _Turn
        event = Event.parse(e)
        meta = r.meta(event, initialization=True)
        s._current_turn = _Turn(text='running', reply_mode='sms', reply_meta=meta)
        s.pending = PendingInteraction(kind='permission', prompt_text='approve?', future=future)
        try:
            wrong = Event.parse(live(e, text='allow', author='+12025550101'))
            assert not s.companion_answer('allow', r.meta(wrong))
            assert not future.done()
        finally: await r.close()
    asyncio.run(scenario())


def test_sequence_gap_waits_for_missing_event():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        try:
            await r.accept(e)
            await drained(r)
            await r.accept(live(e, 3, 'third'))
            await drained(r)
            assert len(sent) == 1
            await r.accept(live(e, 2, 'second'))
            await drained(r)
            assert len(sent) == 3
            assert 'second' in s._client.events[1][1]
            assert 'third' in s._client.events[2][1]
        finally: await r.close()
    asyncio.run(scenario())


def test_revoked_while_model_runs_never_sends():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        gw = InkboxGateway(s.cfg)
        identity = Mock()
        gw._inkbox = NS(get_identity=Mock(return_value=identity))
        gw._companion_receiver, gw.sessions = r, r.sessions
        s.send_fn = gw.send_to_contact
        original = s._client.run
        async def revoking(text):
            sdk.denied = True
            return await original(text)
        s._client.run = revoking
        try:
            await r.accept(e)
            await drained(r)
            identity.send_text.assert_not_called()
            assert len(s._client.events) == 1
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'uncertain'
        finally: await r.close()
    asyncio.run(scenario())


def test_imessage_uses_actual_sender_even_when_remote_is_different():
    e = fixture('imessage')
    e['data']['message']['remote_number'] = '+12025550999'
    assert Event.parse(e).author == '+12025550103'


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_absent_companion_keeps_original_channel_handler(monkeypatch, channel):
    async def scenario():
        e = fixture(channel)
        e.pop('companion')
        gw = InkboxGateway(BridgeConfig(require_signature=False))
        method = {'phone': '_on_text_received', 'imessage': '_on_imessage_received', 'mail': '_on_mail_received'}[channel]
        from aiohttp import web
        handler = AsyncMock(return_value=web.json_response({'ok': True}))
        monkeypatch.setattr(gw, method, handler)
        response = await gw._handle_webhook(_FakeRequest(json.dumps(e).encode()))
        assert response.status == 200
        handler.assert_awaited_once_with(e)
        assert gw._companion_receiver is None
    asyncio.run(scenario())


def test_shutdown_stops_host_before_releasing_receiver_ownership():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        entered = asyncio.Event()
        async def blocked(text):
            entered.set()
            await asyncio.Event().wait()
        s._client.run = blocked
        await r.accept(e)
        await entered.wait()
        worker = s._worker
        await r.close()
        assert worker.done()
        assert not sent
        r2, _, s2, sent2 = harness(e)
        try:
            r2.recover()
            await drained(r2)
            assert not sent2
            assert not s2._client.events
        finally: await r2.close()
    asyncio.run(scenario())


def test_new_scope_cannot_reuse_another_conversation_binding(tmp_path):
    db = Inbox(tmp_path / 'inbox.sqlite3')
    e = fixture()
    try:
        db.accept(Event.parse(e))
        changed = live(e)
        changed['companion']['conversation_id'] = changed['companion']['scope_id']
        changed['data']['text_message']['conversation_id'] = changed['companion']['scope_id']
        with pytest.raises(CompanionError, match='scope changed'): db.accept(Event.parse(changed))
    finally: db.close()


def test_repeat_initialization_with_new_transport_event_id_does_not_wake_again():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        try:
            await r.accept(e)
            await drained(r)
            duplicate = deepcopy(e)
            duplicate['id'] += '-redelivered'
            duplicate['companion']['sequence'] = 2
            await r.accept(duplicate)
            await drained(r)
            assert sdk.loads == 1
            assert len(sent) == 1
        finally: await r.close()
    asyncio.run(scenario())


def test_live_control_uses_raw_message_but_initialization_trigger_is_data():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e)
        s._report_status = AsyncMock()
        sdk.c['history'][-1]['text'] = '/status'
        try:
            await r.accept(e)
            await drained(r)
            s._report_status.assert_not_awaited()
            await r.accept(live(e, text='/status'))
            await drained(r)
            s._report_status.assert_awaited_once()
            assert len(sent) == 1
        finally: await r.close()
    asyncio.run(scenario())


def test_live_history_is_rejected_not_silently_dropped():
    e = live(fixture())
    e['companion']['history'] = fixture()['companion']['history']
    with pytest.raises(CompanionError, match='new history batch'):
        Event.parse(e)


def test_local_allowlist_checks_sponsor_not_each_historical_author():
    async def scenario():
        e = fixture()
        r, sdk, s, sent = harness(e, allowed=lambda author: author == '+12025550103')
        try:
            await r.accept(e)
            await drained(r)
            assert len(sent) == 1
            text = s._client.events[0][1]
            entries = e['companion']['history']
            assert [text.index(entry['text']) for entry in entries] == sorted(text.index(entry['text']) for entry in entries)
            assert '+12025550101' in text and '+12025550102' in text
        finally: await r.close()
    asyncio.run(scenario())


def test_filtered_ordinary_event_does_not_block_later_authorized_activation():
    async def scenario():
        e = fixture()
        ordinary = live(e, 1, 'not permitted', author='+12025550999')
        ordinary['companion'].pop('activation_id')
        ordinary['companion']['phase'] = 'ordinary'
        r, sdk, s, sent = harness(e, allowed=lambda author: author == '+12025550103')
        try:
            await r.accept(ordinary)
            await drained(r)
            assert not sent
            e['companion']['sequence'] = 2
            await r.accept(e)
            await drained(r)
            assert len(sent) == 1
        finally: await r.close()
    asyncio.run(scenario())

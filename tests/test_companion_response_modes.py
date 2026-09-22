"""Sender access and mention policy compose without waking on background inputs."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox_codex.companion import Event
from inkbox_codex.escalation import PendingInteraction
from tests.test_companion import drained, fixture, harness, isolated, live


def message(envelope):
    return envelope['data'].get('text_message') or envelope['data']['message']


def set_input(envelope, access, text):
    item = message(envelope)
    item['sender_access'] = access
    channel = envelope['companion']['channel']
    item[{'phone': 'text', 'imessage': 'content', 'mail': 'body'}[channel]] = text
    return envelope


# All sixteen cells, exercised through real receipt routing on every channel.
@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
@pytest.mark.parametrize('phase', ['initialization', 'live', 'ordinary'])
@pytest.mark.parametrize('response_mode', ['safe', 'relaxed'])
@pytest.mark.parametrize('reply_mode', ['auto', 'mention'])
@pytest.mark.parametrize('access', ['direct', 'sponsored'])
@pytest.mark.parametrize('mentioned', [False, True])
def test_response_grid(channel, phase, response_mode, reply_mode, access, mentioned):
    async def scenario():
        e = fixture(channel)
        text = '@agent current message' if mentioned else 'Current message'
        if phase == 'initialization':
            set_input(e, access, text)
            e['companion']['history'][-1].update(text=text, sender_access=access)
        r, sdk, session, sent = harness(e, reply_mode=reply_mode, response_mode=response_mode)
        session.typing_fn = AsyncMock()
        try:
            incoming = e
            if phase == 'live':
                await r.accept(e)
                await drained(r)
                session._client.events.clear()
                sent.clear()
                session.typing_fn.reset_mock()
                incoming = set_input(live(e), access, text)
                sdk.load_initialization = lambda *a, **kw: pytest.fail('Unexpected live lookup')
            elif phase == 'ordinary':
                incoming = set_input(live(e), access, text)
                incoming['companion'].update(phase='ordinary')
                incoming['companion'].pop('activation_id')
            await r.accept(incoming)
            await drained(r)
            wakes = (response_mode == 'relaxed' or access == 'direct') and (reply_mode == 'auto' or mentioned)
            assert [kind for kind, _ in session._client.events] == ['run' if wakes else 'context']
            assert len(sent) == int(wakes)
            assert f'sender_access={access}' in session._client.events[0][1]
            assert text in session._client.events[0][1]
            assert session._client.interrupts == 0
            if not wakes:
                session.typing_fn.assert_not_awaited()
            assert not await r.accept(incoming)
            await drained(r)
            assert len(session._client.events) == 1
            assert all(row[0] == 'done' for row in r.inbox.db.execute('SELECT state FROM events'))
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('access', [None, '', 'DIRECT', 'future-value', True, [], {}])
@pytest.mark.parametrize('response_mode', ['safe', 'relaxed'])
def test_unknown_access_is_not_promoted_by_history_or_phase(access, response_mode):
    async def scenario():
        e = set_input(fixture(), access, '@agent help')
        if access is None:
            message(e).pop('sender_access')
        # The snapshot still describes a direct sponsor; it cannot upgrade the receipt.
        r, _, session, sent = harness(e, response_mode=response_mode, reply_mode='mention')
        try:
            assert Event.parse(e).sender_access is None
            await r.accept(e)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == [
                'context' if response_mode == 'safe' else 'run',
            ]
            assert 'sender_access=unknown' in session._client.events[0][1]
            assert len(sent) == int(response_mode == 'relaxed')
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
@pytest.mark.parametrize('included', [False, True])
@pytest.mark.parametrize('response_mode', ['safe', 'relaxed'])
def test_live_first_sponsored_message_cannot_wake_via_historical_sponsor(channel, included, response_mode):
    async def scenario():
        e = fixture(channel)
        first = set_input(live(e), 'sponsored', '@agent current guest request')
        if included:
            source = e['companion']['history'][0]
            source['text'] = '@agent current guest request'
            message(first)['id'] = source['id']
            message(first)[{'phone': 'sender_phone_number', 'imessage': 'sender_number', 'mail': 'from_address'}[channel]] = source['author']
        r, sdk, session, sent = harness(e, response_mode=response_mode)
        try:
            await r.accept(first)
            await drained(r)
            expected = ['run' if response_mode == 'relaxed' else 'context']
            if not included:
                expected.insert(0, 'context')
            assert [kind for kind, _ in session._client.events] == expected
            assert len(sent) == int(response_mode == 'relaxed')
            assert sdk.loads == 1
            assert r.inbox.activation(Event.parse(first))[1] == e['companion']['history'][-1]['author']
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('included', [False, True])
def test_live_first_mention_gate_uses_current_message_not_old_sponsor(included):
    async def scenario():
        e = fixture()
        e['companion']['history'][-1]['text'] = '@agent old sponsor request'
        first = set_input(live(e), 'direct', 'Quiet current message')
        if included:
            source = e['companion']['history'][0]
            source['text'] = 'Quiet current message'
            message(first)['id'] = source['id']
            message(first)['sender_phone_number'] = source['author']
        r, _, session, sent = harness(e, reply_mode='mention')
        try:
            await r.accept(first)
            await drained(r)
            assert all(kind == 'context' for kind, _ in session._client.events)
            assert not sent
        finally:
            await r.close()
    asyncio.run(scenario())


def test_live_first_snapshot_author_must_match_current_source():
    async def scenario():
        e = fixture()
        first = live(e)
        message(first)['id'] = e['companion']['history'][0]['id']
        r, _, session, sent = harness(e)
        try:
            await r.accept(first)
            await drained(r)
            assert not session._client.events
            assert not sent
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'pending'
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('reply_mode,text,access', [
    ('auto', 'allow', 'sponsored'), ('auto', 'allow', None),
    ('mention', '@agent allow', 'sponsored'), ('mention', 'allow', 'direct'),
])
def test_context_only_reply_cannot_resolve_pending_approval(reply_mode, text, access):
    async def scenario():
        e = set_input(fixture(), 'direct', '@agent start')
        r, _, session, sent = harness(e, reply_mode=reply_mode)
        entered = asyncio.Event()
        original = session._client.run
        async def approving(prompt):
            session.pending = PendingInteraction(
                kind='permission', prompt_text='Allow?', future=asyncio.get_running_loop().create_future(),
            )
            entered.set()
            answer = await asyncio.wait_for(session.pending.future, 2)
            assert answer == 'allow'
            session.pending = None
            return await original(prompt)
        session._client.run = approving
        try:
            await r.accept(e)
            await asyncio.wait_for(entered.wait(), 2)
            quiet = set_input(live(e, 2), access, text)
            await r.accept(quiet)
            assert not session.pending.future.done()
            assert session._client.interrupts == 0
            assert not sent
            answer = set_input(live(e, 3), 'direct', '@agent allow')
            await r.accept(answer)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['run', 'context']
            assert len(sent) == 1
            assert text in session._client.events[-1][1]
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('access,reply_mode,text,executes', [
    ('sponsored', 'auto', '/clear', False),
    ('sponsored', 'mention', '@agent /clear', False),
    (None, 'auto', '/clear', False),
    ('direct', 'mention', '/clear', False),
    ('direct', 'mention', '@agent /clear', True),
    ('direct', 'auto', '/clear', True),
])
def test_local_commands_cannot_bypass_response_gates(access, reply_mode, text, executes):
    async def scenario():
        e = fixture()
        r, _, session, sent = harness(e, reply_mode=reply_mode)
        try:
            await r.accept(e)
            await drained(r)
            session._reset_session = AsyncMock()
            session._client.events.clear()
            sent.clear()
            await r.accept(set_input(live(e), access, text))
            await drained(r)
            assert session._reset_session.await_count == int(executes)
            assert [kind for kind, _ in session._client.events] == ([] if executes else ['context'])
            assert not sent
        finally:
            await r.close()
    asyncio.run(scenario())


def test_sponsored_context_waits_for_active_turn_without_interrupting():
    async def scenario():
        e = fixture()
        r, _, session, sent = harness(e)
        entered, release = asyncio.Event(), asyncio.Event()
        original = session._client.run
        async def slow(prompt):
            entered.set()
            await release.wait()
            return await original(prompt)
        session._client.run = slow
        try:
            await r.accept(e)
            await asyncio.wait_for(entered.wait(), 2)
            await r.accept(set_input(live(e), 'sponsored', '@agent later background'))
            assert not session._client.events
            assert not sent
            assert session._client.interrupts == 0
            release.set()
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['run', 'context']
            assert len(sent) == 1
        finally:
            release.set()
            await r.close()
    asyncio.run(scenario())


def test_quiet_receipts_are_not_replayed_after_restart_and_direct_message_can_wake():
    async def scenario():
        e = fixture()
        quiet = set_input(live(e), 'sponsored', '@agent background')
        r, _, session, _ = harness(e)
        await r.accept(e)
        await drained(r)
        await r.accept(quiet)
        await drained(r)
        await r.close()
        r, sdk, session, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert not await r.accept(quiet)
            await drained(r)
            assert not session._client.events
            await r.accept(live(e, 3, '@agent summarize'))
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['run']
            assert len(sent) == 1
            assert sdk.loads == 0
        finally:
            await r.close()
    asyncio.run(scenario())

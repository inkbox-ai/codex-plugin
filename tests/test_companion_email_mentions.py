"""Current email recipients compose with Companion access and mention policy."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox_codex.companion import Event
from inkbox_codex.escalation import PendingInteraction
from inkbox_codex.sessions import _Turn
from tests.test_companion import drained, fixture, harness, isolated, live

MAILBOX = 'agent@example.com'


def addressed(envelope, placement):
    message = envelope['data']['message']
    message.update(to_addresses=['other@example.com'], cc_addresses=[], bcc_addresses=[])
    message[f'{placement}_addresses'].append(MAILBOX)
    return envelope


@pytest.mark.parametrize('phase', ['initialization', 'live', 'ordinary'])
@pytest.mark.parametrize('response_mode', ['safe', 'relaxed'])
@pytest.mark.parametrize('access', ['direct', 'sponsored', None])
@pytest.mark.parametrize('placement', ['to', 'cc', 'bcc'])
@pytest.mark.parametrize('text_mention', [False, True])
def test_email_recipient_access_grid(phase, response_mode, access, placement, text_mention):
    async def scenario():
        e = fixture('mail')
        r, sdk, session, sent = harness(e, reply_mode='mention', response_mode=response_mode)
        session.identity_info['email'] = MAILBOX
        session.typing_fn = AsyncMock()
        try:
            if phase == 'live':
                await r.accept(e)
                await drained(r)
                session._client.events.clear()
                session.typing_fn.reset_mock()
                sent.clear()
            incoming = e if phase == 'initialization' else live(e)
            if phase == 'ordinary':
                incoming['companion'].update(phase='ordinary')
                incoming['companion'].pop('activation_id')
            addressed(incoming, placement)
            message = incoming['data']['message']
            message.update(sender_access=access, body='@agent help' if text_mention else 'Please help')
            await r.accept(incoming)
            await drained(r)
            wakes = (response_mode == 'relaxed' or access == 'direct') and (placement == 'to' or text_mention)
            assert [kind for kind, _ in session._client.events] == ['run' if wakes else 'context']
            assert len(sent) == int(wakes)
            assert session._client.interrupts == 0
            if not wakes:
                session.typing_fn.assert_not_awaited()
            assert not await r.accept(incoming)
            assert len(session._client.events) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('response_mode', ['safe', 'relaxed'])
@pytest.mark.parametrize('access', ['direct', 'sponsored', None])
def test_auto_email_does_not_require_to_or_text_mention(response_mode, access):
    async def scenario():
        e = addressed(fixture('mail'), 'cc')
        e['data']['message']['sender_access'] = access
        r, _, session, sent = harness(e, response_mode=response_mode)
        session.identity_info['email'] = MAILBOX
        try:
            await r.accept(e)
            await drained(r)
            wakes = response_mode == 'relaxed' or access == 'direct'
            assert [kind for kind, _ in session._client.events] == ['run' if wakes else 'context']
            assert len(sent) == int(wakes)
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('recipients,mailbox,wakes', [
    (['AGENT@EXAMPLE.COM'], MAILBOX, True),
    (['Helper <Agent@Example.COM>'], MAILBOX, True),
    (['other@example.com', MAILBOX], ' Agent@Example.COM ', True),
    (['notagent@example.com'], MAILBOX, False),
    (['agent@example.com.invalid'], MAILBOX, False),
    (['"agent@example.com" <other@example.com>'], MAILBOX, False),
    ([MAILBOX], '', False),
    ([MAILBOX], 'different@example.com', False),
    (MAILBOX, MAILBOX, False),
    (None, MAILBOX, False),
    ([{'address': MAILBOX}], MAILBOX, False),
])
def test_email_to_matches_the_actual_mailbox_exactly(recipients, mailbox, wakes):
    async def scenario():
        e = fixture('mail')
        e['data']['message']['to_addresses'] = recipients
        r, _, session, sent = harness(e, reply_mode='mention')
        session.identity_info['email'] = mailbox
        try:
            await r.accept(e)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['run' if wakes else 'context']
            assert len(sent) == int(wakes)
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('current_to', [False, True])
def test_live_first_email_uses_current_to_not_initialization_headers(current_to):
    async def scenario():
        e = addressed(fixture('mail'), 'cc' if current_to else 'to')
        current = addressed(live(e, text='New message\n> To: agent@example.com'), 'to' if current_to else 'cc')
        r, _, session, sent = harness(e, reply_mode='mention')
        session.identity_info['email'] = MAILBOX
        try:
            await r.accept(current)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['context', 'run' if current_to else 'context']
            assert len(sent) == int(current_to)
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['phone', 'imessage'])
def test_non_email_to_field_cannot_satisfy_mention(channel):
    async def scenario():
        e = fixture(channel)
        message = e['data'].get('text_message') or e['data']['message']
        message['to_addresses'] = [MAILBOX]
        r, _, session, sent = harness(e, reply_mode='mention')
        session.identity_info['email'] = MAILBOX
        try:
            await r.accept(e)
            await drained(r)
            assert [kind for kind, _ in session._client.events] == ['context']
            assert not sent
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('access,placement,author,accepts', [
    ('direct', 'to', 'Sponsor@Example.COM', True),
    ('direct', 'to', 'different@example.com', False),
    ('direct', 'cc', 'sponsor@example.com', False),
    ('sponsored', 'to', 'sponsor@example.com', False),
    (None, 'to', 'sponsor@example.com', False),
])
def test_email_to_approval_still_requires_eligible_prompted_sender(access, placement, author, accepts):
    async def scenario():
        e = fixture('mail')
        r, _, session, _ = harness(e, reply_mode='mention')
        session.identity_info['email'] = MAILBOX
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(kind='permission', prompt_text='Allow?', future=future)
        session._current_turn = _Turn(text='running', reply_mode='email', reply_meta=r.meta(Event.parse(e)))
        try:
            current = addressed(live(e, text='allow', author=author), placement)
            current['data']['message']['sender_access'] = access
            event = Event.parse(current)
            assert session.companion_answer(event.text, r.meta(event)) == accepts
            assert future.done() == accepts
            if accepts:
                assert future.result() == 'allow'
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('access,executes', [('direct', True), ('sponsored', False), (None, False)])
def test_email_to_controls_keep_the_safe_access_gate(access, executes):
    async def scenario():
        e = fixture('mail')
        r, _, session, sent = harness(e, reply_mode='mention')
        session.identity_info['email'] = MAILBOX
        try:
            await r.accept(e)
            await drained(r)
            session._reset_session = AsyncMock()
            session._client.events.clear()
            sent.clear()
            current = live(e, text='/clear')
            current['data']['message']['sender_access'] = access
            await r.accept(current)
            await drained(r)
            assert session._reset_session.await_count == int(executes)
            assert [kind for kind, _ in session._client.events] == ([] if executes else ['context'])
            assert not sent
        finally:
            await r.close()
    asyncio.run(scenario())

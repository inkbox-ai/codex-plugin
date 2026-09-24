"""Safe pre-submission outages recover without another delivery or operator action."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import companion
from inkbox_codex.codex_client import CodexStartupError
from inkbox_codex.companion import Event, inbox_summary
from tests.test_companion import drained, fixture, harness


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    monkeypatch.setattr(companion, 'RETRY_MAX_SECONDS', 0.001)


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_startup_outage_recovers_after_more_than_five_failures_without_new_input(channel):
    async def scenario():
        envelope = fixture(channel)
        receiver, _, session, sent = harness(envelope)
        host = session._client
        working = False
        repeated_failures = asyncio.Event()
        replied = asyncio.Event()
        attempts = []
        original_send = session.send_fn

        async def ensure():
            attempts.append(True)
            if not working:
                if len(attempts) >= 7:
                    repeated_failures.set()
                raise CodexStartupError('Codex startup temporarily unavailable')
            return host

        async def send(*args):
            await original_send(*args)
            replied.set()

        session._ensure_client = ensure
        session.send_fn = send
        try:
            await receiver.accept(envelope)
            await asyncio.wait_for(repeated_failures.wait(), 1)
            await drained(receiver)
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'failed'
            assert inbox_summary(receiver.cfg)['blocked_conversations'] == 1
            assert not host.events and not sent
            assert not receiver.inbox.db.execute('SELECT 1 FROM activations').fetchone()
            working = True
            await asyncio.wait_for(replied.wait(), 1)
            await drained(receiver)
            assert len(attempts) >= 8
            assert [kind for kind, _ in host.events] == ['run']
            assert len(sent) == 1
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
            assert inbox_summary(receiver.cfg)['blocked_conversations'] == 0
            assert not receiver.retries
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_saved_answer_retries_preflight_forever_without_regenerating():
    async def scenario():
        envelope = fixture()
        receiver, _, session, sent = harness(envelope)
        repeated_failures, replied = asyncio.Event(), asyncio.Event()
        working = False
        attempts = []
        original_send = session.send_fn

        async def send(*args):
            attempts.append(True)
            if not working:
                if len(attempts) >= 7:
                    repeated_failures.set()
                raise PermissionError('Temporary local delivery restriction')
            receiver.reply_sending(args[3])
            await original_send(*args)
            replied.set()

        session.send_fn = send
        try:
            await receiver.accept(envelope)
            await asyncio.wait_for(repeated_failures.wait(), 1)
            await drained(receiver)
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'failed'
            assert len(session._client.events) == 1
            assert not sent
            working = True
            await asyncio.wait_for(replied.wait(), 1)
            await drained(receiver)
            assert len(session._client.events) == len(sent) == 1
            assert not receiver.inbox.db.execute('SELECT 1 FROM replies').fetchone()
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_closing_gateway_cancels_automatic_retries():
    async def scenario():
        envelope = fixture()
        receiver, _, session, _ = harness(envelope)
        session._ensure_client = AsyncMock(side_effect=CodexStartupError('unavailable'))
        await receiver.accept(envelope)
        await drained(receiver)
        assert receiver.retries
        await receiver.close()
        attempts = session._ensure_client.await_count
        await asyncio.sleep(0.01)
        assert session._ensure_client.await_count == attempts
        assert all(timer.cancelled() for timer in receiver.retries.values())
    asyncio.run(scenario())


def test_restart_automatically_retries_persisted_known_preflight_failure():
    async def scenario():
        envelope = fixture()
        receiver, _, _, _ = harness(envelope)
        receiver.inbox.accept(Event.parse(envelope))
        receiver.inbox.state(envelope['id'], 'failed')
        await receiver.close()
        receiver, _, session, sent = harness(envelope)
        try:
            receiver.recover()
            await drained(receiver)
            assert len(session._client.events) == len(sent) == 1
            assert receiver.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_heartbeat_recovery_is_harmless_after_receiver_closes():
    async def scenario():
        receiver, _, _, _ = harness(fixture())
        await receiver.close()
        receiver.recover()
        assert not receiver.tasks
    asyncio.run(scenario())

"""Operator recovery releases queues without silently replaying ambiguous work."""

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import cli, daemon
from inkbox_codex.companion import CompanionError, Event, Inbox, inbox_path, inbox_summary
from inkbox_codex.config import BridgeConfig
from tests.test_companion import drained, fixture, harness, live


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
@pytest.mark.parametrize('live_first', [False, True])
def test_uncertain_initialization_blocks_restart_until_retired_then_only_fresh_turn(channel, live_first):
    async def scenario():
        e = fixture(channel)
        r, _, s, sent = harness(e)
        first = live(e) if live_first else e
        if live_first:
            s._client.append_context = AsyncMock(side_effect=RuntimeError('unknown context outcome'))
        else:
            s._client.run = AsyncMock(side_effect=RuntimeError('unknown turn outcome'))
        await r.accept(first)
        await drained(r)
        await r.accept(live(e, sequence=3))
        await drained(r)
        cfg = r.cfg
        assert not sent
        assert inbox_summary(cfg)['blocked_conversations'] == 1
        assert inbox_summary(cfg)['unfinished_count'] == 2
        await r.close()

        r, sdk, s, sent = harness(e)
        r.recover()
        await drained(r)
        assert not sent and not s._client.events
        payloads = r.inbox.db.execute('SELECT event_id,payload FROM events').fetchall()
        threads = r.inbox.db.execute('SELECT * FROM threads').fetchall()
        await r.close()

        db = Inbox(inbox_path(cfg))
        assert db.recover_receipt(first['id'], action='retire', reason='Inspected; skip previous input') == 'done'
        assert db.db.execute('SELECT event_id,payload FROM events').fetchall() == payloads
        assert db.db.execute('SELECT * FROM threads').fetchall() == threads
        assert db.db.execute('SELECT action,previous_state,next_state FROM recovery_actions').fetchone() == (
            'retire', 'uncertain', 'done')
        assert db.db.execute('SELECT state FROM activations').fetchone()[0] == 'retired'
        db.close()

        r, sdk, s, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert len(sent) == 1
            assert [kind for kind, _ in s._client.events] == ['run']
            assert '@agent next' in s._client.events[0][1]
            assert sdk.loads == 0  # Never replay the retired initialization trigger.
            assert inbox_summary(cfg)['blocked_conversations'] == 0
            assert inbox_summary(cfg)['unfinished_count'] == 0
            assert not await r.accept(first)  # Deduplication still holds.
        finally:
            await r.close()
    asyncio.run(scenario())


def test_explicit_uncertain_turn_retry_requires_acknowledgement_then_reinitializes():
    async def scenario():
        e = fixture()
        r, _, s, _ = harness(e)
        s._client.run = AsyncMock(side_effect=RuntimeError('unknown outcome'))
        await r.accept(e)
        await drained(r)
        cfg = r.cfg
        await r.close()
        db = Inbox(inbox_path(cfg))
        with pytest.raises(CompanionError, match='duplicate'):
            db.recover_receipt(e['id'], action='retry', reason='Investigated')
        assert not db.db.execute('SELECT * FROM recovery_actions').fetchall()
        db.recover_receipt(e['id'], action='retry', reason='Investigated', acknowledge_duplicate_risk=True)
        db.close()
        r, sdk, s, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert len(sent) == 1
            assert sdk.loads == 1
            assert len(s._client.events) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_uncertain_reply_retry_reuses_saved_answer_without_new_model_turn():
    async def scenario():
        e = fixture()
        r, _, s, _ = harness(e)
        async def failed_send(chat_id, content, mode, meta):
            r.reply_sending(meta)
            raise TimeoutError('unknown delivery')
        s.send_fn = failed_send
        await r.accept(e)
        await drained(r)
        cfg = r.cfg
        sources = r.inbox.db.execute('SELECT * FROM submitted_sources').fetchall()
        await r.close()
        db = Inbox(inbox_path(cfg))
        assert db.recover_receipt(e['id'], action='retry', reason='Inspected send', acknowledge_duplicate_risk=True) == 'reply_pending'
        assert db.db.execute('SELECT * FROM submitted_sources').fetchall() == sources
        db.close()
        r, sdk, s, sent = harness(e)
        try:
            r.recover()
            await drained(r)
            assert len(sent) == 1
            assert sent[0][1] == 'Answer'
            assert not s._client.events
            assert sdk.loads == 0
        finally:
            await r.close()
    asyncio.run(scenario())


def test_retire_pending_initialization_does_not_replay_trigger_on_next_input():
    async def scenario():
        e = fixture()
        r, sdk, s, _ = harness(e)
        r.inbox.accept(Event.parse(e))
        cfg = r.cfg
        await r.close()
        db = Inbox(inbox_path(cfg))
        db.recover_receipt(e['id'], action='retire', reason='Skip stale input')
        db.close()
        r, sdk, s, sent = harness(e)
        try:
            await r.accept(live(e))
            await drained(r)
            assert sdk.loads == 0
            assert len(sent) == 1
            assert len(s._client.events) == 1
        finally:
            await r.close()
    asyncio.run(scenario())


def test_exhausted_pre_submission_failure_is_visible_and_retryable_without_ambiguity_ack():
    async def scenario():
        e = fixture()
        r, sdk, s, _ = harness(e)
        s._client = None
        s._ensure_client = AsyncMock(side_effect=RuntimeError('startup failed'))
        r.inbox.accept(Event.parse(e))
        try:
            for _ in range(6):
                await r._drain(e['companion']['scope_id'])
            assert inbox_summary(r.cfg)['blocked_conversations'] == 1
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'failed'
            assert not r.inbox.db.execute('SELECT * FROM activations').fetchall()
            assert r.inbox.recover_receipt(e['id'], action='retry', reason='Launcher repaired') == 'pending'
        finally:
            await r.close()
    asyncio.run(scenario())


def test_recovery_rejects_non_head_and_blank_reason_and_preserves_data(tmp_path):
    e = fixture()
    db = Inbox(tmp_path / 'inbox.sqlite3')
    try:
        db.accept(Event.parse(e))
        db.accept(Event.parse(live(e)))
        with pytest.raises(CompanionError, match='oldest'):
            db.recover_receipt(live(e)['id'], action='retire', reason='Skip')
        with pytest.raises(CompanionError, match='reason'):
            db.recover_receipt(e['id'], action='retire', reason=' ')
        assert db.db.execute('SELECT count(*) FROM events').fetchone()[0] == 2
        assert not db.db.execute('SELECT * FROM recovery_actions').fetchall()
    finally:
        db.close()


def test_cli_list_readonly_and_recover_excludes_foreground_gateway(monkeypatch, capsys):
    cfg = BridgeConfig(identity='test-agent')
    monkeypatch.setattr(cli, 'read_config', lambda: cfg)
    monkeypatch.setattr(daemon, '_maybe_load_env_file', lambda: None)
    monkeypatch.setattr(daemon, 'running_pid', lambda: None)
    e = fixture()
    db = Inbox(inbox_path(cfg))
    db.accept(Event.parse(e))
    db.state(e['id'], 'submitting')
    try:
        assert cli.main(['inbox', 'list']) == 0
        result = json.loads(capsys.readouterr().out)
        assert result['receipts'][0]['state'] == 'submitting'  # Reading does not normalize it.
        assert 'payload' not in result['receipts'][0]
        assert cli.main(['inbox', 'recover', e['id'], '--action', 'retire', '--reason', 'Skip']) == 1
        assert 'Another gateway' in capsys.readouterr().out
        assert not db.db.execute('SELECT * FROM recovery_actions').fetchall()
    finally:
        db.close()
    assert cli.main(['inbox', 'recover', e['id'], '--action', 'retire', '--reason', 'Skip']) == 0
    assert 'does not mean a reply was delivered' in capsys.readouterr().out


def test_legacy_inbox_migrates_without_inventing_receipt_age(tmp_path):
    cfg = BridgeConfig(identity='test-agent')
    path = inbox_path(cfg)
    path.parent.mkdir(parents=True)
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE events(event_id TEXT PRIMARY KEY,scope TEXT,sequence INTEGER,payload TEXT,state TEXT)")
    e = fixture()
    db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (e['id'], e['companion']['scope_id'], 1, json.dumps(e), 'uncertain'))
    db.commit()
    db.close()
    assert inbox_summary(cfg)['oldest_unfinished_age_s'] is None
    inbox = Inbox(path)
    try:
        assert inbox_summary(cfg)['blocked_conversations'] == 1
        assert inbox_summary(cfg)['oldest_unfinished_age_s'] is None
        inbox.recover_receipt(e['id'], action='retire', reason='Skip')
        assert inbox_summary(cfg)['unfinished_count'] == 0
    finally:
        inbox.close()


@pytest.mark.parametrize('state', ['pending', 'failed'])
def test_legacy_retired_head_with_uncertain_activation_requires_ack_before_retry(state):
    cfg = BridgeConfig(identity='test-agent')
    e = fixture()
    event, fresh = Event.parse(e), Event.parse(live(e))
    db = Inbox(inbox_path(cfg))
    try:
        db.accept(event)
        db.state(event.event_id, 'done')
        db.accept(fresh)
        db.state(fresh.event_id, state)
        with db.db:
            db.db.execute('INSERT INTO activations VALUES(?,?,?,?,?)', (
                event.scope, event.activation, event.source_id, event.author, 'uncertain'))
        assert inbox_summary(cfg)['blocked_conversations'] == 1
        with pytest.raises(CompanionError, match='duplicate'):
            db.recover_receipt(fresh.event_id, action='retry', reason='Inspect')
        assert db.activation(fresh)[2] == 'uncertain'
        assert not db.db.execute('SELECT * FROM recovery_actions').fetchall()
        db.recover_receipt(fresh.event_id, action='retire', reason='Skip')
        assert db.activation(fresh)[2] == 'retired'
        assert inbox_summary(cfg)['blocked_conversations'] == 0
    finally:
        db.close()


def test_receipt_age_and_readiness_endpoint_reflect_persisted_block(monkeypatch):
    import time
    from inkbox_codex.gateway import InkboxGateway
    cfg = BridgeConfig(identity='test-agent')
    event = Event.parse(fixture())
    db = Inbox(inbox_path(cfg))
    try:
        db.accept(event)
        db.state(event.event_id, 'uncertain')
        with db.db:
            db.db.execute('UPDATE events SET received_at=?', (time.time() - 60,))
        bridge = InkboxGateway(cfg)
        bridge.sessions = object()
        bridge._companion_receiver = object()
        bridge._codex_ready = True
        bridge._codex_checked_at = time.time()
        response = asyncio.run(bridge._handle_ready(None))
        assert response.status == 503
        summary = json.loads(response.text)['companion']
        assert summary['blocked_conversations'] == 1
        assert summary['oldest_unfinished_age_s'] >= 60
        db.recover_receipt(event.event_id, action='retire', reason='Skip')
        assert asyncio.run(bridge._handle_ready(None)).status == 200
    finally:
        db.close()

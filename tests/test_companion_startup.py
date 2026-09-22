"""Failed host startup stays retryable and preserves the chosen thread."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from inkbox.companion import CompanionResource
from inkbox_codex import sessions as sessions_mod
from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from tests.test_companion import fixture, harness, drained, live
from tests.test_sessions import make_session


def test_thread_start_rejection_does_not_become_uncertain_without_any_turn_submission(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    requests = []
    class RejectThreadStart(CodexAppServerClient):
        async def _ensure_process(self):
            pass
        async def _initialize(self):
            self._initialized = True
        async def _request(self, method, params):
            requests.append(method)
            assert method == 'thread/start', 'No model turn should be attempted'
            raise CodexAppServerError('Synthetic thread configuration rejected before any turn')
        async def disconnect(self):
            pass
    monkeypatch.setattr(sessions_mod, 'CodexAppServerClient', RejectThreadStart)
    async def scenario():
        e = fixture('mail')
        e['data']['message']['body'] = '@agent hello'
        r, sdk, s, sent = harness(e, reply_mode='mention')
        s._client = None
        history_gets = []
        class SnapshotHTTP:
            def get(self, path, params=None):
                history_gets.append((path, params))
                c = e['companion']
                return {
                    **{k: c[k] for k in ('scope_id', 'activation_id', 'conversation_id', 'channel', 'reply_context')},
                    'items': [entry for entry in c['history'] if entry['is_trigger']],
                    'history_complete': True, 'next_cursor': None,
                }
        r.client.companion = CompanionResource(SnapshotHTTP())
        try:
            await r.accept(e)
            await drained(r)
            first = r.inbox.db.execute('SELECT state FROM events').fetchone()[0]
            assert first == 'pending'
            r.retries.pop(e['companion']['scope_id']).cancel()
            await r._drain(e['companion']['scope_id'])
            second = r.inbox.db.execute('SELECT state FROM events').fetchone()[0]
            assert requests == ['thread/start', 'thread/start']
            assert not sent
            assert len(history_gets) == 4
            assert second == 'pending', 'A rejected thread/start must not count as an uncertain model submission'
        finally:
            await r.close()
    asyncio.run(scenario())


def test_resume_retry_preserves_original_thread(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    calls = []
    class FailFirstResume(CodexAppServerClient):
        async def _ensure_process(self):
            pass
        async def _initialize(self):
            self._initialized = True
        async def _request(self, method, params):
            calls.append((method, params.get('threadId')))
            if len(calls) == 1:
                raise CodexAppServerError('Synthetic transient resume rejection')
            if method == 'thread/resume':
                return {'thread': {'id': params['threadId']}}
            if method == 'thread/start':
                return {'thread': {'id': 'unintended-fresh-thread'}}
            if method == 'thread/inject_items':
                return {}
            raise AssertionError(method)
        async def disconnect(self):
            pass
    monkeypatch.setattr(sessions_mod, 'CodexAppServerClient', FailFirstResume)
    async def scenario():
        session = make_session([])
        session.resume_session_id = 'original-thread-with-history'
        try:
            with pytest.raises(CodexAppServerError):
                await session._ensure_client()
            client = await session._ensure_client()
            await client.append_context(['A later background message'])
            assert client.thread_id == 'original-thread-with-history'
            assert ('thread/start', None) not in calls
        finally:
            await session.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['mail', 'imessage'])
@pytest.mark.parametrize('quiet', [False, True])
def test_startup_recovers_once_without_losing_background_context(tmp_path, monkeypatch, channel, quiet):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    instances, inputs = [], []

    class Client:
        thread_id = None
        closed = False

        def __init__(self, *args, **kwargs):
            instances.append(self)

        async def connect(self, resume=None):
            if len(instances) <= 2:
                raise CodexAppServerError('Temporary startup rejection')
            self.thread_id = resume or 'recovered-thread'
            return self.thread_id

        async def disconnect(self):
            self.closed = True

        async def run(self, text):
            assert self.thread_id
            inputs.append(('run', text))
            return 'Recovered reply'

        async def append_context(self, messages):
            assert self.thread_id
            inputs.extend(('context', text) for text in messages)

    monkeypatch.setattr(sessions_mod, 'CodexAppServerClient', Client)

    async def scenario():
        e = fixture(channel)
        message = e['data']['message']
        message['body' if channel == 'mail' else 'content'] = '@agent hello'
        message['sender_access'] = 'sponsored' if quiet else 'direct'
        r, _, session, sent = harness(e, reply_mode='mention')
        session._client = None
        scope = e['companion']['scope_id']
        try:
            await r.accept(e)
            await drained(r)
            for _ in range(2):
                assert session._client is None
                assert instances[-1].closed
                assert not inputs
                assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'pending'
                r.retries.pop(scope).cancel()
                await r._drain(scope)
            assert [kind for kind, _ in inputs] == ['context' if quiet else 'run']
            assert len(sent) == (0 if quiet else 1)
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'done'
            assert not await r.accept(e)
            await drained(r)
            assert len(inputs) == 1
            following = live(e)
            following['data']['message']['sender_access'] = 'direct'
            await r.accept(following)
            await drained(r)
            assert len(inputs) == 2
            assert inputs[-1][0] == 'run'
            assert len(sent) == (1 if quiet else 2)
        finally:
            await r.close()
    asyncio.run(scenario())


def test_cancelled_startup_disposes_client_and_preserves_resume(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))

    async def scenario():
        entered, released = asyncio.Event(), asyncio.Event()
        instances, resumes = [], []

        class Client:
            def __init__(self, *args, **kwargs):
                self.closed = False
                instances.append(self)

            async def connect(self, resume):
                resumes.append(resume)
                entered.set()
                await released.wait()
                return resume

            async def disconnect(self):
                self.closed = True

        monkeypatch.setattr(sessions_mod, 'CodexAppServerClient', Client)
        session = make_session([])
        session.resume_session_id = 'saved-thread'
        task = asyncio.create_task(session._ensure_client())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert instances[0].closed
            assert session._client is None
            released.set()
            assert await session._ensure_client() is instances[1]
            assert resumes == ['saved-thread', 'saved-thread']
        finally:
            await session.close()
    asyncio.run(scenario())


def test_failed_submitted_turn_still_pauses_without_replay(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))

    async def scenario():
        e = fixture()
        r, _, session, sent = harness(e)
        session._client.run = AsyncMock(side_effect=CodexAppServerError('Turn failed after submission'))
        try:
            await r.accept(e)
            await drained(r)
            assert r.inbox.db.execute('SELECT state FROM events').fetchone()[0] == 'uncertain'
            await r.accept(e)
            await drained(r)
            session._client.run.assert_awaited_once()
            assert not sent
        finally:
            await r.close()

    asyncio.run(scenario())

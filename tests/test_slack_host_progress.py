"""Host activity stays on its admitted native or Companion Slack source."""

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex.slack import send_reply
from inkbox_codex.slack_activity import SlackActivity
from tests.test_sessions import make_session
from tests.test_slack_activity import route


@pytest.mark.parametrize('companion', [False, True])
def test_host_tool_events_update_one_progress_message_without_consuming_final(tmp_path, monkeypatch, companion):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    async def scenario():
        sdk = NS(send_message=Mock(return_value=NS(status='sent', message_ts='1234567890.000010')),
                 update_message=Mock(return_value=NS(status='succeeded')),
                 set_processing_status=Mock(return_value=NS(status='succeeded')))
        tracker = SlackActivity(sdk, tmp_path / 'activity.json')
        tracker._progress.interval = 0
        meta = route()
        if companion:
            meta.update(companion=True, sender_access="direct", companion_envelope={'event_type': 'slack.mention_received'})
        started, finished = asyncio.Event(), asyncio.Event()
        session = make_session([])
        session.turn_activity_fn = tracker.notify
        session.turn_progress_fn = tracker.progress
        async def send(chat, text, mode, original):
            send_reply(NS(slack=sdk), original, text)
        session.send_fn = send
        class Client:
            thread_id = 'thread-1'
            activity = None
            async def run(self, text, *, activity_handler):
                self.activity = activity_handler
                activity_handler('commandExecution', 'private arguments must not appear')
                started.set()
                await finished.wait()
                return '*Result*: ready.'
        client = Client()
        session._client = client
        async def admitted():
            return None
        if companion:
            submitted = asyncio.create_task(session.submit_companion('work', 'slack', meta,
                                                                     before_submit=admitted))
        else:
            await session.handle_inbound('work', 'slack', meta)
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.sleep(0)
        await tracker.flush()
        assert sdk.send_message.call_args.kwargs['text'] == 'Running a command…'
        # Later mutable session routing cannot redirect an old host callback.
        session.reply_meta = {**meta, 'conversation_id': 'COTHER', 'source_event_id': 'later'}
        client.activity('mcpToolCall', 'inkbox_slack_search')
        await asyncio.sleep(0)
        await tracker.flush()
        assert sdk.update_message.call_args.args[:3] == (meta['connection_id'], 'C123', '1234567890.000010')
        assert sdk.update_message.call_args.args[3] == 'Searching Slack…'
        finished.set()
        if companion:
            answer = await asyncio.wait_for(submitted, 1)
            await send(session.chat_id, answer, 'slack', meta)
            await tracker.notify(session.chat_id, 'slack', meta, 'completed')
        await asyncio.wait_for(session._worker, 1)
        await tracker.flush()
        assert sdk.send_message.call_count == 2
        assert sdk.send_message.call_args.kwargs['text'] == '*Result*: ready.'
        assert sdk.send_message.call_args.kwargs['conversation_id'] == 'C123'
        assert sdk.update_message.call_args.args[3] == 'Completed.'
        await tracker.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('stage', ['connecting', 'admitting'])
def test_stop_before_host_submission_drops_only_original_request(tmp_path, monkeypatch, stage):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()
        session = make_session([])
        inputs = []
        class Client:
            thread_id = 'thread-1'
            async def run(self, text):
                inputs.append(text)
                return 'Answer'
            async def interrupt(self):
                raise AssertionError('No native turn has been submitted')
        client = Client()
        async def ensure():
            if stage == 'connecting' and not release.is_set():
                started.set()
                await release.wait()
            return client
        session._ensure_client = ensure
        async def admit():
            started.set()
            await release.wait()
        meta = {**route(), 'actor_id': 'U123', 'workspace_id': 'T123'}
        if stage == 'admitting':
            pending = asyncio.create_task(session.submit_companion('original', 'slack', {
                **meta, 'companion': True, 'sender_access': 'direct',
                'companion_envelope': {'event_type': 'slack.mention_received'}}, before_submit=admit))
        else:
            await session.handle_inbound('original', 'slack', route())
        await asyncio.wait_for(started.wait(), 1)
        if stage == 'admitting':
            from inkbox_codex.companion import Receiver
            receiver = Receiver.__new__(Receiver)
            receiver.active_sessions = {session}
            receiver.sender_allowed = lambda *args: True
            assert await receiver.stop_slack({**meta, 'actor_id': 'UOTHER'})
            assert not session._interrupting
            assert await receiver.stop_slack(meta)
        else:
            await session.handle_inbound('/stop', 'slack', route('stop'))
        release.set()
        await asyncio.wait_for(session._worker, 1)
        if stage == 'admitting':
            assert await asyncio.wait_for(pending, 1) is None
        assert inputs == []
        await session.handle_inbound('follow-up', 'slack', route('next'))
        await asyncio.wait_for(session._worker, 1)
        assert len(inputs) == 1 and 'follow-up' in inputs[0]
    asyncio.run(scenario())


def test_stop_before_timeout_wrapper_schedules_model_cannot_start_it(tmp_path, monkeypatch):
    from inkbox_codex.sessions import _Turn
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    async def scenario():
        session = make_session([])
        session.cfg.codex_turn_timeout_s = 10
        inputs = []
        class Client:
            thread_id = 'thread-1'
            async def run(self, text):
                inputs.append(text)
                return 'Answer'
            async def interrupt(self):
                return None  # No turn/start has been written yet.
        session._client = Client()
        original = asyncio.wait_for
        stopped = False
        async def scheduled(operation, timeout):
            nonlocal stopped
            if not stopped:
                stopped = True
                await session._cancel_pending_turn()
            return await original(operation, timeout)
        # Python versions that wrap the coroutine in a task have this boundary.
        monkeypatch.setattr(asyncio, 'wait_for', scheduled)
        await session._run_turn(_Turn(text='original', reply_mode='slack', reply_meta=route()))
        assert inputs == []
        await session._run_turn(_Turn(text='follow-up', reply_mode='slack', reply_meta=route('next')))
        assert inputs == ['follow-up']
    asyncio.run(scenario())


@pytest.mark.parametrize('acquired', [False, True])
def test_progress_recovery_requires_exclusive_gateway_ownership(tmp_path, monkeypatch, acquired):
    from inkbox_codex import gateway, slack_activity
    from inkbox_codex.config import BridgeConfig
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    sdk = NS(slack=NS(), get_identity=lambda _: NS(id='00000000-0000-4000-8000-000000000001',
                                                  agent_handle='agent'))
    monkeypatch.setattr(gateway, 'Inkbox', lambda **kwargs: sdk)
    order = []
    async def recover():
        order.append('recover')
        raise RuntimeError('recovery-stop')
    tracker = NS(recover=recover, notify=AsyncMock(), progress=AsyncMock())
    monkeypatch.setattr(slack_activity, 'SlackActivity', lambda *args, **kwargs: tracker)
    instance = gateway.InkboxGateway(BridgeConfig(api_key='synthetic-key', identity='agent',
        slack_enabled=True, public_url='https://bridge.example.test'))
    instance._start_http_server = AsyncMock()
    def acquire():
        order.append('acquire')
        if not acquired:
            raise RuntimeError('already-running')
        return NS()
    instance._companion = acquire
    with pytest.raises(RuntimeError, match='recovery-stop' if acquired else 'already-running'):
        asyncio.run(instance.run())
    assert order == (['acquire', 'recover'] if acquired else ['acquire'])

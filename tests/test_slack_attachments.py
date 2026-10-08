"""Local uploads retain their original source across tool-process suspension."""

import asyncio
import base64
import json
from pathlib import Path
import os
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.slack import inbound_message, run_tool
from inkbox_codex import slack_turns, tools


@pytest.fixture
def upload_case(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    monkeypatch.setenv('INKBOX_CODEX_CHAT_ID', 'slack:original')
    cfg = BridgeConfig(identity='agent', base_url='https://api.example.test',
                       project_dir=str(tmp_path), slack_enabled=True)
    monkeypatch.setattr(tools, 'read_config', lambda: cfg)
    monkeypatch.setattr('inkbox_codex.config.read_config', lambda: cfg)
    data = dict(identity_id='00000000-0000-4000-8000-000000000001',
                connection_id='00000000-0000-4000-8000-000000000002', workspace_id='T123',
                conversation_id='C123', thread_ts='1234567890.000001', message_ts='1234567890.000002',
                actor_id='U123', message_kinds=['channel', 'mention'],
                event={'type': 'message', 'text': '<@UBOT> Send a chart'})
    _, _, meta = inbound_message({'id': '00000000-0000-4000-8000-000000000003',
        'event_type': 'slack.mention_received', 'data': data}, data['identity_id'])
    assert meta['identity_id'] == data['identity_id']
    context = slack_turns.begin('slack:original', cfg, meta)
    path = tmp_path / 'chart.png'
    path.write_bytes(b'\x89PNG\r\n\x1a\n\x00\xff')
    sdk = NS(list_connections=Mock(return_value=NS(connections=[NS(id=meta['connection_id'],
        identity_id=meta['identity_id'], workspace_id='T123', status='connected')])),
        upload_file=Mock(return_value=NS(id='00000000-0000-4000-8000-000000000004',
            operation='file_upload', status='succeeded', connection_id=meta['connection_id'],
            conversation_id='C123', thread_ts=meta['thread_ts'], file_id='F123')))
    client = NS(slack=sdk, get_identity=Mock(return_value=NS(id=meta['identity_id'])))
    args = {key: meta[key] for key in ('connection_id', 'conversation_id', 'thread_ts', 'source_event_id')}
    args.update(file_path='chart.png', idempotency_key='attachment-1')
    return NS(cfg=cfg, meta=meta, context=context, path=path, sdk=sdk, client=client, args=args)


def send(case, **changes):
    args = {**case.args, **changes}
    context = slack_turns.capture('agent', case.cfg, args)
    return run_tool(case.client, 'agent', 'inkbox_slack_upload_file', args, upload_context=context)


def test_binary_file_is_uploaded_once_to_original_thread(upload_case):
    c = upload_case
    result = send(c)
    assert result['status'] == 'succeeded' and result['file_id'] == 'F123'
    assert send(c) == result
    c.sdk.upload_file.assert_called_once()
    call = c.sdk.upload_file.call_args
    assert call.args == (c.meta['connection_id'],)
    assert call.kwargs['conversation_id'] == 'C123'
    assert call.kwargs['thread_ts'] == c.meta['thread_ts']
    assert base64.b64decode(call.kwargs['content_base64']) == c.path.read_bytes()
    assert call.kwargs['filename'] == 'chart.png'
    assert 'content_base64' not in result and 'file_path' not in result


@pytest.mark.parametrize('companion', [False, True])
@pytest.mark.parametrize('status', ['succeeded', 'failed', 'unknown'])
def test_real_inbound_session_authorizes_tool_and_preserves_final(upload_case, companion, status):
    from tests.test_sessions import make_session
    c = upload_case
    slack_turns.end(c.context)
    slack_turns._path('slack:original', c.meta['source_event_id']).unlink()
    c.sdk.upload_file.return_value.status = status
    c.sdk.upload_file.return_value.file_id = 'F123' if status == 'succeeded' else None
    async def scenario():
        sent = []
        session = make_session(sent)
        session.chat_id, session.cfg = 'slack:original', c.cfg
        meta = {**c.meta, 'sender_access': 'direct'}
        if companion:
            meta.update(companion=True, companion_scope_id='scope-1', companion_activation_id='activation-1',
                        companion_conversation_id='conversation-1',
                        companion_envelope={'event_type': 'slack.mention_received'})
            c.client.companion = NS(activation_messages=Mock(return_value=NS(scope_id='scope-1',
                activation_id='activation-1', conversation_id='conversation-1', channel='slack',
                reply_context={'connection_id': meta['connection_id'], 'slack_conversation_id': 'C123'})))
        answer = 'Chart delivered.' if status == 'succeeded' else 'The chart delivery could not be confirmed.'
        class Host:
            thread_id = 'host-thread'
            async def run(self, text):
                assert meta['source_event_id'] in text
                result = await tools.call_inkbox_tool(c.client, 'agent', 'inkbox_slack_upload_file', c.args)
                assert not result.get('isError'), result
                assert json.loads(result['content'][0]['text'])['status'] == status
                return answer
        session._client = Host()
        async def before_submit():
            return None
        if companion:
            assert await session.submit_companion('Send chart', 'slack', meta, before_submit=before_submit) == answer
            assert not sent  # The Companion receiver owns final delivery.
        else:
            await session.handle_inbound('Send chart', 'slack', meta)
            await session._worker
            assert sent[-1][1] == answer
            assert sent[-1][3]['source_event_id'] == meta['source_event_id']
        assert session._slack_context is None
        c.sdk.upload_file.assert_called_once()
    asyncio.run(scenario())


def test_owned_disconnected_connection_remains_inspectable_without_upload(upload_case):
    c = upload_case
    c.sdk.list_connections.return_value.connections[0].status = 'disconnected'
    c.sdk.get_operation = Mock(return_value=c.sdk.upload_file.return_value)
    result = run_tool(c.client, 'agent', 'inkbox_slack_get_operation', {
        'connection_id': c.meta['connection_id'], 'operation_id': c.sdk.upload_file.return_value.id})
    assert result['status'] == 'succeeded'
    c.sdk.get_operation.assert_called_once()
    with pytest.raises(PermissionError, match='not uniquely active'):
        send(c)
    c.sdk.upload_file.assert_not_called()


def test_reused_model_key_is_scoped_to_distinct_original_requests(upload_case):
    c = upload_case
    first = send(c)
    slack_turns.end(c.context)
    c.meta = {**c.meta, 'source_event_id': 'next-request'}
    c.context = slack_turns.begin('slack:original', c.cfg, c.meta)
    c.args['source_event_id'] = c.meta['source_event_id']
    c.path.write_bytes(b'a different attachment')
    second = send(c)
    assert first['idempotency_key'] != second['idempotency_key']
    assert c.sdk.upload_file.call_count == 2
    assert send(c) == second
    assert c.sdk.upload_file.call_count == 2


@pytest.mark.parametrize('supported', [False, True])
def test_lost_upload_response_returns_inspection_key_without_resend(upload_case, supported):
    c = upload_case
    c.sdk.upload_file.side_effect = TimeoutError()
    lost = send(c)
    assert lost['status'] == 'unknown' and 'id' not in lost
    args = {'connection_id': c.meta['connection_id'], 'idempotency_key': lost['idempotency_key']}
    if not supported:
        with pytest.raises(ValueError, match='cannot inspect an operation without its ID'):
            run_tool(c.client, 'agent', 'inkbox_slack_get_operation', args)
    else:
        class Lookup:
            def get_operation_by_key(self, connection_id, *, idempotency_key):
                assert idempotency_key == lost['idempotency_key']
                return NS(id='00000000-0000-4000-8000-000000000004', connection_id=connection_id,
                          conversation_id='C123', operation='file_upload', status='succeeded', file_id='F123')
        sdk = Lookup()
        sdk.__dict__.update(vars(c.sdk))
        c.client.slack = sdk
        assert run_tool(c.client, 'agent', 'inkbox_slack_get_operation', args)['status'] == 'succeeded'
        assert send(c)['status'] == 'succeeded'
    assert c.sdk.upload_file.call_count == 1


def test_read_only_inspection_survives_local_receipt_write_failure(upload_case, monkeypatch):
    c = upload_case
    c.sdk.upload_file.return_value.status = 'unknown'
    receipt = send(c)
    c.sdk.get_operation = Mock(return_value=NS(**{
        **vars(c.sdk.upload_file.return_value), 'status': 'failed'}))
    monkeypatch.setattr(slack_turns, '_write_context', Mock(side_effect=OSError('storage unavailable')))
    result = run_tool(c.client, 'agent', 'inkbox_slack_get_operation', {
        'connection_id': c.meta['connection_id'], 'operation_id': receipt['id']})
    assert result['status'] == 'failed'
    assert c.sdk.upload_file.call_count == 1


@pytest.mark.parametrize('status', ['unknown', 'in_progress', 'timeout'])
def test_unknown_upload_never_resends_or_migrates_to_followup(upload_case, status):
    c = upload_case
    if status == 'timeout':
        c.sdk.upload_file.side_effect = TimeoutError()
        result = send(c)
        assert result['status'] == 'unknown' and result['idempotency_key'].startswith('codex:upload:')
        assert 'do not upload again' in result['inspection']
    else:
        c.sdk.upload_file.return_value.status = status
        assert send(c)['status'] == status
    with pytest.raises(ValueError, match='unresolved'):
        send(c, idempotency_key='new-key')
    assert send(c)['status'] in {'unknown', 'in_progress'}
    slack_turns.end(c.context)
    newer = {**c.meta, 'source_event_id': '00000000-0000-4000-8000-000000000005'}
    slack_turns.begin('slack:original', c.cfg, newer)
    with pytest.raises(ValueError, match='No matching active'):
        send(c)
    assert c.sdk.upload_file.call_count == 1


@pytest.mark.parametrize('changed', ['connection_id', 'conversation_id', 'thread_ts', 'source_event_id'])
def test_changed_source_cannot_borrow_active_authority(upload_case, changed):
    with pytest.raises(ValueError):
        send(upload_case, **{changed: 'different'})
    upload_case.sdk.upload_file.assert_not_called()


def test_stop_during_file_preparation_prevents_upload(upload_case, monkeypatch):
    from inkbox_codex import slack
    c = upload_case
    original = slack.file_payload
    def prepare(*args, **kwargs):
        payload = original(*args, **kwargs)
        slack_turns.end(c.context)
        return payload
    monkeypatch.setattr(slack, 'file_payload', prepare)
    with pytest.raises(ValueError, match='ended'):
        send(c)
    c.sdk.upload_file.assert_not_called()


def test_tool_executor_suspension_retains_original_source(upload_case, monkeypatch):
    c = upload_case
    async def suspended(fn, *args, **kwargs):
        slack_turns.end(c.context)
        slack_turns.begin('slack:original', c.cfg, {**c.meta,
            'source_event_id': '00000000-0000-4000-8000-000000000005'})
        return fn(*args, **kwargs)
    monkeypatch.setattr(asyncio, 'to_thread', suspended)
    result = asyncio.run(tools.call_inkbox_tool(c.client, 'agent', 'inkbox_slack_upload_file', c.args))
    assert result['isError'] is True
    c.sdk.upload_file.assert_not_called()


def test_companion_upload_rechecks_original_activation(upload_case):
    c = upload_case
    slack_turns.end(c.context)
    c.meta.update(companion=True, companion_scope_id='scope-1', companion_activation_id='activation-1',
                  companion_conversation_id='conversation-1', source_event_id='companion-source')
    c.args['source_event_id'] = c.meta['source_event_id']
    c.context = slack_turns.begin('slack:original', c.cfg, c.meta)
    page = NS(scope_id='scope-1', activation_id='activation-2', conversation_id='conversation-1', channel='slack',
              reply_context={'connection_id': c.meta['connection_id'], 'slack_conversation_id': 'C123'})
    c.client.companion = NS(activation_messages=Mock(return_value=page))
    with pytest.raises(ValueError, match='original Companion activation'):
        send(c)
    c.sdk.upload_file.assert_not_called()
    page.activation_id = 'activation-1'
    assert send(c)['status'] == 'succeeded'
    c.client.companion.activation_messages.assert_called_with('agent', 'activation-1', limit=1)


def test_no_upload_without_durable_intent(upload_case, monkeypatch):
    c = upload_case
    monkeypatch.setattr(slack_turns, '_write_context', Mock(side_effect=OSError('disk unavailable')))
    with pytest.raises(OSError):
        send(c)
    c.sdk.upload_file.assert_not_called()


def test_corrupt_source_receipt_is_not_recreated(upload_case):
    c = upload_case
    slack_turns._path('slack:original', c.meta['source_event_id']).write_text('{broken')
    with pytest.raises(ValueError, match='cannot be replayed'):
        slack_turns.begin('slack:original', c.cfg, c.meta)
    with pytest.raises(ValueError, match='No matching active'):
        send(c)
    c.sdk.upload_file.assert_not_called()


@pytest.mark.parametrize('kind', ['empty', 'too-large', 'directory', 'filename'])
def test_file_validation_precedes_external_effect(upload_case, kind):
    c = upload_case
    if kind == 'empty':
        c.path.write_bytes(b'')
    elif kind == 'too-large':
        with c.path.open('wb') as stream:
            stream.truncate(10 * 1024 * 1024 + 1)
    elif kind == 'directory':
        c.args['file_path'] = str(c.path.parent)
    else:
        c.args['filename'] = '../chart.png'
    with pytest.raises((ValueError, OSError)):
        send(c)
    c.sdk.upload_file.assert_not_called()


def test_restart_retires_tool_authority_but_preserves_receipts(upload_case):
    c = upload_case
    send(c)
    slack_turns.retire(c.cfg)
    with pytest.raises(ValueError, match='No matching active'):
        send(c)
    assert slack_turns._read_context(slack_turns._path('slack:original', c.meta['source_event_id']))['uploads']


def test_failed_stop_checkpoint_still_stops_host_and_cannot_borrow_next_turn(upload_case, monkeypatch):
    from tests.test_sessions import make_session
    c = upload_case
    async def scenario():
        session = make_session([])
        session._slack_context = c.context
        session._turn_active = True
        session._client = NS(interrupt=AsyncMock())
        with monkeypatch.context() as patch:
            patch.setattr(slack_turns, 'end', Mock(side_effect=OSError('storage unavailable')))
            await session._cancel_pending_turn()
        session._client.interrupt.assert_awaited_once()
        assert session._interrupting
        # Even if the old receipt could not be closed, a new durable owner wins.
        slack_turns.begin('slack:original', c.cfg, {**c.meta, 'source_event_id': 'next-source'})
        with pytest.raises(ValueError, match='No matching active'):
            send(c)
        c.sdk.upload_file.assert_not_called()
    asyncio.run(scenario())


def test_relative_file_uses_original_project_root(upload_case, monkeypatch, tmp_path):
    other = tmp_path / 'other'
    other.mkdir()
    monkeypatch.chdir(other)
    assert send(upload_case)['status'] == 'succeeded'


def test_published_sdk_upload_wire_keeps_binary_source_and_receipt(upload_case):
    import httpx
    from inkbox import Inkbox
    c = upload_case
    requests = []
    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={
            'id': '00000000-0000-4000-8000-000000000004', 'connection_id': c.meta['connection_id'],
            'conversation_id': 'C123', 'operation': 'file_upload', 'status': 'succeeded', 'file_id': 'F123'})
    client = Inkbox(api_key='synthetic-test-key', base_url='https://api.example.test')
    client._api_http._client.close()
    client._api_http._client = httpx.Client(base_url='https://api.example.test/api/v1',
        headers={'X-API-Key': 'synthetic-test-key'}, transport=httpx.MockTransport(handle))
    client.get_identity = c.client.get_identity
    client.slack.list_connections = c.sdk.list_connections
    c.client = client
    try:
        assert send(c)['file_id'] == 'F123'
        assert send(c)['status'] == 'succeeded'
        assert len(requests) == 1
        request = requests[0]
        assert request.method == 'POST' and request.url.path.endswith('/files')
        assert request.headers['Idempotency-Key'].startswith('codex:upload:')
        body = json.loads(request.content)
        assert base64.b64decode(body['content_base64']) == c.path.read_bytes()
        assert body['conversation_id'] == 'C123' and body['thread_ts'] == c.meta['thread_ts']
        assert 'file_path' not in body and 'source_event_id' not in body
    finally:
        client.close()


def test_separate_mcp_process_uses_shared_original_source(upload_case, tmp_path):
    c = upload_case
    received = tmp_path / 'received.json'
    script = '''
import asyncio,json,os,sys
from pathlib import Path
from types import SimpleNamespace as NS
from inkbox_codex.mcp_stdio import InkboxMcpServer
args=json.loads(sys.argv[1])
identity='00000000-0000-4000-8000-000000000001'
def upload(connection, **payload):
    Path(sys.argv[2]).write_text(json.dumps(payload))
    return NS(id='00000000-0000-4000-8000-000000000004', operation='file_upload',
              status='succeeded', connection_id=connection, conversation_id=payload['conversation_id'], file_id='F123')
sdk=NS(list_connections=lambda _: NS(connections=[NS(id=args['connection_id'],identity_id=identity,
       workspace_id='T123',status='connected')]),upload_file=upload)
server=InkboxMcpServer()
server._client=NS(slack=sdk,get_identity=lambda _: NS(id=identity))
result=asyncio.run(server.handle({'method':'tools/call','id':1,
    'params':{'name':'inkbox_slack_upload_file','arguments':args}}))
print(json.dumps(result))
'''
    env = {key: os.environ[key] for key in ('PATH', 'SYSTEMROOT') if key in os.environ}
    env.update(INKBOX_CODEX_HOME=str(tmp_path), INKBOX_CODEX_CHAT_ID='slack:original',
               INKBOX_SLACK_ENABLED='1', INKBOX_IDENTITY='agent', INKBOX_API_KEY='synthetic-test-key',
               INKBOX_BASE_URL=c.cfg.base_url, PYTHONDONTWRITEBYTECODE='1',
               PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    child = subprocess.run([sys.executable, '-c', script, json.dumps(c.args), str(received)],
                           cwd=tmp_path, env=env, text=True, capture_output=True, timeout=10, check=True)
    response = json.loads(child.stdout)['result']
    assert not response.get('isError'), response
    receipt = json.loads(response['content'][0]['text'])
    assert receipt['status'] == 'succeeded' and receipt['file_id'] == 'F123'
    payload = json.loads(received.read_text())
    assert base64.b64decode(payload['content_base64']) == c.path.read_bytes()
    assert payload['thread_ts'] == c.meta['thread_ts']
    assert send(c) == receipt
    c.sdk.upload_file.assert_not_called()

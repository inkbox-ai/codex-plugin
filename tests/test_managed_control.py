"""Local control never exposes scheduling mutations on public ingress."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientSession, UnixConnector

from inkbox_codex.config import BridgeConfig
from inkbox_codex.managed import ManagedControl
from inkbox_codex.schedules import ScheduleStore, schedule_path


def test_stale_schedule_rejection_is_proven_but_existing_execution_is_preserved(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    monkeypatch.setattr('inkbox_codex.schedules.time.time', lambda: 1060)
    cfg = BridgeConfig(identity='test-agent', project_dir=str(tmp_path))
    control = ManagedControl(SimpleNamespace(cfg=cfg), tmp_path / 'control.sock')
    with ScheduleStore(schedule_path(cfg)) as store:
        store.use_external_timer()
        saved = store.create('Example', 'Check', {'kind': 'interval', 'seconds': 60}, now=1000)
        revision = store.registrations()[0]['revision']
        store.acknowledge_registration(saved['id'], revision)
        command = {'action': 'invoke_schedule', 'job_id': saved['id'], 'revision': revision,
                   'occurrence_id': 'original', 'due': 1060}
        accepted = control.schedules(command)
        assert accepted['status'] == 'pending'
        store.update(saved['id'], prompt='Changed', now=1060)
        replay = control.schedules(command)
        assert replay['run_id'] == accepted['run_id']
        assert replay['status'] == 'cancelled'
        assert control.schedules({**command, 'occurrence_id': 'stale', 'due': 1120}) == {
            'status': 'rejected', 'execution_stopped': True, 'reason': 'schedule_changed_or_not_due'}
        assert len(store.runs(saved['id'])) == 1
        store.delete(saved['id'])
        assert control.schedules({**command, 'occurrence_id': 'deleted', 'due': 1180}) == {
            'status': 'rejected', 'execution_stopped': True, 'reason': 'schedule_changed_or_not_due'}
        assert control.schedules(command)['run_id'] == accepted['run_id']


def test_private_socket_dispatches_once_and_returns_bounded_display_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path / 'state'))
    monkeypatch.setenv('INKBOX_MANAGED_OWNER_ID', 'test-owner')
    cfg = BridgeConfig(identity='test-agent', project_dir=str(tmp_path))
    with ScheduleStore(schedule_path(cfg)) as store:
        store.use_external_timer()
        job = store.create('Example', 'private prompt', {'kind': 'interval', 'seconds': 60}, now=1000)
        registration, = store.registrations()
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    control = ManagedControl(SimpleNamespace(cfg=cfg, sessions=SimpleNamespace(sessions={}), _managed_draining=False, _managed_ingress=0, managed_status=AsyncMock(return_value={'ready': True})), private / 'control.sock')

    async def scenario():
        await control.start()
        try:
            assert control.path.stat().st_mode & 0o777 == 0o600
            async with ClientSession(connector=UnixConnector(path=str(control.path))) as client:
                async def call(data):
                    async with client.post('http://localhost/control', json=data) as response:
                        return response.status, await response.json()
                code, metadata = await call({'action': 'schedules'})
                assert code == 200
                assert metadata['schedules'][0]['display']['task']['prompt'] == 'private prompt'
                status, health = await call({'action': 'status'})
                assert status == 200 and health['ready']
                assert health['capabilities']['normalized_chat'] == 1
                assert (await call({'action': 'acknowledge_schedule', 'job_id': job['id'], 'revision': registration['revision']}))[0] == 200
                dispatch = {'action': 'invoke_schedule', 'job_id': job['id'], 'revision': registration['revision'],
                            'occurrence_id': 'test-occurrence', 'due': 1060}
                first = await call(dispatch)
                assert first[0] == 200
                assert await call(dispatch) == first
                assert set(first[1]) == {'run_id', 'status', 'execution'}
                assert first[1]['execution'] == {
                    'native_run_id': first[1]['run_id'], 'started_at': None,
                    'finished_at': None, 'output_source_ids': [],
                }
                assert (await call({'action': 'drain'}))[0] == 200
                assert (await call(dispatch))[0] == 503
                assert (await call({'action': 'resume'}))[0] == 200
                assert await call(dispatch) == first
                assert (await call({'action': 'status', 'unexpected': 'secret'}))[0] == 409
                assert (await call({'action': 'invoke_schedule', 'job_id': [], 'revision': True}))[0] == 409
        finally:
            await control.close()
        assert not control.path.exists()
    asyncio.run(scenario())


@pytest.mark.parametrize('kind', ['public_directory', 'existing_file', 'symlink'])
def test_socket_refuses_unsafe_paths(tmp_path, kind):
    directory = tmp_path / 'private'
    directory.mkdir(mode=0o700)
    path = directory / 'control.sock'
    if kind == 'public_directory':
        directory.chmod(0o755)
    elif kind == 'existing_file':
        path.write_text('preserve')
    else:
        path.symlink_to(directory / 'missing')
    control = ManagedControl(None, path)
    with pytest.raises(ValueError):
        asyncio.run(control.start())
    if kind == 'existing_file':
        assert path.read_text() == 'preserve'
    if kind == 'symlink':
        assert path.is_symlink()


def test_pending_registration_is_honest_and_acknowledgement_clears_it(tmp_path):
    with ScheduleStore(tmp_path / 'jobs.db') as store:
        store.use_external_timer()
        job = store.create('Example', 'Do work', {'kind': 'interval', 'seconds': 60})
        assert job['registration_status'] == 'pending'
        assert 'not yet confirmed' in job['notice']
        store.acknowledge_registration(job['id'], job['registration_revision'])
        saved = store.get(job['id'])
        assert saved['registration_status'] == 'registered'
        assert 'notice' not in saved


def test_rejection_is_durable_not_retried_and_edit_can_register_same_job(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    cfg = BridgeConfig(identity='test-agent', project_dir=str(tmp_path))
    control = ManagedControl(SimpleNamespace(cfg=cfg), tmp_path / 'control.sock')
    with ScheduleStore(schedule_path(cfg)) as store:
        store.use_external_timer()
        job = store.create('Example', 'Check', {'kind': 'interval', 'seconds': 300}, now=1000)
        revision = job['registration_revision']
        control.schedules({'action': 'acknowledge_schedule', 'job_id': job['id'], 'revision': revision,
                           'error': 'Minimum interval is 10 minutes (600 seconds).'})
        rejected = store.get(job['id'])
        assert rejected['registration_status'] == 'rejected'
        assert '10 minutes' in rejected['notice']
        assert not store.registrations()[0]['registered']
        assert control.schedules({'action': 'schedules'}) == {'schedules': []}
        assert control.schedules({'action': 'invoke_schedule', 'job_id': job['id'], 'revision': revision,
                                  'occurrence_id': 'rejected', 'due': 1300})['status'] == 'rejected'
        edited = store.update(job['id'], schedule={'kind': 'interval', 'seconds': 600}, now=1100)
        assert edited['registration_status'] == 'pending' and edited['registration_error'] is None
        pending, = control.schedules({'action': 'schedules'})['schedules']
        assert pending['job_id'] == job['id'] and pending['revision'] > revision
        control.schedules({'action': 'acknowledge_schedule', 'job_id': job['id'], 'revision': pending['revision']})
        assert store.get(job['id'])['registration_status'] == 'registered'
        assert control.schedules({'action': 'schedules'}) == {'schedules': []}
    with ScheduleStore(schedule_path(cfg)) as restarted:
        assert restarted.get(job['id'])['registration_status'] == 'registered'
        assert len(restarted.list_jobs()) == 1


def test_drain_fences_new_webhooks_without_interrupting_admitted_ingress():
    from inkbox_codex.gateway import InkboxGateway

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        async def accept(request):
            entered.set()
            await release.wait()
            return 'accepted'
        gateway = SimpleNamespace(_managed_draining=False, _managed_ingress=0, _accept_webhook=accept)
        task = asyncio.create_task(InkboxGateway._handle_webhook(gateway, None))
        await entered.wait()
        assert gateway._managed_ingress == 1
        gateway._managed_draining = True
        assert (await InkboxGateway._handle_webhook(gateway, None)).status == 503
        assert not task.done()
        release.set()
        assert await task == 'accepted'
        assert gateway._managed_ingress == 0
    asyncio.run(scenario())


def test_unicode_questions_fit_actual_handler_wire_budget(tmp_path):
    import json
    from inkbox_codex.managed_chat import ChatRuntime
    from tests.test_managed_chat import spec
    async def scenario():
        ready = asyncio.Event()
        async def run(value, emit, ask):
            ready.set()
            await ask({"kind": "question", "fields": [
                {"id": f"q{i}", "type": "text", "label": "🙂" * 1900} for i in range(2)
            ]})
            return {"text": "Done"}
        async def cancel(_):
            pass
        runtime = ChatRuntime(tmp_path / 'chat.sqlite', 'owner', run, cancel)
        control = ManagedControl(SimpleNamespace(_managed_draining=False), tmp_path / 'control.sock')
        control.chat = runtime
        try:
            await runtime.control({"action": "chat_start", "spec": spec()})
            await ready.wait()
            async def body():
                return {"action": "chat_events", "run_id": "turn", "after": 0}
            response = await control.handle(SimpleNamespace(json=body))
            assert response.status == 200
            assert len(response.body) <= 65536
            value = json.loads(response.body)
            assert value['receipt']['requests'][0]['payload']['fields'][0]['label'] == '🙂' * 1900
            assert any(e['kind'] == 'interaction.created' for e in value['events'])
        finally:
            await runtime.close()
    asyncio.run(scenario())

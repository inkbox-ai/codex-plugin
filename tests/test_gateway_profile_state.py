"""Separate profiles must not share or clear each other's runtime state."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from inkbox_codex import gateway
from inkbox_codex.config import BridgeConfig


@pytest.fixture(autouse=True)
def clear_registry_override(monkeypatch):
    monkeypatch.delenv('INKBOX_CODEX_A2A_REGISTRY_PATH', raising=False)


@pytest.mark.parametrize('custom', [False, True])
def test_configured_home_isolates_tunnel_without_moving_legacy_a2a_registry(monkeypatch, tmp_path, custom):
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    monkeypatch.delenv('INKBOX_CODEX_HOME', raising=False)
    root = tmp_path / '.inkbox-codex'
    if custom:
        root = tmp_path / 'separate-profile'
        monkeypatch.setenv('INKBOX_CODEX_HOME', str(root))
    instance = gateway.InkboxGateway(BridgeConfig(identity='agent', base_url='https://example.test'))
    assert instance._a2a_registry_path == tmp_path / '.inkbox-codex/a2a_tasks.json'
    assert instance._hosted_call_registry_path == root / 'hosted_call_completions.json'
    assert gateway._tunnel_state_dir() == root / 'tunnel'


def test_open_tunnel_only_clears_its_own_profile(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    primary = tmp_path / '.inkbox-codex/tunnel'
    primary.mkdir(parents=True)
    (primary / 'state.json').write_text('preserve-other-profile')
    isolated = tmp_path / 'separate-profile'
    stale = isolated / 'tunnel/state.json'
    stale.parent.mkdir(parents=True)
    stale.write_text('old-profile-tunnel')
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(isolated))
    listener = SimpleNamespace(public_url='https://example.test', tunnel=SimpleNamespace(public_host='example.test'), wait=Mock())
    connect = Mock(return_value=listener)
    monkeypatch.setattr(gateway, 'INKBOX_TUNNEL_AVAILABLE', True)
    monkeypatch.setattr(gateway, 'inkbox_tunnel_connect', connect)
    instance = gateway.InkboxGateway(BridgeConfig(identity='agent', tunnel_name='isolated', base_url='https://example.test'))
    asyncio.run(instance._open_tunnel())
    assert connect.call_args.kwargs['state_dir'] == isolated / 'tunnel'
    assert connect.call_args.kwargs['name'] == 'isolated'
    assert not stale.exists()
    assert (primary / 'state.json').read_text() == 'preserve-other-profile'


def test_existing_custom_profile_preserves_legacy_a2a_dedup_before_catchup(monkeypatch, tmp_path):
    import json
    from unittest.mock import AsyncMock

    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    profile = tmp_path / 'separate-profile'
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(profile))
    legacy = tmp_path / '.inkbox-codex/a2a_tasks.json'
    legacy.parent.mkdir()
    entry = {'task_id': 'task-1', 'message_id': 'message-1', 'context_id': 'context-1',
             'state': 'finalized', 'receipt_delivered': True,
             'progress': {'last_delivered_text': 'Recorded progress'}}
    original = json.dumps({'task-1:message-1': entry})
    legacy.write_text(original)
    instance = gateway.InkboxGateway(BridgeConfig(identity='agent', base_url='https://example.test'))
    task = SimpleNamespace(id='task-1', context_id='context-1', state='working',
        caller=SimpleNamespace(identity_id='caller', organization_id='org', handle='caller'),
        messages=[{'role': 'caller', 'message_id': 'message-1', 'parts': []}])
    instance._identity = SimpleNamespace(
        iter_a2a_tasks=lambda state: [task] if state == 'working' else [],
        a2a_task=lambda task_id: task,
    )
    instance._on_a2a_event = AsyncMock()
    asyncio.run(instance._catch_up_a2a_tasks())
    instance._on_a2a_event.assert_not_awaited()
    assert instance._read_a2a_registry() == {'task-1:message-1': entry}
    assert legacy.read_text() == original
    assert not (profile / 'a2a_tasks.json').exists()


def test_explicit_fresh_profile_registry_does_not_import_other_records(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    profile = tmp_path / 'separate-profile'
    profile.mkdir()
    monkeypatch.setenv('INKBOX_CODEX_A2A_REGISTRY_PATH', str(profile / 'a2a_tasks.json'))
    legacy = tmp_path / '.inkbox-codex/a2a_tasks.json'
    legacy.parent.mkdir()
    legacy.write_text('{"other-task": {"state": "queued"}}')
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(profile))
    instance = gateway.InkboxGateway(BridgeConfig(identity='agent', base_url='https://example.test'))
    assert instance._read_a2a_registry() == {}
    instance._write_a2a_registry('new-task:new-message', {'task_id': 'new-task', 'message_id': 'new-message'}, 'queued')
    assert (profile / 'a2a_tasks.json').is_file()
    assert legacy.read_text() == '{"other-task": {"state": "queued"}}'

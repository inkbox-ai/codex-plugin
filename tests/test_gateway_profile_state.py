"""Separate profiles must not share or clear each other's runtime state."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from inkbox_codex import gateway
from inkbox_codex.config import BridgeConfig


@pytest.mark.parametrize('custom', [False, True])
def test_gateway_registry_and_tunnel_use_the_configured_home(monkeypatch, tmp_path, custom):
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    monkeypatch.delenv('INKBOX_CODEX_HOME', raising=False)
    root = tmp_path / '.inkbox-codex'
    if custom:
        root = tmp_path / 'separate-profile'
        monkeypatch.setenv('INKBOX_CODEX_HOME', str(root))
    instance = gateway.InkboxGateway(BridgeConfig(identity='agent', base_url='https://example.test'))
    assert instance._a2a_registry_path == root / 'a2a_tasks.json'
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

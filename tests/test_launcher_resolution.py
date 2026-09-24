"""Missing Node is repaired using only the selected Codex installation."""

import json
import os
from pathlib import Path

import pytest

from inkbox_codex import launcher


def installed(tmp_path, layout='nested', *, system='darwin', machine='arm64'):
    arch = 'arm64' if machine in {'arm64', 'aarch64'} else 'x64'
    triple = ('aarch64' if arch == 'arm64' else 'x86_64') + '-' + {
        'linux': 'unknown-linux-musl', 'darwin': 'apple-darwin', 'win32': 'pc-windows-msvc'}[system]
    package = tmp_path / 'lib' / 'node_modules' / '@openai' / 'codex'
    target = f'codex-{system}-{arch}'
    script = package / 'bin' / 'codex.js'
    script.parent.mkdir(parents=True)
    script.write_text('#!/usr/bin/env node\n// installed launcher\n')
    script.chmod(0o700)
    (package / 'package.json').write_text(json.dumps({
        'name': '@openai/codex', 'version': '1.0.0', 'bin': {'codex': 'bin/codex.js'},
        'optionalDependencies': {f'@openai/{target}': f'npm:@openai/codex@1.0.0-{system}-{arch}'},
    }))
    native_package = (package / 'node_modules' / '@openai' / target if layout == 'nested'
                      else package.parent / target if layout == 'sibling' else package)
    native = native_package / 'vendor' / triple / 'bin' / ('codex.exe' if system == 'win32' else 'codex')
    native.parent.mkdir(parents=True)
    native.write_bytes(b'native fixture')
    native.chmod(0o700)
    if layout != 'bundled':
        (native_package / 'package.json').write_text(json.dumps({
            'name': '@openai/codex', 'version': f'1.0.0-{system}-{arch}',
        }))
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    (binaries / 'codex').symlink_to(script)
    return script, native, binaries


@pytest.mark.parametrize('layout', ['nested', 'sibling', 'bundled'])
@pytest.mark.parametrize('system,machine', [('darwin', 'arm64'), ('darwin', 'x86_64'), ('linux', 'x86_64'), ('linux', 'aarch64')])
def test_missing_node_resolves_own_native_package(tmp_path, monkeypatch, layout, system, machine):
    script, native, binaries = installed(tmp_path, layout, system=system, machine=machine)
    monkeypatch.setenv('PATH', str(binaries))
    monkeypatch.setattr(launcher.sys, 'platform', system)
    monkeypatch.setattr(launcher.platform, 'machine', lambda: machine)
    assert launcher.resolve_codex_launcher('codex') == str(native)
    assert launcher.resolve_codex_launcher(str(script)) == str(native)


def test_working_node_preserves_selected_launcher(tmp_path, monkeypatch):
    script, native, binaries = installed(tmp_path)
    node = binaries / 'node'
    node.write_text('node executable')
    node.chmod(0o700)
    monkeypatch.setenv('PATH', str(binaries))
    assert launcher.resolve_codex_launcher('codex') == 'codex'
    assert launcher.resolve_codex_launcher(str(script)) == str(script)


def test_explicit_native_path_is_never_replaced(tmp_path, monkeypatch):
    _, native, binaries = installed(tmp_path)
    monkeypatch.setenv('PATH', str(binaries))
    assert launcher.resolve_codex_launcher(str(native)) == str(native)


@pytest.mark.parametrize('invalid', ['missing', 'not_executable', 'wrong_package', 'wrong_version', 'undeclared', 'invalid_json'])
def test_unusable_or_unrelated_native_install_is_not_selected(tmp_path, monkeypatch, invalid):
    script, native, binaries = installed(tmp_path)
    monkeypatch.setenv('PATH', str(binaries))
    monkeypatch.setattr(launcher.sys, 'platform', 'darwin')
    monkeypatch.setattr(launcher.platform, 'machine', lambda: 'arm64')
    metadata = native.parents[3] / 'package.json'
    if invalid == 'missing':
        native.unlink()
    elif invalid == 'not_executable':
        native.chmod(0o600)
    elif invalid == 'wrong_package':
        metadata.write_text(json.dumps({'name': 'another-package', 'version': '1.0.0-darwin-arm64'}))
    elif invalid == 'wrong_version':
        metadata.write_text(json.dumps({'name': '@openai/codex', 'version': '0.1.0-darwin-arm64'}))
    elif invalid == 'invalid_json':
        metadata.write_text('not json')
    else:
        root_metadata = script.parent.parent / 'package.json'
        value = json.loads(root_metadata.read_text())
        value['optionalDependencies'] = {}
        root_metadata.write_text(json.dumps(value))
    assert launcher.resolve_codex_launcher('codex') == 'codex'


def test_arbitrary_node_script_does_not_search_other_installs(tmp_path, monkeypatch):
    _, native, binaries = installed(tmp_path)
    custom = tmp_path / 'custom'
    custom.write_text('#!/usr/bin/env node\n')
    custom.chmod(0o700)
    monkeypatch.setenv('PATH', str(binaries))
    assert launcher.resolve_codex_launcher(str(custom)) == str(custom)


def test_readiness_recovers_missing_node_using_selected_install_without_config_edit(tmp_path, monkeypatch):
    import asyncio
    import sys
    from inkbox_codex.codex_client import probe_codex
    from inkbox_codex.config import BridgeConfig
    _, native, binaries = installed(tmp_path, system='linux', machine='x86_64')
    native.write_text(f'''#!{sys.executable}
import json, sys
for line in sys.stdin:
    request = json.loads(line)
    if request['method'] == 'initialize':
        print(json.dumps({{'id': request['id'], 'result': {{}}}}), flush=True)
    elif request['method'] != 'initialized':
        raise RuntimeError('Unexpected model or thread operation')
''')
    monkeypatch.setenv('PATH', str(binaries))
    monkeypatch.setattr(launcher.sys, 'platform', 'linux')
    monkeypatch.setattr(launcher.platform, 'machine', lambda: 'x86_64')
    cfg = BridgeConfig(codex_bin='codex')
    ok, detail = asyncio.run(probe_codex(cfg))
    assert ok, detail
    assert cfg.codex_bin == 'codex'

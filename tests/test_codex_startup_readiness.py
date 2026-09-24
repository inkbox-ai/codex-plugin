"""Readiness executes the configured launcher, without creating model turns."""

import asyncio
import json
import sys

from inkbox_codex import codex_client
from inkbox_codex.codex_client import CodexAppServerClient, CodexStartupError, probe_codex
from inkbox_codex.config import BridgeConfig


def launcher(tmp_path, body):
    path = tmp_path / 'codex-native'
    path.write_text(f'#!{sys.executable}\n' + body)
    path.chmod(0o700)
    return str(path)


def test_present_npm_launcher_without_node_fails_with_actionable_diagnostic(tmp_path, monkeypatch):
    path = tmp_path / 'codex'
    path.write_text('#!/usr/bin/env node\n')
    path.chmod(0o700)
    monkeypatch.setenv('PATH', str(tmp_path))
    ok, detail = asyncio.run(probe_codex(BridgeConfig(codex_bin='codex')))
    assert not ok
    assert 'Node interpreter not found' in detail
    assert 'exit code 127' in detail


def test_configured_native_launcher_initializes_without_path_codex_or_node(tmp_path, monkeypatch):
    records = tmp_path / 'methods.jsonl'
    path = launcher(tmp_path, f'''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    with open({str(records)!r}, 'a') as out:
        out.write(json.dumps(msg['method']) + '\\n')
    if msg['method'] == 'initialize':
        print(json.dumps({{'id': msg['id'], 'result': {{'userAgent': 'test'}}}}), flush=True)
''')
    monkeypatch.setenv('PATH', '')
    ok, detail = asyncio.run(probe_codex(BridgeConfig(codex_bin=path)))
    assert ok, detail
    assert 'model execution not tested' in detail
    methods = [json.loads(line) for line in records.read_text().splitlines()]
    assert methods[0] == 'initialize'
    assert set(methods) <= {'initialize', 'initialized'}


def test_initialize_exit_retains_safe_diagnostics_not_raw_stderr(tmp_path, caplog):
    path = launcher(tmp_path, '''
import sys
sys.stdin.readline()
sys.stderr.write('Authorization: Bearer synthetic-private-token\\n')
sys.stderr.write('customer conversation body should stay private\\n')
sys.stderr.write('env: node: No such file or directory\\n')
sys.exit(27)
''')
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(codex_bin=path), developer_instructions='test')
        try:
            try:
                await client.connect()
            except CodexStartupError as exc:
                assert 'exit code 27' in str(exc)
                assert 'Node interpreter not found' in str(exc)
                assert 'synthetic-private-token' not in str(exc)
            else:
                raise AssertionError('connect succeeded unexpectedly')
            assert client.thread_id is None
        finally:
            await client.disconnect()
    asyncio.run(scenario())
    assert 'Node interpreter not found' in caplog.text
    assert 'synthetic-private-token' not in caplog.text
    assert 'customer conversation' not in caplog.text


def test_initialize_timeout_is_bounded_and_process_is_reaped(tmp_path, monkeypatch):
    path = launcher(tmp_path, 'import time\ntime.sleep(60)\n')
    monkeypatch.setattr(codex_client, 'STARTUP_TIMEOUT_SECONDS', 0.03)
    observed = []
    original = CodexAppServerClient.disconnect
    async def disconnect(self):
        proc = self._proc
        await original(self)
        observed.append(proc.returncode)
    monkeypatch.setattr(CodexAppServerClient, 'disconnect', disconnect)
    ok, detail = asyncio.run(asyncio.wait_for(probe_codex(BridgeConfig(codex_bin=path)), timeout=2))
    assert not ok
    assert 'timed out' in detail
    assert observed[0] is not None


def test_stderr_flood_without_newlines_is_bounded_and_sanitized(tmp_path):
    path = launcher(tmp_path, '''
import sys
sys.stdin.readline()
sys.stderr.write('synthetic-secret-' * 100000)
sys.stderr.write('\\nenv: node: No such file or directory\\n' * 20)
sys.exit(1)
''')
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(codex_bin=path), developer_instructions='test')
        try:
            await client._ensure_process()
            try:
                await client._initialize()
            except CodexStartupError as exc:
                assert len(str(exc)) < 1000
                assert 'synthetic-secret' not in str(exc)
            assert len(client._stderr_tail) <= 8
        finally:
            await client.disconnect()
    asyncio.run(asyncio.wait_for(scenario(), timeout=3))


def test_initialize_rpc_error_does_not_leak_through_exception_chain(tmp_path):
    import traceback
    path = launcher(tmp_path, '''
import json, sys
msg = json.loads(sys.stdin.readline())
print(json.dumps({'id': msg['id'], 'error': {'message': 'synthetic-private-rpc-token'}}), flush=True)
for line in sys.stdin:
    pass
''')
    async def scenario():
        client = CodexAppServerClient(BridgeConfig(codex_bin=path), developer_instructions='test')
        try:
            await client.connect()
        except CodexStartupError as exc:
            assert 'synthetic-private-rpc-token' not in ''.join(traceback.format_exception(exc))
        else:
            raise AssertionError('connect succeeded unexpectedly')
        finally:
            await client.disconnect()
    asyncio.run(scenario())


def test_probe_reaps_launcher_descendants_holding_stdio(tmp_path, monkeypatch):
    import os
    import signal
    import pytest
    if os.name != 'posix':
        pytest.skip('isolated probe process groups require POSIX')
    child_pid_path = tmp_path / 'child.pid'
    path = launcher(tmp_path, f'''
import json, signal, subprocess, sys
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
with open({str(child_pid_path)!r}, 'w') as out:
    out.write(str(child.pid))
for line in sys.stdin:
    msg = json.loads(line)
    if msg['method'] == 'initialize':
        print(json.dumps({{'id': msg['id'], 'result': {{}}}}), flush=True)
''')
    monkeypatch.setattr(codex_client, 'SHUTDOWN_TIMEOUT_SECONDS', 0.05)
    signaled = []
    killpg = os.killpg
    def recording_killpg(pid, sig):
        signaled.append((pid, sig))
        killpg(pid, sig)
    monkeypatch.setattr(codex_client.os, 'killpg', recording_killpg)
    try:
        ok, detail = asyncio.run(asyncio.wait_for(probe_codex(BridgeConfig(codex_bin=path)), timeout=2))
        assert ok, detail
        assert [sig for _, sig in signaled] == [signal.SIGTERM, signal.SIGKILL]
        assert signaled[0][0] == signaled[1][0] != os.getpgrp()
    finally:
        # Also clean the child if a regression prevents group shutdown.
        if child_pid_path.exists():
            try:
                os.kill(int(child_pid_path.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass

"""An exited launcher must not leave a native host running during recovery."""
import asyncio
import ctypes
import os
import signal
import sys
from types import SimpleNamespace

import pytest

from inkbox_codex import codex_client, companion, sessions
from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.companion import Event
from tests.test_companion import ReconnectingClient, drained, fixture, harness, live


@pytest.fixture
def reap_orphan_hosts():
    # Linux test containers may have a PID 1 that does not reap orphans. Adopt
    # only for this fixture and restore the setting after its child is reaped.
    if sys.platform != 'linux':
        yield
        return
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    try:
        yield
    finally:
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0


@pytest.mark.skipif(os.name != 'posix', reason='POSIX launcher process groups')
@pytest.mark.parametrize('close_before_rescue', [False, True])
def test_recovery_stops_native_descendant_before_fresh_turn(
    tmp_path, monkeypatch, reap_orphan_hosts, close_before_rescue,
):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    monkeypatch.setattr(codex_client, 'SHUTDOWN_TIMEOUT_SECONDS', 0.02)
    pid_file = tmp_path / 'native.pid'
    launcher = tmp_path / 'launcher'
    child_program = (
        'import os, signal, time\n'
        'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
        f'open({str(pid_file)!r}, "w").write(str(os.getpid()))\n'
        'time.sleep(60)\n'
    )
    launcher.write_text(
        f'#!{sys.executable}\nimport subprocess, sys\n'
        f'subprocess.Popen([sys.executable, "-c", {child_program!r}])\n'
    )
    launcher.chmod(0o700)

    async def scenario():
        envelope = fixture('imessage')
        receiver, sdk, session, sent = harness(envelope)
        session.cfg.codex_bin = str(launcher)
        client = CodexAppServerClient(session.cfg, developer_instructions='')
        child_pid = None
        host = None
        child_reaped = False

        def stopped():
            nonlocal child_reaped
            if child_reaped:
                return True
            if sys.platform == 'linux':
                reaped, _ = os.waitpid(child_pid, os.WNOHANG)
                child_reaped = reaped == child_pid
                return child_reaped
            try:
                os.kill(child_pid, 0)
                return False
            except ProcessLookupError:
                return True

        async def wait_until(predicate):
            async with asyncio.timeout(3):
                while not predicate():
                    await asyncio.sleep(0.01)

        class FencedClient(ReconnectingClient):
            async def run(self, text, **kwargs):
                assert host.returncode == 0
                await wait_until(stopped)
                return await super().run(text, **kwargs)

        monkeypatch.setattr(sessions, 'CodexAppServerClient', FencedClient)
        try:
            await client._ensure_process()
            host = client._proc
            await wait_until(lambda: pid_file.exists() and host.returncode is not None)
            child_pid = int(pid_file.read_text())
            assert not stopped()
            assert client.process_group_id == host.pid
            assert os.getpgid(child_pid) == host.pid
            session._client = client
            client.thread_id = 'saved-thread'
            event = Event.parse(envelope)
            receiver.inbox.accept(event)
            receiver.inbox.state(event.event_id, 'uncertain')
            with receiver.inbox.db:
                receiver.inbox.db.execute('INSERT INTO activations VALUES(?,?,?,?,?)',
                    (event.scope, event.activation, event.source_id, event.author, 'uncertain'))
            if close_before_rescue:
                session._current_turn = SimpleNamespace(completion=asyncio.get_running_loop().create_future())
                await session.close()
                session._current_turn = None
                assert session._last_closed_host == (host, host.pid)
                assert not stopped()  # Normal close alone cannot fence the exited wrapper.
            await receiver.accept(live(envelope))
            await drained(receiver)
            assert stopped()
            assert len(sent) == len(session._client.events) == 1
            assert sdk.loads == 0  # No stale initialization replay.
            assert receiver.inbox.db.execute('SELECT state FROM events WHERE event_id=?',
                                             (event.event_id,)).fetchone()[0] == 'quarantined'
            assert not receiver.recovery_hosts
            assert session._last_closed_host is None
        finally:
            if host is not None and (child_pid is None or not child_reaped):
                try:
                    os.killpg(host.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if child_pid is not None:
                await wait_until(stopped)
            await client.disconnect()
            await receiver.close()
    asyncio.run(scenario())


def test_closed_host_pipes_do_not_signal_a_stale_process_group(tmp_path, monkeypatch):
    from unittest.mock import Mock

    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    kill_group = Mock(side_effect=AssertionError('Exited host group must not be signaled'))
    monkeypatch.setattr(companion.os, 'killpg', kill_group)

    async def scenario():
        envelope = fixture()
        receiver, _, session, _ = harness(envelope)
        stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
        stdout.feed_eof()
        stderr.feed_eof()
        session._client = None
        session._last_closed_host = (SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr), 12345)
        try:
            await receiver._fence_session(Event.parse(envelope))
            assert session._last_closed_host is None
            kill_group.assert_not_called()
        finally:
            await receiver.close()
    asyncio.run(scenario())

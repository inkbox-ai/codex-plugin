"""Real process-group recovery without model or network traffic."""

import ctypes
import os
import signal
import subprocess
import sys
import time

import pytest

from inkbox_codex import scheduler
from inkbox_codex.scheduler import (
    _kill_process_handle, _open_process_handle, fence_host, process_stamp,
)


pytestmark = pytest.mark.skipif(
    sys.platform != 'linux',
    reason='requires Linux process ownership inspection and pidfds',
)


@pytest.fixture
def leaderless_group():
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    children = []

    def start(run_id):
        program = '''
import subprocess, sys
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
print(child.pid, flush=True)
sys.stdin.readline()
'''
        leader = subprocess.Popen(
            [sys.executable, '-c', program], start_new_session=True,
            env={**os.environ, 'INKBOX_SCHEDULE_RUN_ID': run_id},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        try:
            child_pid = int(leader.stdout.readline())
            children.append((child_pid, _open_process_handle(child_pid)))
            stamp = process_stamp(leader.pid)
            assert stamp
        finally:
            leader.communicate(timeout=3)
        assert process_stamp(leader.pid) is None
        assert os.getpgid(child_pid) == leader.pid
        return {'id': run_id, 'host_pid': leader.pid, 'host_start': stamp}, child_pid

    try:
        yield start
    finally:
        for child, descriptor in children:
            try:
                _kill_process_handle(descriptor)
            except ProcessLookupError:
                pass
            finally:
                os.close(descriptor)
            try:
                os.waitpid(child, 0)
            except ChildProcessError:
                pass
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0


def test_fences_surviving_child_after_its_recorded_leader_exits(leaderless_group):
    run, child = leaderless_group('owned-scheduled-run')
    fence_host(run)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        reaped, status = os.waitpid(child, os.WNOHANG)
        if reaped:
            assert os.waitstatus_to_exitcode(status) == -signal.SIGKILL
            break
        time.sleep(0.01)
    else:
        pytest.fail('Recorded run child survived recovery fencing')
    assert fence_host(run)
    with pytest.raises(ProcessLookupError):
        os.killpg(run['host_pid'], 0)


def test_leaderless_reused_group_with_different_run_is_not_signalled(leaderless_group):
    unrelated, child = leaderless_group('different-scheduled-run')
    # The recorded numeric group can have been reused and lost its new leader too.
    old_run = {**unrelated, 'id': 'original-scheduled-run'}
    assert not fence_host(old_run)
    assert os.waitpid(child, os.WNOHANG) == (0, 0)
    os.kill(child, 0)


def test_leaderless_group_without_saved_run_identity_is_not_signalled(leaderless_group):
    run, child = leaderless_group('owned-scheduled-run')
    run.pop('id')
    assert not fence_host(run)
    assert os.waitpid(child, os.WNOHANG) == (0, 0)


def test_process_handle_fallback_preserves_errno_and_does_not_signal_reused_pid(
    leaderless_group, monkeypatch,
):
    monkeypatch.delattr(scheduler.os, 'pidfd_open', raising=False)
    monkeypatch.delattr(scheduler.signal, 'pidfd_send_signal', raising=False)
    run, child = leaderless_group('owned-scheduled-run')
    descriptor = _open_process_handle(child)
    try:
        _kill_process_handle(descriptor)
        os.waitpid(child, 0)
        with pytest.raises(ProcessLookupError):
            _kill_process_handle(descriptor)
        with pytest.raises(ProcessLookupError):
            _open_process_handle(child)
    finally:
        os.close(descriptor)
    assert fence_host(run)

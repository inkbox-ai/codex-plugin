"""Scheduled execution through the real local Codex host, with an offline model."""

import asyncio
import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.scheduler import Scheduler
from inkbox_codex.schedules import ScheduleStore
from tests.live import mock_openai


CODEX_BIN = shutil.which("codex")
pytestmark = pytest.mark.skipif(CODEX_BIN is None, reason="requires the real codex CLI")
START = 1767268800.0


@pytest.fixture
def offline_host(tmp_path, monkeypatch):
    requests = []
    state = SimpleNamespace(block=False, received=threading.Event(), release=threading.Event())

    class Handler(mock_openai.Handler):
        def _respond_responses(self, request):
            requests.append(request)
            state.received.set()
            if state.block:
                state.release.wait(timeout=30)
            try:
                super()._respond_responses(request)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="mock"\n'
        '[model_providers.mock]\nname="Mock"\n'
        f'base_url="http://127.0.0.1:{server.server_port}/v1"\nwire_api="responses"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    # The entire model and tool transport is local and does not use credentials.
    for key in tuple(os.environ):
        if key.startswith("INKBOX_API_KEY") or key in {"OPENAI_API_KEY", "CODEX_API_KEY"}:
            monkeypatch.delenv(key)
    cfg = BridgeConfig(
        identity="scheduler-contract-agent",
        api_key="",
        base_url=f"http://127.0.0.1:{server.server_port}",
        project_dir=str(tmp_path),
        codex_bin=CODEX_BIN,
        codex_model="mock-model",
        codex_sandbox="read-only",
        codex_approval_policy="never",
        codex_turn_timeout_s=30,
    )
    try:
        yield cfg, tmp_path / "jobs.sqlite3", requests, state
    finally:
        state.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def run_due(worker):
    deadline = asyncio.get_running_loop().time() + 10
    while True:
        await worker.tick()
        if worker.active is not None or asyncio.get_running_loop().time() >= deadline:
            break
        # A completed host's children can take another tick to finish exiting.
        await asyncio.sleep(0.02)
    assert worker.active is not None, "No due execution was started"
    await asyncio.wait_for(asyncio.shield(worker.active), timeout=40)


def test_real_host_forwards_run_ownership_to_mcp_child(offline_host, monkeypatch):
    from inkbox_codex import scheduler as scheduler_module

    cfg, path, _, _ = offline_host
    marker = path.with_suffix('.mcp-environment.json')
    build_config = scheduler_module.build_inkbox_mcp_server_config

    def recorded_mcp(config):
        server, names = build_config(config)
        server['args'] = ['-c', f'''
import json, os, runpy
from pathlib import Path
Path({str(marker)!r}).write_text(json.dumps({{
    'run_id': os.getenv('INKBOX_SCHEDULE_RUN_ID'),
    'job_id': os.getenv('INKBOX_SCHEDULE_JOB_ID'),
    'chat_id': os.getenv('INKBOX_CODEX_CHAT_ID'),
}}))
runpy.run_module('inkbox_codex.mcp_stdio', run_name='__main__')
''']
        return server, names

    monkeypatch.setattr(scheduler_module, 'build_inkbox_mcp_server_config', recorded_mcp)

    async def scenario():
        with ScheduleStore(path) as store:
            job = store.create('Ownership propagation', 'Write a short report.',
                               {'kind': 'interval', 'seconds': 60}, now=START)
            worker = Scheduler(cfg, store=store, clock=lambda: START + 60)
            try:
                await run_due(worker)
                run = store.runs(job['id'])[0]
                assert run['status'] == 'succeeded', run
                observed = json.loads(marker.read_text())
                assert observed == {'run_id': run['id'], 'job_id': job['id'], 'chat_id': ''}
            finally:
                await worker.close()

    asyncio.run(scenario())


def test_real_host_persists_session_and_catches_up_outage_once(offline_host):
    cfg, path, requests, _ = offline_host
    clock = [START]

    async def scenario():
        with ScheduleStore(path) as store:
            job = store.create(
                name="Local status",
                prompt="Return a short status report. smoke-aabbcc11",
                schedule={"kind": "interval", "seconds": 60},
                now=clock[0],
            )
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                clock[0] += 60
                await run_due(worker)
                first = store.runs(job["id"])[0]
                assert first["status"] == "succeeded", first
                assert "REPLY_OK smoke-aabbcc11" in first["result"]
                thread_id = store.get(job["id"])["thread_id"]
                assert thread_id
                assert len(requests) == 1
            finally:
                await worker.close()

        with ScheduleStore(path) as store:
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                clock[0] += 60
                await run_due(worker)
                assert store.runs(job["id"])[0]["status"] == "succeeded"
                assert store.get(job["id"])["thread_id"] == thread_id
                assert len(requests) == 2
                # A resumed real host sends its earlier completed turn history.
                assert "REPLY_OK smoke-aabbcc11" in json.dumps(requests[1]["input"])
            finally:
                await worker.close()

        # Five scheduled starts pass while no worker or database is open.
        clock[0] = START + 7 * 60 + 31
        with ScheduleStore(path) as store:
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                await worker.tick()
                assert worker.active is None
                assert len(requests) == 2
                misses = [run for run in store.runs(job["id"]) if run["status"] == "missed"]
                assert sum(run["count"] for run in misses) == 5
                store.catch_up(job["id"], now=clock[0])
                await run_due(worker)
                catch_up = store.runs(job["id"])[0]
                assert catch_up["status"] == "succeeded", catch_up
                assert catch_up["count"] == 5
                assert store.get(job["id"])["thread_id"] == thread_id
                assert len(requests) == 3
                await worker.tick()
                assert len(requests) == 3
            finally:
                await worker.close()

    asyncio.run(scenario())


@pytest.mark.skipif(os.name != "posix", reason="requires isolated process groups")
def test_real_host_killed_inflight_is_not_replayed_after_restart(offline_host):
    cfg, path, requests, state = offline_host
    clock = [START]
    state.block = True

    async def scenario():
        with ScheduleStore(path) as store:
            job = store.create(
                name="Interrupted report",
                prompt="Return a short status report. smoke-aabbcc22",
                schedule={"kind": "interval", "seconds": 60},
                catch_up=True,
                now=clock[0],
            )
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                clock[0] += 60
                await worker.tick()
                received = await asyncio.to_thread(state.received.wait, 20)
                assert received, "The real host did not submit a model request"
                run = next(run for run in store.runs(job["id"]) if run["status"] == "running")
                pid = run["host_pid"]
                assert pid and os.getpgid(pid) == pid
                os.killpg(pid, signal.SIGKILL)
                state.release.set()
                await asyncio.wait_for(asyncio.shield(worker.active), timeout=20)
                interrupted = next(item for item in store.runs(job["id"]) if item["id"] == run["id"])
                assert interrupted["status"] == "uncertain"
                saved_thread = store.get(job["id"])["thread_id"]
                assert saved_thread
            finally:
                state.release.set()
                await worker.close()

        clock[0] += 5 * 60 + 31
        with ScheduleStore(path) as store:
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                await worker.tick()
                assert worker.active is None
                assert len(requests) == 1
                assert store.get(job["id"])["thread_id"] == saved_thread
                interrupted = next(item for item in store.runs(job["id"]) if item["id"] == run["id"])
                assert interrupted["status"] == "uncertain"
                assert sum(item["count"] for item in store.runs(job["id"]) if item["status"] == "missed") >= 5
            finally:
                await worker.close()

    asyncio.run(scenario())


@pytest.fixture
def adopt_test_orphans():
    """Reap only the test host's process group after its worker is killed."""
    if sys.platform != "linux":
        pytest.skip("requires Linux child-subreaper support")
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    try:
        yield
    finally:
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0


def _reap_test_group(group_id):
    while True:
        try:
            pid, _ = os.waitpid(-group_id, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def test_killed_scheduler_worker_releases_lock_and_recovers_real_orphan_host(
    offline_host, adopt_test_orphans,
):
    cfg, path, requests, state = offline_host
    state.block = True
    with ScheduleStore(path) as store:
        job = store.create(
            name="Worker outage report",
            prompt="Return a short status report. smoke-aabbcc33",
            schedule={"kind": "interval", "seconds": 60},
            catch_up=True,
            now=START,
        )
    program = """
import asyncio
import sys
from pathlib import Path
from inkbox_codex.config import BridgeConfig
from inkbox_codex.scheduler import Scheduler
from inkbox_codex.schedules import ScheduleStore

async def main():
    cfg = BridgeConfig(identity="scheduler-contract-agent", api_key="", base_url=sys.argv[2],
                      project_dir=sys.argv[3], codex_bin=sys.argv[4], codex_model="mock-model",
                      codex_sandbox="read-only", codex_approval_policy="never", codex_turn_timeout_s=60)
    with ScheduleStore(Path(sys.argv[1])) as store:
        scheduler = Scheduler(cfg, store=store, clock=lambda: float(sys.argv[5]))
        await scheduler.start()
        await asyncio.Event().wait()

asyncio.run(main())
"""
    worker_process = subprocess.Popen(
        [sys.executable, "-c", program, str(path), cfg.base_url, cfg.project_dir, CODEX_BIN, str(START + 60)],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    host_pid = None

    async def scenario():
        nonlocal host_pid
        received = await asyncio.to_thread(state.received.wait, 20)
        assert received, "The worker's real Codex child did not submit a model request"
        with ScheduleStore(path) as store:
            run = next(run for run in store.runs(job["id"]) if run["status"] == "running")
            host_pid = run["host_pid"]
            assert host_pid and host_pid != worker_process.pid
            assert os.getpgid(host_pid) == host_pid
            assert store.get(job["id"])["thread_id"]

            # Kill the plugin worker itself, leaving its isolated host group to recovery.
            worker_process.kill()
            await asyncio.to_thread(worker_process.wait, 10)
            assert worker_process.returncode == -signal.SIGKILL
            previous = next(item for item in store.runs(job["id"]) if item["id"] == run["id"])
            assert previous["status"] == "running", "The killed worker cannot finalize its receipt"

            clock = START + 6 * 60 + 31
            restarted = Scheduler(cfg, store=store, clock=lambda: clock)
            try:
                # This also proves no child retained the dead worker's exclusive lock.
                await asyncio.wait_for(restarted.start(), timeout=10)
                async with asyncio.timeout(10):
                    while True:
                        _reap_test_group(host_pid)
                        await restarted.tick()
                        if not store.orphaned_hosts():
                            break
                        await asyncio.sleep(0.02)
                previous = next(item for item in store.runs(job["id"]) if item["id"] == run["id"])
                assert previous["status"] == "uncertain"
                assert previous["host_pid"] is None
                assert restarted.active is None
                assert len(requests) == 1
                _reap_test_group(host_pid)
                with pytest.raises(ProcessLookupError):
                    os.killpg(host_pid, 0)
                state.release.set()
                await restarted.tick()
                assert len(requests) == 1
                assert sum(item["count"] for item in store.runs(job["id"]) if item["status"] == "missed") == 5
            finally:
                await restarted.close()

    try:
        asyncio.run(scenario())
    finally:
        state.release.set()
        if worker_process.poll() is None:
            worker_process.kill()
        worker_process.communicate(timeout=10)
        if host_pid:
            # Captured from this test's durable receipt; never signal unrelated groups.
            try:
                os.killpg(host_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            _reap_test_group(host_pid)


def test_real_host_missing_session_requires_explicit_reset_and_preserves_history(offline_host):
    cfg, path, requests, _ = offline_host
    clock = [START]

    async def scenario():
        with ScheduleStore(path) as store:
            job = store.create('Session recovery', 'Write a report. smoke-aabbcc33',
                               {'kind': 'interval', 'seconds': 60}, now=clock[0])
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.start()
                clock[0] += 60
                await run_due(worker)
                first = store.runs(job['id'])[0]
                assert first['status'] == 'succeeded'
                old_session = first['thread_id']
                # Simulate a restored receipt whose Codex rollout is unavailable.
                store.db.execute('UPDATE jobs SET thread_id=? WHERE id=?',
                                 ('00000000-0000-4000-8000-000000000001', job['id']))
                store.enqueue(job['id'], now=clock[0])
                await run_due(worker)
                assert store.runs(job['id'])[0]['status'] == 'failed'
                assert len(requests) == 1  # No silent fresh-session replay.
                for _ in range(100):
                    await worker.tick()
                    if not store.orphaned_hosts():
                        break
                    await asyncio.sleep(0.02)
                store.reset_session(job['id'], now=clock[0])
                store.enqueue(job['id'], now=clock[0])
                await run_due(worker)
                latest = store.runs(job['id'])[0]
                assert latest['status'] == 'succeeded'
                assert latest['thread_id'] != old_session
                assert len(requests) == 2
                assert any(run['thread_id'] == old_session and run['result'] == first['result']
                           for run in store.runs(job['id']))
            finally:
                await worker.close()

    asyncio.run(scenario())


def test_worker_crash_during_real_host_initialization_keeps_cleanup_receipt(
    offline_host, adopt_test_orphans,
):
    cfg, path, requests, _ = offline_host
    marker = path.with_suffix('.initializing')
    with ScheduleStore(path) as store:
        job = store.create('Startup outage', 'Write a report.',
                           {'kind': 'interval', 'seconds': 60}, now=START)
    program = '''
import asyncio
import sys
from pathlib import Path
from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig
from inkbox_codex.scheduler import Scheduler
from inkbox_codex.schedules import ScheduleStore

class WaitingClient(CodexAppServerClient):
    async def _initialize(self):
        Path(sys.argv[5]).write_text('initializing')
        await asyncio.Event().wait()

async def main():
    cfg = BridgeConfig(identity='scheduler-contract-agent', api_key='',
                      base_url=sys.argv[2], project_dir=sys.argv[3], codex_bin=sys.argv[4],
                      codex_turn_timeout_s=60)
    with ScheduleStore(Path(sys.argv[1])) as store:
        scheduler = Scheduler(cfg, store=store, client_factory=WaitingClient,
                              clock=lambda: float(sys.argv[6]))
        await scheduler.start()
        await asyncio.Event().wait()

asyncio.run(main())
'''
    worker = subprocess.Popen(
        [sys.executable, '-c', program, str(path), cfg.base_url, cfg.project_dir,
         CODEX_BIN, str(marker), str(START + 60)],
        cwd=Path(__file__).resolve().parents[2], start_new_session=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    host_pid = None

    async def scenario():
        nonlocal host_pid
        async with asyncio.timeout(10):
            while not marker.exists():
                assert worker.poll() is None, 'Scheduler exited before host initialization'
                await asyncio.sleep(0.02)
        with ScheduleStore(path) as store:
            run = store.runs(job['id'])[0]
            assert run['status'] == 'starting'
            assert run['thread_id'] is None
            host_pid = run['host_pid']
            assert host_pid and run['host_start']
            assert os.getpgid(host_pid) == host_pid
            worker.kill()
            await asyncio.to_thread(worker.wait, 5)
            restarted = Scheduler(cfg, store=store, clock=lambda: START + 120)
            try:
                await restarted.start()
                async with asyncio.timeout(10):
                    while True:
                        _reap_test_group(host_pid)
                        await restarted.tick()
                        if not store.orphaned_hosts():
                            break
                        await asyncio.sleep(0.02)
                interrupted = next(item for item in store.runs(job['id']) if item['id'] == run['id'])
                assert interrupted['status'] == 'uncertain'
                assert interrupted['host_pid'] is None
                assert restarted.active is None
                assert requests == []
            finally:
                await restarted.close()

    try:
        asyncio.run(scenario())
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)
        if host_pid:
            try:
                os.killpg(host_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            _reap_test_group(host_pid)


def test_real_host_bounded_job_stops_without_agent_schedule_mutations(offline_host):
    cfg, path, requests, _ = offline_host
    clock = [START]

    async def scenario():
        with ScheduleStore(path) as store:
            job = store.create('Bounded progress', 'Return a short report. smoke-ccdd0011',
                               {'kind': 'interval', 'seconds': 60},
                               duration_seconds=120, max_runs=2, now=clock[0])
        for number in (1, 2):
            # Reopen the durable store and host for each execution.
            clock[0] = START + number * 60 + 1
            with ScheduleStore(path) as store:
                worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
                try:
                    await run_due(worker)
                    assert store.runs(job['id'])[0]['status'] == 'succeeded'
                    assert store.get(job['id'])['run_count'] == number
                finally:
                    await worker.close()
        assert len(requests) == 2
        clock[0] = START + 180
        with ScheduleStore(path) as store:
            worker = Scheduler(cfg, store=store, clock=lambda: clock[0])
            try:
                await worker.tick()
                assert worker.active is None
                assert len(requests) == 2
                assert store.get(job['id'])['stop_reason'] == 'max_runs'
                assert store.get(job['id'])['enabled'] is False
            finally:
                await worker.close()

    asyncio.run(scenario())

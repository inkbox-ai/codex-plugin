"""Scheduled execution through the real local Codex host, with an offline model."""

import asyncio
import json
import os
import shutil
import signal
import threading
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
    await worker.tick()
    assert worker.active is not None, "No due execution was started"
    await asyncio.wait_for(asyncio.shield(worker.active), timeout=40)


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

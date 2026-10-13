"""Scheduler storage failures must not stop ordinary incoming conversations."""

import asyncio
import fcntl
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex import gateway, scheduler, slack_turns
from inkbox_codex.config import BridgeConfig
from inkbox_codex.schedules import ScheduleStore, schedule_path


def _gateway(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    monkeypatch.setattr(gateway, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway, "INKBOX_AVAILABLE", True)
    monkeypatch.setattr(gateway, "Inkbox", lambda **kwargs: SimpleNamespace(
        get_identity=lambda _: SimpleNamespace(agent_handle="test-agent"),
    ))
    sessions = SimpleNamespace(close_all=AsyncMock())
    monkeypatch.setattr(gateway, "SessionManager", lambda **kwargs: sessions)
    monkeypatch.setattr(gateway, "clear_identity_turn_contexts", Mock())
    monkeypatch.setattr(slack_turns, "retire", Mock())
    monkeypatch.setattr(gateway, "inbox_summary", lambda cfg: {"blocked_conversations": 0})
    bridge = gateway.InkboxGateway(BridgeConfig(
        api_key="synthetic-key", identity="test-agent", project_dir=str(tmp_path),
        public_url="https://bridge.example.test",
    ))
    bridge._start_http_server = AsyncMock()
    bridge._runner = SimpleNamespace(cleanup=AsyncMock())
    bridge._companion_receiver = SimpleNamespace(recover=Mock(), close=AsyncMock())
    bridge._companion = lambda: bridge._companion_receiver
    bridge._patch_identity_objects = Mock()
    bridge._catch_up_a2a_tasks = AsyncMock()
    bridge._recover_hosted_call_completions = AsyncMock()
    bridge._codex_ready = True
    bridge._codex_checked_at = time.time()
    return bridge, sessions


@pytest.mark.parametrize("failure", ["corrupt_database", "blocked_directory", "worker_lock"])
def test_failed_scheduler_start_keeps_gateway_ready_and_recovers(monkeypatch, tmp_path, caplog, failure):
    bridge, sessions = _gateway(monkeypatch, tmp_path)
    path = schedule_path(bridge.cfg)
    held_lock = None
    if failure == "corrupt_database":
        path.parent.mkdir(parents=True)
        path.write_bytes(b"not a SQLite database")
        repair = path.unlink
    elif failure == "blocked_directory":
        path.parent.parent.mkdir(parents=True)
        path.parent.write_text("not a directory")
        repair = path.parent.unlink
    else:
        # Even an intentionally disabled scheduler must not take chats offline.
        with ScheduleStore(path) as store:
            store.set_enabled(False)
        held_lock = path.with_suffix(".worker.lock").open("a")
        fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        repair = held_lock.close

    async def scenario():
        retry_wait, resume = asyncio.Event(), asyncio.Event()
        sleep = asyncio.sleep
        intervals = []

        async def interval(seconds):
            if seconds == 30:
                intervals.append(seconds)
                retry_wait.set()
                await resume.wait()
            else:
                await sleep(seconds)

        monkeypatch.setattr(gateway.asyncio, "sleep", interval)
        running = asyncio.create_task(bridge.run())
        try:
            await asyncio.wait_for(retry_wait.wait(), timeout=2)
            assert not running.done()
            bridge._start_http_server.assert_awaited_once()
            bridge._runner.cleanup.assert_not_awaited()
            assert bridge.sessions is sessions
            sessions.close_all.assert_not_awaited()
            ready = await bridge._handle_ready(None)
            assert ready.status == 200
            status = json.loads(ready.text)["scheduler"]
            assert not status["healthy"]
            assert status["attention_required"]
            assert "could not start" in status["last_error"]
            assert "30 seconds" in status["last_error"]
            assert str(path) not in ready.text
            if failure == "worker_lock":
                assert status["enabled"] is False
                # Failed acquisition must not release the other worker's lock.
                with path.with_suffix(".worker.lock").open("a") as contender:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert bridge._scheduler is None
            assert intervals == [30]

            repair()
            resume.set()
            await asyncio.wait_for(asyncio.shield(bridge._scheduler_start_task), timeout=2)
            assert not running.done()
            assert bridge._scheduler_start_error is None
            recovered = await bridge._handle_ready(None)
            assert recovered.status == 200
            assert json.loads(recovered.text)["scheduler"]["healthy"]
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
            if held_lock is not None:
                held_lock.close()
        assert bridge._scheduler_start_task is None
        assert bridge._scheduler is None
        bridge._runner.cleanup.assert_awaited_once()
        sessions.close_all.assert_awaited_once()

    asyncio.run(scenario())
    assert "Scheduled jobs could not start" in caplog.text
    assert str(path) not in caplog.text


def test_scheduler_retry_waits_for_partial_worker_cleanup(monkeypatch, tmp_path):
    bridge, _ = _gateway(monkeypatch, tmp_path)
    first = SimpleNamespace(
        start=AsyncMock(side_effect=OSError("private startup detail")),
        close=AsyncMock(side_effect=[OSError("private cleanup detail"), None]),
    )
    second = SimpleNamespace(start=AsyncMock(), close=AsyncMock())
    factory = Mock(side_effect=[first, second])
    monkeypatch.setattr(scheduler, "Scheduler", factory)

    async def scenario():
        sleeps = asyncio.Queue()
        release = asyncio.Queue()

        async def interval(seconds):
            await sleeps.put(seconds)
            await release.get()

        monkeypatch.setattr(gateway.asyncio, "sleep", interval)
        bridge._scheduler_start_task = asyncio.create_task(bridge._start_scheduler())
        try:
            assert await asyncio.wait_for(sleeps.get(), 1) == 30
            assert factory.call_count == 1
            assert bridge._scheduler is first
            assert "cleanup is incomplete" in bridge._scheduler_start_error
            await release.put(None)
            assert await asyncio.wait_for(sleeps.get(), 1) == 30
            assert first.close.await_count == 2
            assert factory.call_count == 1
            assert bridge._scheduler is None
            await release.put(None)
            await asyncio.wait_for(bridge._scheduler_start_task, 1)
            second.start.assert_awaited_once()
            assert bridge._scheduler_start_error is None
        finally:
            await bridge._cleanup()
        second.close.assert_awaited_once()

    asyncio.run(scenario())


def test_gateway_shutdown_cancels_scheduler_startup_and_closes_partial_worker(monkeypatch, tmp_path):
    bridge, _ = _gateway(monkeypatch, tmp_path)

    async def scenario():
        entered = asyncio.Event()

        async def start():
            entered.set()
            await asyncio.Event().wait()

        worker = SimpleNamespace(start=start, close=AsyncMock())
        monkeypatch.setattr(scheduler, "Scheduler", lambda cfg: worker)
        task = bridge._scheduler_start_task = asyncio.create_task(bridge._start_scheduler())
        await asyncio.wait_for(entered.wait(), 1)
        await bridge._cleanup()
        assert task.cancelled()
        worker.close.assert_awaited_once()
        bridge._runner.cleanup.assert_awaited_once()

    asyncio.run(scenario())


def test_scheduler_cleanup_error_does_not_skip_gateway_cleanup(monkeypatch, tmp_path, caplog):
    bridge, sessions = _gateway(monkeypatch, tmp_path)
    bridge.sessions = sessions
    bridge._scheduler = SimpleNamespace(close=AsyncMock(side_effect=OSError("private error")))
    asyncio.run(bridge._cleanup())
    sessions.close_all.assert_awaited_once()
    bridge._runner.cleanup.assert_awaited_once()
    assert "Scheduled-job cleanup did not complete" in caplog.text
    assert "private error" not in caplog.text

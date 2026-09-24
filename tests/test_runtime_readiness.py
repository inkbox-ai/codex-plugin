"""Gateway liveness must not conceal startup or durable queue failures."""

import asyncio
import json
import sqlite3
import time
from unittest.mock import AsyncMock, Mock

from inkbox_codex import daemon, doctor, gateway
from inkbox_codex.config import BridgeConfig


def summary(blocked=0):
    return {
        "unfinished_count": 2 if blocked else 0,
        "pending_count": 1 if blocked else 0,
        "blocked_conversations": blocked,
        "oldest_unfinished_age_s": 3600 if blocked else None,
    }


def ready_gateway(monkeypatch):
    bridge = gateway.InkboxGateway(BridgeConfig(identity="test-agent"))
    bridge.sessions = object()
    bridge._companion_receiver = Mock()
    bridge._codex_ready = True
    bridge._codex_checked_at = time.time()
    monkeypatch.setattr(gateway, "inbox_summary", lambda cfg: summary())
    return bridge


def test_blocked_inbox_degrades_readiness_without_breaking_liveness(monkeypatch):
    bridge = ready_gateway(monkeypatch)
    monkeypatch.setattr(gateway, "inbox_summary", lambda cfg: summary(blocked=1))

    async def check():
        health = await bridge._handle_health(None)
        ready = await bridge._handle_ready(None)
        assert health.status == 200
        assert json.loads(health.text)["ok"] is True
        assert json.loads(health.text)["ready"] is False
        assert ready.status == 503
        assert json.loads(ready.text)["companion"] == {"readable": True, **summary(1)}

    asyncio.run(check())


def test_readiness_requires_completed_recent_startup_check(monkeypatch):
    bridge = ready_gateway(monkeypatch)

    async def check():
        assert (await bridge._handle_ready(None)).status == 200
        for state, checked_at in [(None, None), (False, time.time()), (True, time.time() - 121)]:
            bridge._codex_ready = state
            bridge._codex_checked_at = checked_at
            assert (await bridge._handle_ready(None)).status == 503
        bridge._codex_ready = True
        bridge._codex_checked_at = time.time()
        bridge.sessions = None
        assert (await bridge._handle_ready(None)).status == 503

    asyncio.run(check())


def test_readiness_does_not_disclose_database_errors(monkeypatch):
    bridge = ready_gateway(monkeypatch)

    def fail(cfg):
        raise sqlite3.OperationalError("private path and message contents")

    monkeypatch.setattr(gateway, "inbox_summary", fail)
    response = asyncio.run(bridge._handle_ready(None))
    assert response.status == 503
    assert json.loads(response.text)["companion"] == {"readable": False}
    assert "private" not in response.text


def test_http_checks_never_spawn_codex(monkeypatch):
    bridge = ready_gateway(monkeypatch)
    probe = AsyncMock(side_effect=AssertionError("HTTP must not launch processes"))
    monkeypatch.setattr(gateway, "probe_codex", probe)

    async def check():
        for _ in range(4):
            assert (await bridge._handle_health(None)).status == 200
            assert (await bridge._handle_ready(None)).status == 200

    asyncio.run(check())
    probe.assert_not_called()


def test_readiness_monitor_refreshes_after_launcher_recovery_and_stops(monkeypatch):
    bridge = ready_gateway(monkeypatch)
    bridge._codex_ready = None
    probe = AsyncMock(side_effect=[(False, "missing interpreter"), (True, "initialize succeeded")])
    monkeypatch.setattr(gateway, "probe_codex", probe)

    async def check():
        first_sleep = asyncio.Event()
        resume = asyncio.Event()
        second_sleep = asyncio.Event()
        waits = []

        async def interval(seconds):
            waits.append(seconds)
            if len(waits) == 1:
                first_sleep.set()
                await resume.wait()
            else:
                second_sleep.set()
                await asyncio.Event().wait()

        monkeypatch.setattr(gateway.asyncio, "sleep", interval)
        monitor = bridge._readiness_monitor(None)
        await anext(monitor)
        await asyncio.wait_for(first_sleep.wait(), timeout=1)
        assert bridge._codex_ready is False
        bridge._companion_receiver.recover.assert_not_called()
        resume.set()
        await asyncio.wait_for(second_sleep.wait(), timeout=1)
        assert bridge._codex_ready is True
        bridge._companion_receiver.recover.assert_called_once_with()
        assert waits == [60, 60]
        await monitor.aclose()

    asyncio.run(check())
    assert probe.await_count == 2


def test_doctor_uses_effective_launcher_and_reports_blocked_queue(monkeypatch, tmp_path):
    cfg = BridgeConfig(codex_bin="/custom/native-codex", project_dir=str(tmp_path))
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor, "read_config", lambda: cfg)
    probe = AsyncMock(return_value=(False, "app-server exited (127): missing interpreter"))
    monkeypatch.setattr(doctor, "probe_codex", probe)
    monkeypatch.setattr(doctor, "inbox_summary", lambda cfg: summary(1))
    rows = {name: (ok, detail) for name, ok, detail in doctor.run_doctor()}
    probe.assert_awaited_once_with(cfg)
    assert rows["codex CLI"][0] is False
    assert "127" in rows["codex CLI"][1]
    assert rows["Companion inbox"][0] is False
    assert "3600s" in rows["Companion inbox"][1]
    assert "automatic recovery" in rows["Companion inbox"][1]


def test_auth_config_check_respects_effective_launcher(monkeypatch):
    selected = []
    monkeypatch.setattr(gateway.shutil, "which", lambda name: selected.append(name) or name)
    monkeypatch.setenv("OPENAI_API_KEY", "test-only")
    assert "credentials configured" in gateway._codex_health(BridgeConfig(codex_bin="/custom/codex"))
    assert selected == ["/custom/codex"]


def test_quarantined_receipts_are_visible_without_blocking_new_inputs(monkeypatch, tmp_path):
    bridge = ready_gateway(monkeypatch)
    retained = {**summary(), 'unfinished_count': 2, 'quarantined_count': 2}
    monkeypatch.setattr(gateway, 'inbox_summary', lambda cfg: retained)
    response = asyncio.run(bridge._handle_ready(None))
    assert response.status == 200
    assert json.loads(response.text)['companion']['quarantined_count'] == 2
    cfg = BridgeConfig(project_dir=str(tmp_path))
    monkeypatch.setattr(daemon, '_maybe_load_env_file', lambda: None)
    monkeypatch.setattr(doctor, 'read_config', lambda: cfg)
    monkeypatch.setattr(doctor, 'probe_codex', AsyncMock(return_value=(True, 'initialized')))
    monkeypatch.setattr(doctor, 'inbox_summary', lambda cfg: retained)
    rows = {name: (ok, detail) for name, ok, detail in doctor.run_doctor()}
    assert rows['Companion inbox'][0]
    assert '2 earlier outcomes unconfirmed' in rows['Companion inbox'][1]
    assert 'not blocking new inputs' in rows['Companion inbox'][1]


def test_recovery_scheduling_failure_does_not_stop_heartbeat(monkeypatch, caplog):
    bridge = ready_gateway(monkeypatch)
    bridge._companion_receiver.recover.side_effect = [RuntimeError('private queue detail'), None]
    probe = AsyncMock(return_value=(True, 'initialized'))
    monkeypatch.setattr(gateway, 'probe_codex', probe)

    async def check():
        first_sleep, second_sleep, resume = asyncio.Event(), asyncio.Event(), asyncio.Event()
        waits = []
        async def interval(seconds):
            waits.append(seconds)
            if len(waits) == 1:
                first_sleep.set()
                await resume.wait()
            else:
                second_sleep.set()
                await asyncio.Event().wait()
        monkeypatch.setattr(gateway.asyncio, 'sleep', interval)
        monitor = bridge._readiness_monitor(None)
        await anext(monitor)
        try:
            await asyncio.wait_for(first_sleep.wait(), 1)
            assert bridge._codex_ready
            resume.set()
            await asyncio.wait_for(second_sleep.wait(), 1)
            assert bridge._companion_receiver.recover.call_count == 2
            assert probe.await_count == 2
        finally:
            await monitor.aclose()
    asyncio.run(check())
    assert 'will retry automatically' in caplog.text
    assert 'private queue detail' not in caplog.text

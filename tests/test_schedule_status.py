"""Aggregate scheduler diagnostics must remain read-only and content-free."""

import json
import sqlite3
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import cli, daemon, doctor
from inkbox_codex.config import BridgeConfig
from inkbox_codex.schedule_tools import schedule_status, schedule_status_detail
from inkbox_codex.schedules import ScheduleStore, schedule_path


@pytest.fixture
def cfg(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    return BridgeConfig(identity="status-agent", project_dir=str(tmp_path / "project"))


def create_job(store, **kwargs):
    return store.create("Private job name", "Private stored prompt", {"kind": "interval", "seconds": 60}, now=100, **kwargs)


def test_no_database_is_healthy_without_creating_state(cfg):
    path = schedule_path(cfg)
    status = schedule_status(cfg, now=100)
    assert status["jobs"] == 0 and status["healthy"] is True
    assert status["heartbeat_age_s"] is None and status["heartbeat_stale"] is False
    assert not path.parent.exists()


def test_no_identity_never_creates_state(cfg):
    cfg.identity = ""
    assert schedule_status(cfg)["healthy"] is True


def test_empty_database_does_not_require_heartbeat(cfg):
    with ScheduleStore(schedule_path(cfg)):
        pass
    assert schedule_status(cfg, now=100)["healthy"] is True


@pytest.mark.parametrize("heartbeat, expected_stale", [(None, True), (100, True), (299, False)])
def test_enabled_jobs_need_fresh_heartbeat(cfg, heartbeat, expected_stale):
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store)
        if heartbeat is not None:
            store.heartbeat(now=heartbeat)
    status = schedule_status(cfg, now=300)
    assert status["jobs"] == status["enabled_jobs"] == 1
    assert status["heartbeat_stale"] is expected_stale
    assert status["needs_gateway"] is expected_stale
    assert status["healthy"] is not expected_stale
    assert ("start or inspect" in schedule_status_detail(status)) is expected_stale


def test_intentional_pause_is_healthy_without_heartbeat(cfg):
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store)
        store.set_enabled(False)
    status = schedule_status(cfg, now=300)
    assert status["enabled"] is False
    assert status["healthy"] is True and status["needs_gateway"] is False
    assert "intentionally paused" in schedule_status_detail(status)


def test_no_enabled_jobs_do_not_need_heartbeat(cfg):
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store, enabled=False)
    status = schedule_status(cfg, now=300)
    assert status["jobs"] == 1 and status["enabled_jobs"] == 0
    assert status["healthy"] is True


def test_missed_and_uncertain_counts_without_any_job_content(cfg):
    with ScheduleStore(schedule_path(cfg)) as store:
        job = create_job(store)
        store.materialize(now=280)
        run = store.claim(now=280)
        store.mark_running(run["id"], "private-thread")
        store.recover_interrupted(now=281)
        store.heartbeat(now=282)
    status = schedule_status(cfg, now=290)
    assert status["missed_pending"] == 2 and status["uncertain"] == 1
    assert status["healthy"] is True and status["attention_required"] is True
    assert "warning: 1 uncertain" in schedule_status_detail(status)
    serialized = json.dumps(status)
    assert all(value not in serialized for value in (job["id"], run["id"], "private-thread", "Private job name", "Private stored prompt"))


def test_orphan_processes_warn_but_active_processes_do_not(cfg):
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store)
        run = store.claim(now=160)
        store.mark_running(run["id"], "private-thread", host_pid=987654, host_start="private-process-start")
        store.heartbeat(now=170)
        assert schedule_status(cfg, now=180)["orphan_hosts"] == 0
        store.recover_interrupted(now=180)
        status = schedule_status(cfg, now=190)
    assert status["orphan_hosts"] == 1 and status["healthy"] is False
    assert "987654" not in json.dumps(status)
    assert "private-process-start" not in json.dumps(status)


def test_persisted_errors_are_sanitized_for_public_readiness(cfg):
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store)
        store.heartbeat(error="private prompt at /private/path token=secret", now=200)
    status = schedule_status(cfg, now=210)
    assert status["healthy"] is False
    assert status["last_error"] == "Scheduler reported an error; inspect local scheduler status."
    assert "private" not in json.dumps(status)
    assert "secret" not in json.dumps(status)


def test_status_uses_sqlite_readonly_and_does_not_change_schema_or_database(cfg, monkeypatch):
    path = schedule_path(cfg)
    with ScheduleStore(path) as store:
        create_job(store)
        store.heartbeat(now=200)
        before_schema = list(store.db.execute("SELECT sql FROM sqlite_master"))
    before_bytes = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns
    connections = []
    connect = sqlite3.connect
    def readonly_connect(database, *args, **kwargs):
        assert database.endswith("?mode=ro") and kwargs["uri"] is True
        db = connect(database, *args, **kwargs)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("CREATE TABLE should_not_exist (value TEXT)")
        connections.append(db)
        return db
    monkeypatch.setattr(sqlite3, "connect", readonly_connect)
    assert schedule_status(cfg, now=210)["healthy"] is True
    assert len(connections) == 1
    assert path.read_bytes() == before_bytes and path.stat().st_mtime_ns == before_mtime
    with connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        assert [row[0] for row in db.execute("SELECT sql FROM sqlite_master")] == [row[0] for row in before_schema]


def test_unreadable_database_has_generic_failure_and_no_schema_creation(cfg):
    path = schedule_path(cfg)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not a SQLite database")
    before = path.read_bytes()
    status = schedule_status(cfg)
    assert status["healthy"] is False
    assert status["last_error"] == "Could not read scheduled-job state."
    assert path.read_bytes() == before
    assert str(path) not in json.dumps(status)


def test_doctor_includes_readonly_scheduler_summary(cfg, monkeypatch):
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor, "read_config", lambda: cfg)
    monkeypatch.setattr(doctor, "probe_codex", AsyncMock(return_value=(True, "available")))
    rows = {name: (ok, detail) for name, ok, detail in doctor.run_doctor()}
    assert rows["Scheduled jobs"][0] is True
    assert "0 jobs" in rows["Scheduled jobs"][1]
    assert not schedule_path(cfg).exists()
    with ScheduleStore(schedule_path(cfg)) as store:
        create_job(store)
    rows = {name: (ok, detail) for name, ok, detail in doctor.run_doctor()}
    assert rows["Scheduled jobs"][0] is False
    assert "start or inspect" in rows["Scheduled jobs"][1]


def test_cli_status_does_not_create_scheduler_storage(cfg, monkeypatch, capsys):
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(cli, "read_config", lambda: cfg)
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
    monkeypatch.delenv("INKBOX_SCHEDULE_JOB_ID", raising=False)
    assert cli.main(["schedule", "status"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["healthy"] is True and result["jobs"] == 0
    assert not schedule_path(cfg).parent.exists()

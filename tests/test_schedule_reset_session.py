"""Explicit session recovery preserves prompts and receipts without replaying work."""

import json

import pytest

from inkbox_codex import cli
from inkbox_codex.config import BridgeConfig
from inkbox_codex.schedules import ScheduleStore, schedule_path


@pytest.fixture
def cfg(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
    monkeypatch.delenv("INKBOX_SCHEDULE_JOB_ID", raising=False)
    monkeypatch.setattr(cli.daemon, "_maybe_load_env_file", lambda: None)
    result = BridgeConfig(identity="schedule-agent", project_dir=str(tmp_path / "project"))
    monkeypatch.setattr(cli, "read_config", lambda: result)
    return result


@pytest.fixture
def store(cfg):
    with ScheduleStore(schedule_path(cfg)) as result:
        yield result


def saved_session(store):
    job = store.create("Review", "Review project status.", {"kind": "interval", "seconds": 60}, owner="contact:one", now=100)
    run = store.enqueue(job["id"], now=110)
    assert store.claim(now=110)["id"] == run["id"]
    store.mark_running(run["id"], "previous-session")
    store.finish(run["id"], "succeeded", result="Previous result", now=111)
    return store.get(job["id"])


def test_reset_preserves_prompt_schedule_and_execution_history(store):
    job = saved_session(store)
    previous_history = store.runs(job["id"])
    reset = store.reset_session(job["id"], now=120)
    assert reset["thread_id"] is None and reset["updated_at"] == 120
    for key in ("id", "name", "prompt", "schedule", "owner", "next_run", "enabled", "catch_up", "created_at"):
        assert reset[key] == job[key]
    assert store.runs(job["id"]) == previous_history
    assert store.runs(job["id"])[0]["thread_id"] == "previous-session"
    assert store.runs(job["id"])[0]["result"] == "Previous result"
    assert store.reset_session(job["id"], now=121)["thread_id"] is None
    # Reset itself queues no work; a later explicitly queued execution is fresh.
    next_run = store.enqueue(job["id"], now=122)
    assert store.claim(now=122)["id"] == next_run["id"]
    assert store.get(job["id"])["thread_id"] is None
    store.mark_running(next_run["id"], "fresh-session")
    assert store.get(job["id"])["thread_id"] == "fresh-session"
    assert store.runs(job["id"])[1]["thread_id"] == "previous-session"


@pytest.mark.parametrize("status", ["pending", "starting", "running", "uncertain"])
def test_reset_refuses_active_work(store, status):
    job = saved_session(store)
    run = store.enqueue(job["id"], now=120)
    if status != "pending":
        store.claim(now=120)
    if status in {"running", "uncertain"}:
        store.mark_running(run["id"], "previous-session")
    if status == "uncertain":
        store.recover_interrupted(now=121)
    before = store.get(job["id"])
    history = store.runs(job["id"])
    with pytest.raises(ValueError, match="active execution"):
        store.reset_session(job["id"], now=122)
    assert store.get(job["id"]) == before
    assert store.runs(job["id"]) == history


def test_reset_refuses_uncleared_host_even_on_finished_run(store):
    job = saved_session(store)
    run = store.enqueue(job["id"], now=120)
    store.claim(now=120)
    store.mark_running(run["id"], "previous-session", host_pid=987654, host_start="test-start")
    store.finish(run["id"], "failed", error="Could not finish", now=121)
    with pytest.raises(ValueError, match="stop previous execution processes"):
        store.reset_session(job["id"], now=122)
    store.clear_host(run["id"])
    assert store.reset_session(job["id"], now=123)["thread_id"] is None


@pytest.mark.parametrize("owner", ["contact:one", "contact:other", ""])
def test_store_reset_requires_local_admin_even_for_matching_owner(store, owner):
    job = saved_session(store)
    with pytest.raises(ValueError, match="local administrator"):
        store.reset_session(job["id"], owner=owner)
    assert store.get(job["id"])["thread_id"] == "previous-session"


def test_cli_reset_clears_stale_session_without_replaying_failed_run(store, capsys):
    job = saved_session(store)
    failed = store.enqueue(job["id"], now=120)
    store.claim(now=120)
    # A connect failure records no new thread and leaves the stale job session.
    store.finish(failed["id"], "failed", error="Session unavailable", now=121)
    before = store.runs(job["id"])
    assert cli.main(["schedule", "reset-session", job["id"]]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["thread_id"] is None
    assert result["prompt"] == job["prompt"]
    assert store.runs(job["id"]) == before
    assert store.runs(job["id"])[0]["status"] == "failed"


@pytest.mark.parametrize("environment", ["INKBOX_CODEX_CHAT_ID", "INKBOX_SCHEDULE_JOB_ID"])
def test_cli_reset_rejects_chat_and_scheduled_context(store, monkeypatch, capsys, environment):
    job = saved_session(store)
    monkeypatch.setenv(environment, "contact:one" if environment == "INKBOX_CODEX_CHAT_ID" else job["id"])
    assert cli.main(["schedule", "reset-session", job["id"]]) == 1
    assert "error" in json.loads(capsys.readouterr().out)
    assert store.get(job["id"])["thread_id"] == "previous-session"


def test_cli_reset_rejects_unknown_job(store, capsys):
    assert cli.main(["schedule", "reset-session", "missing-job"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "Schedule not found"

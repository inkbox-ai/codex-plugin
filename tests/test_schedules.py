"""Durable scheduling behavior, including downtime and explicit recovery."""

from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from threading import Barrier

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex import schedules as schedules_module
from inkbox_codex.schedules import ScheduleStore, schedule_path


UTC = timezone.utc
START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def every_minute(store, *, now=START, **kwargs):
    return store.create(
        name="Daily work",
        prompt="Summarize the project status.",
        schedule={"kind": "interval", "seconds": 60},
        now=now,
        **kwargs,
    )


def once(store, at, *, now=START, **kwargs):
    return store.create(
        name="One-time work",
        prompt="Summarize the project status.",
        schedule={"kind": "once", "at": at.isoformat()},
        now=now,
        **kwargs,
    )


@pytest.fixture
def store(tmp_path):
    with ScheduleStore(tmp_path / "jobs.sqlite3") as result:
        yield result


def test_store_namespace_separates_projects_identities_and_api_origins(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "state"))
    cfg = BridgeConfig(
        identity="agent-a",
        base_url="https://api.example.test",
        project_dir=str(tmp_path / "project-a"),
        api_key="test-only-secret",
    )
    original = schedule_path(cfg)
    assert original == schedule_path(replace(cfg, api_key="rotated-test-only-secret"))
    alternatives = [
        replace(cfg, identity="agent-b"),
        replace(cfg, base_url="https://other.example.test"),
        replace(cfg, project_dir=str(tmp_path / "project-b")),
    ]
    paths = {original, *(schedule_path(item) for item in alternatives)}
    assert len(paths) == 4
    assert all("test-only-secret" not in str(path) for path in paths)


def test_jobs_runs_and_dedicated_session_survive_reopen(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=1)
    with ScheduleStore(path) as store:
        job = every_minute(store, owner="owner-a")
        store.materialize(now=due)
        run = store.claim(now=due)
        store.mark_running(run["id"], "thread-for-this-job")
        store.finish(run["id"], "succeeded", result="First report", now=due)

    with ScheduleStore(path) as store:
        restored = store.get(job["id"], owner="owner-a")
        assert restored["prompt"] == "Summarize the project status."
        assert restored["thread_id"] == "thread-for-this-job"
        assert store.runs(job["id"])[0]["result"] == "First report"
        store.materialize(now=due + timedelta(minutes=1))
        next_run = store.claim(now=due + timedelta(minutes=1))
        assert next_run["job_id"] == job["id"]
        assert next_run["id"] != run["id"]
        assert store.get(job["id"])["thread_id"] == "thread-for-this-job"


def test_different_jobs_never_share_a_session(store):
    first = every_minute(store)
    second = every_minute(store)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    one = store.claim(now=due)
    store.mark_running(one["id"], "first-thread")
    two = store.claim(now=due)
    store.mark_running(two["id"], "second-thread")
    assert {one["job_id"], two["job_id"]} == {first["id"], second["id"]}
    assert store.get(one["job_id"])["thread_id"] == "first-thread"
    assert store.get(two["job_id"])["thread_id"] == "second-thread"


def test_global_disable_survives_restart(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=1)
    with ScheduleStore(path) as store:
        job = every_minute(store, catch_up=True)
        store.set_enabled(False)
    with ScheduleStore(path) as store:
        store.materialize(now=due)
        assert store.claim(now=due) is None
        assert store.runs(job["id"]) == []
        store.set_enabled(True)
        store.materialize(now=due)
        assert store.claim(now=due)["job_id"] == job["id"]


@pytest.mark.parametrize(
    "schedule",
    [
        {"kind": "interval", "seconds": 0},
        {"kind": "interval", "seconds": 59},
        {"kind": "interval", "seconds": float("inf")},
        {"kind": "interval", "seconds": float("nan")},
        {"kind": "once", "at": "2026-01-01T13:00:00"},
        {"kind": "once", "at": "not-a-date"},
        {"kind": "cron", "expression": "* * * * * *"},
        {"kind": "cron", "expression": "61 * * * *"},
        {"kind": "cron", "expression": "* * * * *", "timezone": "Nowhere/Invalid"},
        {"kind": "unsupported"},
    ],
)
def test_invalid_schedules_are_rejected_without_creating_jobs(store, schedule):
    with pytest.raises((ValueError, TypeError)):
        store.create(name="Invalid", prompt="Work", schedule=schedule, now=START)
    assert store.list_jobs() == []


def test_naive_clock_values_are_rejected(store):
    with pytest.raises((ValueError, TypeError)):
        every_minute(store, now=START.replace(tzinfo=None))
    assert store.list_jobs() == []


def test_invalid_update_is_atomic(store):
    job = every_minute(store)
    with pytest.raises((ValueError, TypeError)):
        store.update(
            job["id"], name="Changed", schedule={"kind": "interval", "seconds": 1}, now=START
        )
    assert store.get(job["id"])["name"] == job["name"]
    assert store.get(job["id"])["schedule"] == job["schedule"]


def test_future_one_off_does_not_run_early_and_only_runs_once(store):
    due = START + timedelta(hours=1)
    job = once(store, due)
    store.materialize(now=due - timedelta(seconds=1))
    assert store.claim(now=due - timedelta(seconds=1)) is None
    store.materialize(now=due)
    run = store.claim(now=due)
    assert run["job_id"] == job["id"]
    store.mark_running(run["id"], "one-off-thread")
    store.finish(run["id"], "succeeded", now=due)
    store.materialize(now=due + timedelta(days=30))
    assert store.claim(now=due + timedelta(days=30)) is None
    assert len(store.runs(job["id"])) == 1


def test_downtime_without_catch_up_records_every_missed_occurrence(store):
    job = every_minute(store, catch_up=False)
    resumed = START + timedelta(minutes=5, seconds=31)
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None
    missed = [r for r in store.runs(job["id"]) if r["status"] == "missed"]
    assert sum(r["count"] for r in missed) == 5
    store.materialize(now=resumed)
    assert sum(r["count"] for r in store.runs(job["id"])) == 5

    future = START + timedelta(minutes=6)
    store.materialize(now=future)
    run = store.claim(now=future)
    assert run is not None
    assert run["count"] == 1


def test_automatic_catch_up_coalesces_outage_into_one_run(store):
    job = every_minute(store, catch_up=True)
    resumed = START + timedelta(minutes=5, seconds=31)
    store.materialize(now=resumed)
    run = store.claim(now=resumed)
    assert run["job_id"] == job["id"]
    assert run["count"] == 5
    assert run["first_due"] != run["last_due"]
    assert store.claim(now=resumed) is None
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None


def test_long_cron_outage_is_counted_completely_before_catch_up(store):
    job = store.create(
        name="Frequent report",
        prompt="Review current status",
        schedule={"kind": "cron", "expression": "* * * * *", "timezone": "UTC"},
        catch_up=True,
        now=START,
    )
    resumed = START + timedelta(minutes=3000, seconds=31)
    run = None
    for _ in range(10):
        store.materialize(now=resumed)
        run = store.claim(now=resumed)
        if run is not None:
            break
    assert run is not None
    assert run["job_id"] == job["id"]
    assert run["count"] == 3000
    assert run["first_due"] == (START + timedelta(minutes=1)).timestamp()
    assert run["last_due"] == (START + timedelta(minutes=3000)).timestamp()
    assert store.claim(now=resumed) is None


def test_existing_pending_catch_up_waits_for_full_cron_backlog_collection(store):
    job = store.create(
        name="Frequent report",
        prompt="Review current status",
        schedule={"kind": "cron", "expression": "* * * * *", "timezone": "UTC"},
        catch_up=True,
        now=START,
    )
    store.materialize(now=START + timedelta(minutes=5, seconds=31))
    resumed = START + timedelta(minutes=3000, seconds=31)
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None
    for _ in range(10):
        store.materialize(now=resumed)
        run = store.claim(now=resumed)
        if run is not None:
            break
    assert run is not None
    assert run["count"] == 3000
    assert store.claim(now=resumed) is None


def test_manual_catch_up_preserves_history_and_cannot_replay_it_twice(store):
    job = every_minute(store)
    resumed = START + timedelta(minutes=4, seconds=31)
    store.materialize(now=resumed)
    store.catch_up(job["id"], now=resumed)
    run = store.claim(now=resumed)
    assert run["count"] == 4
    original_misses = [r for r in store.runs(job["id"]) if r["status"] == "missed"]
    assert sum(r["count"] for r in original_misses) == 4
    assert all(r["catchup_run_id"] == run["id"] for r in original_misses)
    store.mark_running(run["id"], "catch-up-thread")
    store.finish(run["id"], "succeeded", now=resumed)
    with pytest.raises(ValueError, match="No missed"):
        store.catch_up(job["id"], now=resumed)
    assert store.claim(now=resumed) is None


def test_disabling_automatic_catch_up_cancels_queued_automatic_work(store):
    job = every_minute(store, catch_up=True)
    resumed = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=resumed)
    store.update(job["id"], catch_up=False, now=resumed)
    assert store.claim(now=resumed) is None
    assert store.status()["missed_pending"] == 3
    store.catch_up(job["id"], now=resumed)
    explicit = store.claim(now=resumed)
    assert explicit["count"] == 3


def test_disabling_catch_up_preserves_current_on_time_occurrence_after_restart(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=2)
    with ScheduleStore(path) as store:
        job = every_minute(store, catch_up=True)
        store.materialize(now=due)
        pending = store.runs(job["id"])[0]
        assert pending["count"] == 2
    with ScheduleStore(path) as store:
        store.update(job["id"], catch_up=False, now=due)
        current = store.claim(now=due)
        assert current["count"] == 1
        assert current["first_due"] == due.timestamp()
        assert current["last_due"] == due.timestamp()
        assert store.status()["missed_pending"] == 1


def test_current_occurrence_is_not_confused_with_later_missed_ticks(store):
    job = every_minute(store, catch_up=True)
    current = START + timedelta(minutes=2)
    store.materialize(now=current)
    later = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=later)
    store.update(job["id"], catch_up=False, now=later)
    assert store.claim(now=later) is None
    missed = [run for run in store.runs(job["id"]) if run["status"] == "missed"]
    assert sum(run["count"] for run in missed) == 3
    assert any(run["first_due"] == run["last_due"] == current.timestamp() for run in missed)


def test_legacy_store_upgrade_preserves_unclassified_coalesced_occurrence_count(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=2)
    later = START + timedelta(minutes=3, seconds=31)
    with ScheduleStore(path) as store:
        job = every_minute(store, catch_up=True)
        store.materialize(now=due)
        store.materialize(now=later)
        store.db.execute('ALTER TABLE runs DROP COLUMN current_due')
        store.db.execute('PRAGMA user_version=1')
    with ScheduleStore(path) as store:
        assert store.db.execute('PRAGMA user_version').fetchone()[0] == 3
        store.update(job["id"], catch_up=False, now=later)
        assert store.claim(now=later) is None
        assert store.status()["missed_pending"] == 3
        unknown = [run for run in store.runs(job["id"]) if "Exact due time unavailable" in run["error"]]
        assert len(unknown) == 1 and unknown[0]["count"] == 1
        store.catch_up(job["id"], now=later)
        assert store.claim(now=later)["count"] == 3


def test_two_connections_can_upgrade_the_same_legacy_store(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    with ScheduleStore(path) as store:
        job = every_minute(store)
        store.db.execute('ALTER TABLE runs DROP COLUMN current_due')
        store.db.execute('PRAGMA user_version=1')
    barrier = Barrier(2)

    def open_store():
        barrier.wait(timeout=5)
        with ScheduleStore(path) as connection:
            return connection.get(job["id"])["prompt"], connection.db.execute('PRAGMA user_version').fetchone()[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(open_store), pool.submit(open_store)]
        results = [future.result(timeout=10) for future in futures]
    assert results == [(job["prompt"], 3), (job["prompt"], 3)]


def test_store_initialization_failure_closes_opened_connection(tmp_path, monkeypatch):
    path = tmp_path / "broken.sqlite3"
    path.write_bytes(b"not a database")
    connections = []
    connect = sqlite3.connect

    def capture_connection(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(schedules_module.sqlite3, "connect", capture_connection)
    with pytest.raises(sqlite3.DatabaseError):
        ScheduleStore(path)
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute('SELECT 1')


@pytest.mark.parametrize("repair", [True, False])
def test_invalid_saved_timezone_can_be_repaired_or_paused_with_failure_receipt(store, monkeypatch, repair):
    job = store.create(
        name="Clock repair", prompt="Write a report",
        schedule={"kind": "cron", "expression": "* * * * *", "timezone": "UTC"},
        now=START,
    )
    store.set_enabled(False)
    zone_info = schedules_module.ZoneInfo

    def missing_zone(name):
        if name == "UTC":
            raise schedules_module.ZoneInfoNotFoundError("Timezone unavailable")
        return zone_info(name)

    monkeypatch.setattr(schedules_module, "ZoneInfo", missing_zone)
    now = START + timedelta(minutes=3, seconds=31)
    changes = {"schedule": {"kind": "interval", "seconds": 60}} if repair else {"enabled": False}
    updated = store.update(job["id"], now=now, **changes)
    assert updated["enabled"] is repair
    failures = [run for run in store.runs(job["id"]) if run["source"] == "schedule_error"]
    assert len(failures) == 1
    assert failures[0]["status"] == "failed"
    assert "could not be counted" in failures[0]["error"]
    assert failures[0]["first_due"] == (START + timedelta(minutes=1)).timestamp()
    assert failures[0]["last_due"] == now.timestamp()
    if repair:
        store.set_enabled(True)
        assert store.claim(now=now + timedelta(minutes=1))["job_id"] == job["id"]


@pytest.mark.parametrize("command", ["catch_up", "update"])
def test_backlog_commands_allow_other_writers_between_bounded_batches(tmp_path, monkeypatch, command):
    path = tmp_path / "jobs.sqlite3"
    with ScheduleStore(path) as store, ScheduleStore(path) as other:
        job = store.create(
            name="Backlog report", prompt="Write a report",
            schedule={"kind": "cron", "expression": "* * * * *", "timezone": "UTC"},
            now=START,
        )
        total = schedules_module.CRON_BATCH_SIZE * 2 + 7
        now = START + timedelta(minutes=total, seconds=31)
        committed_counts = []

        def competing_write(_delay):
            assert not store.db.in_transaction
            other.heartbeat(now=now)
            committed_counts.append(sum(run["count"] for run in other.runs(job["id"]) if run["status"] == "missed"))

        monkeypatch.setattr(schedules_module, "time", SimpleNamespace(time=schedules_module.time.time, sleep=competing_write))
        if command == "catch_up":
            pending = store.catch_up(job["id"], now=now)
            assert pending["count"] == total
        else:
            store.update(job["id"], schedule={"kind": "interval", "seconds": 3600}, now=now)
        assert committed_counts == [schedules_module.CRON_BATCH_SIZE, schedules_module.CRON_BATCH_SIZE * 2]
        assert sum(run["count"] for run in store.runs(job["id"]) if run["status"] == "missed") == total


def test_host_receipt_is_durable_before_handshake_and_preserved_when_running(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=1)
    with ScheduleStore(path) as store:
        job = every_minute(store)
        run = store.claim(now=due)
        store.record_host(run["id"], 123456, "test-boot:123")
    with ScheduleStore(path) as store:
        early = store.runs(job["id"])[0]
        assert early["status"] == "starting"
        assert early["host_pid"] == 123456
        assert early["host_start"] == "test-boot:123"
        store.mark_running(run["id"], "connected-thread")
        store.recover_interrupted(now=due)
        recovered = store.runs(job["id"])[0]
        assert recovered["status"] == "uncertain"
        assert recovered["host_pid"] == 123456
        assert recovered["host_start"] == "test-boot:123"


def test_disabling_automatic_catch_up_does_not_cancel_explicit_manual_request(store):
    job = every_minute(store, catch_up=False)
    resumed = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=resumed)
    requested = store.catch_up(job["id"], now=resumed)
    store.update(job["id"], catch_up=True, now=resumed)
    store.update(job["id"], catch_up=False, now=resumed)
    run = store.claim(now=resumed)
    assert run["id"] == requested["id"]
    assert run["count"] == 3


def test_editing_prompt_releases_cancelled_catch_up_history_for_later_recovery(store):
    job = every_minute(store, catch_up=True)
    resumed = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=resumed)
    store.update(job["id"], prompt="Updated report", catch_up=False, now=resumed)
    assert store.claim(now=resumed) is None
    assert store.status()["missed_pending"] == 3
    store.catch_up(job["id"], now=resumed)
    run = store.claim(now=resumed)
    assert run["count"] == 3
    assert run["prompt"] == "Updated report"


def test_pausing_after_outage_records_pre_pause_misses_but_not_paused_time(store):
    job = every_minute(store)
    paused = START + timedelta(minutes=3, seconds=31)
    store.update(job["id"], enabled=False, now=paused)
    resumed = START + timedelta(hours=4)
    store.update(job["id"], enabled=True, now=resumed)
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None
    misses = [run for run in store.runs(job["id"]) if run["status"] == "missed"]
    assert sum(run["count"] for run in misses) == 3
    assert store.status()["missed_pending"] == 3


def test_schedule_edit_after_outage_preserves_old_schedule_miss_receipts(store):
    job = every_minute(store)
    edited = START + timedelta(minutes=3, seconds=31)
    store.update(job["id"], schedule={"kind": "interval", "seconds": 3600}, now=edited)
    misses = [run for run in store.runs(job["id"]) if run["status"] == "missed"]
    assert sum(run["count"] for run in misses) == 3
    store.materialize(now=edited + timedelta(minutes=59))
    assert store.claim(now=edited + timedelta(minutes=59)) is None
    due = edited + timedelta(hours=1)
    store.materialize(now=due)
    assert store.claim(now=due)["count"] == 1


def test_pending_manual_run_survives_outage_when_automatic_catch_up_is_off(store):
    job = every_minute(store, catch_up=False)
    manual = store.enqueue(job["id"], now=START)
    resumed = START + timedelta(minutes=3, seconds=31)
    run = store.claim(now=resumed)
    assert run["id"] == manual["id"]
    assert run["count"] == 1
    assert sum(item["count"] for item in store.runs(job["id"]) if item["status"] == "missed") == 3


@pytest.mark.parametrize("catch_up", [False, True])
def test_global_disable_retains_downtime_for_policy_on_enable(store, catch_up):
    job = every_minute(store, catch_up=catch_up)
    store.set_enabled(False)
    resumed = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None
    assert store.runs(job["id"]) == []
    store.set_enabled(True)
    store.materialize(now=resumed)
    run = store.claim(now=resumed)
    if catch_up:
        assert run["count"] == 3
    else:
        assert run is None
        assert sum(r["count"] for r in store.runs(job["id"])) == 3


def test_paused_job_does_not_catch_up_intentionally_paused_time(store):
    job = every_minute(store, catch_up=True)
    store.update(job["id"], enabled=False, now=START)
    resumed = START + timedelta(hours=4)
    store.materialize(now=resumed)
    assert store.runs(job["id"]) == []
    store.update(job["id"], enabled=True, now=resumed)
    store.materialize(now=resumed)
    assert store.claim(now=resumed) is None
    store.materialize(now=resumed + timedelta(minutes=1))
    run = store.claim(now=resumed + timedelta(minutes=1))
    assert run["count"] == 1


@pytest.mark.parametrize("catch_up", [False, True])
def test_pending_work_that_aged_during_outage_obeys_catch_up_policy(store, catch_up):
    job = every_minute(store, catch_up=catch_up)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    resumed = START + timedelta(minutes=4, seconds=31)
    store.materialize(now=resumed)
    run = store.claim(now=resumed)
    if catch_up:
        assert run["count"] == 4
    else:
        assert run is None
        assert sum(r["count"] for r in store.runs(job["id"]) if r["status"] == "missed") == 4


def test_prompt_edit_cancels_queued_work_but_preserves_historical_snapshot(store):
    job = every_minute(store)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    store.update(job["id"], prompt="A different task", now=due)
    assert store.claim(now=due) is None
    prior = store.runs(job["id"])[0]
    assert prior["prompt"] == job["prompt"]
    assert prior["status"] == "cancelled"
    assert store.get(job["id"])["prompt"] == "A different task"
    store.materialize(now=due + timedelta(minutes=1))
    run = store.claim(now=due + timedelta(minutes=1))
    assert run["prompt"] == "A different task"


def test_inflight_work_never_overlaps_even_when_new_occurrences_arrive(store):
    job = every_minute(store, catch_up=True)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    run = store.claim(now=due)
    store.mark_running(run["id"], "long-running-thread")
    later = START + timedelta(minutes=5)
    store.materialize(now=later)
    assert store.claim(now=later) is None
    assert len([r for r in store.runs(job["id"]) if r["status"] in {"starting", "running"}]) == 1


@pytest.mark.parametrize("was_running", [False, True])
def test_interrupted_run_requires_explicit_recovery_and_blocks_automatic_replay(tmp_path, was_running):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=1)
    with ScheduleStore(path) as store:
        job = every_minute(store, catch_up=True)
        store.materialize(now=due)
        run = store.claim(now=due)
        if was_running:
            store.mark_running(run["id"], "interrupted-thread")

    later = START + timedelta(minutes=5, seconds=31)
    with ScheduleStore(path) as store:
        store.recover_interrupted(now=later)
        store.materialize(now=later)
        assert next(r for r in store.runs(job["id"]) if r["id"] == run["id"])["status"] == "uncertain"
        assert store.claim(now=later) is None
        with pytest.raises(ValueError):
            store.recover(run["id"], action="retry", now=later)
        assert store.claim(now=later) is None
        store.recover(run["id"], action="retry", acknowledge_duplicate_risk=True, now=later)
        retry = store.claim(now=later)
        assert retry["id"] != run["id"]
        assert retry["job_id"] == job["id"]
        assert retry["prompt"] == run["prompt"]


def test_hard_process_exit_preserves_run_and_prevents_automatic_duplicate(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    child = """
import json
import os
import sys
from inkbox_codex.schedules import ScheduleStore
store = ScheduleStore(sys.argv[1])
job = store.create(name="Crash test", prompt="Do this once", schedule={"kind": "interval", "seconds": 60}, catch_up=True, now=1767268800)
store.materialize(now=1767268860)
run = store.claim(now=1767268860)
store.mark_running(run["id"], "persisted-before-process-exit")
print(json.dumps({"job_id": job["id"], "run_id": run["id"]}), flush=True)
os._exit(17)
"""
    result = subprocess.run([sys.executable, "-c", child, str(path)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 17, result.stderr
    ids = json.loads(result.stdout)
    with ScheduleStore(path) as store:
        store.recover_interrupted(now=1767269160)
        store.materialize(now=1767269160)
        assert store.get(ids["job_id"])["thread_id"] == "persisted-before-process-exit"
        previous = next(run for run in store.runs(ids["job_id"]) if run["id"] == ids["run_id"])
        assert previous["status"] == "uncertain"
        assert store.claim(now=1767269160) is None


def test_dismissing_uncertain_run_does_not_replay_its_prompt(store):
    due = START + timedelta(minutes=1)
    job = once(store, due)
    store.materialize(now=due)
    run = store.claim(now=due)
    store.recover_interrupted(now=due)
    store.recover(run["id"], action="dismiss", now=due)
    assert store.runs(job["id"])[0]["status"] == "dismissed"
    assert store.claim(now=due) is None


def test_retry_uses_failed_execution_prompt_even_after_job_edit(store):
    job = every_minute(store)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    failed = store.claim(now=due)
    store.finish(failed["id"], "failed", error="Could not start", now=due)
    store.update(job["id"], prompt="A newly configured task", now=due)
    store.recover(failed["id"], action="retry", acknowledge_duplicate_risk=True, now=due)
    retry = store.claim(now=due)
    assert retry["prompt"] == failed["prompt"]
    assert store.get(job["id"])["prompt"] == "A newly configured task"


def test_deletion_cancels_pending_work_but_keeps_run_history(store):
    job = every_minute(store)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    store.delete(job["id"])
    assert store.list_jobs() == []
    assert store.claim(now=due) is None
    assert store.runs(job["id"])[0]["status"] == "cancelled"


def test_deleted_job_missed_history_is_not_reported_as_pending_work(store):
    job = every_minute(store)
    resumed = START + timedelta(minutes=3, seconds=31)
    store.materialize(now=resumed)
    store.delete(job["id"])
    assert store.status()["missed_pending"] == 0
    assert sum(run["count"] for run in store.runs(job["id"]) if run["status"] == "missed") == 3


def test_deletion_refuses_inflight_work(store):
    job = every_minute(store)
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    run = store.claim(now=due)
    store.mark_running(run["id"], "still-running")
    with pytest.raises(ValueError):
        store.delete(job["id"])
    assert store.get(job["id"])["enabled"]
    assert store.runs(job["id"])[0]["status"] == "running"


def test_owner_scoping_covers_reads_and_mutations(store):
    job = every_minute(store, owner="owner-a")
    assert store.list_jobs(owner="owner-b") == []
    assert store.runs(owner="owner-b") == []
    for operation in (
        lambda: store.get(job["id"], owner="owner-b"),
        lambda: store.update(job["id"], owner="owner-b", prompt="Changed", now=START),
        lambda: store.enqueue(job["id"], owner="owner-b", now=START),
        lambda: store.catch_up(job["id"], owner="owner-b", now=START),
        lambda: store.delete(job["id"], owner="owner-b"),
    ):
        with pytest.raises((KeyError, ValueError)):
            operation()
    assert store.get(job["id"], owner="owner-a")["prompt"] == job["prompt"]


def test_owner_cannot_recover_someone_elses_uncertain_run(store):
    job = every_minute(store, owner="owner-a")
    due = START + timedelta(minutes=1)
    store.materialize(now=due)
    run = store.claim(now=due)
    store.recover_interrupted(now=due)
    with pytest.raises((KeyError, ValueError)):
        store.recover(run["id"], action="dismiss", owner="owner-b", now=due)
    assert store.runs(job["id"], owner="owner-a")[0]["status"] == "uncertain"


def test_two_connections_cannot_claim_same_run(tmp_path):
    path = tmp_path / "jobs.sqlite3"
    due = START + timedelta(minutes=1)
    with ScheduleStore(path) as store:
        every_minute(store)
        store.materialize(now=due)
    barrier = Barrier(2)

    def claim():
        with ScheduleStore(path) as connection:
            barrier.wait(timeout=5)
            return connection.claim(now=due)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim), pool.submit(claim)]
        results = [future.result(timeout=10) for future in futures]
    assert sum(result is not None for result in results) == 1


def test_cron_uses_timezone_and_preserves_future_schedule(store):
    store.create(
        name="Local morning",
        prompt="Morning report",
        schedule={"kind": "cron", "expression": "0 9 * * *", "timezone": "America/New_York"},
        now=START,
    )
    before = datetime(2026, 1, 1, 13, 59, tzinfo=UTC)
    due = datetime(2026, 1, 1, 14, 0, tzinfo=UTC)
    store.materialize(now=before)
    assert store.claim(now=before) is None
    store.materialize(now=due)
    assert store.claim(now=due) is not None


def test_cron_crosses_leap_day_without_losing_schedule(store):
    start = datetime(2028, 2, 28, 12, 0, tzinfo=UTC)
    due = datetime(2028, 2, 29, 9, 0, tzinfo=UTC)
    store.create(
        name="Leap-day report",
        prompt="Write a report",
        schedule={"kind": "cron", "expression": "0 9 29 2 *", "timezone": "UTC"},
        now=start,
    )
    store.materialize(now=due - timedelta(seconds=1))
    assert store.claim(now=due - timedelta(seconds=1)) is None
    store.materialize(now=due)
    assert store.claim(now=due) is not None


def test_cron_daily_wall_clock_tracks_daylight_saving_change(store):
    before_change = datetime(2026, 3, 7, 12, 0, tzinfo=UTC)
    first_due = datetime(2026, 3, 7, 14, 0, tzinfo=UTC)
    second_due = datetime(2026, 3, 8, 13, 0, tzinfo=UTC)
    job = store.create(
        name="Morning report",
        prompt="Write a report",
        schedule={"kind": "cron", "expression": "0 9 * * *", "timezone": "America/New_York"},
        now=before_change,
    )
    store.materialize(now=first_due)
    first = store.claim(now=first_due)
    store.mark_running(first["id"], "morning-thread")
    store.finish(first["id"], "succeeded", now=first_due)
    store.materialize(now=second_due - timedelta(seconds=1))
    assert store.claim(now=second_due - timedelta(seconds=1)) is None
    store.materialize(now=second_due)
    second = store.claim(now=second_due)
    assert second["job_id"] == job["id"]
    assert second["count"] == 1


def test_cron_nonexistent_spring_time_moves_to_next_valid_local_time(store):
    start = datetime(2026, 3, 7, 12, 0, tzinfo=UTC)
    due = datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
    store.create(
        name="Early report",
        prompt="Write a report",
        schedule={"kind": "cron", "expression": "30 2 * * *", "timezone": "America/New_York"},
        now=start,
    )
    store.materialize(now=due - timedelta(seconds=1))
    assert store.claim(now=due - timedelta(seconds=1)) is None
    store.materialize(now=due)
    assert store.claim(now=due)["count"] == 1


def test_cron_repeated_fall_time_has_two_distinct_occurrences(store):
    start = datetime(2026, 11, 1, 4, 0, tzinfo=UTC)
    first_due = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    second_due = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    job = store.create(
        name="Early report",
        prompt="Write a report",
        schedule={"kind": "cron", "expression": "30 1 * * *", "timezone": "America/New_York"},
        now=start,
    )
    store.materialize(now=first_due)
    first = store.claim(now=first_due)
    store.mark_running(first["id"], "fall-thread")
    store.finish(first["id"], "succeeded", now=first_due)
    store.materialize(now=second_due)
    second = store.claim(now=second_due)
    assert second["id"] != first["id"]
    assert second["job_id"] == job["id"]
    assert first["first_due"] == first_due.timestamp()
    assert second["first_due"] == second_due.timestamp()

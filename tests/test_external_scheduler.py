"""External timing preserves native execution and saved recurrence decisions."""
from datetime import UTC, datetime

import pytest

from inkbox_codex.schedules import ScheduleStore


@pytest.fixture
def store(tmp_path):
    with ScheduleStore(tmp_path / 'schedules.sqlite3') as value:
        value.use_external_timer()
        yield value


def job(store, **changes):
    return store.create('Example', 'Check status', {'kind': 'interval', 'seconds': 60}, now=1000, **changes)


def acknowledge(store):
    registration, = store.registrations()
    store.acknowledge_registration(registration['job_id'], registration['revision'])
    return registration


def test_external_edit_is_atomic_and_replay_does_not_reset_due_time(store):
    saved = job(store)
    schedule = {'kind': 'interval', 'seconds': 120}
    receipt = store.update_external(saved['id'], saved['registration_revision'], 'edit-1', schedule, True, now=1020)
    assert store.get(saved['id'])['next_run'] == 1140
    with ScheduleStore(store.path) as restarted:
        assert restarted.update_external(saved['id'], saved['registration_revision'], 'edit-1', schedule, True, now=1030) == receipt
        assert restarted.get(saved['id'])['next_run'] == 1140
        with pytest.raises(ValueError, match='already used'):
            restarted.update_external(saved['id'], saved['registration_revision'], 'edit-1', schedule, False)
        with pytest.raises(ValueError, match='changed'):
            restarted.update_external(saved['id'], saved['registration_revision'], 'edit-2', schedule, False)


def test_external_edit_receipt_failure_rolls_back_job(store):
    saved = job(store)
    store.db.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON external_updates BEGIN SELECT RAISE(ABORT, 'receipt failed'); END")
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError, match='receipt failed'):
        store.update_external(saved['id'], saved['registration_revision'], 'edit-1', saved['schedule'], False, now=1020)
    assert store.get(saved['id']) == saved
    assert not store.db.in_transaction


def test_concurrent_external_edits_allow_only_one_revision(store):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    saved = job(store)
    gate = Barrier(2)
    def edit(command):
        with ScheduleStore(store.path) as writer:
            gate.wait(timeout=5)
            try:
                return writer.update_external(saved['id'], saved['registration_revision'], command, saved['schedule'], False, now=1020)
            except ValueError:
                return None
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(edit, ('edit-1', 'edit-2')))
    assert sum(value is not None for value in results) == 1
    assert not store.get(saved['id'])['enabled']


def test_local_ticks_never_execute_externally_owned_jobs(store):
    saved = job(store)
    assert store.claim(now=1060) is None
    assert store.runs(saved['id']) == []
    assert not store.registrations()[0]['registered']
    registration = acknowledge(store)
    receipt = store.accept_occurrence(saved['id'], registration['revision'], 'occurrence-1', 1060, now=1060)
    assert receipt['status'] == 'pending'
    assert store.accept_occurrence(saved['id'], registration['revision'], 'occurrence-1', 1060, now=1061)['id'] == receipt['id']
    assert store.accept_occurrence(saved['id'], registration['revision'], 'different-delivery-id', 1060, now=1061)['id'] == receipt['id']
    assert store.claim(now=1061)['id'] == receipt['id']
    assert store.claim(now=1062) is None


def test_edit_pause_and_delete_invalidate_registration(store):
    saved = job(store)
    first = acknowledge(store)
    store.update(saved['id'], prompt='New instructions', now=1020)
    with pytest.raises(ValueError, match='changed'):
        store.acknowledge_registration(saved['id'], first['revision'])
    with pytest.raises(ValueError, match='changed'):
        store.accept_occurrence(saved['id'], first['revision'], 'old', 1060, now=1060)
    second = acknowledge(store)
    store.update(saved['id'], enabled=False, now=1030)
    assert not store.registrations()[0]['enabled']
    with pytest.raises(ValueError):
        store.accept_occurrence(saved['id'], second['revision'], 'paused', 1060, now=1060)
    store.delete(saved['id'])
    registration = acknowledge(store)
    assert registration['deleted'] and not registration['enabled']
    assert store.registrations()[0]['registered']


def test_restart_retains_external_ownership_and_pending_sync(store):
    saved = job(store)
    other = ScheduleStore(store.path)
    try:
        assert other.external_timer()
        assert other.registrations()[0]['job_id'] == saved['id']
        assert not other.registrations()[0]['registered']
        assert other.claim(now=1060) is None
    finally:
        other.close()


@pytest.mark.parametrize('catch_up,expected', [(False, 'missed'), (True, 'pending')])
def test_delayed_dispatch_uses_native_catchup_policy(store, catch_up, expected):
    saved = job(store, catch_up=catch_up)
    registration = acknowledge(store)
    receipt = store.accept_occurrence(saved['id'], registration['revision'], 'delayed', 1060, now=1961)
    assert receipt['status'] == expected
    assert store.accept_occurrence(saved['id'], registration['revision'], 'delayed', 1060, now=1962)['status'] == expected
    if catch_up:
        assert store.claim(now=1962)['id'] == receipt['id']
        assert store.claim(now=1963) is None


def test_future_or_unacknowledged_occurrence_never_executes(store):
    saved = job(store)
    with pytest.raises(ValueError):
        store.accept_occurrence(saved['id'], saved['registration_revision'], 'unregistered', 1060, now=1060)
    registration = acknowledge(store)
    with pytest.raises(ValueError):
        store.accept_occurrence(saved['id'], registration['revision'], 'early', 1060, now=1059)
    assert store.runs(saved['id']) == []


def test_global_pause_retires_acknowledged_schedule(store):
    saved = job(store)
    registration = acknowledge(store)
    store.set_enabled(False)
    assert not store.registrations()[0]['enabled']
    with pytest.raises(ValueError):
        store.accept_occurrence(saved['id'], registration['revision'], 'paused', 1060, now=1060)
    store.set_enabled(True)
    assert not store.registrations()[0]['registered']


def test_timezone_defaults_only_on_creation(store, monkeypatch):
    monkeypatch.setenv('TZ', 'America/New_York')
    now = datetime(2026, 1, 1, tzinfo=UTC).timestamp()
    saved = store.create('Morning', 'Check', {'kind': 'cron', 'expression': '0 9 * * *'}, now=now)
    assert saved['schedule']['timezone'] == 'America/New_York'
    monkeypatch.setenv('TZ', 'Asia/Tokyo')
    assert store.get(saved['id'])['schedule']['timezone'] == 'America/New_York'
    other = store.create('Morning 2', 'Check', {'kind': 'cron', 'expression': '0 9 * * *', 'timezone': 'UTC'}, now=now)
    assert other['schedule']['timezone'] == 'UTC'


def test_cancellation_before_delivery_survives_restart(store):
    saved = job(store)
    registration = acknowledge(store)
    receipt = store.cancel_occurrence(saved['id'], registration['revision'], 'cancel-before', 1060)
    assert receipt['status'] == 'cancelled'
    with ScheduleStore(store.path) as restarted:
        assert restarted.accept_occurrence(saved['id'], registration['revision'], 'cancel-before', 1060, now=1060)['status'] == 'cancelled'
        assert restarted.claim(now=1060) is None
        with pytest.raises(ValueError):
            restarted.accept_occurrence(saved['id'], registration['revision'], 'cancel-before', 1120, now=1120)


def test_cancellation_of_accepted_pending_occurrence_prevents_execution(store):
    saved = job(store)
    registration = acknowledge(store)
    run = store.accept_occurrence(saved['id'], registration['revision'], 'cancel-pending', 1060, now=1060)
    assert store.cancel_occurrence(saved['id'], registration['revision'], 'cancel-pending', 1060)['status'] == 'cancelled'
    assert store.cancellation_requested(run['id'])
    assert store.claim(now=1061) is None
    assert store.accept_occurrence(saved['id'], registration['revision'], 'cancel-pending', 1060, now=1062)['status'] == 'cancelled'


def test_cancellation_keeps_started_execution_ambiguous_until_owner_stops_it(store):
    saved = job(store)
    registration = acknowledge(store)
    run = store.accept_occurrence(saved['id'], registration['revision'], 'cancel-started', 1060, now=1060)
    assert store.claim(now=1060)['id'] == run['id']
    assert store.cancel_occurrence(saved['id'], registration['revision'], 'cancel-started', 1060)['status'] == 'starting'
    assert store.cancellation_requested(run['id'])
    store.mark_running(run['id'], 'native-thread')
    store.finish(run['id'], 'uncertain', now=1061)
    assert store.cancel_occurrence(saved['id'], registration['revision'], 'cancel-started', 1060)['status'] == 'uncertain'


def test_cancellation_alias_stops_same_native_occurrence(store):
    saved = job(store)
    revision = acknowledge(store)['revision']
    run = store.accept_occurrence(saved['id'], revision, 'original', 1060, now=1060)
    assert store.claim(now=1060)['id'] == run['id']
    store.mark_running(run['id'], 'thread')
    assert store.cancel_occurrence(saved['id'], revision, 'alias', 1060)['status'] == 'running'
    assert store.cancellation_requested(run['id'])
    assert store.accept_occurrence(saved['id'], revision, 'original', 1060, now=1061)['id'] == run['id']


def test_timely_external_admission_survives_startup_latency(store):
    saved = job(store)
    registration = acknowledge(store)
    receipt = store.accept_occurrence(saved['id'], registration['revision'], 'cold-start', 1060,
                                     admitted_at=1061, now=1120)
    assert receipt['status'] == 'pending'
    assert store.claim(now=1121)['id'] == receipt['id']
    assert store.accept_occurrence(saved['id'], registration['revision'], 'cold-start', 1060,
                                   admitted_at=1061, now=1200)['id'] == receipt['id']


def test_genuinely_late_external_admission_still_misses(store):
    saved = job(store)
    registration = acknowledge(store)
    assert store.accept_occurrence(saved['id'], registration['revision'], 'late-admission', 1060,
                                   admitted_at=1100, now=1961)['status'] == 'missed'


def test_external_quarantine_never_replays_and_requires_stopped_process(store):
    saved = job(store)
    registration = acknowledge(store)
    receipt = store.accept_occurrence(saved['id'], registration['revision'], 'unknown', 1060, now=1060)
    claimed = store.claim(now=1061)
    assert claimed['id'] == receipt['id']
    store.record_host(claimed['id'], 999999, 'synthetic-start')
    store.finish(claimed['id'], 'uncertain', now=1062)
    with pytest.raises(ValueError, match='previous execution process'):
        store.quarantine_occurrence(saved['id'], registration['revision'], 'unknown', 1060)
    store.clear_host(claimed['id'])
    assert store.quarantine_occurrence(saved['id'], registration['revision'], 'unknown', 1060)['status'] == 'dismissed'
    assert store.accept_occurrence(saved['id'], registration['revision'], 'unknown', 1060, now=1100)['status'] == 'dismissed'
    assert store.claim(now=1101) is None


@pytest.mark.parametrize('delay,expected', [(899, 'pending'), (900, 'pending'), (901, 'missed')])
def test_external_startup_grace_is_fifteen_minutes(store, delay, expected):
    saved = job(store)
    registration = acknowledge(store)
    receipt = store.accept_occurrence(saved['id'], registration['revision'], 'grace', 1060,
                                      admitted_at=1061, now=1060 + delay)
    assert receipt['status'] == expected
    assert store.accept_occurrence(saved['id'], registration['revision'], 'grace', 1060,
                                   admitted_at=1061, now=5000)['status'] == expected


def test_one_shot_late_without_catchup_is_not_run_again(store):
    saved = store.create('Once', 'Check', {'kind': 'once', 'at': datetime.fromtimestamp(1060, UTC).isoformat()}, now=1000)
    registration = acknowledge(store)
    assert store.accept_occurrence(saved['id'], registration['revision'], 'once-late', 1060,
                                   admitted_at=1060, now=1961)['status'] == 'missed'
    assert store.claim(now=1962) is None
    assert store.get(saved['id'])['next_run'] is None

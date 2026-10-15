"""Bounded recurring schedules: deterministic clock, durable receipts, no sends."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier

import pytest

from inkbox_codex.schedules import GRACE_SECONDS, ScheduleStore


def iso(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


@pytest.fixture
def store(tmp_path):
    with ScheduleStore(tmp_path / 'jobs.sqlite3') as result:
        yield result


def create(store, **kwargs):
    return store.create('Project progress', 'Save a progress summary.',
                        {'kind': 'interval', 'seconds': 1800}, now=0, **kwargs)


def finish(store, run, now, status='succeeded'):
    store.mark_running(run['id'], 'job-session')
    store.finish(run['id'], status, now=now)


@pytest.mark.parametrize('bounds', [{'duration_seconds': 10800}, {'max_runs': 6},
                                  {'end_at': iso(10800), 'max_runs': 6}])
def test_half_hour_for_three_hours_is_one_job_with_six_runs(store, bounds):
    job = create(store, **bounds)
    assert store.claim(now=0) is None
    for number in range(1, 7):
        due = number * 1800
        assert store.claim(now=due - 1) is None
        # Normal clock jitter must not discard the inclusive final occurrence.
        run = store.claim(now=due + 1)
        assert run['job_id'] == job['id'] and run['count'] == 1
        finish(store, run, due + 2)
    assert store.claim(now=10800 + GRACE_SECONDS + 1) is None
    stopped = store.get(job['id'])
    assert stopped['run_count'] == 6 and stopped['next_run'] is None
    assert stopped['enabled'] is False
    assert stopped['stop_reason'] == ('max_runs' if 'max_runs' in bounds else 'end_at')
    assert len(store.runs(job['id'])) == 6
    assert len(store.list_jobs()) == 1


def test_end_time_and_count_survive_restart_pause_and_resume(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        job = create(store, duration_seconds=10800, max_runs=6)
        finish(store, store.claim(now=1800), 1801)
        store.update(job['id'], enabled=False, now=1802)
    with ScheduleStore(path) as store:
        resumed = store.update(job['id'], enabled=True, now=4000)
        assert resumed['end_at'] == 10800 and resumed['run_count'] == 1
        assert resumed['remaining_runs'] == 5 and resumed['next_run'] == 5800
        finish(store, store.claim(now=5800), 5801)
        assert store.get(job['id'])['thread_id'] == 'job-session'
        store.materialize(now=20000)
        assert store.get(job['id'])['stop_reason'] == 'end_at'
        assert sum(r['count'] for r in store.runs(job['id']) if r['status'] == 'missed') == 2
        with pytest.raises(ValueError, match='end_at'):
            store.update(job['id'], enabled=True, now=20001)


@pytest.mark.parametrize('catch_up', [False, True])
def test_downtime_after_expiry_counts_only_inside_window_and_never_replays(store, catch_up):
    job = create(store, duration_seconds=10800, catch_up=catch_up)
    assert store.claim(now=20000) is None
    missed = store.runs(job['id'])
    assert len(missed) == 1 and missed[0]['status'] == 'missed'
    assert missed[0]['count'] == 6 and missed[0]['last_due'] == 10800
    assert store.get(job['id'])['run_count'] == 0
    for action in (store.enqueue, store.catch_up):
        with pytest.raises(ValueError, match='end_at'):
            action(job['id'], now=20001)
    assert store.claim(now=25000) is None
    assert store.runs(job['id']) == missed


def test_catch_up_counts_as_one_attempt_not_as_all_missed_ticks(store):
    job = create(store, duration_seconds=10800, max_runs=2, catch_up=True)
    run = store.claim(now=7201)
    assert run['count'] == 4
    finish(store, run, 7202)
    assert store.get(job['id'])['remaining_runs'] == 1
    finish(store, store.claim(now=9000), 9001)
    assert store.claim(now=10800) is None
    assert store.get(job['id'])['stop_reason'] == 'max_runs'


def test_manual_and_failed_attempts_consume_budget_retries_cannot_bypass(store):
    job = create(store, max_runs=2)
    store.enqueue(job['id'], now=1)
    run = store.claim(now=1)
    store.finish(run['id'], 'failed', now=2)
    store.recover(run['id'], acknowledge_duplicate_risk=True, now=3)
    retried = store.claim(now=3)
    finish(store, retried, 4, 'uncertain')
    assert store.get(job['id'])['run_count'] == 2
    for operation in (lambda: store.enqueue(job['id'], now=5),
                      lambda: store.recover(retried['id'], acknowledge_duplicate_risk=True, now=5),
                      lambda: store.update(job['id'], enabled=True, now=5)):
        with pytest.raises(ValueError, match='max_runs'):
            operation()
    assert store.runs(job['id'])[0]['status'] == 'uncertain'
    store.recover(retried['id'], action='dismiss', now=5)
    extended = store.update(job['id'], max_runs=3, enabled=True, now=6)
    assert extended['remaining_runs'] == 1 and extended['stop_reason'] is None
    assert extended['next_run'] == 1806


def test_expiration_cancels_already_queued_manual_run_but_not_started_work(store):
    job = create(store, end_at=iso(1800))
    queued = store.enqueue(job['id'], now=1799)
    assert store.claim(now=1831) is None
    assert store._run(queued['id'])['status'] == 'cancelled'
    assert store.get(job['id'])['run_count'] == 0
    other = store.create('Progress', 'Summarize.', {'kind': 'interval', 'seconds': 60},
                         end_at=iso(1900), now=1840)
    run = store.claim(now=1900)
    store.materialize(now=2000)
    assert store._run(run['id'])['status'] == 'starting'
    finish(store, run, 2001)
    assert store.get(other['id'])['stop_reason'] == 'end_at'
    assert store._run(run['id'])['status'] == 'succeeded'


def test_queued_catch_up_expires_and_releases_missed_receipts(store):
    job = create(store, duration_seconds=3600, catch_up=True)
    store.materialize(now=3500)
    pending = store._active(job['id'])
    assert pending['source'] == 'auto_catch_up'
    assert store.claim(now=3631) is None
    assert store._run(pending['id'])['status'] == 'cancelled'
    missed = [r for r in store.runs(job['id']) if r['status'] == 'missed']
    assert sum(r['count'] for r in missed) == 2
    assert all(r['catchup_run_id'] is None for r in missed)


def test_long_cron_outage_stops_at_inclusive_cutoff_across_batches(store):
    job = store.create('Progress', 'Summarize.', {'kind': 'cron', 'expression': '* * * * *'},
                       end_at=iso(60 * 2500), catch_up=True, now=0)
    for _ in range(4):
        assert store.claim(now=60 * 3000) is None
    stopped = store.get(job['id'])
    assert stopped['stop_reason'] == 'end_at' and stopped['next_run'] is None
    missed = [r for r in store.runs(job['id']) if r['status'] == 'missed']
    assert sum(r['count'] for r in missed) == 2500
    assert max(r['last_due'] for r in missed) == 60 * 2500


def test_absolute_cron_end_includes_final_due_and_excludes_next(store):
    job = store.create('Progress', 'Summarize.',
                       {'kind': 'cron', 'expression': '0 9 * * *', 'timezone': 'America/New_York'},
                       end_at='2030-01-02T14:00:00Z', now=datetime(2030, 1, 1, 15, tzinfo=timezone.utc))
    due = datetime(2030, 1, 2, 14, tzinfo=timezone.utc).timestamp()
    assert job['next_run'] == due
    run = store.claim(now=due)
    finish(store, run, due + 1)
    assert store.get(job['id'])['next_run'] is None
    assert store.claim(now=due + 86400) is None
    assert len(store.runs(job['id'])) == 1


def test_limits_can_be_extended_or_cleared_without_resetting_count(store):
    job = create(store, max_runs=1, duration_seconds=3600)
    finish(store, store.claim(now=1800), 1801)
    changed = store.update(job['id'], duration_seconds=7200, max_runs=None, enabled=True, now=1802)
    assert changed['end_at'] == 9002 and changed['run_count'] == 1
    assert changed['remaining_runs'] is None and changed['stop_reason'] is None
    cleared = store.update(job['id'], end_at=None, now=1803)
    assert cleared['end_at'] is None and cleared['next_run'] == 3602


@pytest.mark.parametrize('bounds', [
    {'max_runs': 0}, {'max_runs': True}, {'max_runs': 1.5}, {'max_runs': 2147483648},
    {'duration_seconds': 0}, {'duration_seconds': True}, {'duration_seconds': None},
    {'duration_seconds': float('inf')}, {'duration_seconds': 31536001},
    {'end_at': '2030-01-01T12:00:00'}, {'end_at': 3600}, {'end_at': 'invalid'},
    {'end_at': iso(0)}, {'end_at': iso(1799)},
    {'duration_seconds': 10800, 'end_at': None},
])
def test_invalid_limits_are_atomic(store, bounds):
    with pytest.raises(ValueError):
        create(store, **bounds)
    assert store.list_jobs() == []
    job = create(store)
    with pytest.raises(ValueError):
        store.update(job['id'], name='Changed', now=0, **bounds)
    assert store.get(job['id']) == job


def test_bound_changes_preserve_active_execution_and_reject_edits(store):
    job = create(store, max_runs=3)
    run = store.claim(now=1800)
    with pytest.raises(ValueError, match='active execution'):
        store.update(job['id'], max_runs=2, now=1801)
    assert store.get(job['id'])['max_runs'] == 3
    assert store._run(run['id'])['status'] == 'starting'


def test_reducing_budget_cancels_pending_without_counting_it(store):
    job = create(store)
    finish(store, store.claim(now=1800), 1801)
    pending = store.enqueue(job['id'], now=1802)
    stopped = store.update(job['id'], max_runs=1, now=1803)
    assert stopped['stop_reason'] == 'max_runs' and stopped['run_count'] == 1
    assert store._run(pending['id'])['status'] == 'cancelled'


def test_competing_connections_cannot_overspend_last_execution(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        job = create(store, max_runs=1)
    barrier = Barrier(2)
    def claim():
        with ScheduleStore(path) as store:
            barrier.wait(timeout=5)
            return store.claim(now=1800)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: claim(), range(2)))
    assert sum(run is not None for run in results) == 1
    with ScheduleStore(path) as store:
        assert store.get(job['id'])['run_count'] == 1
        assert store.get(job['id'])['stop_reason'] == 'max_runs'


def test_legacy_upgrade_backfills_attempts_without_changing_jobs_or_history(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        job = create(store)
        run = store.claim(now=1800)
        finish(store, run, 1801)
        store.enqueue(job['id'], now=1802)
        history = store.runs(job['id'])
        for column in ('end_at', 'max_runs', 'run_count', 'stop_reason'):
            store.db.execute('ALTER TABLE jobs DROP COLUMN ' + column)
        store.db.execute('PRAGMA user_version=2')
    with ScheduleStore(path) as store:
        upgraded = store.get(job['id'])
        assert upgraded['run_count'] == 1
        assert upgraded['max_runs'] is None and upgraded['end_at'] is None
        assert upgraded['enabled'] is True and upgraded['thread_id'] == 'job-session'
        assert store.runs(job['id']) == history
    with ScheduleStore(path) as store:
        assert store.get(job['id'])['run_count'] == 1


def test_extending_window_after_final_due_resumes_future_occurrences(store):
    job = create(store, duration_seconds=1800)
    finish(store, store.claim(now=1800), 1801)
    assert store.get(job['id'])['next_run'] is None
    updated = store.update(job['id'], duration_seconds=3600, now=1802)
    assert updated['enabled'] is True and updated['end_at'] == 5402
    assert updated['next_run'] == 3602
    assert store.claim(now=3602)['job_id'] == job['id']


@pytest.mark.parametrize('bounds,changes,later', [
    ({'max_runs': 1}, {'max_runs': 2}, 1802),
    ({'duration_seconds': 1800}, {'end_at': None}, 1900),
])
def test_uncertain_last_attempt_can_extend_limits_then_explicitly_retry(store, bounds, changes, later):
    job = create(store, **bounds)
    run = store.claim(now=1800)
    finish(store, run, 1801, 'uncertain')
    store.materialize(now=later)
    with pytest.raises(ValueError):
        store.recover(run['id'], acknowledge_duplicate_risk=True, now=later)
    updated = store.update(job['id'], enabled=True, now=later, **changes)
    assert updated['run_count'] == 1 and store._run(run['id'])['status'] == 'uncertain'
    assert store.claim(now=later) is None
    retried = store.recover(run['id'], acknowledge_duplicate_risk=True, now=later)
    assert retried['source'] == 'retry'
    assert store._run(run['id'])['catchup_run_id'] == retried['id']
    assert store.claim(now=later)['id'] == retried['id']


def test_extending_limits_preserves_pending_occurrence(store):
    job = create(store, max_runs=1, duration_seconds=1800)
    store.materialize(now=1800)
    pending = store._active(job['id'])
    changed = store.update(job['id'], max_runs=2, end_at=iso(3601), now=1801)
    assert changed['next_run'] == 3601
    assert store._run(pending['id'])['status'] == 'pending'
    assert store.claim(now=1801)['id'] == pending['id']
    finish(store, pending, 1802)
    assert store.claim(now=3601)['job_id'] == job['id']

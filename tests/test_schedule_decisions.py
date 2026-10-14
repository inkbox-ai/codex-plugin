"""Decision-controlled recurrence uses explicit receipts, not model prose."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from inkbox_codex.schedules import ScheduleStore


@pytest.fixture
def store(tmp_path):
    with ScheduleStore(tmp_path / 'jobs.sqlite3') as result:
        yield result


def job(store, **kwargs):
    return store.create('Monitor progress', 'Continue while work remains; stop when finished.',
                        {'kind': 'interval', 'seconds': 60}, decision_required=True, now=0, **kwargs)


def start(store, now=60):
    run = store.claim(now=now)
    store.mark_running(run['id'], 'monitor-session')
    return run


def test_explicit_continue_then_stop_uses_one_saved_job(store):
    saved = job(store)
    first = start(store)
    store.decide(saved['id'], first['id'], 'continue', now=61)
    store.finish(first['id'], 'succeeded', now=62)
    second = start(store, now=120)
    store.decide(saved['id'], second['id'], 'stop', now=121)
    assert store.get(saved['id'])['stop_reason'] == 'decision_stop'
    assert store._run(second['id'])['status'] == 'running'
    store.finish(second['id'], 'succeeded', result='Complete.', now=122)
    assert store.claim(now=180) is None
    assert len(store.list_jobs()) == 1
    assert [r['decision'] for r in store.runs(saved['id'])] == ['stop', 'continue']
    assert store.get(saved['id'])['run_count'] == 2


@pytest.mark.parametrize('result', ['Continue', 'stop', 'I will run again in five minutes.'])
def test_prose_is_not_a_decision_and_missing_decision_pauses(store, result):
    saved = job(store)
    run = start(store)
    store.finish(run['id'], 'succeeded', result=result, now=61)
    assert store.get(saved['id'])['enabled'] is False
    assert store.get(saved['id'])['stop_reason'] == 'decision_missing'
    assert store.claim(now=120) is None
    resumed = store.update(saved['id'], enabled=True, now=121)
    assert resumed['next_run'] == 181 and resumed['stop_reason'] is None


@pytest.mark.parametrize('decision', [None, 'continue', 'stop'])
@pytest.mark.parametrize('status', ['failed', 'uncertain'])
def test_unsuccessful_run_never_automatically_continues(store, decision, status):
    saved = job(store)
    run = start(store)
    if decision:
        store.decide(saved['id'], run['id'], decision, now=61)
    store.finish(run['id'], status, now=62)
    assert store.get(saved['id'])['stop_reason'] == ('decision_stop' if decision == 'stop' else 'decision_incomplete')
    assert store.claim(now=180) is None
    if status == 'uncertain':
        store.update(saved['id'], enabled=True, now=190)
        assert store.claim(now=250) is None
        retry = store.recover(run['id'], acknowledge_duplicate_risk=True, now=251)
        assert retry['decision'] is None
        assert store.claim(now=251)['id'] == retry['id']


@pytest.mark.parametrize('decision', [None, 'continue', 'stop'])
def test_crash_preserves_decision_but_cannot_continue_automatically(tmp_path, decision):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        saved = job(store)
        run = start(store)
        if decision:
            store.decide(saved['id'], run['id'], decision, now=61)
    with ScheduleStore(path) as store:
        store.recover_interrupted(now=62)
        assert store._run(run['id'])['decision'] == decision
        assert store._run(run['id'])['status'] == 'uncertain'
        assert store.get(saved['id'])['stop_reason'] == ('decision_stop' if decision == 'stop' else 'decision_incomplete')
        assert store.claim(now=120) is None


def test_successful_continue_survives_restart(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        saved = job(store)
        run = start(store)
        store.decide(saved['id'], run['id'], 'continue', now=61)
        store.finish(run['id'], 'succeeded', now=62)
    with ScheduleStore(path) as store:
        store.recover_interrupted(now=90)
        assert store.get(saved['id'])['enabled'] is True
        assert store.claim(now=120)['job_id'] == saved['id']


@pytest.mark.parametrize('bounds,finish_at,reason', [
    ({'max_runs': 1}, 62, 'max_runs'),
    ({'duration_seconds': 60}, 91, 'end_at'),
])
def test_continue_cannot_override_hard_limits(store, bounds, finish_at, reason):
    saved = job(store, **bounds)
    run = start(store)
    store.decide(saved['id'], run['id'], 'continue', now=61)
    store.finish(run['id'], 'succeeded', now=finish_at)
    assert store.get(saved['id'])['stop_reason'] == reason
    assert store.claim(now=120) is None


def test_repeat_decision_is_idempotent_and_conflict_is_rejected(store):
    saved = job(store)
    run = start(store)
    first = store.decide(saved['id'], run['id'], 'stop', now=61)
    assert store.decide(saved['id'], run['id'], 'stop', now=62) == first
    with pytest.raises(ValueError, match='different decision'):
        store.decide(saved['id'], run['id'], 'continue', now=63)
    assert store.get(saved['id'])['enabled'] is False
    assert store._run(run['id'])['decision'] == 'stop'


def test_only_current_running_decision_job_can_decide(store):
    saved = job(store)
    other = store.create('Other', 'Summary.', {'kind': 'interval', 'seconds': 60}, now=0)
    first = store.claim(now=60)
    with pytest.raises(ValueError, match='current running'):
        store.decide(first['job_id'], first['id'], 'stop', now=61)
    store.mark_running(first['id'], 'one')
    second = store.claim(now=60)
    store.mark_running(second['id'], 'two')
    own, ordinary = (first, second) if first['job_id'] == saved['id'] else (second, first)
    with pytest.raises(ValueError, match='current running'):
        store.decide(other['id'], own['id'], 'stop', now=61)
    with pytest.raises(ValueError, match='current running'):
        store.decide(other['id'], ordinary['id'], 'stop', now=61)
    with pytest.raises(ValueError, match='continue or stop'):
        store.decide(saved['id'], own['id'], 'later', now=61)
    assert store._run(own['id'])['decision'] is None
    store.finish(own['id'], 'succeeded', now=62)
    with pytest.raises(ValueError, match='current running'):
        store.decide(saved['id'], own['id'], 'continue', now=63)


def test_manual_pause_cannot_be_undone_by_continue(store):
    saved = job(store)
    run = start(store)
    store.update(saved['id'], enabled=False, now=61)
    store.decide(saved['id'], run['id'], 'continue', now=62)
    store.finish(run['id'], 'succeeded', now=63)
    assert store.get(saved['id'])['enabled'] is False
    assert store.claim(now=120) is None


def test_decision_mode_cannot_change_under_active_run(store):
    saved = job(store)
    run = start(store)
    with pytest.raises(ValueError, match='active execution'):
        store.update(saved['id'], decision_required=False, now=61)
    assert store._run(run['id'])['decision'] is None


def test_competing_decisions_cannot_overwrite_each_other(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        saved = job(store)
        run = start(store)
    barrier = Barrier(2)
    def decide(value):
        with ScheduleStore(path) as store:
            barrier.wait(timeout=5)
            try:
                return store.decide(saved['id'], run['id'], value, now=61)['decision']
            except ValueError:
                return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(decide, ['continue', 'stop']))
    assert sum(r is not None for r in results) == 1
    with ScheduleStore(path) as store:
        assert store._run(run['id'])['decision'] in results


def test_existing_job_upgrade_keeps_unconditional_recurrence(tmp_path):
    path = tmp_path / 'jobs.sqlite3'
    with ScheduleStore(path) as store:
        saved = store.create('Recurring', 'Summarize.', {'kind': 'interval', 'seconds': 60}, now=0)
        run = start(store)
        store.finish(run['id'], 'succeeded', now=61)
        store.db.execute('ALTER TABLE jobs DROP COLUMN decision_required')
        store.db.execute('ALTER TABLE runs DROP COLUMN decision')
        store.db.execute('PRAGMA user_version=3')
    with ScheduleStore(path) as store:
        assert store.get(saved['id'])['decision_required'] is False
        assert store._run(run['id'])['decision'] is None
        assert store.claim(now=120)['job_id'] == saved['id']

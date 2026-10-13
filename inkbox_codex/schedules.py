"""Durable, identity-scoped schedules and execution receipts."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

GRACE_SECONDS = 30
CRON_BATCH_SIZE = 1000
ACTIVE = ('pending', 'starting', 'running', 'uncertain')


def timestamp(value=None) -> float:
    if value is None:
        return time.time()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError('Use a timezone-aware date and time')
        value = value.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('Invalid timestamp')
    return float(value)


def schedule_path(cfg) -> Path:
    if not cfg.identity:
        raise ValueError('Configure an Inkbox identity before managing schedules')
    root = Path(os.getenv('INKBOX_CODEX_HOME') or Path.home() / '.inkbox-codex')
    url = urlsplit(cfg.base_url.strip() or 'https://inkbox.ai')
    origin = urlunsplit((url.scheme.lower(), url.netloc.lower(), url.path.rstrip('/'), '', ''))
    namespace = json.dumps([origin, cfg.identity,
                            str(Path(cfg.project_dir or os.getcwd()).expanduser().resolve())])
    return root / 'schedules' / hashlib.sha256(namespace.encode()).hexdigest() / 'jobs.sqlite3'


def validate_schedule(schedule: dict, now: float) -> tuple[dict, float]:
    if not isinstance(schedule, dict):
        raise ValueError('schedule must be an object')
    kind = schedule.get('kind')
    if kind == 'once':
        try:
            at = datetime.fromisoformat(schedule['at'].replace('Z', '+00:00'))
            due = timestamp(at)
        except (KeyError, TypeError, AttributeError, ValueError):
            raise ValueError('once requires an ISO date and time with an explicit UTC offset') from None
        if due <= now:
            raise ValueError('The one-off execution must be in the future')
        return {'kind': kind, 'at': at.isoformat()}, due
    if kind == 'interval':
        seconds = schedule.get('seconds')
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or not 60 <= seconds <= 31536000:
            raise ValueError('Interval seconds must be between 60 and 31536000')
        return {'kind': kind, 'seconds': seconds}, now + seconds
    if kind == 'cron':
        expr, tz = schedule.get('expression'), schedule.get('timezone', 'UTC')
        if not isinstance(expr, str) or len(expr.split()) != 5 or not croniter.is_valid(expr):
            raise ValueError('Use a valid five-field cron expression')
        try:
            ZoneInfo(tz)
        except (ZoneInfoNotFoundError, TypeError, ValueError):
            raise ValueError('Use a valid IANA timezone') from None
        result = {'kind': kind, 'expression': expr, 'timezone': tz}
        return result, next_cron(result, now)
    raise ValueError('Schedule kind must be once, interval, or cron')


def next_cron(schedule: dict, after: float) -> float:
    try:
        base = datetime.fromtimestamp(after, ZoneInfo(schedule['timezone']))
        due = croniter(schedule['expression'], base, max_years_between_matches=8).get_next(datetime).timestamp()
    except ZoneInfoNotFoundError:
        raise ValueError('Use a valid IANA timezone; edit the saved schedule before resuming') from None
    except (ValueError, OverflowError):
        raise ValueError('Cron expression has no execution in the next eight years') from None
    if due <= after:
        raise ValueError('Cron expression did not advance time')
    return due


def _text(value, name, maximum):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f'{name} must contain 1–{maximum} characters')
    return value


class ScheduleStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.parent.chmod(0o700)
        self.db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        self.path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL DEFAULT 1,
                heartbeat REAL, last_error TEXT
            );
            INSERT OR IGNORE INTO settings(id) VALUES(1);
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, prompt TEXT NOT NULL,
                schedule TEXT NOT NULL, catch_up INTEGER NOT NULL, enabled INTEGER NOT NULL,
                owner TEXT NOT NULL, next_run REAL, thread_id TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL, deleted INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
                status TEXT NOT NULL, source TEXT NOT NULL, prompt TEXT NOT NULL,
                first_due REAL NOT NULL, last_due REAL NOT NULL, count INTEGER NOT NULL,
                created_at REAL NOT NULL, started_at REAL, finished_at REAL,
                thread_id TEXT, host_pid INTEGER, host_start TEXT,
                result TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', catchup_run_id TEXT
            );
            CREATE INDEX IF NOT EXISTS runs_job_status ON runs(job_id,status);
            CREATE UNIQUE INDEX IF NOT EXISTS one_live_run ON runs(job_id)
                WHERE status IN ('pending','starting','running','uncertain');
            PRAGMA user_version=1;
        ''')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.db.close()

    @contextmanager
    def _transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    @staticmethod
    def _job(row):
        value = dict(row)
        value['schedule'] = json.loads(value['schedule'])
        for field in ('enabled', 'catch_up', 'deleted'):
            value[field] = bool(value[field])
        return value

    def get(self, job_id, owner=None):
        row = self.db.execute('SELECT * FROM jobs WHERE id=? AND deleted=0', (job_id,)).fetchone()
        if row is None or (owner is not None and row['owner'] != owner):
            raise ValueError('Schedule not found')
        return self._job(row)

    def create(self, name, prompt, schedule, catch_up=False, enabled=True, owner='', now=None):
        now = timestamp(now)
        name, prompt = _text(name, 'Name', 200), _text(prompt, 'Prompt', 64000)
        if not isinstance(catch_up, bool) or not isinstance(enabled, bool) or not isinstance(owner, str):
            raise ValueError('Invalid schedule settings')
        schedule, due = validate_schedule(schedule, now)
        job_id = uuid.uuid4().hex
        with self._transaction():
            self.db.execute('INSERT INTO jobs(id,name,prompt,schedule,catch_up,enabled,owner,next_run,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                            (job_id, name, prompt, json.dumps(schedule), catch_up, enabled, owner, due, now, now))
        return self.get(job_id)

    def list_jobs(self, owner=None, include_deleted=False):
        rows = self.db.execute('SELECT * FROM jobs WHERE (? OR deleted=0) AND (? IS NULL OR owner=?) ORDER BY created_at,id',
                               (include_deleted, owner, owner)).fetchall()
        return [self._job(row) for row in rows]

    def _active(self, job_id):
        return self.db.execute("SELECT * FROM runs WHERE job_id=? AND status IN ('pending','starting','running','uncertain')", (job_id,)).fetchone()

    def update(self, job_id, owner=None, now=None, **changes):
        now = timestamp(now)
        if not changes or changes.keys() - {'name', 'prompt', 'schedule', 'catch_up', 'enabled'}:
            raise ValueError('Choose name, prompt, schedule, catch_up, or enabled to update')
        with self._transaction():
            job = self.get(job_id, owner)
            active = self._active(job_id)
            if active and active['status'] != 'pending' and changes.keys() & {'prompt', 'schedule'}:
                raise ValueError('Resolve the active execution before changing its prompt or schedule')
            for key in ('name', 'prompt'):
                if key in changes:
                    _text(changes[key], key, 200 if key == 'name' else 64000)
            for key in ('enabled', 'catch_up'):
                if key in changes and not isinstance(changes[key], bool):
                    raise ValueError(f'{key} must be a boolean')
            if job['enabled'] and (changes.get('enabled') is False or changes.keys() & {'prompt', 'schedule'}):
                self._materialize_job(job, now)
                while job['next_run'] is not None and job['next_run'] <= now:
                    self._materialize_job(job, now)
                active = self._active(job_id)
            if 'schedule' in changes:
                validated, due = validate_schedule(changes['schedule'], now)
                changes.update(schedule=json.dumps(validated), next_run=due)
            elif changes.get('enabled') and not job['enabled']:
                s = job['schedule']
                if s['kind'] == 'interval':
                    changes['next_run'] = now + s['seconds']
                elif s['kind'] == 'cron':
                    changes['next_run'] = next_cron(s, now)
                elif job['next_run'] is not None and job['next_run'] <= now:
                    self._insert_run(job, 'missed', 'paused', job['next_run'], job['next_run'], 1, now)
                    changes['next_run'] = None
            cancel_catchup = changes.get('catch_up') is False and active and active['source'] == 'auto_catch_up'
            if active and active['status'] == 'pending' and (changes.get('enabled') is False or changes.keys() & {'prompt', 'schedule'} or cancel_catchup):
                self.db.execute("UPDATE runs SET status='cancelled',finished_at=?,error='Schedule changed before execution' WHERE id=?", (now, active['id']))
                self.db.execute("UPDATE runs SET catchup_run_id=NULL WHERE status='missed' AND catchup_run_id=?", (active['id'],))
            changes['updated_at'] = now
            self.db.execute('UPDATE jobs SET '+','.join(key+'=?' for key in changes)+' WHERE id=?', (*changes.values(), job_id))
        return self.get(job_id, owner)

    def delete(self, job_id, owner=None):
        with self._transaction():
            self.get(job_id, owner)
            active = self._active(job_id)
            if active and active['status'] != 'pending':
                raise ValueError('Resolve the active execution before deleting the schedule')
            self.db.execute('UPDATE jobs SET deleted=1,enabled=0 WHERE id=?', (job_id,))
            self.db.execute("UPDATE runs SET status='cancelled',finished_at=? WHERE job_id=? AND status='pending'", (time.time(), job_id))
        return {'id': job_id, 'deleted': True}

    def runs(self, job_id=None, owner=None, limit=50):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError('Limit must be between 1 and 500')
        if job_id is not None:
            row = self.db.execute('SELECT owner FROM jobs WHERE id=?', (job_id,)).fetchone()
            if row is None or (owner is not None and row['owner'] != owner):
                raise ValueError('Schedule not found')
        return [dict(row) for row in self.db.execute('''SELECT r.* FROM runs r JOIN jobs j ON j.id=r.job_id
            WHERE (? IS NULL OR r.job_id=?) AND (? IS NULL OR j.owner=?)
            ORDER BY r.created_at DESC,r.rowid DESC LIMIT ?''', (job_id, job_id, owner, owner, limit))]

    def _insert_run(self, job, status, source, first, last, count, now):
        run_id = uuid.uuid4().hex
        self.db.execute('''INSERT INTO runs(id,job_id,status,source,prompt,first_due,last_due,count,created_at,thread_id)
            VALUES(?,?,?,?,?,?,?,?,?,?)''', (run_id, job['id'], status, source, job['prompt'], first, last, count, now, job['thread_id']))
        return run_id

    def _run(self, run_id):
        row = self.db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()
        if row is None:
            raise ValueError('Execution not found')
        return dict(row)

    def _coalesce(self, job, now, automatic=True):
        active = self._active(job['id'])
        if active and not (active['status'] == 'pending' and active['source'] in {'catch_up', 'auto_catch_up', 'schedule'}):
            return None
        missed = self.db.execute("SELECT MIN(first_due) first,MAX(last_due) last,SUM(count) count FROM runs WHERE job_id=? AND status='missed' AND source!='paused' AND catchup_run_id IS NULL", (job['id'],)).fetchone()
        if not missed['count']:
            return None
        source = 'auto_catch_up' if automatic else 'catch_up'
        if active:
            run_id = active['id']
            if active['source'] == 'catch_up':
                source = 'catch_up'
            self.db.execute('UPDATE runs SET source=?,first_due=MIN(first_due,?),last_due=MAX(last_due,?),count=count+? WHERE id=?',
                            (source, missed['first'], missed['last'], missed['count'], run_id))
        else:
            run_id = self._insert_run(job, 'pending', source, missed['first'], missed['last'], missed['count'], now)
        self.db.execute("UPDATE runs SET catchup_run_id=? WHERE job_id=? AND status='missed' AND source!='paused' AND catchup_run_id IS NULL", (run_id, job['id']))
        return run_id

    def _materialize_job(self, job, now):
        active = self._active(job['id'])
        if active and active['status'] == 'pending' and active['source'] == 'schedule' and active['last_due'] < now - GRACE_SECONDS:
            heartbeat = self.db.execute('SELECT heartbeat FROM settings WHERE id=1').fetchone()[0]
            if heartbeat is None or heartbeat < now - GRACE_SECONDS:
                self.db.execute("UPDATE runs SET status='missed',error='Execution missed its start window while scheduler was unavailable' WHERE id=?", (active['id'],))
                active = None
        due, schedule = job['next_run'], job['schedule']
        if due is not None and due <= now:
            first = last = due
            count = 1
            if schedule['kind'] == 'interval':
                count = int((now - due) // schedule['seconds']) + 1
                last = due + (count - 1) * schedule['seconds']
                next_due = due + count * schedule['seconds']
            elif schedule['kind'] == 'once':
                next_due = None
            else:
                next_due = next_cron(schedule, last)
                while next_due <= now and count < CRON_BATCH_SIZE:
                    last, count = next_due, count + 1
                    next_due = next_cron(schedule, last)
            caught_up = next_due is None or next_due > now
            on_time = caught_up and last >= now - GRACE_SECONDS and not active
            missed_count = count - int(on_time)
            if missed_count:
                if on_time and schedule['kind'] == 'interval':
                    missed_last = last - schedule['seconds']
                elif on_time and schedule['kind'] == 'cron':
                    missed_last = croniter(schedule['expression'], datetime.fromtimestamp(last, ZoneInfo(schedule['timezone']))).get_prev(datetime).timestamp()
                else:
                    missed_last = last
                self._insert_run(job, 'missed', 'schedule', first, missed_last, missed_count, now)
            self.db.execute('UPDATE jobs SET next_run=? WHERE id=?', (next_due, job['id']))
            job['next_run'] = next_due
            if on_time:
                catchup_id = self._coalesce(job, now) if job['catch_up'] else None
                if catchup_id:
                    self.db.execute('UPDATE runs SET last_due=?,count=count+1 WHERE id=?', (last, catchup_id))
                else:
                    self._insert_run(job, 'pending', 'schedule', last, last, 1, now)
        if job['catch_up'] and (job['next_run'] is None or job['next_run'] > now):
            self._coalesce(job, now)

    def materialize(self, now=None):
        now = timestamp(now)
        with self._transaction():
            if not self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0]:
                return
            for job in self.list_jobs():
                if job['enabled']:
                    self.db.execute('SAVEPOINT materialize_job')
                    try:
                        self._materialize_job(job, now)
                    except (ValueError, ZoneInfoNotFoundError, OverflowError):
                        self.db.execute('ROLLBACK TO materialize_job')
                        job = self.get(job['id'])
                        due = job['next_run'] if job['next_run'] is not None else now
                        run_id = self._insert_run(job, 'failed', 'schedule_error', due, due, 1, now)
                        self.db.execute("UPDATE runs SET error='Schedule calculation failed; edit the schedule and resume the job',finished_at=? WHERE id=?", (now, run_id))
                        self.db.execute('UPDATE jobs SET enabled=0 WHERE id=?', (job['id'],))
                    finally:
                        self.db.execute('RELEASE materialize_job')

    def enqueue(self, job_id, owner=None, now=None):
        now = timestamp(now)
        with self._transaction():
            job = self.get(job_id, owner)
            if not job['enabled'] or self._active(job_id):
                raise ValueError('Enable the job and resolve its active execution first')
            run_id = self._insert_run(job, 'pending', 'manual', now, now, 1, now)
        return self._run(run_id)

    def catch_up(self, job_id, owner=None, now=None):
        now = timestamp(now)
        with self._transaction():
            job = self.get(job_id, owner)
            if not job['enabled']:
                raise ValueError('Resume the job before catching up')
            self._materialize_job(job, now)
            while job['next_run'] is not None and job['next_run'] <= now:
                self._materialize_job(job, now)
            active = self._active(job_id)
            if active and active['source'] in {'catch_up', 'auto_catch_up', 'schedule'} and active['status'] == 'pending':
                run_id = self._coalesce(job, now, automatic=False)
                if run_id is None and active['source'] in {'catch_up', 'auto_catch_up'}:
                    run_id = active['id']
            elif active:
                raise ValueError('Resolve the active execution before catching up')
            else:
                run_id = self._coalesce(job, now, automatic=False)
            if run_id is None:
                raise ValueError('No missed executions to catch up')
            self.db.execute("UPDATE runs SET source='catch_up' WHERE id=?", (run_id,))
        return self._run(run_id)

    def claim(self, now=None):
        now = timestamp(now)
        self.materialize(now)
        with self._transaction():
            if not self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0]:
                return None
            row = self.db.execute("""SELECT r.id FROM runs r JOIN jobs j ON j.id=r.job_id
                WHERE r.status='pending' AND j.enabled=1 AND j.deleted=0
                AND (r.source!='auto_catch_up' OR j.next_run IS NULL OR j.next_run>?)
                AND NOT EXISTS(SELECT 1 FROM runs h WHERE h.job_id=j.id AND h.host_pid IS NOT NULL)
                ORDER BY r.created_at,r.rowid LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            self.db.execute("UPDATE runs SET status='starting',started_at=? WHERE id=?", (now, row['id']))
        return self._run(row['id'])

    def mark_running(self, run_id, thread_id, host_pid=None, host_start=None):
        with self._transaction():
            run = self._run(run_id)
            if run['status'] != 'starting':
                raise ValueError('Execution is not starting')
            self.db.execute("UPDATE runs SET status='running',thread_id=?,host_pid=?,host_start=? WHERE id=?", (thread_id, host_pid, host_start, run_id))
            self.db.execute('UPDATE jobs SET thread_id=? WHERE id=?', (thread_id, run['job_id']))
        return self._run(run_id)

    def finish(self, run_id, status, result='', error='', now=None):
        if status not in {'succeeded', 'failed', 'uncertain'}:
            raise ValueError('Invalid execution outcome')
        with self._transaction():
            run = self._run(run_id)
            if run['status'] not in {'starting', 'running'}:
                raise ValueError('Execution is not active')
            self.db.execute('UPDATE runs SET status=?,result=?,error=?,finished_at=? WHERE id=?',
                            (status, result[:256000], error[:2000], timestamp(now), run_id))
        return self._run(run_id)

    def recover_interrupted(self, now=None):
        with self._transaction():
            self.db.execute("UPDATE runs SET status='uncertain',finished_at=?,error='Gateway stopped before execution outcome was recorded; inspect before retrying' WHERE status IN ('starting','running')", (timestamp(now),))

    def recover(self, run_id, action='retry', acknowledge_duplicate_risk=False, owner=None, now=None):
        now = timestamp(now)
        with self._transaction():
            run = self._run(run_id)
            job = self.get(run['job_id'], owner)
            if run['status'] not in {'uncertain', 'failed'} or action not in {'retry', 'dismiss'}:
                raise ValueError('Only failed or uncertain executions can be retried or dismissed')
            if run['host_pid'] is not None:
                raise ValueError('Wait for the gateway to stop the previous execution process before recovery')
            if action == 'retry' and not acknowledge_duplicate_risk:
                raise ValueError('Retry requires acknowledging possible duplicate side effects')
            if action == 'retry' and not job['enabled']:
                raise ValueError('Resume the job before retrying')
            self.db.execute('UPDATE runs SET status=? WHERE id=?', ('retried' if action == 'retry' else 'dismissed', run_id))
            if action == 'retry':
                if self._active(job['id']):
                    raise ValueError('Resolve the active execution first')
                new_id = self._insert_run(job, 'pending', 'retry', run['first_due'], run['last_due'], run['count'], now)
                self.db.execute('UPDATE runs SET prompt=? WHERE id=?', (run['prompt'], new_id))
                self.db.execute('UPDATE runs SET catchup_run_id=? WHERE id=?', (new_id, run_id))
            else:
                new_id = run_id
        return self._run(new_id)

    def reset_session(self, job_id, owner=None, now=None):
        """Explicitly start the next execution in a fresh session; retain history."""
        if owner is not None:
            raise ValueError('Session reset is available only to the local administrator')
        now = timestamp(now)
        with self._transaction():
            self.get(job_id)
            if self._active(job_id):
                raise ValueError('Resolve the active execution before resetting its session')
            if self.db.execute('SELECT 1 FROM runs WHERE job_id=? AND host_pid IS NOT NULL LIMIT 1',
                               (job_id,)).fetchone():
                raise ValueError('Wait for the gateway to stop previous execution processes before resetting the session')
            self.db.execute('UPDATE jobs SET thread_id=NULL,updated_at=? WHERE id=?', (now, job_id))
        return self.get(job_id)

    def clear_host(self, run_id):
        self.db.execute('UPDATE runs SET host_pid=NULL,host_start=NULL WHERE id=?', (run_id,))

    def orphaned_hosts(self):
        return [dict(row) for row in self.db.execute('SELECT * FROM runs WHERE host_pid IS NOT NULL')]

    def heartbeat(self, error=None, now=None):
        self.db.execute('UPDATE settings SET heartbeat=?,last_error=? WHERE id=1', (timestamp(now), error))

    def set_enabled(self, enabled):
        if not isinstance(enabled, bool):
            raise ValueError('enabled must be a boolean')
        with self._transaction():
            self.db.execute('UPDATE settings SET enabled=? WHERE id=1', (enabled,))
            if not enabled:
                self.db.execute("UPDATE runs SET status='missed',error='Scheduling disabled before execution' WHERE status='pending' AND source='schedule'")
        return self.status()

    def status(self):
        settings = dict(self.db.execute('SELECT enabled,heartbeat,last_error FROM settings WHERE id=1').fetchone())
        settings['enabled'] = bool(settings['enabled'])
        settings['jobs'] = self.db.execute('SELECT COUNT(*) FROM jobs WHERE deleted=0').fetchone()[0]
        settings['executions'] = {row[0]: row[1] for row in self.db.execute('SELECT status,SUM(count) FROM runs GROUP BY status')}
        settings['missed_pending'] = self.db.execute("SELECT COALESCE(SUM(r.count),0) FROM runs r JOIN jobs j ON j.id=r.job_id WHERE j.deleted=0 AND r.status='missed' AND r.source!='paused' AND r.catchup_run_id IS NULL").fetchone()[0]
        return settings

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
        expr, tz = schedule.get('expression'), schedule.get('timezone', os.getenv('TZ') or 'UTC')
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


def validate_bounds(changes, now):
    """Resolve durations once, never when a job resumes or the gateway restarts."""
    result = {key: changes[key] for key in ('end_at', 'max_runs') if key in changes}
    if 'duration_seconds' in changes:
        if 'end_at' in changes:
            raise ValueError('Choose end_at or duration_seconds, not both')
        duration = changes['duration_seconds']
        if isinstance(duration, bool) or not isinstance(duration, int) or not 1 <= duration <= 31536000:
            raise ValueError('duration_seconds must be an integer between 1 and 31536000')
        result['end_at'] = now + duration
    elif result.get('end_at') is not None:
        try:
            result['end_at'] = timestamp(datetime.fromisoformat(result['end_at'].replace('Z', '+00:00')))
        except (TypeError, AttributeError, ValueError, OverflowError):
            raise ValueError('end_at requires an ISO date and time with an explicit UTC offset') from None
        if result['end_at'] <= now:
            raise ValueError('end_at must be in the future')
    maximum = result.get('max_runs')
    if maximum is not None and (isinstance(maximum, bool) or not isinstance(maximum, int) or not 1 <= maximum <= 2147483647):
        raise ValueError('max_runs must be an integer between 1 and 2147483647')
    return result


class ScheduleStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.parent.chmod(0o700)
        self.db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        try:
            self._initialize()
        except BaseException:
            self.db.close()
            raise

    def _initialize(self):
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
                result TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', catchup_run_id TEXT,
                current_due REAL
            );
            CREATE INDEX IF NOT EXISTS runs_job_status ON runs(job_id,status);
            CREATE UNIQUE INDEX IF NOT EXISTS one_live_run ON runs(job_id)
                WHERE status IN ('pending','starting','running','uncertain');
        ''')
        with self._transaction():
            columns = {row['name'] for row in self.db.execute('PRAGMA table_info(runs)')}
            if 'current_due' not in columns:
                self.db.execute('ALTER TABLE runs ADD COLUMN current_due REAL')
            if 'decision' not in columns:
                self.db.execute('ALTER TABLE runs ADD COLUMN decision TEXT')
            columns = {row['name'] for row in self.db.execute('PRAGMA table_info(jobs)')}
            for name, definition in (('end_at', 'REAL'), ('max_runs', 'INTEGER'),
                                     ('run_count', 'INTEGER NOT NULL DEFAULT 0'), ('stop_reason', 'TEXT'),
                                     ('decision_required', 'INTEGER NOT NULL DEFAULT 0'),
                                     ('registration_revision', 'INTEGER NOT NULL DEFAULT 1'),
                                     ('registered_revision', 'INTEGER NOT NULL DEFAULT 0'),
                                     ('registration_error', 'TEXT')):
                if name not in columns:
                    self.db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {definition}')
                    if name == 'run_count':
                        self.db.execute('UPDATE jobs SET run_count=(SELECT COUNT(*) FROM runs WHERE job_id=jobs.id AND started_at IS NOT NULL)')
            settings_columns = {row['name'] for row in self.db.execute('PRAGMA table_info(settings)')}
            if 'timer_owner' not in settings_columns:
                self.db.execute("ALTER TABLE settings ADD COLUMN timer_owner TEXT NOT NULL DEFAULT 'local'")
            self.db.execute('''CREATE TABLE IF NOT EXISTS external_occurrences (
                occurrence_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
                revision INTEGER NOT NULL, due REAL NOT NULL, run_id TEXT REFERENCES runs(id),
                UNIQUE(job_id,revision,due)
            )''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS external_cancellations (
                occurrence_id TEXT PRIMARY KEY, job_id TEXT NOT NULL,
                revision INTEGER NOT NULL, due REAL NOT NULL
            )''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS external_updates (
                command_id TEXT PRIMARY KEY, request TEXT NOT NULL, receipt TEXT NOT NULL
            )''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS schedule_registration_changed
                AFTER UPDATE OF name,prompt,schedule,catch_up,enabled,next_run,deleted,end_at,max_runs,
                    run_count,stop_reason,decision_required,thread_id ON jobs
                BEGIN UPDATE jobs SET registration_revision=registration_revision+1 WHERE id=NEW.id; END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS schedule_run_inserted AFTER INSERT ON runs
                BEGIN UPDATE jobs SET registration_revision=registration_revision+1 WHERE id=NEW.job_id; END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS schedule_run_changed AFTER UPDATE OF status ON runs
                WHEN NEW.status<>OLD.status
                BEGIN UPDATE jobs SET registration_revision=registration_revision+1 WHERE id=NEW.job_id; END''')
            if self.db.execute('PRAGMA user_version').fetchone()[0] < 4:
                self.db.execute('PRAGMA user_version=4')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.db.close()

    @contextmanager
    def _transaction(self):
        savepoint = 'nested_' + uuid.uuid4().hex if self.db.in_transaction else None
        self.db.execute('SAVEPOINT ' + savepoint if savepoint else 'BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('RELEASE ' + savepoint if savepoint else 'COMMIT')
        except BaseException:
            if savepoint:
                self.db.execute('ROLLBACK TO ' + savepoint)
                self.db.execute('RELEASE ' + savepoint)
            else:
                self.db.execute('ROLLBACK')
            raise

    def _job(self, row):
        value = dict(row)
        value['schedule'] = json.loads(value['schedule'])
        for field in ('enabled', 'catch_up', 'deleted', 'decision_required'):
            value[field] = bool(value[field])
        if self.external_timer():
            value['registration_status'] = (
                'registered' if value['registered_revision'] == value['registration_revision']
                else 'pending'
            )
            if value['registration_status'] == 'pending':
                value['notice'] = 'Saved locally; external registration is pending. Offline execution is not yet confirmed.'
        value['remaining_runs'] = None if value['max_runs'] is None else max(0, value['max_runs'] - value['run_count'])
        return value

    def get(self, job_id, owner=None):
        row = self.db.execute('SELECT * FROM jobs WHERE id=? AND deleted=0', (job_id,)).fetchone()
        if row is None or (owner is not None and row['owner'] != owner):
            raise ValueError('Schedule not found')
        return self._job(row)

    def create(self, name, prompt, schedule, catch_up=False, enabled=True, owner='', now=None,
               decision_required=False, **bounds):
        now = timestamp(now)
        if bounds.keys() - {'end_at', 'duration_seconds', 'max_runs'}:
            raise ValueError('Unexpected schedule settings')
        bounds = validate_bounds(bounds, now)
        name, prompt = _text(name, 'Name', 200), _text(prompt, 'Prompt', 64000)
        if (not isinstance(catch_up, bool) or not isinstance(enabled, bool)
                or not isinstance(decision_required, bool) or not isinstance(owner, str)):
            raise ValueError('Invalid schedule settings')
        schedule, due = validate_schedule(schedule, now)
        if bounds.get('end_at') is not None and due > bounds['end_at']:
            raise ValueError('The schedule has no occurrence on or before end_at')
        job_id = uuid.uuid4().hex
        with self._transaction():
            self.db.execute('INSERT INTO jobs(id,name,prompt,schedule,catch_up,enabled,owner,next_run,created_at,updated_at,decision_required) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                            (job_id, name, prompt, json.dumps(schedule), catch_up, enabled, owner, due, now, now, decision_required))
            if bounds:
                self.db.execute('UPDATE jobs SET '+','.join(key+'=?' for key in bounds)+' WHERE id=?', (*bounds.values(), job_id))
        return self.get(job_id)

    def list_jobs(self, owner=None, include_deleted=False):
        rows = self.db.execute('SELECT * FROM jobs WHERE (? OR deleted=0) AND (? IS NULL OR owner=?) ORDER BY created_at,id',
                               (include_deleted, owner, owner)).fetchall()
        return [self._job(row) for row in rows]

    def _active(self, job_id):
        return self.db.execute("SELECT * FROM runs WHERE job_id=? AND status IN ('pending','starting','running','uncertain')", (job_id,)).fetchone()

    def update(self, job_id, owner=None, now=None, **changes):
        now = timestamp(now)
        if not changes or changes.keys() - {'name', 'prompt', 'schedule', 'catch_up', 'enabled', 'end_at', 'duration_seconds', 'max_runs', 'decision_required'}:
            raise ValueError('Choose name, prompt, schedule, catch_up, enabled, or schedule limits to update')
        bounds = validate_bounds(changes, now)
        changes.pop('duration_seconds', None)
        changes.update(bounds)
        for key in ('name', 'prompt'):
            if key in changes:
                _text(changes[key], key, 200 if key == 'name' else 64000)
        for key in ('enabled', 'catch_up', 'decision_required'):
            if key in changes and not isinstance(changes[key], bool):
                raise ValueError(f'{key} must be a boolean')
        if 'schedule' in changes:
            validated, due = validate_schedule(changes['schedule'], now)
            changes.update(schedule=json.dumps(validated), next_run=due)
        changes['updated_at'] = now
        edits_task = bool(changes.keys() & {'prompt', 'schedule', 'decision_required'})
        edits_limits = bool(bounds)
        pauses = changes.get('enabled') is False
        while True:
            incomplete = False
            with self._transaction():
                job = self.get(job_id, owner)
                active = self._active(job_id)
                if (active and active['status'] != 'pending' and (edits_task or edits_limits)
                        and (active['status'] != 'uncertain' or edits_task)):
                    raise ValueError('Resolve the active execution before changing its prompt, schedule, or limits')
                if job['enabled'] and (pauses or edits_task or edits_limits):
                    calculated = self._materialize_safely(job, now)
                    if not calculated and 'schedule' in changes:
                        # Replacing the invalid schedule repairs an otherwise enabled job.
                        changes.setdefault('enabled', True)
                    incomplete = calculated and job['next_run'] is not None and job['next_run'] <= now
                    active = self._active(job_id)
                if not incomplete:
                    restarts = changes.get('enabled') and not job['enabled']
                    extends_window = ('end_at' in bounds and job['next_run'] is None and job['enabled']
                                      and (bounds['end_at'] is None or job['end_at'] is None or bounds['end_at'] > job['end_at']))
                    if 'schedule' not in changes and (restarts or extends_window):
                        s = job['schedule']
                        if s['kind'] == 'interval':
                            changes['next_run'] = now + s['seconds']
                        elif s['kind'] == 'cron':
                            changes['next_run'] = next_cron(s, now)
                        elif job['next_run'] is not None and job['next_run'] <= now:
                            self._insert_run(job, 'missed', 'paused', job['next_run'], job['next_run'], 1, now)
                            changes['next_run'] = None
                    updated = {**job, **changes}
                    if changes.get('enabled'):
                        reason = self._limit_reason(updated, now)
                        if reason:
                            raise ValueError('Extend or clear the '+reason+' limit before resuming')
                        changes['stop_reason'] = None
                    if ('schedule' in changes or bounds or changes.get('enabled')) and updated['enabled']:
                        if updated['end_at'] is not None and updated['next_run'] is not None and updated['next_run'] > updated['end_at']:
                            raise ValueError('The schedule has no occurrence on or before end_at')
                    cancel_catchup = changes.get('catch_up') is False and active and active['source'] == 'auto_catch_up'
                    if active and active['status'] == 'pending' and (pauses or edits_task or cancel_catchup):
                        self.db.execute("UPDATE runs SET status='cancelled',finished_at=?,error='Schedule changed before execution' WHERE id=?", (now, active['id']))
                        if cancel_catchup and not (pauses or edits_task):
                            self._restore_current_occurrence(job, active, now)
                        self.db.execute("UPDATE runs SET catchup_run_id=NULL WHERE status='missed' AND catchup_run_id=?", (active['id'],))
                    self.db.execute('UPDATE jobs SET '+','.join(key+'=?' for key in changes)+' WHERE id=?', (*changes.values(), job_id))
                    updated = self.get(job_id)
                    reason = self._limit_reason(updated, now)
                    if reason:
                        self._stop_job(updated, reason, now)
            if not incomplete:
                return self.get(job_id, owner)
            # Commit each bounded batch before allowing another writer to proceed.
            time.sleep(0.001)

    def _restore_current_occurrence(self, job, active, now):
        represented = self.db.execute("SELECT COALESCE(SUM(count),0) FROM runs WHERE status='missed' AND catchup_run_id=?", (active['id'],)).fetchone()[0]
        remaining = active['count'] - represented
        if remaining <= 0:
            return
        snapshot = {**job, 'prompt': active['prompt']}
        current = active['current_due']
        if current is not None:
            heartbeat = self.db.execute('SELECT heartbeat FROM settings WHERE id=1').fetchone()[0]
            timely = current >= now - GRACE_SECONDS or (heartbeat is not None and heartbeat >= now - GRACE_SECONDS)
            self._insert_run(snapshot, 'pending' if timely else 'missed', 'schedule', current, current, remaining, now)
        else:
            # Older receipts did not store the unpaired occurrence's exact due time.
            run_id = self._insert_run(snapshot, 'missed', 'schedule', active['first_due'], active['last_due'], remaining, now)
            self.db.execute("UPDATE runs SET error='Exact due time unavailable in an older receipt; remaining occurrence count preserved for manual catch-up' WHERE id=?", (run_id,))

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
        current_due = last if status == 'pending' and source == 'schedule' else None
        self.db.execute('''INSERT INTO runs(id,job_id,status,source,prompt,first_due,last_due,count,created_at,thread_id,current_due)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)''', (run_id, job['id'], status, source, job['prompt'], first, last, count, now, job['thread_id'], current_due))
        return run_id

    def _run(self, run_id):
        row = self.db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()
        if row is None:
            raise ValueError('Execution not found')
        return dict(row)

    @staticmethod
    def _limit_reason(job, now):
        if job['max_runs'] is not None and job['run_count'] >= job['max_runs']:
            return 'max_runs'
        # Keep the normal dispatch grace so a tick just after the final due time
        # can start it. No queued or catch-up execution survives this deadline.
        if job['end_at'] is not None and now > job['end_at'] + GRACE_SECONDS:
            return 'end_at'
        return None

    def _stop_job(self, job, reason, now):
        self.db.execute('UPDATE jobs SET enabled=0,next_run=NULL,stop_reason=? WHERE id=?', (reason, job['id']))
        self.db.execute("UPDATE runs SET catchup_run_id=NULL WHERE status='missed' AND catchup_run_id IN (SELECT id FROM runs WHERE job_id=? AND status='pending')", (job['id'],))
        self.db.execute("UPDATE runs SET status='cancelled',finished_at=?,error=? WHERE job_id=? AND status='pending'",
                        (now, 'Schedule stopped before execution ('+reason+')', job['id']))
        job.update(enabled=False, next_run=None, stop_reason=reason)

    def _require_budget(self, job, now):
        reason = self._limit_reason(job, now)
        if reason:
            raise ValueError('Schedule reached its '+reason+' limit; extend or clear it before requesting another execution')

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
        if self._limit_reason(job, now) == 'max_runs':
            self._stop_job(job, 'max_runs', now)
            return
        active = self._active(job['id'])
        if active and active['status'] == 'pending' and active['source'] == 'schedule' and active['last_due'] < now - GRACE_SECONDS:
            heartbeat = self.db.execute('SELECT heartbeat FROM settings WHERE id=1').fetchone()[0]
            if heartbeat is None or heartbeat < now - GRACE_SECONDS:
                self.db.execute("UPDATE runs SET status='missed',error='Execution missed its start window while scheduler was unavailable' WHERE id=?", (active['id'],))
                active = None
        due, schedule = job['next_run'], job['schedule']
        horizon = min(now, job['end_at']) if job['end_at'] is not None else now
        if due is not None and due > horizon and job['end_at'] is not None and due > job['end_at']:
            self.db.execute('UPDATE jobs SET next_run=NULL WHERE id=?', (job['id'],))
            job['next_run'] = due = None
        if due is not None and due <= horizon:
            first = last = due
            count = 1
            if schedule['kind'] == 'interval':
                count = int((horizon - due) // schedule['seconds']) + 1
                last = due + (count - 1) * schedule['seconds']
                next_due = due + count * schedule['seconds']
            elif schedule['kind'] == 'once':
                next_due = None
            else:
                next_due = next_cron(schedule, last)
                while next_due <= horizon and count < CRON_BATCH_SIZE:
                    last, count = next_due, count + 1
                    next_due = next_cron(schedule, last)
            if next_due is not None and job['end_at'] is not None and next_due > job['end_at']:
                next_due = None
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
                    self.db.execute('UPDATE runs SET last_due=?,current_due=?,count=count+1 WHERE id=?', (last, last, catchup_id))
                else:
                    self._insert_run(job, 'pending', 'schedule', last, last, 1, now)
        if self._limit_reason(job, now) and job['next_run'] is None:
            self._stop_job(job, 'end_at', now)
        elif job['catch_up'] and (job['next_run'] is None or job['next_run'] > now):
            self._coalesce(job, now)

    def _materialize_safely(self, job, now):
        self.db.execute('SAVEPOINT materialize_job')
        try:
            self._materialize_job(job, now)
            return True
        except (ValueError, ZoneInfoNotFoundError, OverflowError):
            self.db.execute('ROLLBACK TO materialize_job')
            restored = self.get(job['id'])
            due = restored['next_run'] if restored['next_run'] is not None else now
            run_id = self._insert_run(restored, 'failed', 'schedule_error', due, max(due, now), 1, now)
            self.db.execute("UPDATE runs SET error='Schedule calculation failed; additional due occurrences could not be counted; edit the schedule and resume the job',finished_at=? WHERE id=?", (now, run_id))
            self.db.execute('UPDATE jobs SET enabled=0 WHERE id=?', (job['id'],))
            job.update(restored, enabled=False)
            return False
        finally:
            self.db.execute('RELEASE materialize_job')

    def external_timer(self):
        return self.db.execute('SELECT timer_owner FROM settings WHERE id=1').fetchone()[0] == 'external'

    def use_external_timer(self):
        """Persist ownership so an accidental environment change cannot double-fire jobs."""
        with self._transaction():
            self.db.execute("UPDATE settings SET timer_owner='external' WHERE id=1")

    def registrations(self):
        """Return durable timer metadata without exposing prompts or execution output."""
        if not self.external_timer():
            raise ValueError('External scheduling is not configured')
        enabled = bool(self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0])
        rows = []
        for job in self.list_jobs(include_deleted=True):
            active = self._active(job['id'])
            rows.append({
                'job_id': job['id'], 'revision': job['registration_revision'],
                'enabled': enabled and job['enabled'] and not job['deleted'],
                'deleted': job['deleted'], 'schedule': job['schedule'], 'catch_up': job['catch_up'],
                'next_due': None if active else job['next_run'],
                'end_at': job['end_at'], 'max_runs': job['max_runs'], 'run_count': job['run_count'],
                'decision_required': job['decision_required'],
                'registered': job['registered_revision'] == job['registration_revision'],
                'error': job['registration_error'],
            })
        return rows

    def acknowledge_registration(self, job_id, revision, *, error=None):
        if not self.external_timer() or type(revision) is not int or revision < 1:
            raise ValueError('Invalid schedule registration')
        if error is not None and (not isinstance(error, str) or len(error) > 128):
            raise ValueError('Invalid registration error')
        with self._transaction():
            changed = self.db.execute('''UPDATE jobs SET registered_revision=?,registration_error=?
                WHERE id=? AND registration_revision=?''',
                (revision if error is None else 0, error, job_id, revision)).rowcount
            if changed != 1:
                raise ValueError('Schedule changed before registration was acknowledged')

    def update_external(self, job_id, revision, command_id, schedule, enabled, now=None):
        """Commit a revision-checked edit and its replay receipt together."""
        if (not self.external_timer() or type(revision) is not int or revision < 1
                or not isinstance(job_id, str) or not isinstance(schedule, dict)
                or type(enabled) is not bool or not isinstance(command_id, str)
                or not 1 <= len(command_id) <= 128):
            raise ValueError('Invalid schedule update')
        request = json.dumps([job_id, revision, schedule, enabled], sort_keys=True, allow_nan=False)
        with self._transaction():
            saved = self.db.execute('SELECT request,receipt FROM external_updates WHERE command_id=?', (command_id,)).fetchone()
            if saved:
                if saved['request'] != request:
                    raise ValueError('Update identifier already used')
                return json.loads(saved['receipt'])
            job = self.get(job_id)
            if job['registration_revision'] != revision:
                raise ValueError('Schedule changed before update')
            changes = {'enabled': enabled}
            if schedule != job['schedule']:
                changes['schedule'] = schedule
            updated = self.update(job_id, now=now, **changes)
            receipt = {'job_id': job_id, 'revision': updated['registration_revision'], 'enabled': updated['enabled']}
            self.db.execute('INSERT INTO external_updates VALUES(?,?,?)', (command_id, request, json.dumps(receipt)))
            return receipt

    def cancel_occurrence(self, job_id, revision, occurrence_id, due):
        due = timestamp(due)
        if (not self.external_timer() or type(revision) is not int or revision < 1
                or not isinstance(occurrence_id, str) or not 1 <= len(occurrence_id) <= 128):
            raise ValueError('Invalid scheduled occurrence')
        with self._transaction():
            prior = self.db.execute('SELECT * FROM external_occurrences WHERE occurrence_id=? OR (job_id=? AND revision=? AND due=?)', (occurrence_id, job_id, revision, due)).fetchone()
            if prior and (prior['job_id'], prior['revision'], prior['due']) != (job_id, revision, due):
                raise ValueError('Occurrence was already used')
            saved = self.db.execute('SELECT * FROM external_cancellations WHERE occurrence_id=?', (occurrence_id,)).fetchone()
            if saved and (saved['job_id'], saved['revision'], saved['due']) != (job_id, revision, due):
                raise ValueError('Cancellation was already used')
            self.db.execute('INSERT OR IGNORE INTO external_cancellations VALUES(?,?,?,?)', (occurrence_id, job_id, revision, due))
            if prior and prior['run_id']:
                self.db.execute("UPDATE runs SET status='cancelled',finished_at=? WHERE id=? AND status='pending'", (time.time(), prior['run_id']))
                return self._run(prior['run_id'])
            return {'status': 'cancelled'}

    def quarantine_occurrence(self, job_id, revision, occurrence_id, due):
        due = timestamp(due)
        with self._transaction():
            prior = self.db.execute('SELECT * FROM external_occurrences WHERE occurrence_id=?', (occurrence_id,)).fetchone()
            if prior is None or (prior['job_id'], prior['revision'], prior['due']) != (job_id, revision, due) or not prior['run_id']:
                raise ValueError('Occurrence does not match')
            run = self._run(prior['run_id'])
            if run['status'] == 'dismissed':
                return run
            return self.recover(run['id'], action='dismiss')

    def cancellation_requested(self, run_id):
        return bool(self.db.execute('SELECT 1 FROM external_cancellations c JOIN external_occurrences o ON o.job_id=c.job_id AND o.revision=c.revision AND o.due=c.due WHERE o.run_id=?', (run_id,)).fetchone())

    def accept_occurrence(self, job_id, revision, occurrence_id, due, now=None, *, admitted_at=None):
        """Deduplicate dispatch before using the native catch-up and execution rules."""
        now, due = timestamp(now), timestamp(due)
        if (not self.external_timer() or type(revision) is not int or revision < 1
                or not isinstance(occurrence_id, str) or not 1 <= len(occurrence_id) <= 128):
            raise ValueError('Invalid scheduled occurrence')
        with self._transaction():
            cancelled = self.db.execute('SELECT * FROM external_cancellations WHERE occurrence_id=? OR (job_id=? AND revision=? AND due=?)', (occurrence_id, job_id, revision, due)).fetchone()
            if cancelled:
                if (cancelled['job_id'], cancelled['revision'], cancelled['due']) != (job_id, revision, due):
                    raise ValueError('Cancellation was already used')
                return self.cancel_occurrence(job_id, revision, cancelled['occurrence_id'], due)
            prior = self.db.execute('SELECT * FROM external_occurrences WHERE occurrence_id=? OR (job_id=? AND revision=? AND due=?)',
                                    (occurrence_id, job_id, revision, due)).fetchone()
            if prior is not None:
                if (prior['job_id'], prior['revision'], prior['due']) != (job_id, revision, due):
                    raise ValueError('Occurrence was already used for a different schedule')
                return self._run(prior['run_id']) if prior['run_id'] else {'status': 'missed'}
            job = self.get(job_id)
            if (not self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0]
                    or not job['enabled'] or job['registration_revision'] != revision
                    or job['registered_revision'] != revision or job['next_run'] != due or due > now
                    or self._active(job_id)):
                raise ValueError('Schedule changed or is not due')
            admitted = timestamp(admitted_at) if admitted_at is not None else now
            if not due <= admitted <= now:
                raise ValueError('Invalid admission time')
            self._materialize_safely(job, now if job['catch_up'] else due)
            active = self._active(job_id)
            if active and not job['catch_up'] and now > due + 900:
                self.db.execute("UPDATE runs SET status='missed',error='Execution missed its managed start window' WHERE id=?", (active['id'],))
                active = None
            if self._limit_reason(job, now):
                self._stop_job(job, self._limit_reason(job, now), now)
                active = self._active(job_id)
            run_id = active['id'] if active else None
            self.db.execute('INSERT INTO external_occurrences(occurrence_id,job_id,revision,due,run_id) VALUES(?,?,?,?,?)',
                            (occurrence_id, job_id, revision, due, run_id))
            return self._run(run_id) if run_id else {'status': 'missed'}

    def materialize(self, now=None):
        now = timestamp(now)
        with self._transaction():
            if self.external_timer() or not self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0]:
                return
            for job in self.list_jobs():
                if job['enabled']:
                    self._materialize_safely(job, now)

    def enqueue(self, job_id, owner=None, now=None):
        now = timestamp(now)
        with self._transaction():
            job = self.get(job_id, owner)
            self._require_budget(job, now)
            if not job['enabled'] or self._active(job_id):
                raise ValueError('Enable the job and resolve its active execution first')
            run_id = self._insert_run(job, 'pending', 'manual', now, now, 1, now)
        return self._run(run_id)

    def catch_up(self, job_id, owner=None, now=None):
        now = timestamp(now)
        while True:
            with self._transaction():
                job = self.get(job_id, owner)
                self._require_budget(job, now)
                if not job['enabled']:
                    raise ValueError('Resume the job before catching up')
                active = self._active(job_id)
                if active and (active['status'] != 'pending' or active['source'] not in {'catch_up', 'auto_catch_up', 'schedule'}):
                    raise ValueError('Resolve the active execution before catching up')
                self._materialize_job(job, now)
                incomplete = job['next_run'] is not None and job['next_run'] <= now
                if not incomplete:
                    active = self._active(job_id)
                    run_id = self._coalesce(job, now, automatic=False)
                    if run_id is None and active and active['source'] in {'catch_up', 'auto_catch_up'}:
                        run_id = active['id']
                    if run_id is None:
                        raise ValueError('No missed executions to catch up')
                    self.db.execute("UPDATE runs SET source='catch_up' WHERE id=?", (run_id,))
            if not incomplete:
                return self._run(run_id)
            time.sleep(0.001)

    def claim(self, now=None):
        now = timestamp(now)
        self.materialize(now)
        with self._transaction():
            if not self.db.execute('SELECT enabled FROM settings WHERE id=1').fetchone()[0]:
                return None
            row = self.db.execute("""SELECT r.id,r.job_id FROM runs r JOIN jobs j ON j.id=r.job_id
                WHERE r.status='pending' AND j.enabled=1 AND j.deleted=0
                AND ((SELECT timer_owner FROM settings WHERE id=1)='local'
                     OR r.source IN ('manual','retry','catch_up')
                     OR EXISTS(SELECT 1 FROM external_occurrences o WHERE o.run_id=r.id))
                AND (j.end_at IS NULL OR j.end_at+? >= ?)
                AND (j.max_runs IS NULL OR j.run_count<j.max_runs)
                AND (r.source!='auto_catch_up' OR j.next_run IS NULL OR j.next_run>?)
                AND NOT EXISTS(SELECT 1 FROM runs h WHERE h.job_id=j.id AND h.host_pid IS NOT NULL)
                ORDER BY r.created_at,r.rowid LIMIT 1""", (GRACE_SECONDS, now, now)).fetchone()
            if row is None:
                return None
            self.db.execute("UPDATE runs SET status='starting',started_at=? WHERE id=?", (now, row['id']))
            self.db.execute('UPDATE jobs SET run_count=run_count+1 WHERE id=?', (row['job_id'],))
            job = self.get(row['job_id'])
            if self._limit_reason(job, now) == 'max_runs':
                self._stop_job(job, 'max_runs', now)
        return self._run(row['id'])

    def mark_running(self, run_id, thread_id, host_pid=None, host_start=None):
        with self._transaction():
            run = self._run(run_id)
            if run['status'] != 'starting':
                raise ValueError('Execution is not starting')
            self.db.execute("UPDATE runs SET status='running',thread_id=?,host_pid=?,host_start=? WHERE id=?",
                            (thread_id, host_pid if host_pid is not None else run['host_pid'],
                             host_start if host_pid is not None else run['host_start'], run_id))
            self.db.execute('UPDATE jobs SET thread_id=? WHERE id=?', (thread_id, run['job_id']))
        return self._run(run_id)

    def record_host(self, run_id, host_pid, host_start):
        """Persist ownership immediately after process creation, before its handshake."""
        if isinstance(host_pid, bool) or not isinstance(host_pid, int) or host_pid <= 0:
            raise ValueError('Invalid execution process ID')
        with self._transaction():
            run = self._run(run_id)
            if run['status'] not in {'starting', 'running'}:
                raise ValueError('Execution is not active')
            self.db.execute('UPDATE runs SET host_pid=?,host_start=? WHERE id=?', (host_pid, host_start, run_id))
        return self._run(run_id)

    def finish(self, run_id, status, result='', error='', now=None):
        if status not in {'succeeded', 'failed', 'uncertain'}:
            raise ValueError('Invalid execution outcome')
        now = timestamp(now)
        with self._transaction():
            run = self._run(run_id)
            if run['status'] not in {'starting', 'running'}:
                raise ValueError('Execution is not active')
            self.db.execute('UPDATE runs SET status=?,result=?,error=?,finished_at=? WHERE id=?',
                            (status, result[:256000], error[:2000], now, run_id))
            self._apply_decision(self.get(run['job_id']), run, status, now)
        return self._run(run_id)

    def recover_interrupted(self, now=None):
        now = timestamp(now)
        with self._transaction():
            interrupted = self.db.execute("SELECT * FROM runs WHERE status IN ('starting','running')").fetchall()
            self.db.execute("UPDATE runs SET status='uncertain',finished_at=?,error='Gateway stopped before execution outcome was recorded; inspect before retrying' WHERE status IN ('starting','running')", (now,))
            for run in interrupted:
                self._apply_decision(self.get(run['job_id']), run, 'uncertain', now)

    def decide(self, job_id, run_id, decision, now=None):
        """Persist an explicit recurrence decision for this job's active run."""
        if decision not in ('continue', 'stop'):
            raise ValueError('decision must be continue or stop')
        now = timestamp(now)
        with self._transaction():
            job = self.get(job_id)
            run = self._run(run_id)
            if run['job_id'] != job_id or run['status'] != 'running' or not job['decision_required']:
                raise ValueError('Decisions require the current running execution of a decision-required job')
            if run['decision'] is not None and run['decision'] != decision:
                raise ValueError('This execution already recorded a different decision')
            self.db.execute('UPDATE runs SET decision=? WHERE id=?', (decision, run_id))
            if decision == 'stop':
                self._stop_job(job, self._limit_reason(job, now) or 'decision_stop', now)
        return self._run(run_id)

    def _apply_decision(self, job, run, status, now):
        if not job['decision_required']:
            return
        reason = self._limit_reason(job, now)
        if not reason:
            if run['decision'] == 'stop':
                reason = 'decision_stop'
            elif status != 'succeeded':
                reason = 'decision_incomplete'
            elif run['decision'] != 'continue':
                reason = 'decision_missing'
        if reason:
            self._stop_job(job, reason, now)

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
            if action == 'retry':
                self._require_budget(job, now)
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
            self.db.execute('UPDATE jobs SET registration_revision=registration_revision+1')
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

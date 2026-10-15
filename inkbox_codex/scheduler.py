"""Gateway-owned scheduler with durable execution and isolated Codex sessions."""
from __future__ import annotations

import asyncio
import ctypes
import errno
import fcntl
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .codex_client import CodexAppServerClient
from .schedules import ScheduleStore, schedule_path
from .tools import build_inkbox_mcp_server_config

logger = logging.getLogger(__name__)


def process_stamp(pid):
    if pid is None:
        return None
    try:
        if Path('/proc').is_dir():
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            return Path('/proc/sys/kernel/random/boot_id').read_text().strip() + ':' + fields[19]
        result = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart='], capture_output=True, text=True, timeout=2)
        return result.stdout.strip() or None
    except (OSError, IndexError, subprocess.TimeoutExpired):
        return None


def _pidfd_syscall(number, *args):
    # Some portable Python builds omit pidfd wrappers despite kernel support.
    if (sys.platform != 'linux' or os.uname().machine not in {'x86_64', 'aarch64'}
            or ctypes.sizeof(ctypes.c_void_p) != 8):
        raise OSError(errno.ENOSYS, 'Process handles are unavailable')
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    result = libc.syscall(number, *args)
    if result == -1:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))
    return result


def _open_process_handle(pid):
    if hasattr(os, 'pidfd_open'):
        return os.pidfd_open(pid)
    return _pidfd_syscall(434, pid, 0)


def _kill_process_handle(descriptor):
    if hasattr(signal, 'pidfd_send_signal'):
        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
    else:
        _pidfd_syscall(424, descriptor, signal.SIGKILL, ctypes.c_void_p(), 0)


def _fence_leaderless_members(run):
    """Signal only retained run-owned children, never a recycled group number."""
    if not run.get('id'):
        return
    marker = f"INKBOX_SCHEDULE_RUN_ID={run['id']}".encode()
    try:
        processes = tuple(Path('/proc').iterdir())
    except OSError:
        return
    for process in processes:
        if not process.name.isdecimal():
            continue
        descriptor = None
        try:
            fields = (process / 'stat').read_text().rsplit(')', 1)[1].split()
            if (int(fields[2]) != run['host_pid'] or int(fields[3]) != run['host_pid']
                    or process.stat().st_uid != os.getuid()):
                continue
            descriptor = _open_process_handle(int(process.name))
            if marker not in (process / 'environ').read_bytes().split(b'\0'):
                continue
            # A pidfd keeps signalling tied to this process even if its PID is reused.
            _kill_process_handle(descriptor)
        except (OSError, IndexError, ValueError):
            continue
        finally:
            if descriptor is not None:
                os.close(descriptor)


def fence_host(run):
    """Never signal a process group after its recorded identity has changed."""
    pid = run['host_pid']
    if pid is None:
        return True
    stamp = run['host_start'] or ''
    if ':' in stamp:
        try:
            if stamp.split(':', 1)[0] != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
                return True
        except OSError:
            pass
    current = process_stamp(pid)
    if current is not None:
        if not run['host_start']:
            return False
        if current != run['host_start']:
            return True
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            return True
        except OSError:
            return False
    else:
        _fence_leaderless_members(run)
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


class Scheduler:
    def __init__(self, cfg, *, store=None, client_factory=None, clock=time.time):
        self.cfg = cfg
        self.store = store
        self._owns_store = store is None
        self.client_factory = client_factory or CodexAppServerClient
        self.clock = clock
        self.active = None
        self._loop_task = None
        self._lock = None
        self._unrecorded_hosts = {}

    async def start(self):
        if self._loop_task is not None:
            return
        path = self.store.path if self.store else schedule_path(self.cfg)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock = path.with_suffix('.worker.lock').open('a')
        path.with_suffix('.worker.lock').chmod(0o600)
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.store is None:
                self.store = ScheduleStore(path)
            self.store.recover_interrupted(self.clock())
            await self._fence_orphans()
        except BaseException:
            self._lock.close()
            self._lock = None
            if self._owns_store and self.store is not None:
                self.store.close()
                self.store = None
            raise
        self._loop_task = asyncio.create_task(self._loop())

    async def _fence_orphans(self):
        for run_id, host in list(self._unrecorded_hosts.items()):
            if await asyncio.to_thread(fence_host, host):
                self.store.clear_host(run_id)
            else:
                self.store.record_host(run_id, host['host_pid'], host['host_start'])
            self.store.finish(run_id, 'failed', error='Stopped before Codex initialization completed',
                              now=self.clock())
            self._unrecorded_hosts.pop(run_id)
        for run in self.store.orphaned_hosts():
            if await asyncio.to_thread(fence_host, run):
                self.store.clear_host(run['id'])

    async def tick(self):
        if self.active is not None and self.active.done():
            try:
                self.active.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                self.store.recover_interrupted(self.clock())
                raise
            finally:
                self.active = None
        self.store.materialize(self.clock())
        if self.active is None:
            await self._fence_orphans()
            run = self.store.claim(self.clock())
            if run:
                self.active = asyncio.create_task(self._execute(run))
        self.store.heartbeat(now=self.clock())

    async def _loop(self):
        while True:
            try:
                await self.tick()
            except Exception:
                logger.exception('Scheduler tick failed; retained jobs will be checked again')
                try:
                    self.store.heartbeat(error='Scheduler tick failed; inspect gateway logs', now=self.clock())
                except Exception:
                    logger.error('Scheduler receipt store is unavailable')
            await asyncio.sleep(1)

    async def _execute(self, run):
        client = None
        submitted = False
        outcome = None
        result = ''
        error = ''
        try:
            job = self.store.get(run['job_id'])
            server, _ = build_inkbox_mcp_server_config(self.cfg)
            env = dict(os.environ)
            for key in list(env):
                if key.startswith(('INKBOX_CODEX_CHAT_', 'INKBOX_CODEX_TURN_', 'INKBOX_SCHEDULE_')):
                    env.pop(key, None)
            env['INKBOX_SCHEDULE_JOB_ID'] = job['id']
            env['INKBOX_SCHEDULE_RUN_ID'] = run['id']
            server['env'] = {**server['env'], 'INKBOX_CODEX_CHAT_ID': '',
                             'INKBOX_SCHEDULE_JOB_ID': job['id'],
                             'INKBOX_SCHEDULE_RUN_ID': run['id']}
            instructions = (
                'You are running a saved scheduled task. Use only this job’s dedicated session; '
                'there is no incoming message or implicit reply recipient. The saved prompt defines '
                'the task and any authorized recipients. Use Inkbox tools only when the prompt asks '
                'for a message or other action. Your final response is saved in local execution history '
                'and is not automatically sent. Do not create or change schedules. '
                'If a required action needs approval, explain that it was not performed. '
                'The current saved task supersedes conflicting instructions in previous runs.'
            )
            if job['decision_required']:
                instructions += (
                    ' This job requires a continuation decision. After evaluating the saved task, '
                    'call inkbox_schedule_decide with decision="continue" only if another run is '
                    'needed, or decision="stop" when finished. Record one decision before ending '
                    'this run; ordinary prose is not a decision. Continue requires successful '
                    'completion and never overrides time/run limits. Missing decisions or failures '
                    'pause the job. This tool affects only your current job; do not create another '
                    'schedule or use general schedule mutations.'
                )
            def process_started(pid):
                host = {'id': run['id'], 'host_pid': pid, 'host_start': process_stamp(pid)}
                self._unrecorded_hosts[run['id']] = host
                self.store.record_host(run['id'], pid, host['host_start'])
                self._unrecorded_hosts.pop(run['id'])

            client = self.client_factory(self.cfg, developer_instructions=instructions,
                                         mcp_server_config=server, isolate_process_group=True,
                                         process_env=env, process_started=process_started)
            async with asyncio.timeout(self.cfg.codex_turn_timeout_s):
                thread_id = await client.connect(job['thread_id'])
                self.store.mark_running(run['id'], thread_id)
                prompt = (f"Scheduled execution: {run['id']}\n"
                          f"Occurrences represented: {run['count']}\n"
                          f"Scheduled UTC range: {run['first_due']} through {run['last_due']}\n"
                          'Perform the saved task once, accounting for the range above.\n\n' + run['prompt'])
                submitted = True
                result = await client.run(prompt)
                outcome = 'succeeded'
        except asyncio.CancelledError:
            outcome = 'uncertain' if submitted else 'failed'
            error = 'Execution interrupted; inspect its session before retrying' if submitted else 'Stopped before turn submission'
            raise
        except Exception:
            outcome = 'uncertain' if submitted else 'failed'
            error = ('Execution outcome is unconfirmed; inspect its session before retrying' if submitted
                     else 'Codex could not start this execution; check gateway readiness and retry')
            logger.warning('Scheduled execution %s ended without a confirmed result', run['id'])
        finally:
            stopped = client is None
            if client is not None:
                pid = getattr(client, 'process_group_id', None)
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=15)
                    if pid is None:
                        stopped = True
                    else:
                        try:
                            os.killpg(pid, 0)
                        except ProcessLookupError:
                            stopped = True
                except Exception:
                    logger.warning('Scheduled execution process cleanup is not confirmed')
            if stopped:
                self._unrecorded_hosts.pop(run['id'], None)
            elif run['id'] in self._unrecorded_hosts:
                host = self._unrecorded_hosts[run['id']]
                try:
                    self.store.record_host(run['id'], host['host_pid'], host['host_start'])
                    self._unrecorded_hosts.pop(run['id'])
                except Exception:
                    logger.warning('Scheduled process ownership could not be saved; execution remains blocked')
            if outcome and run['id'] not in self._unrecorded_hosts:
                self.store.finish(run['id'], outcome, result=result, error=error, now=self.clock())
            if stopped:
                self.store.clear_host(run['id'])

    async def close(self):
        if self._loop_task:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None
        if self.active:
            self.active.cancel()
            await asyncio.gather(self.active, return_exceptions=True)
            self.active = None
        if self._unrecorded_hosts:
            await self._fence_orphans()
        if self.store is not None and self._owns_store:
            self.store.close()
            self.store = None
        if self._lock:
            self._lock.close()
            self._lock = None

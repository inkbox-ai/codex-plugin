"""Private local control for an externally supervised gateway."""
from __future__ import annotations

import asyncio
import os
import sqlite3
import stat
from pathlib import Path

from aiohttp import web

from .schedules import OccurrenceRejected, ScheduleStore, schedule_path


class ManagedControl:
    def __init__(self, gateway, path: Path):
        self.gateway = gateway
        self.path = path
        self.runner = None
        self.inode = None
        self.ingress = None

    async def start(self):
        parent = self.path.parent.stat()
        if (not self.path.is_absolute() or not stat.S_ISDIR(parent.st_mode)
                or parent.st_uid != os.getuid() or parent.st_mode & 0o077
                or self.path.exists() or self.path.is_symlink()):
            raise ValueError('Managed control requires a private directory and unused socket')
        app = web.Application(client_max_size=65536)
        app.router.add_post('/control', self.handle)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        try:
            await web.UnixSite(self.runner, str(self.path)).start()
            self.inode = self.path.stat().st_ino
            self.path.chmod(0o600)
            if activation := os.getenv("INKBOX_HOSTED_ACTIVATION_ID"):
                from .managed_ingress import ManagedIngress
                self.ingress = ManagedIngress(self.gateway, activation)
                self.ingress.start()
        except BaseException:
            await self.close()
            raise

    async def close(self):
        if self.ingress is not None:
            await self.ingress.close()
            self.ingress = None
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None
        if self.inode is not None:
            try:
                if self.path.lstat().st_ino == self.inode:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            self.inode = None

    def schedules(self, data):
        with ScheduleStore(schedule_path(self.gateway.cfg)) as store:
            if not store.external_timer():
                raise ValueError('External scheduling is not ready')
            action = data['action']
            if action == 'schedules':
                if set(data) != {'action'}:
                    raise ValueError('Unexpected fields')
                return {'schedules': store.registrations()}
            fields = {'action', 'job_id', 'revision'}
            if action == 'update_schedule':
                if set(data) != fields | {'command_id', 'schedule', 'enabled'}:
                    raise ValueError('Unexpected fields')
                return store.update_external(data['job_id'], data['revision'], data['command_id'],
                                             data['schedule'], data['enabled'])
            if action == 'acknowledge_schedule':
                if set(data) != fields:
                    raise ValueError('Unexpected fields')
                store.acknowledge_registration(data['job_id'], data['revision'])
                return {'acknowledged': True}
            if action == 'quarantine_schedule' and set(data) in (fields | {'occurrence_id', 'due'}, fields | {'occurrence_id', 'due', 'admitted_at'}):
                receipt = store.quarantine_occurrence(data['job_id'], data['revision'], data['occurrence_id'], data['due'])
                return {'status': receipt['status'], **({'execution_stopped': True} if receipt['status'] == 'uncertain' and receipt.get('host_pid') is None else {})}
            if action == 'cancel_schedule' and set(data) in (fields | {'occurrence_id', 'due'}, fields | {'occurrence_id', 'due', 'admitted_at'}):
                receipt = store.cancel_occurrence(data['job_id'], data['revision'], data['occurrence_id'], data['due'])
                return {'status': receipt['status'], **({'execution_stopped': True} if receipt['status'] == 'uncertain' and receipt.get('host_pid') is None else {})}
            if action != 'invoke_schedule' or set(data) not in (fields | {'occurrence_id', 'due'}, fields | {'occurrence_id', 'due', 'admitted_at'}):
                raise ValueError('Unknown action or fields')
            try:
                receipt = store.accept_occurrence(data['job_id'], data['revision'], data['occurrence_id'], data['due'], admitted_at=data.get('admitted_at'))
            except OccurrenceRejected:
                return {'status': 'rejected', 'execution_stopped': True}
            return {'run_id': receipt.get('id'), 'status': receipt['status'], **({'execution_stopped': True} if receipt['status'] == 'uncertain' and receipt.get('host_pid') is None else {})}

    async def handle(self, request):
        try:
            data = await request.json()
            if not isinstance(data, dict) or not isinstance(data.get('action'), str):
                raise TypeError('Invalid request')
            if data['action'] == 'quarantine_delivery' and set(data) == {'action', 'event_id'}:
                if self.ingress is None:
                    raise ValueError('Receiver unavailable')
                result = self.ingress.inbox.quarantine(data['event_id'])
            elif data['action'] in {'conversations', 'conversation'}:
                from .managed_conversations import read
                result = await read(self.gateway.sessions, data)
            elif data['action'] in {'status', 'drain', 'resume'}:
                if set(data) != {'action'}:
                    raise ValueError('Unexpected fields')
                if data['action'] != 'status':
                    self.gateway._managed_draining = data['action'] == 'drain'
                result = await self.gateway.managed_status()
            else:
                if data['action'] == 'invoke_schedule' and self.gateway._managed_draining:
                    return web.json_response({'error': 'temporarily_unavailable'}, status=503)
                self.gateway._managed_ingress += 1
                try:
                    result = await asyncio.to_thread(self.schedules, data)
                finally:
                    self.gateway._managed_ingress -= 1
            return web.json_response(result)
        except (ValueError, TypeError, KeyError):
            return web.json_response({'error': 'invalid_or_stale_request'}, status=409)
        except (OSError, sqlite3.Error, RuntimeError, TimeoutError):
            return web.json_response({'error': 'temporarily_unavailable'}, status=503)

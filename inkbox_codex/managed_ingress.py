"""Dispatch durable managed deliveries through the existing channel handlers."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path

from .managed_inbox import ManagedInbox
from .managed_work import DeliveryWork, scope

logger = logging.getLogger(__name__)


class ManagedIngress:
    def __init__(self, gateway, activation):
        self.gateway = gateway
        key = hashlib.sha256(f'{gateway.cfg.base_url}|{gateway.cfg.identity}'.encode()).hexdigest()
        root = Path(os.getenv('INKBOX_CODEX_HOME') or Path.home() / '.inkbox-codex')
        self.inbox = ManagedInbox(root / 'managed' / key / 'events.sqlite3', activation)
        self.tasks = []

    def start(self):
        self.tasks = [asyncio.create_task(self.loop()) for _ in range(4)]
        self.tasks.append(asyncio.create_task(self.loop(protocol_only=True)))

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        self.inbox.close()

    async def loop(self, *, protocol_only=False):
        while True:
            claimed = self.inbox.claim(protocol_only=protocol_only)
            if claimed is None:
                await asyncio.sleep(0.25)
                continue
            event_id, payload = claimed
            completed = False
            try:
                async with asyncio.timeout(86400):
                    payload = await refresh_media(self.gateway, payload)
                    if payload.get('companion') is not None:
                        await self.gateway._companion().accept(payload)
                        completed = True
                    else:
                        work = DeliveryWork()
                        with scope((work,)):
                            response = await self.gateway._route_inkbox_event(payload['event_type'], payload)
                        work.seal()
                        completed = 200 <= response.status < 300 and await work.done
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001
                logger.warning("Managed delivery outcome is unknown (%s)", type(error).__name__)
                completed = False
            finally:
                self.inbox.finish(event_id, completed=completed)


async def refresh_media(gateway, payload):
    """Refresh expiring media at dispatch, including deliveries with no retry.

    Read through the identity-scoped SDK. Only media is replaced; routing and
    sender authorization continue to use the accepted delivery. Lookup failures
    propagate so a media-only message cannot silently run without its files.
    """
    from dataclasses import asdict, is_dataclass

    channel = payload.get('event_type')
    if channel not in {'text.received', 'imessage.received'}:
        return payload
    key = 'text_message' if channel == 'text.received' else 'message'
    message = (payload.get('data') or {}).get(key) or {}
    if not message.get('media'):
        return payload
    message_id = message.get('id')
    if not message_id:
        raise ValueError('Media message identity required')
    lookup = gateway._identity.get_text if channel == 'text.received' else gateway._identity.get_imessage
    fresh = await asyncio.to_thread(lookup, message_id)
    media = fresh.get('media') if isinstance(fresh, dict) else fresh.media
    if not media:
        raise ValueError('Fresh media unavailable')
    media = [asdict(item) if is_dataclass(item) else dict(item) for item in media]
    return {**payload, 'data': {**payload['data'], key: {**message, 'media': media}}}

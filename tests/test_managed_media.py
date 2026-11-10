import asyncio
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from inkbox_codex.managed_inbox import ManagedInbox
from inkbox_codex.managed_ingress import refresh_media
from inkbox_codex.media import download_media
from tests.test_managed_inbox import event


@pytest.mark.parametrize('channel,key,lookup', [
    ('imessage.received', 'message', 'get_imessage'),
    ('text.received', 'text_message', 'get_text'),
])
def test_queued_media_refreshes_without_delivery_retry(tmp_path, monkeypatch, channel, key, lookup):
    monkeypatch.setenv('INKBOX_CODEX_MEDIA_DIR', str(tmp_path / 'media'))

    async def scenario():
        app = web.Application()
        async def expired(_):
            return web.Response(status=403)

        async def fresh(_):
            return web.Response(body=b'attachment')

        app.router.add_get('/expired', expired)
        app.router.add_get('/fresh', fresh)
        async with TestServer(app) as server:
            activation = str(uuid4())
            original = event(activation) | {'event_type': channel, 'data': {key: {
                'id': 'source-message', 'media': [{'url': str(server.make_url('/expired'))}]}}}
            inbox = ManagedInbox(tmp_path / 'inbox' / 'events.sqlite3', activation)
            try:
                inbox.accept(original)
                _, queued = inbox.claim()
                assert not await download_media(queued['data'][key]['media'], prefix='old')
                get = Mock(return_value={'media': [{'url': str(server.make_url('/fresh'))}]})
                gateway = SimpleNamespace(_identity=SimpleNamespace(**{lookup: get}))
                refreshed = await refresh_media(gateway, queued)
                get.assert_called_once_with('source-message')
                files = await download_media(refreshed['data'][key]['media'], prefix='new')
                assert len(files) == 1
                assert queued == original
                assert refreshed['hosted_delivery'] == original['hosted_delivery']
                get.side_effect = PermissionError('access denied')
                with pytest.raises(PermissionError):
                    await refresh_media(gateway, queued)
            finally:
                inbox.close()
    asyncio.run(scenario())


def test_dispatch_refresh_failure_is_unknown_without_starting_agent(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    from inkbox_codex.managed_ingress import ManagedIngress
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))

    async def scenario():
        activation = str(uuid4())
        gateway = SimpleNamespace(cfg=SimpleNamespace(base_url='https://example.test', identity='synthetic'),
                                  _identity=SimpleNamespace(get_imessage=Mock(side_effect=PermissionError('denied'))),
                                  _route_inkbox_event=AsyncMock())
        ingress = ManagedIngress(gateway, activation)
        payload = event(activation) | {'event_type': 'imessage.received', 'data': {'message': {
            'id': 'source-message', 'media': [{'url': 'https://example.test/expired'}]}}}
        ingress.inbox.accept(payload)
        task = asyncio.create_task(ingress.loop())
        try:
            async with asyncio.timeout(2):
                while ingress.inbox.summary()['unknown'] != 1:
                    await asyncio.sleep(.001)
            gateway._route_inkbox_event.assert_not_awaited()
            assert ingress.inbox.claim() is None
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            ingress.inbox.close()
    asyncio.run(scenario())

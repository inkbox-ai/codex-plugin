import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from aiohttp import web

from inkbox_codex.managed_ingress import ManagedIngress
from inkbox_codex.managed_work import (
    DeliveryWork,
    TurnQueue,
    attach,
    finish,
    scope,
    spawn,
)
from inkbox_codex.sessions import _Turn
from tests.test_managed_inbox import event
from tests.test_sessions import make_session


def test_acceptance_is_not_completion_and_conversations_are_independent(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    async def scenario():
        queue = TurnQueue()
        async def route(name, payload):
            queue.put_nowait(_Turn(text=payload['id']))
            return web.json_response({'ok': True})
        gateway = SimpleNamespace(cfg=SimpleNamespace(base_url='https://api.example.test', identity='example'),
                                  _route_inkbox_event=route)
        activation = str(uuid4())
        ingress = ManagedIngress(gateway, activation)
        try:
            first = ingress.inbox.accept(event(activation))
            ingress.inbox.accept(event(activation, 2))
            assert first['state'] == 'accepted'
            ingress.start()
            one = await asyncio.wait_for(queue.get(), 2)
            two = await asyncio.wait_for(queue.get(), 2)
            assert ingress.inbox.summary()['active'] == 2
            finish(two, 'completed')
            await asyncio.sleep(0)
            assert ingress.inbox.accept(event(activation, 2))['state'] == 'completed'
            assert ingress.inbox.accept(event(activation))['state'] == 'accepted'
            finish(one, 'failed')
            await asyncio.sleep(0)
            assert ingress.inbox.accept(event(activation))['state'] == 'unknown'
        finally:
            await ingress.close()
    asyncio.run(scenario())


def test_batched_turn_settles_both_deliveries_and_recovery_is_retained():
    async def scenario():
        first, second = DeliveryWork(), DeliveryWork()
        queue = TurnQueue()
        turn = _Turn(text='Combined messages')
        with scope((first,)):
            attach(turn)
        with scope((second,)):
            attach(turn)
            queue.put_nowait(turn)
        first.seal()
        second.seal()
        turn = queue.get_nowait()
        release = asyncio.Event()
        async def recover():
            await release.wait()
        with scope(turn.managed_work):
            recovery = spawn(recover())
        finish(turn, 'completed')
        assert not first.done.done() and not second.done.done()
        release.set()
        await recovery
        await asyncio.sleep(0)
        assert await first.done and await second.done
    asyncio.run(scenario())


def test_uncertain_terminal_reply_retains_managed_inbound_payload(tmp_path, monkeypatch):
    import inkbox.remote_mcp

    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))

    async def scenario():
        sent = []
        session = make_session(sent)
        session.identity_info.update(id='identity', api_origin='https://api.example.test')
        session._ensure_client = AsyncMock(return_value=SimpleNamespace(
            thread_id='thread', run=AsyncMock(return_value='Automatic fallback'),
        ))
        journal = SimpleNamespace(lookup=lambda _: {'state': 'pending', 'submitted': True})
        monkeypatch.setattr(inkbox.remote_mcp, 'RemoteMcp', lambda **_: SimpleNamespace(
            terminal_replies=lambda **_: journal,
        ))

        async def route(_, payload):
            session._queue.put_nowait(_Turn(
                text='Synthetic inbound',
                reply_mode='email',
                reply_meta={'message_id': payload['id'], 'sender': 'sender@example.test'},
            ))
            await session._drain()
            return web.json_response({'ok': True})

        gateway = SimpleNamespace(
            cfg=SimpleNamespace(base_url='https://api.example.test', identity='example'),
            _route_inkbox_event=route,
        )
        activation = str(uuid4())
        payload = event(activation)
        ingress = ManagedIngress(gateway, activation)
        try:
            ingress.inbox.accept(payload)
            ingress.start()
            async with asyncio.timeout(2):
                while ingress.inbox.accept(payload)['state'] == 'accepted':
                    await asyncio.sleep(0.01)
            assert ingress.inbox.accept(payload)['state'] == 'unknown'
            row = ingress.inbox.db.execute('SELECT payload FROM deliveries').fetchone()
            assert json.loads(row[0]) == payload
            assert ingress.inbox.claim() is None and sent == []
        finally:
            await ingress.close()

    asyncio.run(scenario())


def test_shutdown_keeps_unfinished_outcome_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv('INKBOX_CODEX_HOME', str(tmp_path))
    async def scenario():
        entered = asyncio.Event()
        async def route(*_):
            entered.set()
            await asyncio.Event().wait()
        gateway = SimpleNamespace(cfg=SimpleNamespace(base_url='https://api.example.test', identity='example'),
                                  _route_inkbox_event=route)
        activation = str(uuid4())
        ingress = ManagedIngress(gateway, activation)
        ingress.inbox.accept(event(activation))
        ingress.start()
        await asyncio.wait_for(entered.wait(), 2)
        await ingress.close()
        reopened = ManagedIngress(gateway, activation)
        try:
            assert reopened.inbox.accept(event(activation))['state'] == 'unknown'
            assert reopened.inbox.claim() is None
        finally:
            await reopened.close()
    asyncio.run(scenario())


def test_managed_receipt_requires_signature_even_when_ordinary_signatures_are_optional(tmp_path, monkeypatch):
    import json
    from unittest.mock import AsyncMock

    import inkbox_codex.gateway as module
    from inkbox_codex.gateway import InkboxGateway
    from inkbox_codex.managed_inbox import ManagedInbox

    async def scenario():
        activation = str(uuid4())
        inbox = ManagedInbox(tmp_path / 'inbox' / 'events.sqlite3', activation)
        gateway = object.__new__(InkboxGateway)
        gateway.cfg = SimpleNamespace(require_signature=False)
        gateway._provider_secret = lambda _: 'synthetic'
        gateway._managed_control = SimpleNamespace(ingress=SimpleNamespace(inbox=inbox, tasks=[]))
        provider = SimpleNamespace(name='inkbox', verify=lambda **_: False)
        monkeypatch.setattr(module, 'match_provider', lambda _: provider)
        request = SimpleNamespace(read=AsyncMock(return_value=json.dumps(event(activation)).encode()),
                                  headers={}, url='https://receiver.example.test/webhook')
        try:
            assert (await gateway._accept_webhook(request)).status == 401
            assert inbox.summary()['pending'] == 0
            provider.verify = lambda **_: True
            response = await gateway._accept_webhook(request)
            assert response.status == 202
            assert json.loads(response.body)['state'] == 'accepted'
            assert inbox.summary()['pending'] == 1
            request.read.return_value = json.dumps(event(str(uuid4()))).encode()
            assert (await gateway._accept_webhook(request)).status == 409
            assert inbox.summary()['pending'] == 1
        finally:
            inbox.close()
    asyncio.run(scenario())


def test_hosted_mode_ignores_legacy_subscription_without_bypassing_signature(monkeypatch):
    import json
    from unittest.mock import AsyncMock

    import inkbox_codex.gateway as module
    from inkbox_codex.gateway import InkboxGateway
    monkeypatch.setenv('INKBOX_HOSTED_ACTIVATION_ID', str(uuid4()))
    async def scenario():
        gateway = object.__new__(InkboxGateway)
        gateway.cfg = SimpleNamespace(require_signature=False)
        gateway._provider_secret = lambda _: 'synthetic'
        gateway._route_inkbox_event = AsyncMock()
        provider = SimpleNamespace(name='inkbox', verify=lambda **_: True)
        monkeypatch.setattr(module, 'match_provider', lambda _: provider)
        request = SimpleNamespace(read=AsyncMock(return_value=b'{"event_type":"message.received"}'), headers={}, url='https://receiver.example.test/webhook')
        response = await gateway._accept_webhook(request)
        assert json.loads(response.body)['ignored'] == 'managed-delivery-required'
        gateway._route_inkbox_event.assert_not_awaited()
        provider.verify = lambda **_: False
        assert (await gateway._accept_webhook(request)).status == 401
    asyncio.run(scenario())

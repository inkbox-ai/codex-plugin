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
                get = Mock(return_value={'id': 'source-message', 'media': [{'url': str(server.make_url('/fresh'))}]})
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


@pytest.mark.parametrize("legacy", [False, True])
def test_media_refresh_preserves_accepted_transcript_and_attachment_identity(legacy):
    async def scenario():
        accepted = {"id": "attachment-1", "url": "https://example.test/expired",
                    "content_type": "audio/wav", "transcription": {"status": "completed", "text": "A recorded update."}}
        original = {"event_type": "imessage.received", "data": {"message": {
            "id": "source-message", "content": "Original caption", "media": [accepted]}}}
        fresh = {"url": "https://example.test/refreshed", "content_type": "audio/wav"}
        if not legacy:
            fresh["id"] = "attachment-1"
        gateway = SimpleNamespace(_identity=SimpleNamespace(get_imessage=Mock(return_value={
            "id": "source-message", "content": "Ignored replacement text", "media": [fresh]})))
        result = await refresh_media(gateway, original)
        refreshed = result["data"]["message"]
        assert refreshed["content"] == "Original caption"
        assert refreshed["media"][0] == {**accepted, "url": "https://example.test/refreshed"}
        assert original["data"]["message"]["media"][0]["url"] == "https://example.test/expired"
    asyncio.run(scenario())


def test_media_refresh_matches_ids_after_reordering_and_keeps_fresh_terminal_errors():
    async def scenario():
        original = {"event_type": "imessage.received", "data": {"message": {"id": "source-message", "media": [
            {"id": "one", "transcription": {"status": "pending"}},
            {"id": "two", "transcription": {"status": "completed", "text": "Original result"}},
        ]}}}
        gateway = SimpleNamespace(_identity=SimpleNamespace(get_imessage=Mock(return_value={
            "id": "source-message", "media": [
                {"id": "two", "url": "https://example.test/two"},
                {"id": "one", "url": "https://example.test/one", "transcription": {"status": "failed", "error_code": "source_unavailable"}},
            ]})))
        result = (await refresh_media(gateway, original))["data"]["message"]["media"]
        assert [item["id"] for item in result] == ["one", "two"]
        assert result[0]["url"] == "https://example.test/one"
        assert result[0]["transcription"] == {"status": "failed", "error_code": "source_unavailable"}
        assert result[1]["transcription"] == {"status": "completed", "text": "Original result"}
    asyncio.run(scenario())


@pytest.mark.parametrize("foreign_message,foreign_attachment", [(True, False), (False, True)])
def test_media_refresh_rejects_foreign_message_or_attachment(foreign_message, foreign_attachment):
    async def scenario():
        original = {"event_type": "imessage.received", "data": {"message": {"id": "source-message", "media": [
            {"id": "attachment-1", "transcription": {"status": "completed", "text": "Accepted result"}},
        ]}}}
        gateway = SimpleNamespace(_identity=SimpleNamespace(get_imessage=Mock(return_value={
            "id": "other-message" if foreign_message else "source-message", "media": [
                {"id": "other-attachment" if foreign_attachment else "attachment-1", "url": "https://example.test/other"},
            ]})))
        with pytest.raises(ValueError, match="identity"):
            await refresh_media(gateway, original)
    asyncio.run(scenario())


def test_media_refresh_matches_legacy_position_alongside_reordered_known_ids():
    accepted = [{"id": "one"}, {"content_type": "audio/wav"}, {"id": "three"}]
    original = {"event_type": "imessage.received", "data": {"message": {"id": "source", "media": accepted}}}
    fresh = [{"id": key, "url": f"https://example.test/{key}"} for key in ("three", "two", "one")]
    fresh[1].update(content_type="audio/wav", transcription={"status": "completed", "text": "Hello"})
    lookup = Mock(return_value={"id": "source", "media": fresh})
    gateway = SimpleNamespace(_identity=SimpleNamespace(get_imessage=lookup))
    result = asyncio.run(refresh_media(gateway, original))["data"]["message"]["media"]
    assert result == [
        {**accepted[0], "url": fresh[2]["url"]},
        {**accepted[1], "url": fresh[1]["url"], "transcription": fresh[1]["transcription"]},
        {**accepted[2], "url": fresh[0]["url"]},
    ]


@pytest.mark.parametrize("message_id,accepted,fresh", [
    (None, [{}], [{"id": "new"}]),
    ("other", [{}], [{"id": "new"}]),
    ("source", [{}], [{"id": "one"}, {"id": "two"}]),
    ("source", [{"content_type": "audio/wav"}], [{"id": "new", "content_type": "image/png"}]),
    ("source", [{}, {"id": "known"}], [{"id": "one"}, {"id": "other"}]),
    ("source", [{}, {"id": "known"}], [{"id": "known"}, {"id": "other"}]),
    ("source", [{"id": "known"}, {}], [{"id": "other"}, {"id": "known"}]),
    ("source", [{}, {}], [{"id": "same"}, {"id": "same"}]),
])
def test_media_refresh_rejects_incompatible_positional_matches(message_id, accepted, fresh):
    original = {"event_type": "imessage.received", "data": {"message": {"id": "source", "media": accepted}}}
    lookup = Mock(return_value={"id": message_id, "media": fresh})
    gateway = SimpleNamespace(_identity=SimpleNamespace(get_imessage=lookup))
    with pytest.raises(ValueError, match="identity|content type"):
        asyncio.run(refresh_media(gateway, original))

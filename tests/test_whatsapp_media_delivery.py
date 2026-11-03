"""WhatsApp media reaches the host as native image input or a voice transcript."""
import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from inkbox_codex import gateway
from inkbox_codex.imessage import IMessageState
from inkbox_codex.whatsapp import receive, run_tool
from tests.test_whatsapp import sdk, bridge, message, CONVERSATION, MESSAGE, MEDIA
from tests.test_whatsapp_autoquote import admitted_source
from tests.test_imessage_sessions import make_session, finish_burst


@pytest.mark.parametrize("caption", ["", "What is in this picture?"])
def test_distinct_images_reach_native_turn_and_survive_receipt_reload(sdk, monkeypatch, tmp_path, caption):
    async def scenario():
        gw = bridge(sdk)
        gw._resolve_contact_full = AsyncMock(return_value=None)
        session, sent, _ = make_session(enabled=False)
        turns = []
        async def run(text, **kwargs):
            turns.append((text, kwargs))
            return "Image received"
        session._client.run = run
        async def admit(body, mode, meta):
            gw._channel_store("whatsapp").admit("contact", body, meta)
            await session.handle_inbound(body, mode, meta)
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=admit)))
        downloaded = []
        async def download(items, *, prefix):
            assert items[0]["url"].startswith("https://")
            path = tmp_path / (prefix + '.png')
            path.write_bytes(b'image-fixture')
            downloaded.append(str(path))
            return [{"path": str(path), "content_type": "image/png"}]
        monkeypatch.setattr(gateway, "download_media", download)
        for index in (31, 32):
            msg = message(id=str(UUID(int=index)), content=caption, media=[{"id": MEDIA}])
            await receive(gw, {"id": f"event-{index}", "data": {"message": msg}})
        await finish_burst(session)
        assert len(turns) == 2
        assert [options["image_paths"] for _, options in turns] == [[path] for path in downloaded]
        assert downloaded[0] != downloaded[1]
        pending = IMessageState(gw.cfg, channel="whatsapp").replay_pending()
        assert [row["meta"]["whatsapp_image_paths"] for row in pending] == [[path] for path in downloaded]
        assert all(row["meta"]["media"] for row in pending)
    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", ["success", "failure", "unconfigured"])
def test_voice_notes_preserve_text_and_do_not_invent_transcripts(sdk, monkeypatch, outcome):
    async def scenario():
        gw = bridge(sdk)
        gw.cfg.realtime.api_key = "synthetic" if outcome != "unconfigured" else ""
        monkeypatch.setattr(gateway, "download_media", AsyncMock(return_value=[{
            "path": "/tmp/voice-fixture.ogg", "content_type": "audio/ogg"}]))
        transcribe = AsyncMock(return_value="Please send the summary")
        if outcome == "failure":
            transcribe.side_effect = ValueError("Synthetic failure")
        monkeypatch.setattr("inkbox_codex.slack_media.transcribe_audio", transcribe)
        paths = []
        body = await gw._with_media("Original caption", [{"url": "https://example.com/audio"}],
                                    prefix="whatsapp-fixture", image_paths=paths)
        assert "Original caption" in body and not paths
        assert ("Please send the summary" in body) == (outcome == "success")
        assert ("do not guess" in body) == (outcome != "success")
        if outcome == "unconfigured":
            transcribe.assert_not_awaited()
    asyncio.run(scenario())


def test_source_bound_tool_defaults_to_active_conversation_without_quote(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"text": "Answer"})
        import json
        assert json.loads(sdk.calls[-1].content) == {"conversation_id": CONVERSATION, "text": "Answer"}
    asyncio.run(scenario())


def test_unavailable_media_does_not_drop_caption_or_owned_id(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        gw._resolve_contact_full = AsyncMock(return_value=None)
        inbound = AsyncMock()
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=inbound)))
        monkeypatch.setattr(gw._identity, "get_whatsapp_media", Mock(side_effect=ValueError("Unavailable")))
        await receive(gw, {"data": {"message": message(content="Please inspect", media=[{"id": MEDIA}])}})
        body, mode, meta = inbound.call_args.args
        assert "Please inspect" in body and "unavailable" in body
        assert mode == "whatsapp" and meta["whatsapp_media_ids"] == [MEDIA]
        assert meta["whatsapp_image_paths"] == []
    asyncio.run(scenario())

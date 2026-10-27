"""Inbound attachments reach the host without changing message admission."""

import asyncio
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex import slack_media
from inkbox_codex.slack import inbound_message, run_tool
from tests.test_slack import CONNECTION, IDENTITY, event, gw


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_MEDIA_DIR", str(tmp_path / "media"))
    resource = NS(get_file=Mock(return_value=NS(name="../../memo.m4a", size=5, mimetype="audio/mp4")),
                  download_file=Mock(return_value=b"audio"),
                  list_connections=Mock(return_value=NS(connections=[NS(id=CONNECTION)])))
    return NS(slack=resource, get_identity=Mock(return_value=NS(id=IDENTITY)))


def audio_event(**changes):
    return event(event={"type": "message", "text": "", "files": [
        {"id": "F_AUDIO", "name": "memo.m4a", "mimetype": "audio/mp4", "size": 5}]}, **changes)


def test_download_tool_writes_private_file_without_remote_path(client, tmp_path):
    result = run_tool(client, "agent", "inkbox_slack_download_file", {
        "connection_id": CONNECTION, "file_id": "F_AUDIO"})
    path = Path(result["path"])
    assert path.parent == tmp_path / "media"
    assert path.suffix == ".m4a" and path.read_bytes() == b"audio"
    assert path.stat().st_mode & 0o777 == 0o600
    client.slack.download_file.assert_called_once_with(CONNECTION, "F_AUDIO")


def test_download_tool_checks_identity_before_reading(client):
    with pytest.raises(ValueError, match="does not belong"):
        run_tool(client, "agent", "inkbox_slack_download_file", {
            "connection_id": "another-connection", "file_id": "F_AUDIO"})
    client.slack.get_file.assert_not_called()
    client.slack.download_file.assert_not_called()


@pytest.mark.parametrize("size", [0, -1, True, slack_media.MAX_FILE_BYTES + 1])
def test_oversized_or_invalid_metadata_is_not_downloaded(client, size):
    client.slack.get_file.return_value.size = size
    with pytest.raises(ValueError, match="10 MiB"):
        slack_media.download_file(client.slack, CONNECTION, "F_AUDIO")
    client.slack.download_file.assert_not_called()


def test_actual_size_is_checked_before_writing(client, tmp_path):
    client.slack.download_file.return_value = b"a" * (slack_media.MAX_FILE_BYTES + 1)
    with pytest.raises(ValueError, match="10 MiB"):
        slack_media.download_file(client.slack, CONNECTION, "F_AUDIO")
    assert not (tmp_path / "media").exists()


def test_gateway_delivers_audio_only_attachment_locally_once(gw, client):
    gw._inkbox = client
    async def scenario():
        await gw._on_slack_received(audio_event())
        await gw._on_slack_received(audio_event())
    asyncio.run(scenario())
    assert len(gw.sessions.turns) == 1
    _, body, mode, meta = gw.sessions.turns[0]
    assert mode == "slack" and meta["raw_text"] == ""
    assert '"status": "downloaded"' in body and '"path":' in body
    client.slack.download_file.assert_called_once()


def test_denied_sender_does_not_download(gw, client):
    gw._inkbox = client
    asyncio.run(gw._on_slack_received(audio_event(sender_access="denied")))
    client.slack.get_file.assert_not_called()
    assert not gw.sessions.turns


def test_failed_attachment_does_not_discard_message_or_leak_error(client):
    client.slack.download_file.side_effect = RuntimeError("sensitive detail")
    _, _, meta = inbound_message(audio_event(), IDENTITY)
    body = asyncio.run(slack_media.with_attachments(client, "original message", meta))
    assert body.startswith("original message") and '"status": "unavailable"' in body
    assert "sensitive detail" not in body


def test_batch_limit_retains_retrieval_instructions(client):
    meta = {"connection_id": CONNECTION, "slack_files": [{"id": f"F_{i}"} for i in range(6)]}
    body = asyncio.run(slack_media.with_attachments(client, "message", meta))
    assert client.slack.download_file.call_count == 4
    assert "inkbox_slack_download_file" in body


def test_released_sdk_download_wire(client):
    import httpx
    from inkbox import Inkbox
    requests = []
    def respond(request):
        requests.append(request)
        if request.url.path.endswith("/content"):
            return httpx.Response(200, content=b"audio")
        return httpx.Response(200, json={"id": "F_AUDIO", "name": "memo.m4a", "size": 5,
                                         "mimetype": "audio/mp4", "downloadable": True})
    sdk = Inkbox(api_key="synthetic-test-key", base_url="https://api.example.test")
    sdk._api_http._client.close()
    sdk._api_http._client = httpx.Client(base_url="https://api.example.test/api/v1",
        headers={"X-API-Key": "synthetic-test-key"}, transport=httpx.MockTransport(respond))
    try:
        result = slack_media.download_file(sdk.slack, CONNECTION, "F_AUDIO")
        assert Path(result["path"]).read_bytes() == b"audio"
        assert len(requests) == 2
        assert requests[-1].url.path.endswith(f"/connections/{CONNECTION}/files/F_AUDIO/content")
    finally:
        sdk.close()


def test_companion_live_audio_reaches_host(tmp_path, monkeypatch, client):
    from tests.test_slack_companion import fixture, gateway, live, session_for
    from tests.test_companion import drained
    async def scenario():
        initial = fixture()
        gateway_instance, _, slack = gateway(monkeypatch, tmp_path, initial)
        gateway_instance.cfg.realtime.api_key = "synthetic-voice-key"
        transcribe = AsyncMock(return_value="Please inspect the uploaded report.")
        monkeypatch.setattr(slack_media, "transcribe_audio", transcribe)
        slack.get_file = client.slack.get_file
        slack.download_file = client.slack.download_file
        receiver = gateway_instance._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gateway_instance, initial)
            session._client.events.clear()
            incoming = live(initial)
            incoming["data"]["event"]["files"] = [{"id": "F_AUDIO", "name": "memo.m4a"}]
            await receiver.accept(incoming)
            await drained(receiver)
            assert len(session._client.events) == 1
            kind, text = session._client.events[0]
            assert kind == "run" and '"status": "downloaded"' in text
            assert "Please inspect the uploaded report." in text
            assert '"path":' in text
            slack.download_file.assert_called_once()
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_gateway_transcribes_before_host_without_promoting_control_commands(gw, client, monkeypatch):
    gw._inkbox = client
    gw.cfg.realtime.api_key = "synthetic-voice-key"
    transcribe = AsyncMock(return_value="/stop")
    monkeypatch.setattr(slack_media, "transcribe_audio", transcribe)
    asyncio.run(gw._on_slack_received(audio_event()))
    _, body, _, meta = gw.sessions.turns[0]
    assert '"transcript": "/stop"' in body
    assert meta["raw_text"] == ""
    assert transcribe.await_args.args[1] == "synthetic-voice-key"


def test_transcription_failure_preserves_download(client, monkeypatch):
    monkeypatch.setattr(slack_media, "transcribe_audio", AsyncMock(side_effect=TimeoutError("private")))
    _, body, meta = inbound_message(audio_event(), IDENTITY)
    cfg = NS(realtime=NS(api_key="synthetic-voice-key"))
    body = asyncio.run(slack_media.with_attachments(client, body, meta, cfg=cfg))
    assert '"status": "downloaded"' in body and '"transcription_status": "failed"' in body
    assert "private" not in body and "synthetic-voice-key" not in body


def test_missing_voice_key_does_not_call_transcription(client, monkeypatch):
    transcribe = AsyncMock()
    monkeypatch.setattr(slack_media, "transcribe_audio", transcribe)
    _, body, meta = inbound_message(audio_event(), IDENTITY)
    body = asyncio.run(slack_media.with_attachments(client, body, meta))
    assert "not transcribed" in body and "INKBOX_REALTIME_API_KEY" in body
    transcribe.assert_not_awaited()


@pytest.mark.parametrize("status,payload,expected", [
    (200, {"text": "Inspect the report."}, "Inspect the report."),
    (200, {"text": " "}, None),
    (200, {"text": "a" * (slack_media.MAX_TRANSCRIPT_CHARS + 1)}, None),
    (401, {"error": "not authorized"}, None),
])
def test_transcription_multipart_wire(tmp_path, monkeypatch, status, payload, expected):
    from aiohttp import web
    path = tmp_path / "memo.m4a"
    path.write_bytes(b"synthetic-audio")
    async def scenario():
        seen = []
        async def handle(request):
            assert request.headers["Authorization"] == "Bearer synthetic-voice-key"
            reader = await request.multipart()
            fields = {}
            async for part in reader:
                fields[part.name] = bytes(await part.read())
                if part.name == "file":
                    assert part.filename == "memo.m4a" and part.headers["Content-Type"] == "audio/mp4"
            assert fields == {"model": b"gpt-transcribe", "file": b"synthetic-audio"}
            seen.append(True)
            return web.json_response(payload, status=status)
        app = web.Application()
        app.router.add_post("/transcriptions", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        monkeypatch.setattr(slack_media, "TRANSCRIPTION_URL", f"http://127.0.0.1:{port}/transcriptions")
        try:
            attachment = {"path": str(path), "content_type": "audio/mp4"}
            if expected is None:
                with pytest.raises(Exception):
                    await slack_media.transcribe_audio(attachment, "synthetic-voice-key")
            else:
                assert await slack_media.transcribe_audio(attachment, "synthetic-voice-key") == expected
            assert seen == [True]
        finally:
            await runner.cleanup()
    asyncio.run(scenario())

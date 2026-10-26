"""Dedicated WhatsApp routing and sends use the real candidate SDK offline."""

import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from inkbox import Inkbox, _config as sdk_config

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.imessage import IMessageState, read_turn_context
from inkbox_codex.tools import call_inkbox_tool, mcp_tool_list
from inkbox_codex.whatsapp import receive, record_status

IDENTITY = str(UUID(int=1))
CONVERSATION = str(UUID(int=2))
MESSAGE = str(UUID(int=3))
MEDIA = str(UUID(int=4))
OUTBOUND = str(UUID(int=5))
NOW = "2026-01-01T00:00:00+00:00"


def message(**changes):
    return {"id": MESSAGE, "agent_identity_id": IDENTITY, "conversation_id": CONVERSATION,
            "direction": "inbound", "local_number": "+15550000001", "remote_number": "+15550000002",
            "sender_number": "+15550000002", "participants": ["+15550000002"], "is_group": False,
            "content": "Question", "media": [], "status": "received", "delivery_state": None,
            "recipients": [], "is_read": False, "created_at": NOW, "updated_at": NOW, **changes}


def media():
    return {"id": MEDIA, "filename": "hello.txt", "content_type": "text/plain", "size": 5,
            "status": "ready", "url": "https://example.com/renewed-attachment"}


@pytest.fixture
def sdk(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_BASE_URL", "https://example.com")
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
    monkeypatch.setattr(sdk_config, "_CONFIG_PATH", tmp_path / "unused-sdk-config")
    state = NS(requests=[], status=201, calls=[], identity_enabled=False)

    def handle(request):
        assert request.url.host == "example.com"
        state.requests.append(request)
        path = request.url.path
        if path == "/api/v1/identities/agent":
            return httpx.Response(200, json={"id": IDENTITY, "organization_id": "org_example", "agent_handle": "agent",
                "whatsapp_enabled": state.identity_enabled, "created_at": NOW, "updated_at": NOW})
        if path == "/api/v1/identities/agent/channel-status":
            return httpx.Response(200, json={"whatsapp": {"enabled": True, "number": "+15550000001", "status": "ready"}})
        if request.method == "POST" and path == "/api/v1/whatsapp/messages":
            state.calls.append(request)
            if state.status != 201:
                return httpx.Response(state.status, json={"detail": {"error": "whatsapp_send_pending"}})
            return httpx.Response(201, json={"message": message(id=OUTBOUND, direction="outbound", status="queued")})
        if path == "/api/v1/message-sends/lookup":
            return httpx.Response(200, json={"message_id": OUTBOUND})
        if path == f"/api/v1/whatsapp/media/{MEDIA}":
            return httpx.Response(200, json=media())
        if path == "/api/v1/whatsapp/media" and request.method == "POST":
            return httpx.Response(201, json=media())
        if path == f"/api/v1/whatsapp/messages/{MESSAGE}":
            return httpx.Response(200, json=message())
        raise AssertionError(f"Unexpected mocked request: {request.method} {path}")

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    state.client = Inkbox(api_key="ApiKey_synthetic_test_only", base_url="https://example.com")
    yield state
    state.client.close()


def bridge(sdk):
    gw = InkboxGateway(BridgeConfig(identity="agent", base_url="https://example.com", allow_all_users=True,
                                   imessage_threaded_replies=False))
    gw._inkbox = sdk.client
    gw._identity = sdk.client.get_identity("agent")
    return gw


def result(value):
    assert not value.get("isError"), value
    return json.loads(value["content"][0]["text"])


def test_all_ten_tools_exist_without_a_cached_enablement_flag(sdk):
    assert len([tool for tool in mcp_tool_list() if "whatsapp" in tool["name"]]) == 10
    status = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_get_whatsapp_status", {})))
    assert status["enabled"] and status["status"] == "ready"
    sent = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"conversation_id": CONVERSATION, "text": "Hello"})))
    assert sent["id"] == OUTBOUND and sent["idempotency_key"]
    payload = json.loads(sdk.calls[-1].content)
    assert payload["text"] == "Hello" and "content" not in payload
    assert "reply_to_message_id" not in payload and "thread_id" not in payload


def test_media_upload_and_original_request_lookup_use_owned_ids(sdk, tmp_path):
    path = tmp_path / "hello.txt"
    path.write_text("hello")
    uploaded = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_upload_whatsapp_media", {"media_path": str(path)})))
    assert uploaded["id"] == MEDIA
    sent = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": "+15550000002", "media_id": MEDIA})))
    assert json.loads(sdk.calls[-1].content)["media_ids"] == [MEDIA]
    key = sent["idempotency_key"]
    found = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {"idempotency_key": key})))
    assert found["message_id"] == OUTBOUND
    lookup = sdk.requests[-1]
    assert dict(lookup.url.params) == {"sender_kind": "whatsapp_identity", "sender_id": IDENTITY, "operation": "whatsapp.send"}
    assert lookup.headers["Idempotency-Key"] == key


def test_uncertain_tool_error_preserves_key_and_media_for_read_only_recovery(sdk):
    sdk.status = 409
    value = asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"conversation_id": CONVERSATION, "text": "Hello", "media_id": MEDIA}))
    assert value["isError"]
    error = json.loads(value["content"][0]["text"])
    assert error["idempotency_key"] == sdk.calls[-1].headers["Idempotency-Key"]
    assert error["media_id"] == MEDIA and len(sdk.calls) == 1


def test_inbound_validates_owner_and_group_before_model_wake_and_renews_media(sdk):
    async def run():
        gw = bridge(sdk)
        gw._resolve_contact_full = AsyncMock(return_value=None)
        gw._with_media = AsyncMock(return_value="Question with attachment")
        inbound = AsyncMock()
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=inbound)))
        payload = {"id": "event-one", "data": {"message": message(media=[media()])}}
        response = await receive(gw, payload)
        assert response.status == 200
        inbound.assert_awaited_once()
        args = inbound.call_args.args
        assert args[1] == "whatsapp" and args[2]["conversation_id"] == CONVERSATION
        assert args[2]["channel"] == "whatsapp"
        assert gw._with_media.call_args.args[1][0]["url"] == media()["url"]
        await receive(gw, payload)
        inbound.assert_awaited_once()
        for changes in ({"agent_identity_id": str(UUID(int=99))}, {"is_group": True}, {"sender_number": "+15550000001"}):
            rejected = {"id": "different", "data": {"message": message(**changes)}}
            await receive(gw, rejected)
            inbound.assert_awaited_once()
    asyncio.run(run())


def test_plain_reply_is_durable_without_native_imessage_flag_and_status_never_wakes(sdk):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
                "message_id": MESSAGE, "imessage_event_ids": ["event-one"]}
        assert await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        await gw._imessage_turn_state("contact", "result", meta, "Saved answer")
        assert gw._channel_store("whatsapp").summary()["reply_pending"] == 1
        assert IMessageState(gw.cfg).summary()["pending"] == 0
        await gw.send_to_contact("contact", "Saved answer", "whatsapp", {**meta, "imessage_final_reply": True})
        key = sdk.calls[-1].headers["Idempotency-Key"]
        await gw.send_to_contact("contact", "Saved answer", "whatsapp", {**meta, "imessage_final_reply": True})
        assert sdk.calls[-1].headers["Idempotency-Key"] == key
        gw.sessions = NS(get=Mock())
        record_status(gw, {"data": {"message": message(id=OUTBOUND, direction="outbound", status="delivered", delivery_state="delivered", updated_at="2026-01-01T00:00:02+00:00")}})
        record_status(gw, {"data": {"message": message(id=OUTBOUND, direction="outbound", status="sent", delivery_state="pending", updated_at=NOW)}})
        assert gw._channel_store("whatsapp").lookup_outbound(OUTBOUND)["delivery_state"] == "delivered"
        gw.sessions.get.assert_not_called()
        assert gw._channel_store("whatsapp").summary()["done"] == 1
    asyncio.run(run())


def test_signature_is_required_even_when_generic_signature_check_is_disabled(sdk):
    from tests.test_webhook_providers import _FakeRequest

    gw = bridge(sdk)
    gw.cfg.require_signature = False
    gw.cfg.external_events_enabled = True
    gw._on_external_event = AsyncMock()
    request = _FakeRequest(json.dumps({"event_type": "whatsapp.delivered", "data": {"message": message()}}).encode())
    response = asyncio.run(gw._handle_webhook(request))
    assert response.status == 401
    gw._on_external_event.assert_not_awaited()


def test_saved_whatsapp_answer_recovers_without_replaying_model(sdk):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002", "message_id": MESSAGE}
        store = gw._channel_store("whatsapp")
        store.admit("contact", "Question", meta)
        store.mark(meta, "reply_pending", reply="Saved answer", chat_id="contact")
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        assert len(sdk.calls) == 1 and json.loads(sdk.calls[-1].content)["text"] == "Saved answer"
        gw.sessions.get.assert_not_called()
        assert store.summary()["done"] == 1
    asyncio.run(run())


def test_concurrent_cross_channel_bursts_keep_original_reply_ownership(tmp_path, monkeypatch):
    from tests.test_imessage_sessions import make_session, source, finish_burst
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, sent, _ = make_session()
        await session.handle_inbound("WhatsApp first", "whatsapp", source(1))
        await session.handle_inbound("iMessage middle", "imessage", source(2))
        await session.handle_inbound("WhatsApp last", "whatsapp", source(3))
        await finish_burst(session)
        assert [item[1] for item in sent] == ["whatsapp", "imessage", "whatsapp"]
        assert len(session._client.calls) == 3
        assert "WhatsApp first" in session._client.calls[0] and "iMessage middle" not in session._client.calls[0]
        assert "iMessage source context" not in session._client.calls[0]
        assert [item[2]["message_id"] for item in sent] == ["message-1", "message-2", "message-3"]
    asyncio.run(run())


def test_pending_whatsapp_permission_cannot_be_answered_over_another_channel(tmp_path, monkeypatch):
    from tests.test_imessage_sessions import make_session, source
    from inkbox_codex.escalation import PendingInteraction
    from inkbox_codex.sessions import _Turn
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, _, _ = make_session(enabled=False)
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction("permission", "Approve?", future)
        session._current_turn = _Turn(text="original", reply_mode="whatsapp", reply_meta=source(1, channel="whatsapp"))
        session._worker = asyncio.get_running_loop().create_future()
        try:
            await session.handle_inbound("yes", "sms", source(2))
            assert not future.done()
            assert session._reply_route(session._current_turn)[0] == "whatsapp"
            await session.handle_inbound("yes", "whatsapp", source(3))
            assert future.done() and future.result() == "yes"
        finally:
            session._worker.cancel()
            if not future.done():
                future.cancel()
    asyncio.run(run())


def test_early_delivery_callback_survives_accepted_response_correlation(sdk):
    gw = bridge(sdk)
    record_status(gw, {"data": {"message": message(id=OUTBOUND, direction="outbound", status="delivered", delivery_state="delivered", updated_at=NOW)}})
    store = gw._channel_store("whatsapp")
    store.record_outbound({"id": OUTBOUND, "status": "queued", "delivery_state": "pending"}, {"channel": "whatsapp"}, "contact")
    route = store.lookup_outbound(OUTBOUND)
    assert route["status"] == "delivered" and route["delivery_state"] == "delivered"
    assert route["chat_id"] == "contact" and "unknown_route" not in route

"""Dedicated WhatsApp routing and sends use the real candidate SDK offline."""

import asyncio
from datetime import datetime, timezone
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from inkbox import Inkbox, _config as sdk_config

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.imessage import IMessageState, clear_turn_context, read_turn_context
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
    state = NS(requests=[], status=201, calls=[], identity_enabled=False, limits_status=200,
               failure_detail={"error": "whatsapp_send_pending"},
               conversation_status=200, on_conversation_read=lambda: None)
    state.conversation = {"id": CONVERSATION, "agent_identity_id": IDENTITY,
        "local_number": "+15550000001", "remote_number": None, "is_group": True,
        "participants": ["+15550000002", "+15550000003"], "created_at": NOW, "updated_at": NOW}

    def handle(request):
        assert request.url.host == "example.com"
        state.requests.append(request)
        path = request.url.path
        if path == "/api/v1/identities/agent":
            return httpx.Response(200, json={"id": IDENTITY, "organization_id": "org_example", "agent_handle": "agent",
                "whatsapp_enabled": state.identity_enabled, "created_at": NOW, "updated_at": NOW})
        if path == "/api/v1/whatsapp/limits":
            return httpx.Response(state.limits_status, json={"agent_identity_id": IDENTITY, "limits": {"synthetic": 123}, "usage": {"whatsapp.texts_per_agent_identity_per_24h": 7}})
        if path == "/api/v1/identities/agent/channel-status":
            return httpx.Response(200, json={"whatsapp": {"enabled": True, "number": "+15550000001", "status": "ready"}})
        if path == f"/api/v1/whatsapp/conversations/{CONVERSATION}":
            state.on_conversation_read()
            return httpx.Response(state.conversation_status, json=state.conversation)
        if path == "/api/v1/whatsapp/conversations":
            groups = request.url.params.get("include_groups") == "true"
            return httpx.Response(200, json=[{
                "id": CONVERSATION, "agent_identity_id": IDENTITY,
                "local_number": "+15550000001", "remote_number": None,
                "participants": [f"+15550000{n:03d}" for n in range(2, 11)],
                "is_group": True, "created_at": NOW, "updated_at": NOW,
                "latest_has_media": False, "unread_count": 1, "total_count": 1,
            }] if groups else [])
        if request.method == "POST" and path == "/api/v1/whatsapp/messages":
            state.calls.append(request)
            if state.status != 201:
                return httpx.Response(state.status, json={"detail": state.failure_detail})
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


def test_all_fifteen_tools_exist_without_a_cached_enablement_flag(sdk):
    assert len([tool for tool in mcp_tool_list() if "whatsapp" in tool["name"]]) == 15
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
        for changes in ({"agent_identity_id": str(UUID(int=99))}, {"is_group": True, "participants": ["+15550000003"]}, {"sender_number": "+15550000001"}):
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
        record_status(gw, {"event_type": "whatsapp.delivered", "data": {"message": message(id=OUTBOUND, direction="outbound", status="delivered", delivery_state="delivered", updated_at="2026-01-01T00:00:02+00:00")}})
        record_status(gw, {"event_type": "whatsapp.delivered", "data": {"message": message(id=OUTBOUND, direction="outbound", status="sent", delivery_state="pending", updated_at=NOW)}})
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
    record_status(gw, {"event_type": "whatsapp.delivered", "data": {"message": message(id=OUTBOUND, direction="outbound", status="delivered", delivery_state="delivered", updated_at=NOW)}})
    store = gw._channel_store("whatsapp")
    store.record_outbound({"id": OUTBOUND, "status": "queued", "delivery_state": "pending"}, {"channel": "whatsapp"}, "contact")
    route = store.lookup_outbound(OUTBOUND)
    assert route["status"] == "delivered" and route["delivery_state"] == "delivered"
    assert route["chat_id"] == "contact" and "unknown_route" not in route


def test_status_includes_independent_limits_without_breaking_older_api(sdk):
    status = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_get_whatsapp_status", {})))
    assert set(status["limits"]) == {"agent_identity_id", "limits", "usage"}
    assert status["limits"]["limits"] == {"synthetic": 123}
    assert status["limits"]["usage"] == {"whatsapp.texts_per_agent_identity_per_24h": 7}
    sdk.limits_status = 404
    status = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_get_whatsapp_status", {})))
    assert status["enabled"] and "limits" not in status


@pytest.mark.parametrize("selector", [{"conversation_id": CONVERSATION}, {"to": "+15550000002"}, {"to": " +1 (555) 000-0002 "}])
def test_uncertain_source_tool_send_fences_final_and_survives_recovery(sdk, monkeypatch, selector):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
                "message_id": MESSAGE, "imessage_event_ids": ["original-event"]}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = 409
        failed = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            **selector, "text": "Hello", "media_id": MEDIA,
        })
        assert failed["isError"]
        key = json.loads(failed["content"][0]["text"])["idempotency_key"]
        blocked = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "conversation_id": CONVERSATION, "text": "Changed answer", "idempotency_key": "new-unattempted-key",
        })
        details = json.loads(blocked["content"][0]["text"])
        assert details["idempotency_key"] == key and details["media_id"] == MEDIA
        assert len(sdk.calls) == 1
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is True
        await gw._imessage_turn_state("contact", "done", meta)
        store = gw._channel_store("whatsapp")
        assert store.summary()["uncertain"] == 1
        with store._db() as db:
            saved = json.loads(db.execute("SELECT meta FROM receipts").fetchone()["meta"])
        assert saved["whatsapp_tool_sends"][key]["media_id"] == MEDIA
        gw.sessions = NS(get=Mock())
        sdk.status = 201
        await gw._recover_imessage_inputs()
        gw.sessions.get.assert_not_called()
        assert len(sdk.calls) == 1

    asyncio.run(run())


@pytest.mark.parametrize("selector", [{"conversation_id": CONVERSATION}, {"to": "+15550000002"}, {"to": " +1 (555) 000-0002 "}])
@pytest.mark.parametrize("status,code", [(429, "whatsapp_daily_limit_exceeded"), (422, "whatsapp_reply_required_for_email_addresses")])
def test_known_rejected_source_send_can_retry_the_original_key(sdk, monkeypatch, selector, status, code):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
                "message_id": MESSAGE}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {**selector, "text": "Hello", "idempotency_key": "original-key"}
        sdk.failure_detail = {"error": code}
        sdk.status = status
        failure = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert failure["isError"]
        assert json.loads(failure["content"][0]["text"])["error_code"] == code
        rejected_attempts = len(sdk.calls)
        sdk.status = 201
        result(await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args))
        assert len(sdk.calls) == rejected_attempts + 1
        assert {call.headers["Idempotency-Key"] for call in sdk.calls} == {"original-key"}
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is True
        assert gw._channel_store("whatsapp").summary()["done"] == 1

    asyncio.run(run())


@pytest.mark.parametrize("status,code", [(403, "whatsapp_unrecognized_policy"), (422, "whatsapp_unrecognized_policy"), (429, "whatsapp_unrecognized_policy"), (402, "whatsapp_cycle_limit_exceeded")])
def test_unrecognized_send_rejection_keeps_the_original_attempt_fenced(sdk, monkeypatch, status, code):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002", "message_id": MESSAGE}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = status
        sdk.failure_detail = {"error": code}
        failed = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"conversation_id": CONVERSATION, "text": "Hello"})
        assert failed["isError"]
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is True
        assert gw._channel_store("whatsapp").summary()["uncertain"] == 1

    asyncio.run(run())


@pytest.mark.parametrize("uncertain", [False, True])
def test_explicit_other_recipient_keeps_the_ordinary_origin_reply(sdk, monkeypatch, uncertain):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002", "message_id": MESSAGE}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = 409 if uncertain else 201
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": "+15550000003", "text": "Hello"})
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is False
        sdk.status = 201
        await gw.send_to_contact("contact", "Hello", "whatsapp", {**meta, "imessage_final_reply": True})
        assert len(sdk.calls) == 2
        assert json.loads(sdk.calls[0].content)["to"] == "+15550000003"
        assert json.loads(sdk.calls[1].content)["conversation_id"] == CONVERSATION

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["send", "upload"])
def test_source_revoked_during_identity_resolution_cannot_send_or_upload(sdk, monkeypatch, tmp_path, operation):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
                "message_id": MESSAGE}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        original = sdk.client.get_identity

        def changed(handle):
            clear_turn_context("contact")
            return original(handle)

        monkeypatch.setattr(sdk.client, "get_identity", changed)
        file = tmp_path / "attachment.txt"
        file.write_text("attachment")
        name, args = (("inkbox_send_whatsapp", {"to": "+15550000003", "text": "Explicit outreach"})
                      if operation == "send" else ("inkbox_upload_whatsapp_media", {"media_path": str(file)}))
        failed = await call_inkbox_tool(sdk.client, "agent", name, args)
        assert failed["isError"]
        assert not [request for request in sdk.requests if request.method == "POST"]

    asyncio.run(run())


@pytest.mark.parametrize("observed", ["2026-01-01T00:00:01Z", "2026-01-01T01:00:01+01:00", None, "invalid"])
def test_old_or_invalid_status_cannot_regress_an_accepted_sdk_observation(sdk, observed):
    gw = bridge(sdk)
    store = gw._channel_store("whatsapp")
    store.record_outbound(NS(id=OUTBOUND, status="delivered", delivery_state="delivered",
                            updated_at=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)),
                          {"channel": "whatsapp"}, "contact")
    record_status(gw, {"event_type": "whatsapp.sent", "data": {"message": message(
        id=OUTBOUND, direction="outbound", status="sent", delivery_state="pending", updated_at=observed)}})
    saved = store.lookup_outbound(OUTBOUND)
    assert saved["status"] == "delivered" and saved["delivery_state"] == "delivered"


def test_whatsapp_stop_interrupts_own_running_work_without_imessage_flag(tmp_path, monkeypatch):
    from tests.test_imessage_sessions import make_session, source
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, sent, _ = make_session(enabled=False)
        session._client.gate = asyncio.Event()
        await session.handle_inbound("Long running work", "whatsapp", source(1))
        session._flush_imessage_burst()
        await session._client.started.wait()
        await session.handle_inbound("/stop", "whatsapp", source(2, raw_text="/stop"))
        await session._worker
        assert session._client.interrupts == 1
        assert [text for text, _, _ in sent] == ["Stopped."]

    asyncio.run(run())


def test_whatsapp_stop_drops_own_queue_and_burst_but_preserves_other_work(tmp_path, monkeypatch):
    from tests.test_imessage_sessions import make_session, source
    from inkbox_codex.sessions import _Turn
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, sent, states = make_session(enabled=False)
        other = _Turn(text="Other work", reply_mode="sms", reply_meta=source(9))
        session._current_turn = other
        session._turn_active = True
        session._worker = asyncio.create_task(asyncio.Event().wait())
        try:
            await session.handle_inbound("First", "whatsapp", source(1))
            session._flush_imessage_burst()
            await session.handle_inbound("Second", "whatsapp", source(2))
            await session.handle_inbound("/stop", "whatsapp", source(3, raw_text="/stop"))
            assert session._imessage_burst is None and session._queue.empty()
            assert session._client.interrupts == 0 and session._current_turn is other
            assert sent[-1][0] == "Stopped."
            assert {meta["message_id"] for state, meta, _ in states if state == "cancelled"} == {"message-1", "message-2"}
        finally:
            session._worker.cancel()
            await asyncio.gather(session._worker, return_exceptions=True)

    asyncio.run(run())


def test_whatsapp_stop_cancels_its_parked_question(tmp_path, monkeypatch):
    from tests.test_imessage_sessions import make_session, source
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, sent, _ = make_session(enabled=False)
        session.mode, session.reply_meta = "whatsapp", source(1, channel="whatsapp")
        question = asyncio.create_task(session._escalate("resume", "Choose a conversation"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert session.pending is not None
        await session.handle_inbound("/stop", "whatsapp", source(2, raw_text="/stop"))
        assert await question is None
        assert sent[-1][0] == "Stopped."

    asyncio.run(run())


def test_whatsapp_stop_retires_tool_authority_before_waiting_for_interrupt(sdk, monkeypatch):
    from tests.test_imessage_sessions import make_session, source

    async def run():
        gw = bridge(sdk)
        session, _, _ = make_session(enabled=False, hook=gw._imessage_turn_state)
        client = session._client
        client.gate = asyncio.Event()
        await session.handle_inbound("Long running work", "whatsapp", source(1))
        session._flush_imessage_burst()
        await client.started.wait()
        assert read_turn_context(session.chat_id) is not None

        async def interrupted():
            assert read_turn_context(session.chat_id) is None
            client.gate.set()

        monkeypatch.setattr(client, "interrupt", interrupted)
        await session.handle_inbound("/stop", "whatsapp", source(2, raw_text="/stop"))
        await session._worker

    asyncio.run(run())


@pytest.mark.parametrize("command", ["/stop", "/clear", "/resume"])
def test_foreign_channel_cannot_control_active_whatsapp_work(tmp_path, monkeypatch, command):
    from tests.test_imessage_sessions import make_session, source
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        session, sent, _ = make_session(enabled=False)
        client = session._client
        client.gate = asyncio.Event()
        await session.handle_inbound("Long running work", "whatsapp", source(1))
        session._flush_imessage_burst()
        await client.started.wait()
        await session.handle_inbound(command, "sms", source(2, raw_text=command))
        assert client.interrupts == 0 and not session._worker.done()
        assert not sent
        client.gate.set()
        await session._worker

    asyncio.run(run())


@pytest.mark.parametrize("status", [201, 409, 429])
def test_explicit_quote_fence_survives_different_final_text_rejection_and_lookup(sdk, monkeypatch, status):
    async def run():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
                "message_id": MESSAGE, "imessage_event_ids": ["quote-source"]}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = status
        if status == 429:
            sdk.failure_detail = {"error": "whatsapp_daily_limit_exceeded"}
        args = {"conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE, "text": "Answer\n", "idempotency_key": "quote-attempt"}
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        attempted_calls = len(sdk.calls)
        assert attempted_calls >= 1 and all(request.headers["Idempotency-Key"] == "quote-attempt" for request in sdk.calls)
        assert read_turn_context("contact")["whatsapp_explicit_quote_attempted"] is True
        await call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {"idempotency_key": "quote-attempt"})
        assert await gw._imessage_turn_state("contact", "result", meta, "Different final answer") is True
        assert meta["whatsapp_explicit_quote_attempted"] is True
        with pytest.raises(ValueError, match="explicit quoted reply"):
            await gw.send_to_contact("contact", "Different final answer", "whatsapp", {**meta, "imessage_final_reply": True})
        assert len(sdk.calls) == attempted_calls
    asyncio.run(run())

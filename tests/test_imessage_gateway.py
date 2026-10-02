"""Source-bound sends, replay boundaries, and Companion integration (offline)."""
import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.imessage import source_metadata, read_turn_context, record_tool_send
from inkbox_codex.companion import Event
from tests.test_companion import fixture, harness, live


class Identity:
    def __init__(self):
        self.calls = []
        self.preflights = []
        self.error = None

    def get_imessage(self, message_id):
        return NS(id=message_id, conversation_id="conversation")

    def get_imessage_thread(self, message_id, *, limit=50, cursor=None):
        self.preflights.append((message_id, limit))
        return NS(conversation_id="conversation", messages=[], next_cursor=None)

    def get_imessage_conversation_thread(self, conversation_id, thread_id, *, limit=50, cursor=None):
        return NS(messages=[], next_cursor=None)

    def send_imessage(self, *, reply_to_message_id=None, plain_reply_fallback=True,
                      idempotency_key=None, **kwargs):
        self.calls.append({**kwargs, "reply_to_message_id": reply_to_message_id,
                           "plain_reply_fallback": plain_reply_fallback,
                           "idempotency_key": idempotency_key})
        if self.error:
            raise self.error
        return NS(id="outbound", status="queued", reply_to_message_id=reply_to_message_id,
                  thread_id=None, thread_root_message_id=None)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


def bridge(enabled=True):
    gw = InkboxGateway(BridgeConfig(identity="example-agent", imessage_threaded_replies=enabled,
                                   allow_all_users=True))
    identity = Identity()
    gw._identity = identity
    gw._inkbox = NS(get_identity=lambda _: identity)
    return gw, identity


def metadata(source="source", *, parent="parent"):
    return {"sender": "+15550100100", "conversation_id": "conversation",
            **source_metadata({"id": source, "content": "request", "reply_to_message_id": parent}, "event-" + source)}


def test_inbound_preserves_source_authority_and_media_boundary():
    async def run():
        gw, _ = bridge()
        inbound = AsyncMock()
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=inbound)))
        gw._with_media = AsyncMock(return_value="request with media")
        gw._lookup_imessage_conversation_summary = AsyncMock(return_value={})
        gw._resolve_contact_full = AsyncMock(return_value=None)
        await gw._on_imessage_received_once({"id": "event", "data": {"message": {
            "id": "source", "direction": "inbound", "remote_number": "+15550100100",
            "conversation_id": "conversation", "content": "request", "sender_access": "direct",
            "reply_to_message_id": None, "thread_root_message_id": "visible-root",
            "thread_id": "opaque-thread", "media": [{"url": "https://example.com/image.png"}],
        }}})
        meta = inbound.await_args.args[2]
        assert meta["imessage_event_id"] == "event"
        assert meta["imessage_reply_target"] == "source"
        assert meta["reply_to_message_id"] is None
        assert meta["thread_id"] == "opaque-thread"
        assert meta["sender_access"] == "direct"
        assert meta["attachments"] == [{"url": "https://example.com/image.png"}]
    asyncio.run(run())


def test_overlapping_requests_checkpoint_source_targets_before_ack_and_send():
    from tests.test_imessage_sessions import make_session

    async def run():
        gw, identity = bridge()
        session, _, _ = make_session(hook=gw._imessage_turn_state, send=gw.send_to_contact)
        client = session._client
        client.gate = asyncio.Event()
        await session.handle_inbound("First task", "imessage", metadata("first", parent=None))
        session._flush_imessage_burst()
        await client.started.wait()
        await session.handle_inbound("Second task", "imessage", metadata("second", parent=None))
        session._flush_imessage_burst()
        await session.handle_inbound("Third task", "imessage", metadata("third", parent=None))
        session._flush_imessage_burst()
        store = gw._threaded_imessage_state()
        assert store.summary()["pending"] == 2
        # Inspect the durable state before releasing the first model, without
        # invoking startup recovery against a still-running worker.
        with store._db() as db:
            rows = db.execute("SELECT meta,state FROM receipts ORDER BY rowid").fetchall()
        assert [row["state"] for row in rows] == ["running", "pending", "pending"]
        assert [json.loads(row["meta"])["imessage_reply_target"] for row in rows] == ["first", "second", "third"]
        client.gate.set()
        await session._worker
        assert [call["reply_to_message_id"] for call in identity.calls] == ["first", "second", "third"]
        assert all(call["plain_reply_fallback"] is True for call in identity.calls)
        assert store.summary()["done"] == 3
    asyncio.run(run())


def test_gateway_keeps_actual_queued_metadata_and_stable_idempotency():
    async def run():
        gw, identity = bridge()
        meta = metadata()
        store = gw._threaded_imessage_state()
        store.admit("contact", "request", meta)
        store.mark(meta, "reply_pending", reply="answer")
        route = {**meta, "imessage_final_reply": True}
        await gw.send_to_contact("contact", "answer", "imessage", route)
        await gw.send_to_contact("contact", "answer", "imessage", deepcopy(route))
        assert identity.calls[0] == identity.calls[1]
        assert identity.calls[0]["reply_to_message_id"] == "source"
        assert identity.calls[0]["plain_reply_fallback"] is True
        assert identity.preflights == [("source", 1), ("source", 1)]
        saved = store.lookup_outbound("outbound")
        assert saved["chat_id"] == "contact" and saved["status"] == "queued"
        assert saved["thread_id"] is None and saved["thread_root_message_id"] is None
        assert store.summary()["done"] == 1
    asyncio.run(run())


def test_off_uses_original_sdk_request_shape():
    async def run():
        gw, identity = bridge(False)
        identity.send_imessage = Mock(return_value=NS(id="outbound"))
        await gw.send_to_contact("contact", "answer", "imessage", metadata())
        identity.send_imessage.assert_called_once_with(conversation_id="conversation", text="answer")
        assert gw._imessage_state is None
    asyncio.run(run())


def test_failed_target_send_never_retries_as_plain():
    async def run():
        gw, identity = bridge()
        identity.error = TimeoutError("ambiguous send")
        meta = metadata()
        store = gw._threaded_imessage_state()
        store.admit("contact", "request", meta)
        store.mark(meta, "reply_pending", reply="answer")
        with pytest.raises(TimeoutError):
            await gw.send_to_contact("contact", "answer", "imessage", {**meta, "imessage_final_reply": True})
        assert len(identity.calls) == 1
        assert identity.calls[0]["reply_to_message_id"] == "source"
        assert store.replay_pending() == []
        assert store.summary()["uncertain"] == 1
    asyncio.run(run())


def test_interim_approval_send_does_not_complete_model_receipt():
    async def run():
        gw, _ = bridge()
        meta = metadata()
        await gw._imessage_turn_state("contact", "admitted", meta, "request")
        await gw._imessage_turn_state("contact", "started", meta)
        await gw.send_to_contact("contact", "May I run this command?", "imessage", meta)
        assert gw._threaded_imessage_state().summary()["running"] == 1
    asyncio.run(run())


def test_preflight_error_retains_saved_answer_without_replaying_model():
    async def run():
        gw, identity = bridge()
        meta = metadata()
        await gw._imessage_turn_state("contact", "admitted", meta, "request")
        await gw._imessage_turn_state("contact", "started", meta)
        await gw._imessage_turn_state("contact", "result", meta, "saved answer")
        original = identity.get_imessage_thread
        identity.get_imessage_thread = Mock(side_effect=TimeoutError("read timeout"))
        # Preserve the real capability contract while the read transport fails.
        identity.get_imessage_thread.__signature__ = __import__("inspect").signature(original)
        with pytest.raises(TimeoutError):
            await gw.send_to_contact("contact", "saved answer", "imessage", {**meta, "imessage_final_reply": True})
        await gw._imessage_turn_state("contact", "uncertain", meta)
        assert gw._threaded_imessage_state().summary()["reply_pending"] == 1
        assert not identity.calls
        identity.get_imessage_thread = original
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        gw.sessions.get.assert_not_called()
        assert [call["text"] for call in identity.calls] == ["saved answer"]
    asyncio.run(run())


def test_consumed_control_is_not_replayed_and_does_not_replace_active_context():
    async def run():
        gw, _ = bridge()
        active = metadata("active")
        await gw._imessage_turn_state("contact", "started", active)
        context = read_turn_context("contact")
        control = metadata("control")
        await gw._imessage_turn_state("contact", "admitted", control, "/clear")
        await gw._imessage_turn_state("contact", "consumed", control)
        assert read_turn_context("contact")["nonce"] == context["nonce"]
        assert gw._threaded_imessage_state().replay_pending() == []
    asyncio.run(run())


def test_active_tool_output_suppresses_only_exact_answer_and_controls_keep_context():
    async def run():
        gw, _ = bridge()
        meta = metadata()
        await gw._imessage_turn_state("contact", "admitted", meta, "request")
        await gw._imessage_turn_state("contact", "started", meta)
        active = read_turn_context("contact")
        await gw._imessage_turn_state("contact", "done", metadata("control"))
        assert read_turn_context("contact")["nonce"] == active["nonce"]
        record_tool_send(active, NS(id="tool-output", status="queued"), "key", text="answer", target="source")
        assert await gw._imessage_turn_state("contact", "result", meta, "different answer") is False
        assert await gw._imessage_turn_state("contact", "result", meta, "answer") is True
        await gw._imessage_turn_state("contact", "done", meta)
        assert read_turn_context("contact") is None
    asyncio.run(run())


def test_restart_replays_only_unsubmitted_inputs_and_saved_unsent_answers():
    async def run():
        gw, identity = bridge()
        store = gw._threaded_imessage_state()
        for name in ("pending", "running", "reply_pending", "sending", "cancelled", "done"):
            meta = metadata(name, parent=None)
            store.admit("contact", "request-" + name, meta)
            if name != "pending":
                store.mark(meta, name, reply="saved answer" if name == "reply_pending" else None)
        received = []

        async def inbound(text, mode, meta):
            assert await gw._imessage_turn_state("contact", "admitted", meta, text) is True
            received.append((text, mode, meta))
        gw.sessions = NS(get=lambda _: NS(handle_inbound=inbound))
        await gw._recover_imessage_inputs()
        assert [item[0] for item in received] == ["request-pending"]
        assert [item["text"] for item in identity.calls] == ["saved answer"]
        assert store.summary()["uncertain"] == 2
        assert store.summary()["done"] == 2
    asyncio.run(run())


def test_late_failure_uses_original_tool_route_without_model_recovery():
    async def run():
        gw, _ = bridge()
        meta = metadata()
        await gw._imessage_turn_state("contact-original", "started", meta)
        record_tool_send(read_turn_context("contact-original"), NS(id="outbound", status="queued"),
                         "key", text="answer", target="source")
        await gw._imessage_turn_state("contact-original", "done", meta)
        notice = AsyncMock()
        gw.sessions = NS(get=Mock(return_value=NS(append_delivery_notice=notice)))
        gw._notify_delivery_failure = AsyncMock()
        result = await gw._on_imessage_delivery_failed({"data": {"message": {
            "id": "outbound", "conversation_id": "different-conversation",
            "remote_number": "+15550100999", "status": "failed",
        }}})
        assert json.loads(result.text)["retained"] == "failed-threaded-output"
        gw.sessions.get.assert_called_once_with("contact-original")
        notice.assert_awaited_once()
        gw._notify_delivery_failure.assert_not_awaited()
        assert gw._threaded_imessage_state().summary()["outbound_failed_count"] == 1
    asyncio.run(run())


def test_callback_before_send_response_never_wakes_legacy_retry():
    async def run():
        gw, _ = bridge()
        gw._notify_delivery_failure = AsyncMock()
        result = await gw._on_imessage_delivery_failed({"data": {"message": {"id": "early-output"}}})
        assert json.loads(result.text)["retained"] == "unmatched-imessage-failure"
        gw._notify_delivery_failure.assert_not_awaited()
        assert gw._threaded_imessage_state().summary()["outbound_failed_count"] == 1
    asyncio.run(run())


def test_companion_live_native_reply_targets_live_source_not_sponsor():
    original = fixture("imessage")
    r, _, session, _ = harness(original)
    session.cfg.imessage_threaded_replies = True
    incoming = live(original)
    incoming["data"]["message"].update(reply_to_message_id="parent", thread_id="opaque-native-thread")
    event = Event.parse(incoming)
    try:
        meta = r.meta(event, source_id=original["data"]["message"]["id"])
        assert meta["message_id"] == event.source_id
        assert meta["imessage_reply_target"] == event.source_id
        assert meta["thread_id"] == "opaque-native-thread"
        initial = r.meta(event, source_id=original["data"]["message"]["id"], initialization=True)
        assert initial["message_id"] == original["data"]["message"]["id"]
        assert initial["imessage_reply_target"] is None
        assert initial["thread_id"] is None
    finally:
        r.inbox.close()

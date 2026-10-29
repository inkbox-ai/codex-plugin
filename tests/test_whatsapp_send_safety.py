"""Offline regressions for validated intents and durable WhatsApp delivery state."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import threading
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from inkbox_codex.imessage import IMessageState, clear_turn_context, read_turn_context
from inkbox_codex.tools import call_inkbox_tool
from inkbox_codex.whatsapp import record_status, run_tool, WhatsAppReplyRejected
from tests.test_whatsapp import sdk, bridge, message, result, CONVERSATION, MESSAGE, IDENTITY, MEDIA, OUTBOUND, NOW


def source(**extra):
    return {"channel": "whatsapp", "conversation_id": CONVERSATION, "sender": "+15550000002",
            "message_id": MESSAGE, "imessage_event_ids": ["source-one"], **extra}


@pytest.mark.parametrize("invalid", [
    {"text": ""}, {"text": " \n\t"}, {"text": None}, {"text": 42}, {"text": "x" * 4001},
    {"media_id": "not-a-uuid"}, {"media_id": 123}, {"idempotency_key": ""},
    {"idempotency_key": " "}, {"idempotency_key": "x" * 256}, {"idempotency_key": "bad\nkey"},
    {"idempotency_key": "non-ascii-😀"}, {"idempotency_key": 7}, {"conversation_id": "bad"},
    {"to": []}, {"group_name": "bad"}, {"reply_to_message_id": "bad"},
    {"conversation_id": None, "to": "+1１２３４５６７８９０"},
])
def test_invalid_send_never_checkpoints_or_calls_http_and_correction_is_usable(sdk, monkeypatch, invalid):
    async def scenario():
        gw = bridge(sdk)
        meta = source()
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        before = len(sdk.requests)
        bad = {"conversation_id": CONVERSATION, "text": "Answer", **invalid}
        with pytest.raises(ValueError):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", bad)
        assert len(sdk.requests) == before
        assert not read_turn_context("contact").get("whatsapp_tool_sends")
        assert not read_turn_context("contact").get("whatsapp_explicit_quote_attempted")
        sent = run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "conversation_id": CONVERSATION, "text": "Corrected", "idempotency_key": "corrected"})
        assert sent["idempotency_key"] == "corrected" and len(sdk.calls) == 1
    asyncio.run(scenario())


def test_keys_and_explicit_quote_fence_are_persisted_before_sdk_mutation(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = source()
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        identity = gw._identity
        original = identity.send_whatsapp
        observed = []
        def send(**kwargs):
            context = read_turn_context("contact")
            assert context["whatsapp_tool_sends"]["quoted"]["state"] == "sending"
            assert context["whatsapp_explicit_quote_attempted"] is True
            # A new connection observes the durable pre-request checkpoint.
            store = IMessageState(gw.cfg, channel="whatsapp")
            with store._db() as db:
                row = db.execute("SELECT meta FROM receipts WHERE event_id='source-one'").fetchone()
            persisted = json.loads(row["meta"])
            assert persisted["whatsapp_explicit_quote_attempted"] is True
            assert persisted["whatsapp_tool_sends"]["quoted"]["state"] == "sending"
            observed.append(True)
            return original(**kwargs)
        monkeypatch.setattr(identity, "send_whatsapp", send)
        monkeypatch.setattr(sdk.client, "get_identity", lambda _: identity)
        run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE,
            "text": "Answer", "idempotency_key": "quoted"})
        assert observed == [True]
    asyncio.run(scenario())


def test_nonfinal_prompts_are_distinct_but_final_and_legacy_keys_stay_exact(sdk):
    async def scenario():
        gw = bridge(sdk)
        meta = source()
        for _ in range(2):
            await gw.send_to_contact("contact", "Approve?", "whatsapp", meta)
        first, second = [req.headers["Idempotency-Key"] for req in sdk.calls]
        assert first != second
        legacy = "codex-wa-" + hashlib.sha256(json.dumps([
            gw.cfg.base_url, IDENTITY, CONVERSATION, ["source-one"], "Answer"], sort_keys=True).encode()).hexdigest()
        for _ in range(2):
            await gw.send_to_contact("contact", "Answer", "whatsapp", {**meta, "final_reply": True})
        assert [req.headers["Idempotency-Key"] for req in sdk.calls[2:]] == [legacy, legacy]
        await gw.send_to_contact("contact", "Answer", "whatsapp", {
            **meta, "final_reply": True, "whatsapp_idempotency_key": "saved-legacy-key"})
        assert sdk.calls[-1].headers["Idempotency-Key"] == "saved-legacy-key"
    asyncio.run(scenario())


@pytest.mark.parametrize("state", ["pending", "reply_pending"])
@pytest.mark.parametrize("current_change", [False, True])
def test_recovery_requires_all_saved_and_current_group_participants(sdk, state, current_change):
    async def scenario():
        gw = bridge(sdk)
        meta = source(conversation_kind="group", participants=["+15550000002", "+15550000003"])
        store = gw._channel_store("whatsapp")
        store.admit("group", "Question", meta)
        store.mark(meta, state, reply="Saved answer" if state == "reply_pending" else None)
        gw.cfg.allow_all_users = False
        gw.cfg.allowed_users = ["+15550000002", "+15550000003"] if current_change else ["+15550000002"]
        current = Mock(return_value=NS(id=CONVERSATION, agent_identity_id=IDENTITY, is_group=True,
            participants=["+15550000002", "+15550000003", "+15550000004"]))
        gw._identity.get_whatsapp_conversation = current
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        assert store.summary()["cancelled"] == 1 and not sdk.calls
        gw.sessions.get.assert_not_called()
        assert current.call_count == int(current_change)
    asyncio.run(scenario())


@pytest.mark.parametrize("final", [False, True])
def test_group_outputs_and_approval_prompts_check_current_members(sdk, final):
    async def scenario():
        gw = bridge(sdk)
        meta = source(conversation_kind="group", participants=["+15550000002", "+15550000003"], final_reply=final)
        gw.cfg.allow_all_users = False
        gw.cfg.allowed_users = list(meta["participants"])
        gw._identity.get_whatsapp_conversation = Mock(return_value=NS(
            id=CONVERSATION, agent_identity_id=IDENTITY, is_group=True,
            participants=["+15550000002", "+15550000003", "+15550000004"]))
        with pytest.raises(PermissionError):
            await gw.send_to_contact("group", "Approve?", "whatsapp", meta)
        assert not sdk.calls
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", ["conversation", "recipients"])
@pytest.mark.parametrize("attachment", [False, True])
@pytest.mark.parametrize("supplied_key", [None, "legacy-plain-key"])
@pytest.mark.parametrize("change", ["unchanged", "joined", "read-failed", "cancelled", "retired", "uncertain"])
def test_source_group_tool_rechecks_members_before_checkpoint(sdk, monkeypatch, selector, attachment, supplied_key, change):
    from inkbox_codex import turn_authority as authority
    from tests.test_whatsapp_autoquote import admitted_source

    async def scenario():
        gw = bridge(sdk)
        members = ["+15550000002", "+15550000003"]
        gw.cfg.allow_all_users = False
        gw.cfg.allowed_users = members
        monkeypatch.setenv("INKBOX_ALLOW_ALL_USERS", "false")
        monkeypatch.setenv("INKBOX_ALLOWED_USERS", ",".join(members))
        meta = await admitted_source(gw, is_group=True, participants=list(members))
        await gw._imessage_turn_state("group", "admitted", meta, "Question")
        await gw._imessage_turn_state("group", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "group")
        before = read_turn_context("group")
        if change == "retired":
            host = authority.new_host("group", gw.cfg)
            monkeypatch.setenv(authority.HOST_ENV, host)
            token = authority.begin("group", gw.cfg, host, "whatsapp", before["nonce"])
        store = gw._channel_store("whatsapp")

        def persisted_meta():
            with store._db() as db:
                return json.loads(db.execute("SELECT meta FROM receipts").fetchone()["meta"])

        durable_before = persisted_meta()
        reads = []

        def reading():
            assert read_turn_context("group") == before
            assert persisted_meta() == durable_before
            assert not sdk.calls
            reads.append(True)
            if change == "cancelled":
                clear_turn_context("group")
            elif change == "retired":
                authority.retire(token)

        sdk.on_conversation_read = reading
        # Membership changes only after the original A/B request is admitted.
        if change == "joined":
            sdk.conversation["participants"].append("+15550000004")
        elif change == "read-failed":
            sdk.conversation_status = 503
        elif change == "uncertain":
            sdk.status = 409
        args = ({"conversation_id": CONVERSATION} if selector == "conversation"
                else {"to": list(reversed(members))})
        args.update({"media_id": MEDIA} if attachment else {"text": "Answer"})
        if supplied_key:
            args["idempotency_key"] = supplied_key
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        if change == "unchanged":
            sent = result(value)
            assert len(sdk.calls) == 1
            assert sdk.calls[0].headers["Idempotency-Key"] == sent["idempotency_key"]
            payload = json.loads(sdk.calls[0].content)
            assert payload.get("media_ids") == ([MEDIA] if attachment else None)
            assert payload.get("reply_to_message_id") == (MESSAGE if selector == "conversation" and not supplied_key else None)
            if supplied_key:
                assert sent["idempotency_key"] == supplied_key
            context = read_turn_context("group")
            assert context["whatsapp_tool_sends"][sent["idempotency_key"]]["state"] == "accepted"
            assert await gw._imessage_turn_state("group", "result", meta, "Different automatic answer") is True
            assert store.summary()["done"] == 1 and len(sdk.calls) == 1
        elif change == "uncertain":
            assert value.get("isError"), value
            error = json.loads(value["content"][0]["text"])
            key = error["idempotency_key"]
            assert len(sdk.calls) == 1 and sdk.calls[0].headers["Idempotency-Key"] == key
            assert error["media_id"] == (MEDIA if attachment else None)
            assert read_turn_context("group")["whatsapp_tool_sends"][key]["state"] == "sending"
            sdk.on_conversation_read = lambda: None
            retry = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                **args, "idempotency_key": "replacement-key"})
            assert retry.get("isError"), retry
            assert json.loads(retry["content"][0]["text"])["idempotency_key"] == key
            found = result(await call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {
                "idempotency_key": key}))
            assert found["message_id"] == OUTBOUND
            assert sdk.requests[-1].headers["Idempotency-Key"] == key
            assert await gw._imessage_turn_state("group", "result", meta, "Different automatic answer") is True
            assert store.summary()["uncertain"] == 1 and len(sdk.calls) == 1
        else:
            assert value.get("isError"), value
            assert not sdk.calls
            assert persisted_meta() == durable_before
            assert read_turn_context("group") == (None if change == "cancelled" else before)
            error = json.loads(value["content"][0]["text"])
            if change == "read-failed":
                sdk.conversation_status = 200
                retry_args = dict(args)
                if error.get("idempotency_key"):
                    retry_args["idempotency_key"] = error["idempotency_key"]
                sent = result(await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry_args))
                assert len(sdk.calls) == 1
                payload = json.loads(sdk.calls[0].content)
                expected = {key: val for key, val in args.items() if key not in {"media_id", "idempotency_key"}}
                if selector == "recipients":
                    expected["to"] = sorted(members)
                if attachment:
                    expected["media_ids"] = [MEDIA]
                if selector == "conversation" and not supplied_key:
                    expected["reply_to_message_id"] = MESSAGE
                assert payload == expected
                assert sdk.calls[0].headers["Idempotency-Key"] == sent["idempotency_key"]
                if supplied_key:
                    assert sent["idempotency_key"] == supplied_key
                assert read_turn_context("group")["whatsapp_tool_sends"][sent["idempotency_key"]]["state"] == "accepted"
                assert await gw._imessage_turn_state("group", "result", meta, "Different automatic answer") is True
                assert store.summary()["done"] == 1 and len(sdk.calls) == 1
            if supplied_key:
                assert error["idempotency_key"] == supplied_key
            else:
                assert "idempotency_key" not in error
        assert reads == [True] * (2 if change == "read-failed" else 1)
    asyncio.run(scenario())


def test_overlong_saved_answer_is_definitively_failed_and_not_replayed(sdk):
    async def scenario():
        gw = bridge(sdk)
        meta = source()
        store = gw._channel_store("whatsapp")
        store.admit("contact", "Question", meta)
        store.mark(meta, "reply_pending", reply="x" * 4001)
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        assert store.summary()["failed"] == 1 and store.pending_replies() == []
        assert len(sdk.calls) == 1
        assert "answer was not sent" in json.loads(sdk.calls[0].content)["text"]
        assert len(json.loads(sdk.calls[0].content)["text"]) < 4000
        await gw._recover_imessage_inputs()
        assert len(sdk.calls) == 1
        gw.sessions.get.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["singleton", "group", "canonical"])
@pytest.mark.parametrize("uncertain", [False, True])
def test_equivalent_source_selectors_fence_a_different_automatic_answer(sdk, monkeypatch, kind, uncertain):
    async def scenario():
        gw = bridge(sdk)
        native_id = "ABCDEFAB-ABCD-ABCD-ABCD-ABCDEFABCDEF"
        meta = source(**({"conversation_kind": "group", "participants": ["+15550000002", "+15550000003"]} if kind == "group" else {}))
        if kind == "canonical":
            meta["conversation_id"] = native_id
        selector = {"conversation_id": str(UUID(native_id))} if kind == "canonical" else {
            "to": ["+15550000003", "+15550000002"] if kind == "group" else ["+15550000002"]}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = 409 if uncertain else 201
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**selector, "text": "Explicit answer"})
        assert await gw._imessage_turn_state("contact", "result", meta, "Different automatic answer") is True
        assert len(sdk.calls) == 1
        assert gw._channel_store("whatsapp").summary()["uncertain" if uncertain else "done"] == 1
    asyncio.run(scenario())


def test_status_read_and_outbound_write_serialize_without_losing_route(sdk, monkeypatch):
    gw = bridge(sdk)
    store = gw._channel_store("whatsapp")
    read, release, writing, committed = (threading.Event() for _ in range(4))
    original_db = store._db

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor
        def fetchone(self):
            value = self.cursor.fetchone()
            read.set()
            assert release.wait(2)
            return value

    class Connection:
        def __init__(self, db):
            self.db = db
        def execute(self, sql, *args):
            cursor = self.db.execute(sql, *args)
            if sql.startswith("SELECT route"):
                return Cursor(cursor)
            return cursor

    @contextmanager
    def paused_db():
        with original_db() as db:
            yield Connection(db)

    monkeypatch.setattr(store, "_db", paused_db)
    outbound_store = IMessageState(gw.cfg, channel="whatsapp")
    event = {"event_type": "whatsapp.delivered", "data": {"message": message(
        id=OUTBOUND, direction="outbound", status="delivered", delivery_state="delivered", updated_at=NOW)}}
    def correlate():
        writing.set()
        outbound_store.record_outbound(message(id=OUTBOUND, direction="outbound"), source(), "contact")
        committed.set()
    with ThreadPoolExecutor(max_workers=2) as pool:
        status = pool.submit(record_status, gw, event)
        assert read.wait(2)
        outbound = pool.submit(correlate)
        assert writing.wait(2)
        try:
            assert not committed.wait(0.1), "outbound writer must wait for the status transaction"
        finally:
            release.set()
        status.result(timeout=2)
        outbound.result(timeout=2)
    route = outbound_store.lookup_outbound(OUTBOUND)
    assert route["chat_id"] == "contact" and route["meta"]["conversation_id"] == CONVERSATION
    assert route["delivery_state"] == "delivered" and not route.get("unknown_route")

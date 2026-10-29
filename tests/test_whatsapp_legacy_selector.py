"""Payload-less checkpoints cannot supply missing destination evidence."""

import asyncio
import hashlib
import json

import pytest

from inkbox_codex import imessage, whatsapp
from inkbox_codex.imessage import checkpoint_whatsapp_tool_send, read_turn_context, imessage_turn_context_path
from inkbox_codex.tools import call_inkbox_tool
from tests.test_whatsapp import sdk, bridge, CONVERSATION, MESSAGE, MEDIA, OUTBOUND, result
from tests.test_whatsapp_autoquote import admitted_source

MEMBERS = ["+15550000002", "+15550000003"]


async def legacy(sdk, monkeypatch, group, state, selector, generated=False, quoted=False, media_id=MEDIA):
    gw = bridge(sdk)
    meta = await admitted_source(gw, is_group=group, participants=MEMBERS if group else MEMBERS[:1])
    await gw._imessage_turn_state("contact", "admitted", meta, "Question")
    await gw._imessage_turn_state("contact", "started", meta)
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
    context = read_turn_context("contact")
    args = {**selector, "text": "Answer", "media_id": media_id}
    if quoted:
        args["reply_to_message_id"] = MESSAGE
    key = "opaque-original-key"
    if generated:
        recipient = selector.get("to")
        material = [context["nonce"], selector.get("conversation_id"), "Answer", media_id]
        if quoted or selector.get("group_name") or isinstance(recipient, list):
            material += [recipient, MESSAGE if quoted else None, selector.get("group_name")]
        key = "codex-wa-" + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()
    # Historical writers persisted these fields only, even for accepted sends.
    checkpoint_whatsapp_tool_send(context, key, state=state, text="Answer", media_id=media_id,
                                  quote_target=MESSAGE if quoted else None)
    return gw, {**args, "idempotency_key": key}


def snapshot(gw):
    with gw._channel_store("whatsapp")._db() as db:
        journal = [tuple(row) for row in db.execute("SELECT * FROM receipts")]
        outbound = [tuple(row) for row in db.execute("SELECT * FROM outbound")]
    return imessage_turn_context_path("contact").read_bytes(), journal, outbound


@pytest.mark.parametrize("group", [False, True])
@pytest.mark.parametrize("state", ["accepted", "sending", "rejected"])
@pytest.mark.parametrize("retry_key", ["original", "omitted", "replacement"])
@pytest.mark.parametrize("selector", ["conversation", "to", "list", "named", "renamed", "other-conversation"])
def test_opaque_plain_legacy_selector_never_guessed(sdk, monkeypatch, group, state, retry_key, selector):
    async def scenario():
        gw, original = await legacy(sdk, monkeypatch, group, state, {"conversation_id": CONVERSATION})
        before = snapshot(gw)
        def no_rewrite(*args, **kwargs):
            pytest.fail("A blocked legacy retry must not rewrite its checkpoint or journal")
        monkeypatch.setattr(imessage, "_write_context", no_rewrite)
        monkeypatch.setattr(imessage.IMessageState, "mark", no_rewrite)
        monkeypatch.setattr(imessage.IMessageState, "record_outbound", no_rewrite)
        recipients = MEMBERS if group else MEMBERS[:1]
        retry = {"text": "Answer", "media_id": MEDIA}
        if selector in {"conversation", "other-conversation"}:
            retry["conversation_id"] = CONVERSATION if selector == "conversation" else OUTBOUND
        else:
            retry["to"] = recipients[0] if selector == "to" and not group else recipients
            if selector in {"named", "renamed"}:
                retry["to"] = MEMBERS
                retry["group_name"] = "Original group" if selector == "named" else "Different group"
        if retry_key != "omitted":
            retry["idempotency_key"] = original["idempotency_key"] if retry_key == "original" else "replacement"
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        assert value.get("isError"), value
        error = json.loads(value["content"][0]["text"])
        assert error["idempotency_key"] == original["idempotency_key"]
        assert error["media_id"] == MEDIA
        assert "read-only lookup" in error["error"]
        assert not sdk.calls
        assert snapshot(gw) == before
        # Lookup returns an ID, not the original request's selector or bytes.
        found = result(await call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {
            "idempotency_key": original["idempotency_key"]}))
        assert found["message_id"] == OUTBOUND
        assert not sdk.calls and snapshot(gw) == before
    asyncio.run(scenario())


@pytest.mark.parametrize("group", [False, True])
@pytest.mark.parametrize("state", ["accepted", "sending", "rejected"])
@pytest.mark.parametrize("generated", [False, True])
def test_provable_quoted_legacy_request_keeps_exact_body_and_key(sdk, monkeypatch, group, state, generated):
    async def scenario():
        gw, args = await legacy(sdk, monkeypatch, group, state, {"conversation_id": CONVERSATION},
                                generated=generated, quoted=True)
        before = snapshot(gw)
        for change in ({"reply_to_message_id": OUTBOUND}, {"conversation_id": OUTBOUND},
                       {"conversation_id": None, "reply_to_message_id": None, "to": MEMBERS if group else MEMBERS[0]}):
            bad = {k: v for k, v in {**args, **change}.items() if v is not None}
            value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", bad)
            assert value.get("isError") and not sdk.calls
            assert snapshot(gw) == before
        args.pop("reply_to_message_id")
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        if state == "sending":
            assert value.get("isError") and not sdk.calls
            assert snapshot(gw) == before
        else:
            assert result(value)["idempotency_key"] == args["idempotency_key"]
            assert len(sdk.calls) == 1
            assert json.loads(sdk.calls[0].content) == {"conversation_id": CONVERSATION, "text": "Answer",
                "media_ids": [MEDIA], "reply_to_message_id": MESSAGE}
            assert sdk.calls[0].headers["Idempotency-Key"] == args["idempotency_key"]
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", [
    {"conversation_id": CONVERSATION}, {"to": MEMBERS[0]}, {"to": MEMBERS[:1]},
    {"to": MEMBERS}, {"to": MEMBERS, "group_name": "Original group"},
])
@pytest.mark.parametrize("retry_key", ["original", "omitted"])
@pytest.mark.parametrize("state", ["accepted", "rejected"])
def test_exact_old_hash_proves_plain_legacy_request(sdk, monkeypatch, selector, retry_key, state):
    async def scenario():
        group = isinstance(selector.get("to"), list) and len(selector["to"]) > 1
        gw, args = await legacy(sdk, monkeypatch, group, state, selector, generated=True)
        key = args["idempotency_key"]
        before = snapshot(gw)
        # A selector flip must not rewrite the original attempt.
        changed = {"conversation_id": OUTBOUND, "text": "Answer", "media_id": MEDIA, "idempotency_key": key}
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", changed)
        assert value.get("isError") and not sdk.calls and snapshot(gw) == before
        if group:
            for name in ("Different group", None):
                changed = {**args, "group_name": name}
                if changed.get("group_name") == args.get("group_name"):
                    continue
                if name is None:
                    changed.pop("group_name")
                value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", changed)
                assert value.get("isError") and not sdk.calls and snapshot(gw) == before
        if retry_key == "omitted":
            args.pop("idempotency_key")
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert result(value)["idempotency_key"] == key
        assert len(sdk.calls) == 1
        assert json.loads(sdk.calls[0].content) == {**selector, "text": "Answer", "media_ids": [MEDIA]}
        assert sdk.calls[0].headers["Idempotency-Key"] == key
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", [{"conversation_id": CONVERSATION}, {"to": MEMBERS[0]},
                                      {"to": MEMBERS, "group_name": "Original group"}])
@pytest.mark.parametrize("status,code,state", [
    (201, None, "accepted"), (403, "whatsapp_contact_not_allowed", "rejected"),
    (429, "whatsapp_daily_limit_exceeded", "rejected"),
    (429, "unknown_error", "sending"), (409, "whatsapp_send_pending", "sending"),
    (500, "unknown_error", "sending"),
])
def test_historical_writer_outcome_never_authorizes_ambiguous_replay(sdk, monkeypatch, selector, status, code, state):
    async def scenario():
        gw = bridge(sdk)
        group = isinstance(selector.get("to"), list)
        meta = await admitted_source(gw, is_group=group, participants=MEMBERS if group else MEMBERS[:1])
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        original_checkpoint = imessage.checkpoint_whatsapp_tool_send
        def old_checkpoint(*args, **kwargs):
            kwargs.pop("payload", None)
            return original_checkpoint(*args, **kwargs)
        args = {**selector, "text": "Answer", "media_id": MEDIA, "idempotency_key": "opaque-original-key"}
        sdk.status, sdk.failure_detail = status, {"error": code}
        with monkeypatch.context() as old:
            old.setattr(imessage, "checkpoint_whatsapp_tool_send", old_checkpoint)
            old.setattr(whatsapp, "_source_bound", lambda *_: True)
            value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert bool(value.get("isError")) is (status != 201)
        saved = read_turn_context("contact")["whatsapp_tool_sends"]["opaque-original-key"]
        assert saved == {"state": state, "text": "Answer", "media_id": MEDIA, "quote_target": None}
        before = snapshot(gw)
        # The installed SDK retries 429 responses within the original call.
        # Those attempts must keep identical bytes/key; the plugin retry below
        # must not initiate any additional request.
        count = 3 if status == 429 else 1
        assert len(sdk.calls) == count
        assert len({request.content for request in sdk.calls}) == 1
        assert {request.headers["Idempotency-Key"] for request in sdk.calls} == {args["idempotency_key"]}
        sdk.status = 201
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert value.get("isError")
        assert len(sdk.calls) == count and snapshot(gw) == before
    asyncio.run(scenario())


def test_checkpoint_lock_cannot_upgrade_an_ambiguous_legacy_record(sdk, monkeypatch):
    async def scenario():
        gw, args = await legacy(sdk, monkeypatch, False, "rejected", {"to": MEMBERS[0]})
        before = snapshot(gw)
        with pytest.raises(ValueError, match="read-only lookup"):
            checkpoint_whatsapp_tool_send(read_turn_context("contact"), args["idempotency_key"],
                state="sending", text="Answer", media_id=MEDIA,
                payload={"conversation_id": CONVERSATION, "text": "Answer", "media_ids": [MEDIA],
                         "idempotency_key": args["idempotency_key"]})
        assert not sdk.calls and snapshot(gw) == before
    asyncio.run(scenario())


@pytest.mark.parametrize("retry_key", [None, "replacement"])
def test_media_uuid_spelling_cannot_escape_ambiguous_checkpoint(sdk, monkeypatch, retry_key):
    async def scenario():
        media_id = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
        gw, original = await legacy(sdk, monkeypatch, False, "rejected", {"to": MEMBERS[0]}, media_id=media_id)
        before = snapshot(gw)
        args = {"conversation_id": CONVERSATION, "text": "Answer", "media_id": media_id.lower()}
        if retry_key:
            args["idempotency_key"] = retry_key
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert value.get("isError")
        error = json.loads(value["content"][0]["text"])
        assert error["idempotency_key"] == original["idempotency_key"]
        assert error["media_id"] == media_id
        assert not sdk.calls and snapshot(gw) == before
    asyncio.run(scenario())


def test_explicit_full_payload_key_still_identifies_its_own_group_request(sdk, monkeypatch):
    async def scenario():
        _, _ = await legacy(sdk, monkeypatch, True, "rejected", {"conversation_id": CONVERSATION})
        before = read_turn_context("contact")["whatsapp_tool_sends"]["opaque-original-key"]
        payload = {"to": MEMBERS, "group_name": "Original group", "text": "Answer",
                   "media_ids": [MEDIA], "idempotency_key": "complete-original-key"}
        checkpoint_whatsapp_tool_send(read_turn_context("contact"), "complete-original-key",
            state="rejected", text="Answer", media_id=MEDIA, payload=payload)
        args = {k: v for k, v in payload.items() if k != "media_ids"}
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**args, "media_id": MEDIA})
        assert result(value)["idempotency_key"] == "complete-original-key"
        assert len(sdk.calls) == 1
        assert json.loads(sdk.calls[0].content) == {k: v for k, v in payload.items() if k != "idempotency_key"}
        assert sdk.calls[0].headers["Idempotency-Key"] == "complete-original-key"
        assert read_turn_context("contact")["whatsapp_tool_sends"]["opaque-original-key"] == before
    asyncio.run(scenario())

@pytest.mark.parametrize("explicit", [False, True])
def test_proven_legacy_selector_is_not_blocked_by_another_rejected_selector(sdk, monkeypatch, explicit):
    async def scenario():
        gw, args = await legacy(sdk, monkeypatch, True, "rejected", {"conversation_id": CONVERSATION}, generated=True)
        context = read_turn_context("contact")
        sibling_key = "codex-wa-" + hashlib.sha256(json.dumps([context["nonce"], None, "Answer", MEDIA, MEMBERS, None, None], sort_keys=True).encode()).hexdigest()
        checkpoint_whatsapp_tool_send(context, sibling_key, state="rejected", text="Answer", media_id=MEDIA)
        key = args["idempotency_key"]
        if not explicit: args.pop("idempotency_key")
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert result(value)["idempotency_key"] == key
        assert len(sdk.calls) == 1
        assert json.loads(sdk.calls[0].content) == {"conversation_id": CONVERSATION, "text": "Answer", "media_ids": [MEDIA]}
    asyncio.run(scenario())

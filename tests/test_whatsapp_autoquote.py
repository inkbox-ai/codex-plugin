"""Source-anchored replies preserve durable intent and explicit outreach routing."""

import asyncio
import hashlib
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from inkbox_codex.imessage import clear_turn_context, read_turn_context
from inkbox_codex.tools import call_inkbox_tool
from inkbox_codex.whatsapp import auto_reply_kwargs, receive, run_tool
from inkbox_codex.whatsapp_v2 import receive_reaction
from tests.test_imessage_sessions import make_session, finish_burst
from tests.test_whatsapp import sdk, bridge, message, CONVERSATION, MESSAGE, IDENTITY, OUTBOUND, MEDIA
from tests.test_whatsapp_send_safety import source
from tests.test_whatsapp_v2 import channel


async def admitted_source(gw, **changes):
    gw._resolve_contact_full = AsyncMock(return_value=None)
    gw._with_media = AsyncMock(side_effect=lambda text, *_args, **_kwargs: text)
    inbound = AsyncMock()
    gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=inbound)))
    await receive(gw, {"id": f"event-{changes.get('id', MESSAGE)}", "data": {"message": message(**changes)}})
    inbound.assert_awaited_once()
    return inbound.call_args.args[2]


@pytest.mark.parametrize("group", [False, True])
@pytest.mark.parametrize("quoted", [False, True])
def test_new_ordinary_final_quotes_trigger_not_parent_after_restart(sdk, group, quoted):
    async def scenario():
        gw = bridge(sdk)
        changes = {"is_group": group, "participants": ["+15550000002", "+15550000003"] if group else ["+15550000002"]}
        if quoted:
            changes.update(reply_to_message_id=OUTBOUND, thread_id=OUTBOUND, thread_root_message_id=OUTBOUND)
        meta = await admitted_source(gw, **changes)
        store = gw._channel_store("whatsapp")
        store.admit("contact", "Question", meta)
        store.mark(meta, "reply_pending", reply="Answer")
        restarted = bridge(sdk)
        restarted.sessions = NS(get=Mock())
        await restarted._recover_imessage_inputs()
        assert len(sdk.calls) == 1
        assert json.loads(sdk.calls[0].content) == {
            "conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE}
        expected_key = "codex-wa-" + hashlib.sha256(json.dumps([
            gw.cfg.base_url, IDENTITY, CONVERSATION, meta["imessage_event_ids"], "Answer",
            {"reply_to_message_id": MESSAGE}], sort_keys=True).encode()).hexdigest()
        assert sdk.calls[0].headers["Idempotency-Key"] == expected_key
        restarted.sessions.get.assert_not_called()
        await restarted._recover_imessage_inputs()
        assert len(sdk.calls) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("saved_key", [None, "retained-original-key"])
def test_legacy_plain_payload_and_hash_survive_restart_with_old_target(sdk, saved_key):
    async def scenario():
        gw = bridge(sdk)
        meta = source(whatsapp_reply_target=MESSAGE, reply_to_message_id=OUTBOUND,
                      imessage_reply_target=MESSAGE)
        if saved_key:
            meta["whatsapp_idempotency_key"] = saved_key
        old_payload = {"conversation_id": CONVERSATION, "text": "Saved answer"}
        old_hash = "codex-wa-" + hashlib.sha256(json.dumps([
            gw.cfg.base_url, IDENTITY, CONVERSATION, ["source-one"], "Saved answer"],
            sort_keys=True).encode()).hexdigest()
        store = gw._channel_store("whatsapp")
        store.admit("contact", "Old question", meta)
        store.mark(meta, "reply_pending", reply="Saved answer")
        restarted = bridge(sdk)
        restarted.sessions = NS(get=Mock())
        await restarted._recover_imessage_inputs()
        assert len(sdk.calls) == 1
        assert json.loads(sdk.calls[0].content) == old_payload
        assert sdk.calls[0].headers["Idempotency-Key"] == (saved_key or old_hash)
        restarted.sessions.get.assert_not_called()
    asyncio.run(scenario())


def test_legacy_pending_input_never_acquires_anchor_during_recovery(sdk):
    async def scenario():
        gw = bridge(sdk)
        old = source(whatsapp_reply_target=MESSAGE)
        gw._channel_store("whatsapp").admit("contact", "Old question", old)
        async def answer(_body, mode, meta):
            from inkbox_codex.sessions import ContactSession
            meta = ContactSession._imessage_meta(meta, _body)
            await gw.send_to_contact("contact", "Answer", mode, {**meta, "final_reply": True})
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=answer)))
        await gw._recover_imessage_inputs()
        assert len(sdk.calls) == 1
        assert "reply_to_message_id" not in json.loads(sdk.calls[0].content)
    asyncio.run(scenario())


@pytest.mark.parametrize("group", [False, True])
def test_burst_anchors_first_source_in_original_conversation(sdk, group):
    async def scenario():
        gw = bridge(sdk)
        members = ["+15550000002", "+15550000003"] if group else ["+15550000002"]
        first = await admitted_source(gw, is_group=group, participants=members)
        second = await admitted_source(gw, id=str(UUID(int=30)), is_group=group, participants=members)
        session, sent, _ = make_session(enabled=False)
        await session.handle_inbound("First fragment", "whatsapp", first)
        await session.handle_inbound("Second fragment", "whatsapp", second)
        await finish_burst(session)
        assert len(sent) == 1
        text, mode, meta = sent[0]
        await gw.send_to_contact("contact", text, mode, meta)
        assert len(sdk.calls) == 1
        body = json.loads(sdk.calls[0].content)
        assert body["reply_to_message_id"] == MESSAGE and body["conversation_id"] == CONVERSATION
        assert len(meta["imessage_sources"]) == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", ["source", "other-conversation", "recipient", "group-recipients"])
def test_tool_autoquote_respects_explicit_destination(sdk, monkeypatch, selector):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw, is_group=True, participants=["+15550000002", "+15550000003"])
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {"source": {"conversation_id": CONVERSATION},
                "other-conversation": {"conversation_id": OUTBOUND},
                "recipient": {"to": "+15550000004"},
                "group-recipients": {"to": ["+15550000002", "+15550000003"]}}[selector]
        run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**args, "text": "Tool answer"})
        body = json.loads(sdk.calls[0].content)
        expected = {**args, "text": "Tool answer"}
        if selector == "source":
            expected["reply_to_message_id"] = MESSAGE
        assert body == expected
        suppressed = await gw._imessage_turn_state("contact", "result", meta, "Different final")
        assert suppressed is (selector == "source")
    asyncio.run(scenario())


@pytest.mark.parametrize("status", [201, 409, 429])
@pytest.mark.parametrize("explicit", [False, True])
def test_quoted_tool_attempt_allows_only_confirmed_distinct_sends(sdk, monkeypatch, status, explicit):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        sdk.status = status
        if status == 429:
            sdk.failure_detail = {"error": "whatsapp_daily_limit_exceeded"}
        args = {"conversation_id": CONVERSATION, "text": "Tool reply"}
        if explicit:
            args.update(reply_to_message_id=OUTBOUND, idempotency_key="original-quote")
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        first_count = len(sdk.calls)
        assert first_count >= 1
        target = OUTBOUND if explicit else MESSAGE
        assert all(json.loads(call.content)["reply_to_message_id"] == target for call in sdk.calls)
        plain = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "to": "+15550000002", "text": "Additional detail", "idempotency_key": "new-key"})
        if status == 201:
            assert not plain.get("isError") and len(sdk.calls) == first_count + 1
            assert json.loads(sdk.calls[-1].content) == {"to": "+15550000002", "text": "Additional detail"}
        else:
            assert plain["isError"] and len(sdk.calls) == first_count
        final_count = len(sdk.calls)
        assert await gw._imessage_turn_state("contact", "result", meta, "Different final") is True
        clear_turn_context("contact")
        restarted = bridge(sdk)
        restarted.sessions = NS(get=Mock())
        await restarted._recover_imessage_inputs()
        assert len(sdk.calls) == final_count
    asyncio.run(scenario())


def test_automatic_quote_retry_keeps_same_payload_and_key_after_known_rejection(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {"conversation_id": CONVERSATION, "text": "Tool reply"}
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        args["idempotency_key"] = sdk.calls[-1].headers["Idempotency-Key"]
        sdk.status = 201
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert len(sdk.calls) >= 2
        assert len({call.content for call in sdk.calls}) == 1
        assert len({call.headers["Idempotency-Key"] for call in sdk.calls}) == 1
        assert json.loads(sdk.calls[-1].content)["reply_to_message_id"] == MESSAGE
    asyncio.run(scenario())


def test_verified_reaction_context_remains_plain(channel):
    async def scenario():
        await receive_reaction(channel.gateway, {"data": {"reaction": channel.reaction}})
        meta = channel.inbound.call_args.args[2]
        assert "whatsapp_auto_reply_target" not in meta
        await channel.gateway.send_to_contact("contact", "Thanks", "whatsapp", {**meta, "final_reply": True})
        body = json.loads(channel.requests[-1].content)
        assert body == {"conversation_id": CONVERSATION, "text": "Thanks"}
    asyncio.run(scenario())


@pytest.mark.parametrize("target", ["reaction:event:1", MESSAGE])
def test_reaction_pseudo_or_real_ids_are_never_automatic_quotes(target):
    assert auto_reply_kwargs(source(reaction="like", whatsapp_auto_reply_target=target)) == {}


def test_automatic_target_cannot_borrow_an_unadmitted_message():
    with pytest.raises(ValueError, match="admitted source"):
        auto_reply_kwargs(source(whatsapp_auto_reply_target=OUTBOUND))


def test_unknown_recovery_key_stays_plain_even_in_new_anchored_turn(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "conversation_id": CONVERSATION, "text": "Retained answer", "idempotency_key": "prior-send"})
        assert json.loads(sdk.calls[0].content) == {"conversation_id": CONVERSATION, "text": "Retained answer"}
        assert sdk.calls[0].headers["Idempotency-Key"] == "prior-send"
    asyncio.run(scenario())


@pytest.mark.parametrize("status", [409, 429])
def test_automatic_quoted_failure_never_replays_as_plain_on_restart(sdk, status):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        store = gw._channel_store("whatsapp")
        store.admit("contact", "Question", meta)
        store.mark(meta, "reply_pending", reply="Answer")
        sdk.status, sdk.failure_detail = status, {"error": "whatsapp_daily_limit_exceeded"}
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        first_count = len(sdk.calls)
        assert first_count >= 1
        assert all(json.loads(call.content) == {
            "conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE} for call in sdk.calls)
        restarted = bridge(sdk)
        restarted.sessions = NS(get=Mock())
        sdk.status = 201
        await restarted._recover_imessage_inputs()
        assert len(sdk.calls) == first_count
        assert restarted._channel_store("whatsapp").summary()["uncertain"] == 1
    asyncio.run(scenario())


def test_known_quoted_key_cannot_change_message_on_retry(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {"conversation_id": CONVERSATION, "text": "Original answer"}
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        args["idempotency_key"] = sdk.calls[-1].headers["Idempotency-Key"]
        first_count = len(sdk.calls)
        with pytest.raises(ValueError, match="original message"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**args, "text": "Changed answer"})
        assert len(sdk.calls) == first_count
    asyncio.run(scenario())


@pytest.mark.parametrize("change", [
    {"text": "Changed answer"}, {"media_id": OUTBOUND}, {"media_id": None},
    {"reply_to_message_id": OUTBOUND}, {"conversation_id": OUTBOUND},
])
def test_explicit_quote_retry_preserves_entire_retained_request(sdk, monkeypatch, change):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {"conversation_id": CONVERSATION, "text": "Original answer", "media_id": MEDIA,
                "reply_to_message_id": MESSAGE, "idempotency_key": "retained-quote"}
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        first = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert first["isError"]
        before = read_turn_context("contact")
        store = gw._channel_store("whatsapp")
        with store._db() as db:
            durable_before = db.execute("SELECT meta FROM receipts").fetchall()
        assert before["whatsapp_tool_sends"]["retained-quote"]["state"] == "rejected"
        count = len(sdk.calls)
        sdk.status = 201
        with pytest.raises(ValueError, match="original message"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**args, **change})
        assert len(sdk.calls) == count
        assert read_turn_context("contact") == before
        with store._db() as db:
            assert db.execute("SELECT meta FROM receipts").fetchall() == durable_before
        # A mismatch must not poison a later exact retry or allocate another key.
        sent = run_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert sent["idempotency_key"] == "retained-quote"
        assert len(sdk.calls) == count + 1
        assert len({call.content for call in sdk.calls}) == 1
        assert {call.headers["Idempotency-Key"] for call in sdk.calls} == {"retained-quote"}
        assert read_turn_context("contact")["whatsapp_tool_sends"]["retained-quote"]["state"] == "accepted"

    asyncio.run(scenario())


@pytest.mark.parametrize("quoted", [False, True])
def test_legacy_checkpoint_replays_original_payload_and_key(sdk, monkeypatch, quoted):
    from inkbox_codex.imessage import _write_context, imessage_turn_context_path

    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        args = {"conversation_id": CONVERSATION, "text": "Original answer", "media_id": MEDIA,
                "idempotency_key": "legacy-request"}
        if quoted:
            args["reply_to_message_id"] = MESSAGE
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        first = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert first["isError"]
        context = read_turn_context("contact")
        context["whatsapp_tool_sends"]["legacy-request"].pop("payload")
        _write_context(imessage_turn_context_path("contact"), context)
        before = len(sdk.calls)
        sdk.status = 201
        with pytest.raises(ValueError, match="original message"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                **args, "reply_to_message_id": OUTBOUND})
        with pytest.raises(ValueError, match="original message"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                **args, "conversation_id": OUTBOUND})
        assert len(sdk.calls) == before and read_turn_context("contact") == context
        replay = {key: value for key, value in args.items() if key != "reply_to_message_id"}
        if not quoted:
            # Without a quote, opaque keys cannot distinguish recipients from
            # conversation_id. Even a matching conversation is only a guess.
            with pytest.raises(ValueError, match="read-only lookup") as failure:
                run_tool(sdk.client, "agent", "inkbox_send_whatsapp", replay)
            assert failure.value.idempotency_key == "legacy-request"
            assert len(sdk.calls) == before and read_turn_context("contact") == context
            return
        sent = run_tool(sdk.client, "agent", "inkbox_send_whatsapp", replay)
        assert sent["idempotency_key"] == "legacy-request"
        assert len(sdk.calls) == before + 1
        assert len({call.content for call in sdk.calls}) == 1
        assert {call.headers["Idempotency-Key"] for call in sdk.calls} == {"legacy-request"}
        assert ("reply_to_message_id" in json.loads(sdk.calls[-1].content)) is quoted

    asyncio.run(scenario())


def test_stale_tool_context_cannot_overwrite_checkpoint(sdk, monkeypatch):
    from inkbox_codex import tools

    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        stale = read_turn_context("contact")
        args = {"conversation_id": CONVERSATION, "text": "Original answer",
                "reply_to_message_id": MESSAGE, "idempotency_key": "same-key"}
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        rejected = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert rejected["isError"]
        retained = read_turn_context("contact")
        count = len(sdk.calls)
        monkeypatch.setattr(tools, "_imessage_active_context", lambda _: stale)
        sdk.status = 201
        with pytest.raises(ValueError, match="original message"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {**args, "text": "Different answer"})
        assert len(sdk.calls) == count and read_turn_context("contact") == retained
        run_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert len(sdk.calls) == count + 1
        assert len({call.content for call in sdk.calls}) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("evidence", ["accepted-same-key", "legacy-flag"])
def test_plain_send_cannot_reuse_quoted_key_or_bypass_legacy_fence(sdk, monkeypatch, evidence):
    async def scenario():
        from inkbox_codex.imessage import _write_context, imessage_turn_context_path, read_turn_context
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        if evidence == "accepted-same-key":
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                "conversation_id": CONVERSATION, "reply_to_message_id": MESSAGE,
                "text": "Quoted answer", "idempotency_key": "quote-key"})
        else:
            context = read_turn_context("contact")
            context["whatsapp_explicit_quote_attempted"] = True
            _write_context(imessage_turn_context_path("contact"), context)
        before = len(sdk.calls)
        with pytest.raises(ValueError, match="do not replace"):
            run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                "to": "+15550000002", "text": "Plain answer", "idempotency_key": "quote-key"})
        assert len(sdk.calls) == before
    asyncio.run(scenario())


@pytest.mark.parametrize("include", [None, True])
def test_registered_list_tool_can_discover_existing_large_groups(sdk, include):
    from inkbox_codex.tools import mcp_tool_list
    tool = next(t for t in mcp_tool_list() if t["name"] == "inkbox_list_whatsapp_conversations")
    assert tool["inputSchema"]["properties"]["include_groups"]["type"] == "boolean"
    result = asyncio.run(call_inkbox_tool(sdk.client, "agent", tool["name"],
                                        {} if include is None else {"include_groups": include}))
    assert not result.get("isError"), result
    assert sdk.requests[-1].url.params["include_groups"] == ("true" if include else "false")
    rows = json.loads(result["content"][0]["text"])
    assert len(rows) == (1 if include else 0)
    if include:
        assert rows[0]["is_group"] and len(rows[0]["participants"]) == 9

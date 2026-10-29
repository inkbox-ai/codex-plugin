"""New groups and retained group-creation attempts are not source replies."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock
from uuid import UUID

import pytest

from inkbox_codex import whatsapp
from inkbox_codex.imessage import read_turn_context, clear_turn_context, _write_context, imessage_turn_context_path
from inkbox_codex.tools import build_inkbox_mcp_server_config, call_inkbox_tool
from tests.test_whatsapp import sdk, bridge, CONVERSATION, MESSAGE, OUTBOUND, MEDIA
from tests.test_whatsapp_autoquote import admitted_source


MEMBERS = ["+15550000002", "+15550000003"]


async def start(sdk, monkeypatch):
    sdk.response_conversation = OUTBOUND
    gw = bridge(sdk)
    meta = await admitted_source(gw, is_group=True, participants=list(MEMBERS))
    await gw._imessage_turn_state("contact", "admitted", meta, "Question")
    await gw._imessage_turn_state("contact", "started", meta)
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
    return gw, meta


@pytest.mark.parametrize("selector", ["new", "source"])
@pytest.mark.parametrize("allow_all", [False, True])
def test_gateway_allowlist_survives_mcp_environment_boundary(sdk, monkeypatch, selector, allow_all):
    async def scenario():
        gw, _ = await start(sdk, monkeypatch)
        gw.cfg.allowed_users = MEMBERS[:1]
        gw.cfg.allow_all_users = allow_all
        server, _ = build_inkbox_mcp_server_config(gw.cfg)
        # Model a host that forwards only the declared tool environment.
        for key in ("INKBOX_ALLOWED_USERS", "INKBOX_ALLOW_ALL_USERS"):
            monkeypatch.delenv(key, raising=False)
            if key in server["env"]:
                monkeypatch.setenv(key, server["env"][key])
        args = {"text": "Answer", **({"to": MEMBERS} if selector == "new"
                                    else {"conversation_id": CONVERSATION})}
        result = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert bool(result.get("isError")) is not allow_all
        assert len(sdk.calls) == int(allow_all)
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", ["new", "named-new", "source", "other-group"])
def test_only_exact_source_conversation_owns_reply(sdk, monkeypatch, selector):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        args = {"text": "Answer"}
        if selector in {"new", "named-new"}:
            args["to"] = list(reversed(MEMBERS))
            if selector == "named-new":
                args["group_name"] = "New discussion"
        else:
            args["conversation_id"] = CONVERSATION if selector == "source" else OUTBOUND
        whatsapp.run_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        expected = {**args}
        if "to" in expected:
            expected["to"] = MEMBERS
        if selector == "source":
            expected["reply_to_message_id"] = MESSAGE
        assert json.loads(sdk.calls[0].content) == expected
        context = read_turn_context("contact")
        assert bool(context.get("whatsapp_tool_sends")) is (selector != "other-group")
        assert bool(context["sent_outputs"]) is (selector == "source")
        suppressed = await gw._imessage_turn_state("contact", "result", meta, "Answer")
        assert suppressed is (selector == "source")
        if not suppressed:
            await gw.send_to_contact("contact", "Answer", "whatsapp", {**meta, "final_reply": True})
            assert json.loads(sdk.calls[-1].content) == {
                "conversation_id": CONVERSATION, "text": "Answer", "reply_to_message_id": MESSAGE}
            assert len(sdk.calls) == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("allowed", [True, False])
def test_new_group_checks_explicit_recipients_not_source_membership(sdk, monkeypatch, allowed):
    async def scenario():
        await start(sdk, monkeypatch)
        monkeypatch.setenv("INKBOX_ALLOW_ALL_USERS", "false")
        monkeypatch.setenv("INKBOX_ALLOWED_USERS", ",".join(MEMBERS if allowed else MEMBERS[:1]))
        sdk.conversation["participants"].append("+15550000004")
        sdk.on_conversation_read = lambda: pytest.fail("A new group must not read source membership")
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "to": MEMBERS, "text": "New discussion", "group_name": "Project"})
        assert bool(value.get("isError")) is not allowed
        assert len(sdk.calls) == int(allowed)
        assert bool(read_turn_context("contact").get("whatsapp_tool_sends")) is allowed
    asyncio.run(scenario())


def test_distinct_groups_with_identical_members_keep_conversation_identity(sdk, monkeypatch):
    async def scenario():
        gw, _ = await start(sdk, monkeypatch)
        second_message = str(UUID(int=6))
        meta = await admitted_source(gw, id=second_message, conversation_id=OUTBOUND, is_group=True, participants=list(MEMBERS))
        await gw._imessage_turn_state("second-group", "admitted", meta, "Question")
        await gw._imessage_turn_state("second-group", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "second-group")
        for conversation in (CONVERSATION, OUTBOUND):
            whatsapp.run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
                "conversation_id": conversation, "text": "Answer"})
        assert [json.loads(call.content) for call in sdk.calls] == [
            {"conversation_id": CONVERSATION, "text": "Answer"},
            {"conversation_id": OUTBOUND, "text": "Answer", "reply_to_message_id": second_message},
        ]
        assert len(read_turn_context("second-group")["whatsapp_tool_sends"]) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", [MEMBERS, ["+15550000004", "+15550000005"], "+15550000004"])
def test_completed_unrelated_outreach_is_not_payload_deduplicated(sdk, monkeypatch, selector):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        for _ in range(2):
            whatsapp.run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": selector, "text": "Hello"})
        assert len(sdk.calls) == 2
        assert sdk.calls[0].content == sdk.calls[1].content
        assert sdk.calls[0].headers["Idempotency-Key"] != sdk.calls[1].headers["Idempotency-Key"]
        assert bool(read_turn_context("contact").get("whatsapp_tool_sends")) is isinstance(selector, list)
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is False
    asyncio.run(scenario())


@pytest.mark.parametrize("change", [
    {"text": "Changed"}, {"media_id": None}, {"group_name": "Changed"},
    {"to": ["+15550000002", "+15550000004"]},
    {"to": None, "conversation_id": CONVERSATION},
])
def test_retained_group_key_cannot_change_intent(sdk, monkeypatch, change):
    async def scenario():
        await start(sdk, monkeypatch)
        args = {"to": MEMBERS, "text": "Hello", "group_name": "Project", "media_id": MEDIA,
                "idempotency_key": "saved-group-key"}
        sdk.status, sdk.failure_detail = 429, {"error": "whatsapp_daily_limit_exceeded"}
        with monkeypatch.context() as old:
            old.setattr(whatsapp, "_source_bound", lambda *_: True)
            await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        count, retained = len(sdk.calls), read_turn_context("contact")
        retry = {**args, **change}
        if retry.get("conversation_id"):
            retry.pop("group_name")
        sdk.status = 201
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        assert value.get("isError"), value
        assert len(sdk.calls) == count
        assert read_turn_context("contact") == retained
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert not value.get("isError"), value
        assert sdk.calls[-1].content == sdk.calls[0].content
        assert {call.headers["Idempotency-Key"] for call in sdk.calls} == {"saved-group-key"}
    asyncio.run(scenario())


@pytest.mark.parametrize("status", [409, 429])
@pytest.mark.parametrize("retry_key", ["original", "omitted", "replacement"])
def test_legacy_derived_key_without_payload_keeps_recovery_safe(sdk, monkeypatch, status, retry_key):
    async def scenario():
        await start(sdk, monkeypatch)
        args = {"to": MEMBERS, "text": "Hello", "group_name": "Project", "media_id": MEDIA}
        sdk.status, sdk.failure_detail = status, {"error": "whatsapp_daily_limit_exceeded"}
        with monkeypatch.context() as old:
            old.setattr(whatsapp, "_source_bound", lambda *_: True)
            await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        key = sdk.calls[0].headers["Idempotency-Key"]
        retained = read_turn_context("contact")
        retained["whatsapp_tool_sends"][key].pop("payload")
        _write_context(imessage_turn_context_path("contact"), retained)
        count = len(sdk.calls)
        sdk.status = 201
        retry = dict(args)
        if retry_key != "omitted":
            retry["idempotency_key"] = key if retry_key == "original" else "replacement"
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        if status == 409:
            assert value.get("isError"), value
            assert json.loads(value["content"][0]["text"])["idempotency_key"] == key
            assert len(sdk.calls) == count and read_turn_context("contact") == retained
        else:
            assert not value.get("isError"), value
            assert len(sdk.calls) == count + 1
            assert sdk.calls[-1].content == sdk.calls[0].content
            assert sdk.calls[-1].headers["Idempotency-Key"] == ("replacement" if retry_key == "replacement" else key)
    asyncio.run(scenario())


@pytest.mark.parametrize("retry_key", ["original", "omitted", "replacement"])
def test_payloadless_uncertain_custom_key_is_never_replaced(sdk, monkeypatch, retry_key):
    async def scenario():
        await start(sdk, monkeypatch)
        args = {"to": MEMBERS, "text": "Hello", "media_id": MEDIA, "idempotency_key": "custom-key"}
        sdk.status = 409
        with monkeypatch.context() as old:
            old.setattr(whatsapp, "_source_bound", lambda *_: True)
            await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        retained = read_turn_context("contact")
        retained["whatsapp_tool_sends"]["custom-key"].pop("payload")
        _write_context(imessage_turn_context_path("contact"), retained)
        retry = {k: v for k, v in args.items() if k != "idempotency_key"}
        if retry_key != "omitted":
            retry["idempotency_key"] = "custom-key" if retry_key == "original" else "replacement"
        sdk.status = 201
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        assert value.get("isError"), value
        error = json.loads(value["content"][0]["text"])
        assert error["idempotency_key"] == "custom-key" and error["media_id"] == MEDIA
        assert len(sdk.calls) == 1 and read_turn_context("contact") == retained
    asyncio.run(scenario())


@pytest.mark.parametrize("named", [False, True])
@pytest.mark.parametrize("status", [201, 409, 429])
@pytest.mark.parametrize("retry_key", ["original", "omitted", "replacement"])
def test_retained_group_intent_keeps_bytes_key_and_uncertainty(
        sdk, monkeypatch, named, status, retry_key):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        args = {"to": list(reversed(MEMBERS)), "text": "Group message", "media_id": MEDIA}
        if named:
            args["group_name"] = "New discussion"
        sdk.status = status
        sdk.failure_detail = {"error": "whatsapp_daily_limit_exceeded"} if status == 429 else {"error": "whatsapp_send_pending"}
        # Produce the historical checkpoint through the old source classifier.
        with monkeypatch.context() as old:
            old.setattr(whatsapp, "_source_bound", lambda *_: True)
            await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        first = sdk.calls[0]
        key = first.headers["Idempotency-Key"]
        count = len(sdk.calls)
        retained = read_turn_context("contact")
        sdk.status = 201
        retry = dict(args)
        if retry_key != "omitted":
            retry["idempotency_key"] = key if retry_key == "original" else "replacement"
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        if status == 409:
            assert value.get("isError"), value
            assert json.loads(value["content"][0]["text"])["idempotency_key"] == key
            assert len(sdk.calls) == count
            assert read_turn_context("contact") == retained
            assert await gw._imessage_turn_state("contact", "result", meta, "Answer") is True
            clear_turn_context("contact")
            restarted = bridge(sdk)
            restarted.sessions = NS(get=Mock())
            await restarted._recover_imessage_inputs()
            restarted.sessions.get.assert_not_called()
            assert len(sdk.calls) == count
            assert restarted._channel_store("whatsapp").summary()["uncertain"] == 1
        else:
            assert not value.get("isError"), value
            assert len(sdk.calls) == count + 1
            assert sdk.calls[-1].content == first.content
            should_reuse = retry_key in {"original", "omitted"}
            assert (sdk.calls[-1].headers["Idempotency-Key"] == key) is should_reuse
            assert await gw._imessage_turn_state("contact", "result", meta, "Group message") is False
    asyncio.run(scenario())

@pytest.mark.parametrize("retry_key", ["omitted", "replacement"])
def test_new_group_uncertainty_is_retained_before_any_retry(sdk, monkeypatch, retry_key):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        args = {"to": ["+15550000004", "+15550000005"], "text": "Hello"}
        sdk.status = 409
        first = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", dict(args))
        assert first.get("isError")
        key = sdk.calls[0].headers["Idempotency-Key"]
        context = read_turn_context("contact")
        assert context["whatsapp_tool_sends"][key]["state"] == "sending"
        sdk.status = 201
        retry = {**args, **({"idempotency_key": "replacement"} if retry_key == "replacement" else {})}
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", retry)
        assert value.get("isError"), value
        assert json.loads(value["content"][0]["text"])["idempotency_key"] == key
        assert len(sdk.calls) == 1 and not read_turn_context("contact")["sent_outputs"]
    asyncio.run(scenario())


def test_confirmed_recipient_send_to_source_conversation_suppresses_duplicate(sdk, monkeypatch):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        sdk.response_conversation = CONVERSATION
        whatsapp.run_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": MEMBERS, "text": "Answer"})
        assert await gw._imessage_turn_state("contact", "result", meta, "Answer") is True
        assert len(sdk.calls) == 1
        # Keep the caller's selector; a roster alone must never pick between groups.
        assert json.loads(sdk.calls[0].content) == {"to": MEMBERS, "text": "Answer"}
    asyncio.run(scenario())


@pytest.mark.parametrize("selector", ["recovered-group", "other-contact"])
@pytest.mark.parametrize("new_key", [False, True])
def test_uncertain_group_cannot_escape_through_an_unowned_selector(sdk, monkeypatch, selector, new_key):
    async def scenario():
        gw, meta = await start(sdk, monkeypatch)
        sdk.status = 409
        first = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {
            "to": MEMBERS, "text": "Hello", "idempotency_key": "original-group"})
        assert first.get("isError") and len(sdk.calls) == 1
        lookup = await call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {
            "idempotency_key": "original-group"})
        assert not lookup.get("isError")
        sdk.status = 201
        retained = read_turn_context("contact")
        args = {"text": "Hello", **({"conversation_id": OUTBOUND} if selector == "recovered-group"
                                   else {"to": "+15550000004"})}
        if new_key:
            args["idempotency_key"] = "replacement"
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert value.get("isError"), value
        assert json.loads(value["content"][0]["text"])["idempotency_key"] == "original-group"
        assert len(sdk.calls) == 1 and read_turn_context("contact") == retained
        assert await gw._imessage_turn_state("contact", "result", meta, "Hello") is True
    asyncio.run(scenario())

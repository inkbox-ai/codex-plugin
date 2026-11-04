"""Direct-only boundaries reject unsupported destinations before side effects."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex.imessage import checkpoint_whatsapp_tool_send, read_turn_context
from inkbox_codex.tools import call_inkbox_tool
from inkbox_codex.whatsapp import TOOLS, receive
from inkbox_codex.whatsapp_v2 import receive_reaction
from tests.test_whatsapp import sdk, bridge, message, CONVERSATION, MESSAGE, result
from tests.test_whatsapp_autoquote import admitted_source
from tests.test_whatsapp_v2 import channel


@pytest.mark.parametrize("recipient", [[], ["+15550000002"], ["+15550000002", "+15550000003"],
    "+15550000002,+15550000003", " +15550000002", "+1 (555) 000-0002", "+1١٢٣٤٥٦٧٨٩", 15550000002])
def test_invalid_recipient_never_reaches_sdk(sdk, recipient):
    value = asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": recipient, "text": "Hello"}))
    assert value["isError"] and not sdk.requests


def test_schema_and_scalar_wire_contract(sdk):
    send = next(tool for tool in TOOLS if tool["name"] == "inkbox_send_whatsapp")
    assert send["inputSchema"]["properties"]["to"]["type"] == "string"
    assert "group_name" not in send["inputSchema"]["properties"]
    listing = next(tool for tool in TOOLS if tool["name"] == "inkbox_list_whatsapp_conversations")
    assert "include_groups" not in listing["inputSchema"]["properties"]
    response = result(asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": "+15550000002", "text": "Hello"})))
    assert json.loads(sdk.calls[0].content) == {"to": "+15550000002", "text": "Hello"}
    assert len(response["participants"]) == len(response["recipients"]) == 1
    assert "is_group" not in response


@pytest.mark.parametrize("shape", [{"is_group": True}, {"isGroup": True},
    {"participants": ["+15550000002", "+15550000003"]},
    {"recipients": [{"remote_number": "+15550000002"}, {"remote_number": "+15550000003"}]},
    {"group_name": "Unsupported"}, {"groupName": "Unsupported"}, {"group_id": "unsupported"},
    {"unresolved_participant_count": 1}])
def test_group_inbound_never_opens_session(sdk, shape):
    gw = bridge(sdk)
    gw.sessions = NS(get=Mock())
    response = asyncio.run(receive(gw, {"data": {"message": message(**shape)}}))
    assert json.loads(response.body)["ignored"] == "unsupported-message"
    gw.sessions.get.assert_not_called()
    assert not sdk.calls


@pytest.mark.parametrize("target", [False, True])
def test_group_reactions_never_open_session(channel, target):
    if target:
        channel.message["participants"] = ["+15550000002", "+15550000003"]
    else:
        channel.reaction["group_name"] = "Unsupported"
    asyncio.run(receive_reaction(channel.gateway, {"data": {"reaction": channel.reaction}}))
    channel.inbound.assert_not_awaited()


@pytest.mark.parametrize("state", ["pending", "reply_pending"])
def test_retained_group_routes_never_replay_or_reply(sdk, state):
    async def scenario():
        gw = bridge(sdk)
        meta = {"channel": "whatsapp", "conversation_id": CONVERSATION, "message_id": MESSAGE,
                "sender": "+15550000002", "conversation_kind": "group", "participants": ["+15550000002"]}
        store = gw._channel_store("whatsapp")
        store.admit("old", "Question", meta)
        if state == "reply_pending":
            store.mark(meta, state, reply="Answer", chat_id="old")
        gw.sessions = NS(get=Mock())
        await gw._recover_imessage_inputs()
        gw.sessions.get.assert_not_called()
        assert not sdk.calls
    asyncio.run(scenario())


@pytest.mark.parametrize("saved", [{"to": ["+15550000002"]},
    {"to": ["+15550000002", "+15550000003"]}, {"conversation_id": CONVERSATION, "group_name": "Old"}])
@pytest.mark.parametrize("state", ["accepted", "rejected", "sending"])
@pytest.mark.parametrize("retry_key", [None, "original", "replacement"])
def test_retained_unsupported_intent_never_becomes_direct_send(sdk, monkeypatch, saved, state, retry_key):
    async def scenario():
        gw = bridge(sdk)
        meta = await admitted_source(gw)
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        checkpoint_whatsapp_tool_send(read_turn_context("contact"), "original", state=state,
            text="Answer", payload={**saved, "text": "Answer", "idempotency_key": "original"})
        before = read_turn_context("contact")
        args = {"to": "+15550000002", "text": "Answer"}
        if retry_key:
            args["idempotency_key"] = retry_key
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args)
        assert value["isError"] and not sdk.calls
        assert read_turn_context("contact") == before
        found = result(await call_inkbox_tool(sdk.client, "agent", "inkbox_lookup_whatsapp_send", {"idempotency_key": "original"}))
        assert found["message_id"] and not sdk.calls
        assert await gw._imessage_turn_state("contact", "result", meta, "Automatic answer") is True
        assert gw._channel_store("whatsapp").summary()["cancelled"] == 1
        restarted = bridge(sdk)
        restarted.sessions = NS(get=Mock())
        await restarted._recover_imessage_inputs()
        restarted.sessions.get.assert_not_called()
        assert not sdk.calls
    asyncio.run(scenario())


def test_other_channel_group_context_does_not_block_explicit_direct_send(sdk, monkeypatch):
    async def scenario():
        gw = bridge(sdk)
        meta = {"channel": "imessage", "conversation_id": CONVERSATION, "message_id": MESSAGE,
                "sender": "+15550000002", "conversation_kind": "group", "participants": ["+15550000002", "+15550000003"]}
        await gw._imessage_turn_state("contact", "admitted", meta, "Question")
        await gw._imessage_turn_state("contact", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
        value = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"to": "+15550000002", "text": "Hello"})
        assert not value.get("isError"), value
        assert json.loads(sdk.calls[0].content)["to"] == "+15550000002"
    asyncio.run(scenario())


@pytest.mark.parametrize("name", ["group_name", "groupName", "group_id", "group_members", "is_group", "isGroup", "unresolved_participant_count", "include_groups"])
def test_removed_arguments_rejected_even_without_schema_validation(sdk, name):
    from inkbox_codex.whatsapp_v2 import send_selector
    args = {"to": "+15550000002", "text": "Hello", name: None}
    with pytest.raises(ValueError, match="direct messages only"):
        send_selector(args)
    value = asyncio.run(call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", args))
    assert value["isError"] and not sdk.requests

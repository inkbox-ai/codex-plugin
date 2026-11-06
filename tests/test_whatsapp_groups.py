"""Existing group routes stay isolated and use the shared mention policy."""
import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex import whatsapp
from inkbox_codex.tools import call_inkbox_tool
from tests.test_whatsapp import sdk, bridge, message, CONVERSATION, MESSAGE, result
from tests.test_group_reply_mode import Client
from tests.test_whatsapp_v2 import channel
from tests.test_sessions import make_session


def group(**changes):
    return message(is_group=True, group_name="Weekend plans", remote_number=None,
                   participants=["+15550000002", "+15550000003"], recipients=[],
                   unresolved_participant_count=0, **changes)


def test_group_sessions_are_conversation_scoped_not_sender_scoped(sdk, monkeypatch):
    async def scenario():
        monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
        gw = bridge(sdk)
        inbound = AsyncMock()
        gw.sessions = NS(get=Mock(return_value=NS(handle_inbound=inbound)))
        for msg in [group(), group(id="00000000-0000-0000-0000-000000000012", sender_number="+15550000003"),
                    group(id="00000000-0000-0000-0000-000000000013", conversation_id="00000000-0000-0000-0000-000000000014")]:
            assert json.loads((await whatsapp.receive(gw, {"data": {"message": msg}})).body) == {"ok": True}
        keys = [call.args[0] for call in gw.sessions.get.call_args_list]
        assert keys[0] == keys[1] and keys[0] != keys[2]
        assert keys[0] == gw._thread_key("whatsapp", CONVERSATION)
        meta = inbound.call_args.args[2]
        assert meta["conversation_kind"] == "group" and meta["group_name"] == "Weekend plans"
        assert meta["remote_number"] is None
    asyncio.run(scenario())


@pytest.mark.parametrize("reply_mode", ["mention", "auto"])
def test_whatsapp_group_mentions_and_background_context(sdk, reply_mode):
    async def scenario():
        sent, typing = [], []
        session = make_session(sent, typing)
        session.cfg.group_reply_mode = reply_mode
        client = session._client = Client()
        for index, raw in enumerate(["Background", "@agent summarize", "Thanks"]):
            meta = {**group(), "channel": "whatsapp", "sender": "+15550000002", "conversation_kind": "group",
                    "message_id": f"00000000-0000-0000-0000-{index + 20:012d}", "raw_text": raw}
            await session.handle_inbound("Group name @agent is metadata only\n" + raw, "whatsapp", meta)
            session._flush_imessage_burst()
            await session._worker
        assert [event[0] for event in client.events] == (["context", "run", "context"] if reply_mode == "mention" else ["run"] * 3)
        assert len(sent) == (1 if reply_mode == "mention" else 3)
        assert not typing
    asyncio.run(scenario())


def test_old_sdk_group_event_reports_upgrade_without_session(sdk, monkeypatch):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: False)
    gw = bridge(sdk)
    gw.sessions = NS(get=Mock())
    response = asyncio.run(whatsapp.receive(gw, {"data": {"message": group()}}))
    assert json.loads(response.body)["ignored"] == "group-sdk-upgrade-required"
    gw.sessions.get.assert_not_called()


def test_old_sdk_read_cannot_mislabel_group_or_thread_as_direct():
    dropped = NS(participants=["+15550000002"], remote_number=None)
    for value in (dropped, [dropped], NS(messages=[dropped])):
        with pytest.raises(RuntimeError, match="0.7.29"):
            whatsapp.require_group_metadata(value)


@pytest.mark.parametrize("change", [{"sender_number": None}, {"unresolved_participant_count": 1}, {"remote_number": "+15550000002"}])
def test_unsupported_group_members_never_wake(sdk, monkeypatch, change):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
    gw = bridge(sdk)
    gw.sessions = NS(get=Mock())
    msg = {**group(), **change}
    asyncio.run(whatsapp.receive(gw, {"data": {"message": msg}}))
    gw.sessions.get.assert_not_called()


def test_group_allowlist_checks_every_member(sdk, monkeypatch):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
    gw = bridge(sdk)
    gw._sender_allowed = lambda sender: sender == "+15550000002"
    gw.sessions = NS(get=Mock())
    response = asyncio.run(whatsapp.receive(gw, {"data": {"message": group()}}))
    assert json.loads(response.body)["ignored"] == "sender-not-allowed"
    gw.sessions.get.assert_not_called()


def test_group_reply_checks_live_roster_and_uses_conversation_only(sdk, monkeypatch):
    async def scenario():
        monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
        gw = bridge(sdk)
        live = NS(**{**sdk.conversation, "is_group": True, "group_name": "Renamed", "remote_number": None,
                     "participants": ["+15550000002", "+15550000003"], "unresolved_participant_count": 0})
        gw._identity.get_whatsapp_conversation = Mock(return_value=live)
        gw._identity.send_whatsapp = Mock(return_value=NS(id="00000000-0000-0000-0000-000000000009"))
        meta = {**group(), "conversation_kind": "group", "sender": "+15550000002", "message_id": MESSAGE}
        await whatsapp.send_reply(gw, "group", "Answer", meta)
        kwargs = gw._identity.send_whatsapp.call_args.kwargs
        assert kwargs["conversation_id"] == CONVERSATION and "to" not in kwargs
        live.unresolved_participant_count = 1
        with pytest.raises(PermissionError):
            await whatsapp.send_reply(gw, "group", "Another answer", meta)
        assert gw._identity.send_whatsapp.call_count == 1
    asyncio.run(scenario())


@pytest.mark.skipif(not whatsapp.group_sdk_available(), reason="Group model integration requires a compatible SDK")
def test_group_quote_media_and_reaction_use_real_sdk(channel):
    from tests.test_whatsapp_v2 import CONVERSATION as conversation, MESSAGE as target
    from inkbox_codex.whatsapp_v2 import receive_reaction
    channel.message.update(is_group=True, group_name="Weekend", remote_number=None,
                           participants=["+15550000002", "+15550000003"], recipients=[], unresolved_participant_count=0)
    async def scenario():
        await receive_reaction(channel.gateway, {"data": {"reaction": channel.reaction}})
        assert channel.inbound.await_count == 1
        meta = channel.inbound.call_args.args[2]
        assert meta["conversation_kind"] == "group" and meta["remote_number"] is None
        assert channel.gateway.sessions.get.call_args.args[0] == channel.gateway._thread_key("whatsapp", conversation)
        channel.gateway._resolve_contact_full.assert_not_awaited()
        sent = await call_inkbox_tool(channel.client, "agent", "inkbox_send_whatsapp", {
            "conversation_id": conversation, "reply_to_message_id": target, "text": "Photo reply",
            "media_id": "00000000-0000-0000-0000-000000000099"})
        assert not sent.get("isError"), sent
        post = next(request for request in channel.requests if request.method == "POST")
        wire = json.loads(post.content)
        assert wire["conversation_id"] == conversation and wire["reply_to_message_id"] == target
        assert wire["media_ids"] == ["00000000-0000-0000-0000-000000000099"] and "to" not in wire
        reacted = await call_inkbox_tool(channel.client, "agent", "inkbox_react_whatsapp", {
            "message_id": target, "reaction": "love", "idempotency_key": "group-reaction"})
        assert not reacted.get("isError"), reacted
    asyncio.run(scenario())


def test_group_reply_gate_is_definite_rejection_not_uncertain():
    error = NS(status_code=409, detail={"error": "whatsapp_group_reply_required"})
    assert whatsapp.known_send_rejection(error)
    assert not whatsapp.known_send_rejection(NS(status_code=409, detail={"error": "other_conflict"}))


@pytest.mark.skipif(not whatsapp.group_sdk_available(), reason="Group model integration requires a compatible SDK")
def test_durable_group_turn_preserves_route_for_tools_and_restart(sdk, monkeypatch):
    from tests.test_whatsapp_autoquote import admitted_source
    from inkbox_codex.imessage import read_turn_context
    async def scenario():
        gw = bridge(sdk)
        sdk.conversation.update(is_group=True, group_name="Weekend", remote_number=None,
                                participants=["+15550000002", "+15550000003"], unresolved_participant_count=0)
        meta = await admitted_source(gw, is_group=True, group_name="Weekend", remote_number=None,
                                     participants=["+15550000002", "+15550000003"], recipients=[], unresolved_participant_count=0)
        await gw._imessage_turn_state("group", "admitted", meta, "Question")
        await gw._imessage_turn_state("group", "started", meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "group")
        restored = read_turn_context("group")["route_meta"]
        assert restored["is_group"] is True and restored["group_name"] == "Weekend"
        assert restored["remote_number"] is None and restored["unresolved_participant_count"] == 0
        assert not whatsapp.unsupported_shape(restored)
        sent = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"text": "Group answer"})
        assert not sent.get("isError"), sent
        assert json.loads(sdk.calls[-1].content)["conversation_id"] == CONVERSATION
        assert read_turn_context("group")["route_meta"]["is_group"] is True
        monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15550000002,+15550000003")
        monkeypatch.setenv("INKBOX_ALLOW_ALL_USERS", "false")
        sdk.conversation["participants"].append("+15550000004")
        before = len(sdk.calls)
        blocked = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"text": "Do not deliver"})
        assert blocked.get("isError") and len(sdk.calls) == before
    asyncio.run(scenario())

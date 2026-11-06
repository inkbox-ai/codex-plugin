"""WhatsApp uses the shared Companion inbox, scoped grants, and source gates."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex import whatsapp
from inkbox_codex.companion import CompanionError, Event
from inkbox_codex.imessage import read_turn_context, write_turn_context
from tests.test_companion import fixture, harness, drained
from tests.test_whatsapp import IDENTITY, sdk


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


SPONSOR = "+12025550103"
PEER = "+12025550101"


def envelope():
    value = fixture("imessage")
    value["event_type"] = "whatsapp.received"
    value["companion"]["channel"] = value["companion"]["reply_context"]["channel"] = "whatsapp"
    value["data"]["message"].update(agent_identity_id=IDENTITY, remote_number=None,
        local_number="+15550000001", group_name="Planning", participants=[PEER, SPONSOR],
        unresolved_participant_count=0, agent_mentioned=True,
        mentions=[{"text": "@123", "phone_number": None, "is_agent": True}], content="@123 pick a time")
    value["data"]["message"].pop("service")
    value["companion"]["history"][-1]["text"] = "@123 pick a time"
    return value


def setup(value, monkeypatch, **kwargs):
    monkeypatch.setattr(whatsapp, "group_sdk_available", lambda: True)
    receiver, sdk, session, sent = harness(value, **kwargs)
    gateway = NS(_identity=NS(id=IDENTITY, agent_handle="test-agent"), cfg=session.cfg,
                 _resolve_contact_full=AsyncMock(return_value=None))
    receiver.prepare_whatsapp = lambda incoming: whatsapp.prepare_companion(gateway, incoming)
    return receiver, sdk, session, sent, gateway


def next_event(value, *, native=False, sender=PEER, sequence=2):
    result = deepcopy(value)
    result["id"] += f"-{sequence}"
    result["companion"] = {k: v for k, v in result["companion"].items()
                           if k not in {"history", "history_complete", "history_next_cursor", "reply_context"}}
    result["companion"].update(phase="live", sequence=sequence)
    result["data"]["message"].update(id=f"20000000-0000-4000-8000-{sequence + 200:012d}",
        sender_number=sender, sender_access="direct" if sender == SPONSOR else "sponsored",
        content="@123 next" if native else "a quiet update", agent_mentioned=native)
    return result


@pytest.mark.parametrize("response_mode,expected", [("safe", ["run", "context"]), ("relaxed", ["run", "run"])])
def test_shared_history_native_mentions_and_sponsored_response_mode(monkeypatch, response_mode, expected):
    async def scenario():
        value = envelope()
        receiver, sdk, session, sent, gateway = setup(value, monkeypatch, reply_mode="mention",
            response_mode=response_mode, allowed=lambda number: number == SPONSOR)
        session.cfg.allowed_users = [SPONSOR]
        try:
            assert await receiver.accept(value)
            await drained(receiver)
            assert await receiver.accept(next_event(value, native=True))
            await drained(receiver)
            assert [kind for kind, _ in session._client.events] == expected
            assert sdk.loads == 1 and sdk.authorizations >= 3
            assert all(item[2] == "whatsapp" for item in sent)
            assert all(item[3]["conversation_id"] == value["companion"]["conversation_id"] for item in sent)
            assert "@test-agent" in session._client.events[0][1]
            meta = sent[0][3]
            assert meta["contact"] is None and meta["contact_memories"] == []
            assert not await receiver.accept(value)
            await drained(receiver)
            assert len(session._client.events) == 2
            assert session._client.events[1][1].count("Can we meet tomorrow afternoon?") == 0
            gateway._resolve_contact_full.assert_not_awaited()
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_live_first_recovers_snapshot_without_waking_on_historical_mentions(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, sdk, session, sent, _ = setup(value, monkeypatch, reply_mode="mention", response_mode="relaxed")
        try:
            await receiver.accept(next_event(value, native=False))
            await drained(receiver)
            assert [kind for kind, _ in session._client.events] == ["context", "context"]
            assert not sent and sdk.loads == 1
            assert "Can we meet tomorrow afternoon?" in session._client.events[0][1]
            assert "a quiet update" in session._client.events[1][1]
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_revoked_activation_blocks_new_turn_and_cannot_cross_destination(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, sdk, session, sent, gateway = setup(value, monkeypatch)
        try:
            await receiver.accept(value)
            await drained(receiver)
            sdk.denied = True
            assert await receiver.accept(next_event(value, native=True, sender=SPONSOR))
            await drained(receiver)
            assert len(session._client.events) == len(sent) == 1
            meta = sent[0][3]
            write_turn_context("wa-companion", session.cfg, meta)
            route = read_turn_context("wa-companion")["route_meta"]
            assert route["companion_sponsor"] == SPONSOR
            with pytest.raises(PermissionError, match="revoked"):
                whatsapp.authorize_companion(NS(companion=sdk), session.cfg, route)
            sdk.denied = False
            with pytest.raises(CompanionError, match="source"):
                whatsapp.authorize_companion(NS(companion=sdk), session.cfg,
                    {**route, "conversation_id": "20000000-0000-4000-8000-000000000099"})
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_companion_source_rejects_direct_or_unresolved_group(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, _, _, _, _ = setup(value, monkeypatch)
        try:
            for change in ({"is_group": False}, {"unresolved_participant_count": 1},
                           {"sender_number": "+19999999999"}, {"agent_identity_id": "wrong"}):
                invalid = deepcopy(value)
                invalid["data"]["message"].update(change)
                if "is_group" in change or "agent_identity_id" in change:
                    with pytest.raises(CompanionError):
                        await receiver.accept(invalid)
                else:
                    assert not await receiver.accept(invalid)
        finally:
            await receiver.close()
    asyncio.run(scenario())



def test_grant_bypasses_local_peer_filter_only_for_its_group_and_rechecks_before_send(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, sdk, session, _, gateway = setup(value, monkeypatch)
        cfg = session.cfg
        cfg.allowed_users, cfg.allow_all_users = [SPONSOR], False
        prepared = await receiver.prepare_whatsapp(value)
        route = receiver.meta(Event.parse(prepared))
        route.update(companion_receipt_token="stable-receipt", companion_reply_event_id=value["id"])
        conversation = {**value["data"]["message"], "id": value["companion"]["conversation_id"]}
        identity = NS(id=IDENTITY, get_whatsapp_conversation=Mock(return_value=conversation),
                      send_whatsapp=Mock(return_value={"id": "accepted"}))
        checkpoint = Mock()
        gateway._identity, gateway._inkbox = identity, NS(companion=sdk)
        gateway._sender_allowed = lambda number: number == SPONSOR
        gateway._channel_store = Mock(return_value=NS(mark=Mock(), record_outbound=Mock()))
        gateway._companion = Mock(return_value=NS(reply_sending=checkpoint))
        try:
            whatsapp.validate_group_destination(identity, route["conversation_id"], cfg,
                client=gateway._inkbox, route=route)
            other = "20000000-0000-4000-8000-000000000099"
            identity.get_whatsapp_conversation.return_value = {**conversation, "id": other}
            with pytest.raises(PermissionError):
                whatsapp.validate_group_destination(identity, other, cfg, client=gateway._inkbox, route=route)
            identity.get_whatsapp_conversation.return_value = conversation
            await whatsapp.send_reply(gateway, "group", "Answer", route)
            first = identity.send_whatsapp.call_args.kwargs
            await whatsapp.send_reply(gateway, "group", "Answer", route)
            assert identity.send_whatsapp.call_args.kwargs == first
            assert first["conversation_id"] == route["conversation_id"] and "to" not in first
            assert checkpoint.call_count == 2
            interim = {key: value for key, value in route.items() if key != "companion_reply_event_id"}
            await whatsapp.send_reply(gateway, "group", "Allow this command?", interim)
            prompt_key = identity.send_whatsapp.call_args.kwargs["idempotency_key"]
            await whatsapp.send_reply(gateway, "group", "Allow this command?", interim)
            assert identity.send_whatsapp.call_args.kwargs["idempotency_key"] != prompt_key
            sdk.denied = True
            with pytest.raises(PermissionError, match="revoked"):
                await whatsapp.send_reply(gateway, "group", "Blocked", route)
            assert identity.send_whatsapp.call_count == 4
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_ordinary_companion_envelope_does_not_grant_sponsored_access(monkeypatch):
    async def scenario():
        value = envelope()
        value["companion"] = {key: value["companion"][key] for key in ("scope_id", "conversation_id", "channel", "sequence")}
        value["companion"]["phase"] = "ordinary"
        receiver, _, session, sent, _ = setup(value, monkeypatch, allowed=lambda number: number == SPONSOR)
        try:
            await receiver.accept(value)
            await drained(receiver)
            assert not session._client.events and not sent
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_retry_identity_does_not_depend_on_contact_display_name(monkeypatch):
    async def scenario():
        value = envelope()
        value["data"]["message"]["mentions"] = [{"text": "@123", "phone_number": PEER, "is_agent": False}]
        receiver, _, _, _, gateway = setup(value, monkeypatch)
        gateway._resolve_contact_full = AsyncMock(side_effect=[{"name": "Alex"}, {"name": "New name"}])
        try:
            await receiver.accept(value)
            await drained(receiver)
            assert not await receiver.accept(value)
            await drained(receiver)
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.skipif(not whatsapp.group_sdk_available(), reason="Actual group models require compatible SDK")
def test_explicit_tool_preserves_companion_grant_after_durable_context_restore(sdk, monkeypatch):
    from tests.test_whatsapp import bridge, CONVERSATION, MESSAGE
    from tests.test_whatsapp_groups import group
    from inkbox_codex.tools import call_inkbox_tool
    async def scenario():
        gateway = bridge(sdk)
        sdk.conversation.update(is_group=True, group_name="Planning", remote_number=None,
            participants=["+15550000002", "+15550000003"], unresolved_participant_count=0)
        scope, activation = "20000000-0000-4000-8000-000000000001", "20000000-0000-4000-8000-000000000002"
        page = NS(scope_id=scope, activation_id=activation, conversation_id=CONVERSATION,
            channel="whatsapp", reply_context={"channel": "whatsapp", "conversation_id": CONVERSATION})
        check = Mock(return_value=page)
        monkeypatch.setattr(type(sdk.client.companion), "activation_messages", lambda self, *args, **kwargs: check(*args, **kwargs))
        meta = {**group(), "channel": "whatsapp", "sender": "+15550000003", "message_id": MESSAGE,
            "conversation_kind": "group", "companion": True, "companion_scope_id": scope,
            "companion_activation_id": activation, "companion_sponsor": "+15550000002"}
        write_turn_context("companion-tools", gateway.cfg, meta)
        monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "companion-tools")
        monkeypatch.setenv("INKBOX_IDENTITY", "agent")
        monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15550000002")
        monkeypatch.setenv("INKBOX_ALLOW_ALL_USERS", "false")
        result = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"text": "Same group answer"})
        assert not result.get("isError"), result
        assert check.called and len(sdk.calls) == 1
        check.side_effect = PermissionError("activation revoked")
        result = await call_inkbox_tool(sdk.client, "agent", "inkbox_send_whatsapp", {"text": "Do not send"})
        assert result.get("isError") and len(sdk.calls) == 1
        result = await call_inkbox_tool(sdk.client, "agent", "inkbox_get_whatsapp_thread", {"message_id": MESSAGE})
        assert result.get("isError") and "supplied Companion history" in str(result)
    asyncio.run(scenario())


@pytest.mark.parametrize("valid", [True, False])
def test_signed_whatsapp_companion_reaches_shared_receiver(monkeypatch, valid):
    import json
    from inkbox_codex.gateway import InkboxGateway
    from inkbox_codex.config import BridgeConfig
    from tests.test_gateway_dedup import _FakeRequest
    async def scenario():
        gateway = InkboxGateway(BridgeConfig(require_signature=False))
        receiver = NS(accept=AsyncMock(return_value=True))
        monkeypatch.setattr(gateway, "_companion", lambda: receiver)
        monkeypatch.setattr("inkbox_codex.gateway.match_provider", lambda _: NS(name="inkbox", verify=lambda **_: valid))
        response = await gateway._handle_webhook(_FakeRequest(json.dumps(envelope()).encode()))
        assert response.status == (200 if valid else 401)
        assert receiver.accept.await_count == int(valid)
    asyncio.run(scenario())


def test_live_first_native_mention_inside_snapshot_wakes_current_source(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, sdk, session, sent, _ = setup(value, monkeypatch, reply_mode="mention", response_mode="relaxed")
        incoming = next_event(value, native=True)
        source = incoming["data"]["message"]
        sdk.c["history"].append({"id": source["id"], "author": PEER, "text": source["content"],
            "historical": False, "is_trigger": False, "attachments": [], "sender_access": "sponsored"})
        try:
            await receiver.accept(incoming)
            await drained(receiver)
            assert [kind for kind, _ in session._client.events] == ["run"]
            assert len(sent) == 1 and sent[0][3]["source_message_id"] == source["id"]
            assert sent[0][3]["whatsapp_mentioned"] is True
            assert sent[0][3]["message_id"] == source["id"]
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_native_self_mention_answers_pending_approval_without_cancelling(monkeypatch):
    from inkbox_codex.escalation import PendingInteraction
    async def scenario():
        value = envelope()
        receiver, _, session, sent, _ = setup(value, monkeypatch, reply_mode="mention")
        waiting = asyncio.Event()
        original = session._client.run
        async def approving(text):
            session.pending = PendingInteraction(kind="permission", prompt_text="approve?",
                future=asyncio.get_running_loop().create_future())
            waiting.set()
            assert await asyncio.wait_for(session.pending.future, 3) == "allow"
            session.pending = None
            return await original(text)
        session._client.run = approving
        try:
            await receiver.accept(value)
            await asyncio.wait_for(waiting.wait(), 3)
            answer = next_event(value, native=True, sender=SPONSOR)
            answer["data"]["message"]["content"] = "@123 allow"
            await receiver.accept(answer)
            await drained(receiver)
            assert len(sent) == 1 and len(session._client.events) == 1
            meta = sent[0][3]
            assert session._companion_control_text("@123 /stop", meta) == "/stop"
            assert session._companion_control_text("@999 /stop", meta) == "@999 /stop"
            other = {**meta, "mentions": [{"text": "@123", "is_agent": False, "display_name": "agent"}]}
            assert session._companion_control_text("@123 /stop", other) == "@123 /stop"
        finally:
            await receiver.close()
    asyncio.run(scenario())



def test_activation_read_outage_is_owned_by_durable_drain_and_retries(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, sdk, session, sent, _ = setup(value, monkeypatch)
        monkeypatch.setattr("inkbox_codex.companion.RETRY_MAX_SECONDS", 0.01)
        try:
            await receiver.accept(value)
            await drained(receiver)
            original = sdk.activation_messages
            calls = 0
            def temporarily_unavailable(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise TimeoutError("temporary authorization read outage")
                return original(*args, **kwargs)
            sdk.activation_messages = temporarily_unavailable
            incoming = next_event(value, native=True, sender=SPONSOR)
            assert await receiver.accept(incoming)
            async def completed():
                while receiver.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (incoming["id"],)).fetchone()[0] != "done":
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(completed(), 3)
            assert calls == 2 and len(sent) == 2 and len(session._client.events) == 2
        finally:
            await receiver.close()
    asyncio.run(scenario())



def test_history_only_snapshot_does_not_label_trigger_with_absent_live_mentions(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, _, session, sent, _ = setup(value, monkeypatch, reply_mode="mention", response_mode="relaxed")
        metas = []
        original = session.submit_companion
        async def capture(text, mode, meta, **kwargs):
            metas.append(deepcopy(meta))
            return await original(text, mode, meta, **kwargs)
        session.submit_companion = capture
        try:
            await receiver.accept(next_event(value, native=True))
            await drained(receiver)
            assert [kind for kind, _ in session._client.events] == ["context", "run"]
            history, current = metas
            assert history["companion_context_only"] is True
            assert history["whatsapp_mentioned"] is False and history["mentions"] == []
            assert history["original_text"] is None and history["rendered_text"] == ""
            assert current["whatsapp_mentioned"] is True and current["mentions"][0]["is_agent"] is True
            assert len(sent) == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("change", [{"participants": [PEER]}, {"unresolved_participant_count": 1}])
def test_inadmissible_current_roster_does_not_block_next_sequence(monkeypatch, change):
    async def scenario():
        value = envelope()
        receiver, _, session, _, _ = setup(value, monkeypatch)
        stale = deepcopy(value)
        stale["data"]["message"].update(change)
        try:
            assert not await receiver.accept(stale)
            assert not receiver.inbox.scopes()
            following = deepcopy(value)
            following["companion"]["sequence"] = 2
            following["id"] += "-next"
            assert await receiver.accept(following)
            await drained(receiver)
            assert len(session._client.events) == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_retry_ignores_current_projection_but_rejects_changed_source(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, _, session, _, _ = setup(value, monkeypatch)
        try:
            await receiver.accept(value)
            await drained(receiver)
            retry = deepcopy(value)
            retry["data"]["message"].update(group_name="Renamed", participants=[SPONSOR, PEER, "+12025550999"],
                reactions=[{"emoji": "👍"}], is_read=True, updated_at="later", status="read",
                reply_to_message_id="20000000-0000-4000-8000-000000000091",
                thread_id="20000000-0000-4000-8000-000000000092",
                thread_root_message_id="20000000-0000-4000-8000-000000000093")
            retry["data"]["message"]["mentions"][0]["phone_number"] = PEER
            assert not await receiver.accept(retry)
            await drained(receiver)
            assert len(session._client.events) == 1
            stored = receiver.inbox.db.execute("SELECT payload FROM events WHERE event_id=?", (value["id"],)).fetchone()[0]
            import json
            assert json.loads(stored)["data"]["message"].get("reply_to_message_id") == value["data"]["message"].get("reply_to_message_id")
            retry["data"]["message"]["content"] = "changed source body"
            with pytest.raises(CompanionError, match="Conflicting"):
                await receiver.accept(retry)
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("live", [False, True])
def test_companion_current_media_reaches_native_host_input(monkeypatch, live):
    async def scenario():
        value = envelope()
        receiver, _, session, _, gateway = setup(value, monkeypatch, response_mode="relaxed")
        gateway._identity.get_whatsapp_media = Mock(return_value=NS(status="ready", url="https://example.test/image", content_type="image/png", size=12))
        async def download(text, attachments, *, prefix, image_paths):
            assert attachments[0]["url"] == "https://example.test/image"
            image_paths.append("/tmp/companion-fixture.png")
            return text + "\n[refreshed attachment context]"
        gateway._with_media = download
        seen = []
        async def run(text, **kwargs):
            seen.append((text, kwargs)); return "Answer"
        session._client.run = run
        try:
            if live:
                await receiver.accept(value)
                await drained(receiver)
                value = next_event(value, native=True)
            value["data"]["message"].update(content="", media=[{"id": "media-owned", "url": None}])
            assert await receiver.accept(value)
            await drained(receiver)
            assert seen[-1][1]["image_paths"] == ["/tmp/companion-fixture.png"]
            assert "refreshed attachment context" in seen[-1][0]
            gateway._identity.get_whatsapp_media.assert_called_once_with("media-owned")
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_overlong_companion_answer_sends_notice_and_unblocks_next_event(monkeypatch):
    async def scenario():
        value = envelope()
        receiver, _, session, _, gateway = setup(value, monkeypatch, response_mode="relaxed")
        gateway.cfg.base_url = "https://example.test"
        gateway._identity.send_whatsapp = Mock(return_value=NS(id="sent"))
        gateway._channel_store = Mock(return_value=Mock())
        gateway._companion = lambda: receiver
        allowed = AsyncMock(return_value=True)
        monkeypatch.setattr(whatsapp, "current_route_allowed", allowed)
        async def send(chat, content, mode, meta):
            await whatsapp.send_reply(gateway, chat, content, meta)
        session.send_fn = send
        session._client.run = AsyncMock(side_effect=["x" * 4001, "Next answer"])
        try:
            await receiver.accept(value)
            await drained(receiver)
            assert gateway._identity.send_whatsapp.call_args.kwargs["text"] == whatsapp.REPLY_REJECTED_NOTICE
            assert receiver.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (value["id"],)).fetchone()[0] == "done"
            await receiver.accept(next_event(value, native=True))
            await drained(receiver)
            assert gateway._identity.send_whatsapp.call_count == 2
            assert gateway._identity.send_whatsapp.call_args.kwargs["text"] == "Next answer"
            assert allowed.await_count == 2
            assert not receiver.inbox.scopes()
        finally:
            await receiver.close()
    asyncio.run(scenario())

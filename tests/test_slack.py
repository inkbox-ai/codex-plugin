"""Offline Slack routing, replies, tool contracts, and approval boundaries."""

import asyncio
import copy
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.prompts import frame_inbound
from inkbox_codex.sessions import SessionManager, _Turn
from inkbox_codex.slack import (
    SLACK_ATTENTION_EVENTS, SLACK_INCOMING_EVENTS, inbound_message,
    reconcile_subscription, run_tool, send_reply,
)
from inkbox_codex.tools import build_inkbox_mcp_server_config, call_inkbox_tool, mcp_tool_list
from tests.test_sessions import make_session


IDENTITY = "00000000-0000-4000-8000-000000000001"
CONNECTION = "00000000-0000-4000-8000-000000000002"


def event(**overrides):
    result = {"id": "evt_1", "event_type": "slack.mention_received", "data": {
        "identity_id": IDENTITY, "connection_id": CONNECTION, "workspace_id": "T_TEST",
        "conversation_id": "C_TEST", "message_ts": "1234567890.000001", "actor_id": "U_ALICE",
        "thread_ts": None, "message_kinds": ["channel", "mention"],
        "event": {"type": "message", "text": "<@U_AGENT> hello"},
    }}
    result["data"].update(overrides)
    return result


class Sessions:
    def __init__(self):
        self.turns = []
        self.keys = set()

    def has_session(self, key):
        return key in self.keys

    def get(self, key):
        self.keys.add(key)

        async def handle(text, mode, meta):
            self.turns.append((key, text, mode, meta))

        return NS(handle_inbound=handle)


@pytest.fixture
def gw(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    result = InkboxGateway(BridgeConfig(slack_enabled=True))
    result._identity = NS(id=IDENTITY)
    result.sessions = Sessions()
    return result


def test_mention_thread_followup_and_duplicate_deliveries(gw):
    async def scenario():
        first = event()
        await gw._on_slack_received(first)
        await gw._on_slack_received(copy.deepcopy(first))
        followup = event(message_ts="1234567890.000002", thread_ts="1234567890.000001",
                         message_kinds=["channel", "thread"])
        followup["id"] = "evt_2"
        await gw._on_slack_received(followup)
        assert len(gw.sessions.turns) == 2
        assert gw.sessions.turns[0][0] == gw.sessions.turns[1][0]
        assert gw.sessions.turns[0][3]["thread_ts"] == "1234567890.000001"

    asyncio.run(scenario())


@pytest.mark.parametrize("overrides", [
    {"identity_id": "different"}, {"actor_id": None}, {"thread_ts": 123.4},
    {"event": {"text": "echo", "bot_id": "B_BOT"}},
    {"event": {"text": "echo", "subtype": "bot_message"}},
    {"message_kinds": ["channel"]},
    {"message_kinds": ["channel", "thread"], "thread_ts": "1234567880.000001"},
])
def test_ignored_inputs_do_not_wake(gw, overrides):
    asyncio.run(gw._on_slack_received(event(**overrides)))
    assert not gw.sessions.turns


def test_disabled_and_disallowed_senders_do_not_wake(gw):
    gw.cfg.slack_enabled = False
    asyncio.run(gw._on_slack_received(event()))
    gw.cfg.slack_enabled = True
    gw.cfg.allowed_users = ["T_OTHER:U_ALICE"]
    asyncio.run(gw._on_slack_received(event()))
    assert not gw.sessions.turns
    gw.cfg.allowed_users = ["T_TEST:U_ALICE"]
    asyncio.run(gw._on_slack_received(event()))
    assert len(gw.sessions.turns) == 1


def test_retry_after_session_admission_failure(gw):
    original = gw.sessions.get

    async def fail(*args):
        raise RuntimeError("admission failed")

    gw.sessions.get = lambda key: NS(handle_inbound=fail)
    with pytest.raises(RuntimeError):
        asyncio.run(gw._on_slack_received(event()))
    gw.sessions.get = original
    asyncio.run(gw._on_slack_received(event()))
    assert len(gw.sessions.turns) == 1


def test_dm_and_thread_sessions_are_separate_and_preserve_coordinates():
    top = inbound_message(event(conversation_id="D_TEST", message_kinds=["dm"]), IDENTITY)
    threaded = inbound_message(event(conversation_id="D_TEST", message_kinds=["dm", "thread"],
                                     thread_ts="1234567890.000001"), IDENTITY)
    assert top[0] != threaded[0]
    assert top[2]["thread_ts"] is None
    assert threaded[2]["thread_ts"] == "1234567890.000001"
    other_workspace = inbound_message(event(connection_id="another-connection"), IDENTITY)
    assert other_workspace[0] != inbound_message(event(), IDENTITY)[0]


def test_files_are_metadata_not_claimed_downloads():
    incoming = inbound_message(event(event={"files": [{"id": "F_TEST", "name": "brief.txt"}]}), IDENTITY)
    framed = frame_inbound("slack", incoming[2], incoming[1])
    assert "F_TEST" in framed and "not downloaded" in framed
    assert CONNECTION in framed and "1234567890.000001" in framed


def test_watched_sessions_survive_manager_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    manager = SessionManager(BridgeConfig(), Mock(), {}, {})
    key = inbound_message(event(), IDENTITY)[0]
    assert not manager.has_session(key)
    manager._save_session_id(key, "codex-thread")
    restarted = SessionManager(BridgeConfig(), Mock(), {}, {})
    assert restarted.has_session(key)


def test_replies_preserve_formatting_thread_and_idempotency(gw):
    client = Mock()
    client.slack.send_message.return_value = NS(id="action-1", status="sent")
    gw._inkbox = client
    _, _, meta = inbound_message(event(), IDENTITY)
    asyncio.run(gw.send_to_contact("session", "*hello*\n```code```", "slack", meta))
    first = client.slack.send_message.call_args
    send_reply(client, meta, "*hello*\n```code```")
    assert client.slack.send_message.call_args == first
    assert first.kwargs["thread_ts"] == "1234567890.000001"
    assert first.kwargs["text"] == "*hello*\n```code```"
    send_reply(client, {**meta, "source_event_id": "evt_2"}, "*hello*\n```code```")
    assert client.slack.send_message.call_args.kwargs["idempotency_key"] != first.kwargs["idempotency_key"]


@pytest.mark.parametrize("status", ["sending", "unknown", "failed"])
def test_unconfirmed_send_is_not_automatically_retried(gw, status):
    client = Mock()
    client.slack.send_message.return_value = NS(id="action-1", status=status)
    _, _, meta = inbound_message(event(), IDENTITY)
    with pytest.raises(RuntimeError, match="action-1"):
        send_reply(client, meta, "hello")
    assert client.slack.send_message.call_count == 1
    assert gw._note_sync_send_failure("session", "slack", meta, "hello", status) is None


def test_subscription_reconciliation_does_not_remove_other_receivers():
    client = Mock()
    unrelated = NS(id="other", url="https://other.example/webhook", event_types=["slack.mention_received"])
    client.webhooks.subscriptions.list.return_value = [unrelated]
    reconcile_subscription(client, IDENTITY, "https://agent.example/webhook?channel=slack")
    created = client.webhooks.subscriptions.create.call_args.kwargs
    assert created == {
        "agent_identity_id": IDENTITY,
        "url": "https://agent.example/webhook?channel=slack",
        "event_types": ["slack.dm_received", "slack.group_dm_received",
                        "slack.mention_received", "slack.thread_reply_received"],
    }
    assert set(created["event_types"]) == set(SLACK_ATTENTION_EVENTS)
    assert "slack.channel_message_received" not in created["event_types"]
    client.webhooks.subscriptions.delete.assert_not_called()
    existing = NS(id="ours", status="active", **created)
    client.webhooks.subscriptions.list.return_value = [unrelated, existing]
    reconcile_subscription(client, IDENTITY, existing.url)
    assert client.webhooks.subscriptions.create.call_count == 1
    client.webhooks.subscriptions.update.assert_not_called()


def test_slack_tools_opt_in_and_child_process_config(monkeypatch):
    monkeypatch.delenv("INKBOX_SLACK_ENABLED", raising=False)
    assert not any(t["name"].startswith("inkbox_slack_") for t in mcp_tool_list())
    monkeypatch.setenv("INKBOX_SLACK_ENABLED", "1")
    assert len([t for t in mcp_tool_list() if t["name"].startswith("inkbox_slack_")]) == 6
    config, names = build_inkbox_mcp_server_config(BridgeConfig(slack_enabled=True))
    assert config["env"]["INKBOX_SLACK_ENABLED"] == "1"
    assert "mcp__inkbox__inkbox_slack_send_message" in names


def test_tools_scope_reads_and_writes_to_configured_identity(monkeypatch):
    monkeypatch.setenv("INKBOX_SLACK_ENABLED", "1")
    client = Mock()
    client.get_identity.return_value = NS(id=IDENTITY)
    client.slack.list_connections.return_value = NS(connections=[NS(id=CONNECTION)])
    run_tool(client, "agent", "inkbox_slack_search", {"q": "release"})
    client.slack.search_messages.assert_called_once_with(identity_id=IDENTITY, q="release")
    with pytest.raises(ValueError, match="does not belong"):
        run_tool(client, "agent", "inkbox_slack_list_messages",
                 {"connection_id": "not-owned", "conversation_id": "C_TEST"})
    client.slack.list_messages.assert_not_called()
    client.slack.send_message.return_value = {"status": "unknown", "id": "action-1"}
    result = asyncio.run(call_inkbox_tool(client, "agent", "inkbox_slack_send_message", {
        "connection_id": CONNECTION, "conversation_id": "C_TEST", "text": "hello",
        "thread_ts": "1234567890.000001", "idempotency_key": "test-send-1",
    }))
    assert json.loads(result["content"][0]["text"])["status"] == "unknown"
    assert client.slack.send_message.call_count == 1


@pytest.mark.parametrize("mode", ["auto", "mention"])
def test_group_approval_cannot_be_answered_by_another_slack_user(mode):
    async def scenario():
        from inkbox_codex.escalation import PendingInteraction

        session = make_session([])
        session.cfg.group_reply_mode = mode
        meta = inbound_message(event(), IDENTITY)[2]
        meta["slack_mentioned"] = False
        session._current_turn = _Turn(text="work", reply_mode="slack", reply_meta=meta)
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(kind="permission", future=future, prompt_text="Allow?")
        session._worker = asyncio.create_task(asyncio.Event().wait())
        await session.handle_inbound("yes", "slack", {**meta, "sender": "T_TEST:U_BOB", "raw_text": "yes"})
        assert not future.done()
        queued = session._queue.get_nowait()
        assert queued.context_only
        await session.handle_inbound("yes", "slack", {**meta, "raw_text": "yes"})
        assert future.done()
        session._worker.cancel()
        await asyncio.gather(session._worker, return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.parametrize("kinds", [["channel", "thread"], ["group_dm", "thread"]])
def test_slack_mention_mode_keeps_unmentioned_followups_as_context(kinds):
    from tests.test_group_reply_mode import Client

    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = "mention"
        client = session._client = Client()
        for index, mentioned in enumerate((False, True, False, True, False)):
            payload = event(
                message_ts=f"1234567890.00000{index + 1}", thread_ts="1234567890.000001",
                message_kinds=kinds + (["mention"] if mentioned else []),
                # Typed names and other users' mentions are not native agent mentions.
                event={"text": "<@U_AGENT> help" if mentioned else "@agent <@U_OTHER> chatter"},
            )
            _, body, meta = inbound_message(payload, IDENTITY)
            await session.handle_inbound(body, "slack", meta)
            await session._worker
        assert [kind for kind, _ in client.events] == ["context", "run", "context", "run", "context"]
        assert len(sent) == 2
        assert client.interrupts == 0
        assert all(reply[2] == "slack" and reply[3]["thread_ts"] == "1234567890.000001" for reply in sent)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode,kinds", [
    ("auto", ["channel", "thread"]), ("auto", ["group_dm"]),
    ("mention", ["dm"]), ("mention", ["dm", "thread"]),
])
def test_slack_default_and_direct_dm_replies_are_unchanged(mode, kinds):
    from tests.test_group_reply_mode import Client

    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = mode
        client = session._client = Client()
        _, body, meta = inbound_message(event(message_kinds=kinds, event={"text": "Hello"}), IDENTITY)
        await session.handle_inbound(body, "slack", meta)
        await session._worker
        assert [kind for kind, _ in client.events] == ["run"]
        assert len(sent) == 1

    asyncio.run(scenario())


def test_quiet_slack_followup_does_not_interrupt_or_change_active_reply():
    from tests.test_group_reply_mode import Client

    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = "mention"
        started, finish = asyncio.Event(), asyncio.Event()

        class SlowClient(Client):
            async def run(self, prompt):
                self.events.append(("run", prompt))
                started.set()
                await finish.wait()
                return "Answer"

        client = session._client = SlowClient()
        _, body, meta = inbound_message(event(), IDENTITY)
        await session.handle_inbound(body, "slack", meta)
        await started.wait()
        _, quiet, quiet_meta = inbound_message(event(
            actor_id="U_BOB", message_ts="1234567890.000002", thread_ts="1234567890.000001",
            message_kinds=["channel", "thread"], event={"text": "Thanks"},
        ), IDENTITY)
        await session.handle_inbound(quiet, "slack", quiet_meta)
        assert client.interrupts == 0
        assert session.reply_meta["sender"] == meta["sender"]
        finish.set()
        await session._worker
        assert [kind for kind, _ in client.events] == ["run", "context"]
        assert len(sent) == 1 and sent[0][3]["sender"] == meta["sender"]

    asyncio.run(scenario())


def test_tools_and_replies_match_real_slack_sdk_wire(monkeypatch):
    pytest.importorskip("inkbox.slack", reason="requires the Slack-capable SDK preview")
    import httpx
    from inkbox import Inkbox

    requests = []
    connection = {
        "id": CONNECTION, "identity_id": IDENTITY, "workspace_id": "T_TEST",
        "workspace_name": "Example", "bot_user_id": "U_AGENT", "status": "connected",
        "scopes": [], "created_at": "2026-01-01T00:00:00Z",
    }
    action = {
        "id": "00000000-0000-4000-8000-000000000003", "connection_id": CONNECTION,
        "status": "sent", "conversation_id": "C_TEST", "message_ts": "1234567890.000003",
        "thread_ts": "1234567890.000001",
    }

    def handle(request):
        requests.append(request)
        path = request.url.path
        if path.endswith("/connections"):
            payload = {"connections": [connection], "installation_available": False}
        elif path.endswith("/conversations"):
            payload = {"conversations": [{"id": "C_TEST"}], "next_cursor": "page-2"}
        elif request.method == "POST" or "/actions/" in path:
            payload = action
        else:
            payload = {"messages": [], "next_cursor": "page-2", "has_more": True}
        return httpx.Response(200, json=payload)

    client = Inkbox(api_key="synthetic-test-key", base_url="https://api.example")
    monkeypatch.setattr(client, "get_identity", lambda handle: NS(id=IDENTITY))
    client._api_http._client.close()
    client._api_http._client = httpx.Client(
        base_url="https://api.example/api/v1", transport=httpx.MockTransport(handle),
    )
    try:
        for name, args in [
            ("list_connections", {}),
            ("list_conversations", {"connection_id": CONNECTION}),
            ("list_messages", {"connection_id": CONNECTION, "conversation_id": "C_TEST",
                               "thread_ts": "1234567890.000001", "cursor": "page-1"}),
            ("search", {"q": "release"}),
            ("send_message", {"connection_id": CONNECTION, "conversation_id": "C_TEST",
                              "text": "Hello", "idempotency_key": "test-1"}),
            ("get_action", {"connection_id": CONNECTION, "action_id": action["id"]}),
        ]:
            run_tool(client, "agent", "inkbox_slack_" + name, args)
        search = next(r for r in requests if r.url.path.endswith("/search"))
        assert dict(search.url.params) == {"q": "release", "identity_id": IDENTITY, "limit": "50"}
        history = next(r for r in requests if r.method == "GET" and r.url.path.endswith("/messages"))
        assert history.url.params["thread_ts"] == "1234567890.000001"
        assert history.url.params["cursor"] == "page-1"
        send = next(r for r in requests if r.method == "POST")
        assert send.headers["Idempotency-Key"] == "test-1"
        assert json.loads(send.content) == {"conversation_id": "C_TEST", "text": "Hello"}
        send_reply(client, inbound_message(event(), IDENTITY)[2], "Reply")
        assert json.loads(requests[-1].content)["thread_ts"] == "1234567890.000001"
    finally:
        client.close()


@pytest.mark.parametrize("event_type", SLACK_INCOMING_EVENTS)
def test_event_router_recognizes_all_five_message_events(gw, event_type):
    from unittest.mock import AsyncMock

    gw._on_slack_received = AsyncMock()
    envelope = event()
    envelope["event_type"] = event_type
    asyncio.run(gw._route_inkbox_event(event_type, envelope))
    gw._on_slack_received.assert_awaited_once_with(envelope)


def test_new_event_router_preserves_attention_and_overlap_dedup(gw):
    async def scenario():
        plain = event(message_kinds=["channel"], event={"text": "Unrelated chatter"})
        plain["event_type"] = "slack.channel_message_received"
        await gw._route_inkbox_event(plain["event_type"], plain)
        assert not gw.sessions.turns
        mention = event()
        mention["event_type"] = "slack.mention_received"
        await gw._route_inkbox_event(mention["event_type"], mention)
        duplicate = copy.deepcopy(mention)
        duplicate["event_type"] = "slack.thread_reply_received"
        await gw._route_inkbox_event(duplicate["event_type"], duplicate)
        followup = event(message_ts="1234567890.000002", thread_ts="1234567890.000001",
                         message_kinds=["channel", "thread"], event={"text": "Follow-up"})
        followup.update(id="evt_2", event_type="slack.thread_reply_received")
        await gw._route_inkbox_event(followup["event_type"], followup)
        assert len(gw.sessions.turns) == 2
        assert gw.sessions.turns[0][0] == gw.sessions.turns[1][0]

    asyncio.run(scenario())


@pytest.mark.parametrize("event_type,kinds,conversation", [
    ("slack.dm_received", ["dm"], "D_TEST"),
    ("slack.group_dm_received", ["group_dm"], "G_TEST"),
])
def test_new_direct_event_router_wakes_with_reply_coordinates(gw, event_type, kinds, conversation):
    envelope = event(conversation_id=conversation, message_kinds=kinds,
                     event={"text": "Hello"})
    envelope["event_type"] = event_type
    asyncio.run(gw._route_inkbox_event(event_type, envelope))
    assert len(gw.sessions.turns) == 1
    assert gw.sessions.turns[0][3]["conversation_id"] == conversation

"""Native WhatsApp work retains receipt and control ownership through delivery."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest

from inkbox_codex.escalation import PendingInteraction
from inkbox_codex.sessions import ContactSession
from inkbox_codex.whatsapp_v2 import receive_reaction
from tests.test_imessage_sessions import Client, finish_burst, make_session, source
from tests.test_whatsapp import sdk
from tests.test_whatsapp_v2 import channel, CONVERSATION, MESSAGE


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


def gateway_session(gateway):
    session = ContactSession(
        "contact-example", gateway.cfg, gateway.send_to_contact, {}, {"handle": "agent"},
        imessage_turn_fn=gateway._imessage_turn_state,
        on_send_failure=Mock(wraps=gateway._note_sync_send_failure),
    )
    session._client = Client("Thanks.")
    gateway.sessions = NS(get=Mock(return_value=session))
    return session


@pytest.mark.parametrize("failure", ["pending", "timeout"])
def test_reaction_reply_uncertainty_never_creates_another_turn_or_send(channel, monkeypatch, failure):
    original = httpx.MockTransport.handle_request
    posts = []

    def request(transport, request):
        if request.method == "POST" and request.url.path == "/api/v1/whatsapp/messages":
            posts.append(request)
            if failure == "timeout":
                raise httpx.ReadTimeout("Response outcome is unknown", request=request)
            return httpx.Response(409, json={"detail": {"error": "whatsapp_send_pending"}})
        return original(transport, request)

    monkeypatch.setattr(httpx.MockTransport, "handle_request", request)

    async def scenario():
        gateway = channel.gateway
        session = gateway_session(gateway)
        method = Mock(wraps=gateway._identity.send_whatsapp)
        monkeypatch.setattr(gateway._identity, "send_whatsapp", method)
        event = {"event_type": "whatsapp.reaction_received", "data": {"reaction": deepcopy(channel.reaction)}}
        await receive_reaction(gateway, event)
        await session._worker
        assert len(session._client.calls) == 1
        assert "reaction context" in session._client.calls[0]
        assert method.call_count == 1
        assert posts and len({request.headers["Idempotency-Key"] for request in posts}) == 1
        session.on_send_failure.assert_not_called()
        assert session._queue.empty()
        store = gateway._channel_store("whatsapp")
        assert store.summary()["uncertain"] == 1
        assert not store.pending_replies() and not store.replay_pending()
        await gateway._recover_imessage_inputs()
        assert method.call_count == 1 and len(session._client.calls) == 1
        await receive_reaction(gateway, event)
        assert method.call_count == 1 and len(session._client.calls) == 1
        await session.close()

    asyncio.run(scenario())


def test_silent_reaction_is_durable_without_output_or_model_replay(channel):
    async def scenario():
        session = gateway_session(channel.gateway)
        session._client.reply = "[SILENT]"
        event = {"data": {"reaction": deepcopy(channel.reaction)}}
        await receive_reaction(channel.gateway, event)
        await session._worker
        assert len(session._client.calls) == 1
        assert channel.gateway._channel_store("whatsapp").summary()["done"] == 1
        assert not [request for request in channel.requests if request.method == "POST"]
        # Receipt dedup still applies when the gateway's in-memory event cache is gone.
        fresh = gateway_session(channel.gateway)
        meta = deepcopy(session.reply_meta)
        await fresh.handle_inbound("The same reaction", "whatsapp", meta)
        assert not fresh._client.calls and fresh._worker is None
        await fresh.close()
        await session.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["poll", "permission"])
@pytest.mark.parametrize("raw_text", ["", "/stop", "yes"])
def test_direct_reaction_cannot_answer_or_cancel_an_outstanding_question(kind, raw_text):
    async def scenario():
        session, sent, states = make_session(enabled=False)
        client = session._client
        client.gate = asyncio.Event()
        await session.handle_inbound("Work", "whatsapp", source(1))
        session._flush_imessage_burst()
        await client.started.wait()
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(kind, "Question", future)
        session._pending_reply_route = ("whatsapp", deepcopy(session._current_turn.reply_meta))
        session._approval_waiters = 1 if kind == "permission" else 0
        await session.handle_inbound("A peer reacted", "whatsapp", source(
            2, reaction="like", raw_text=raw_text, imessage_event_id="reaction:example:2:applied",
        ))
        assert not future.done()
        assert client.interrupts == 0 and not sent
        assert session._queue.qsize() == 1
        assert not [entry for entry in states if entry[0] == "consumed"]
        future.set_result("an actual answer")
        client.gate.set()
        await session._worker
        assert len(client.calls) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("command", ["/clear", "/resume"])
@pytest.mark.parametrize("origin, incoming", [
    ("whatsapp", "whatsapp"), ("sms", "whatsapp"), ("whatsapp", "sms"),
])
def test_foreign_control_during_delivery_preserves_host_and_original_route(command, origin, incoming):
    async def scenario():
        sending, release = asyncio.Event(), asyncio.Event()

        async def send(*_):
            sending.set()
            await release.wait()

        session, sent, _ = make_session(enabled=False, send=send)
        session.resume_session_id = "existing-conversation"
        client = session._client
        await session.handle_inbound("Request", origin, source(1, conversation_kind="group"))
        session._flush_imessage_burst()
        await sending.wait()
        assert session._current_turn is None and session._delivering_turn is not None
        await session.handle_inbound(command, incoming, source(
            2, sender="+15555550124", raw_text=command, conversation_kind="group",
        ))
        assert session.resume_session_id == "existing-conversation"
        assert session._client is client and session._resume_task is None
        assert len(sent) == 1 and sent[0][1] == origin
        assert sent[0][2]["message_id"] == "message-1"
        release.set()
        await session._worker
        assert session._delivering_turn is None and len(sent) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("command", ["/clear", "/resume"])
@pytest.mark.parametrize("origin, incoming", [("imessage", "whatsapp"), ("whatsapp", "sms")])
def test_foreign_control_cannot_discard_a_buffered_other_channel(command, origin, incoming):
    async def scenario():
        session, sent, _ = make_session()
        client = session._client
        session.resume_session_id = "existing-conversation"
        await session.handle_inbound("Buffered request", origin, source(1))
        burst = session._imessage_burst
        assert burst is not None
        await session.handle_inbound(command, incoming, source(2, raw_text=command))
        assert session._imessage_burst is burst
        assert session._client is client and session.resume_session_id == "existing-conversation"
        assert session._resume_task is None and not sent
        await finish_burst(session)
        assert len(client.calls) == 1 and sent[0][1] == origin

    asyncio.run(scenario())


def test_stop_own_queued_work_while_other_author_runs_preserves_other_work_and_tombstones(channel):
    async def scenario():
        session = gateway_session(channel.gateway)
        client = session._client
        client.gate = asyncio.Event()
        group = {"channel": "whatsapp", "conversation_id": CONVERSATION,
                 "conversation_kind": "group", "participants": ["+15550000002", "+15550000003"]}
        own = {**group, "sender": "+15550000002", "message_id": MESSAGE,
               "imessage_event_id": "own-queued"}
        other = {**group, "sender": "+15550000003", "message_id": str(UUID(int=20)),
                 "imessage_event_id": "other-running"}
        await session.handle_inbound("Other active task", "whatsapp", other)
        session._flush_imessage_burst()
        await client.started.wait()
        await session.handle_inbound("Own queued task", "whatsapp", own)
        session._flush_imessage_burst()
        remaining = {**other, "message_id": str(UUID(int=21)), "imessage_event_id": "other-queued"}
        await session.handle_inbound("Other queued task", "whatsapp", remaining)
        session._flush_imessage_burst()
        stop = {**own, "message_id": str(UUID(int=22)), "imessage_event_id": "stop-own",
                "raw_text": "/stop"}
        # Keep this author-control test independent of transport group preflights.
        session.send_fn = AsyncMock()
        await session.handle_inbound("/stop", "whatsapp", stop)
        assert client.interrupts == 0 and not client.gate.is_set()
        assert [turn.reply_meta["imessage_event_id"] for turn in session._queue._queue] == ["other-queued"]
        assert session.send_fn.call_args.args[1] == "Stopped."
        store = channel.gateway._channel_store("whatsapp")
        assert store.summary()["cancelled"] == 1 and store.summary()["done"] == 1
        before = session.send_fn.call_count
        await session.handle_inbound("/stop", "whatsapp", stop)
        await session.handle_inbound("Own queued task", "whatsapp", own)
        assert session.send_fn.call_count == before
        assert session._imessage_burst is None and session._queue.qsize() == 1
        client.gate.set()
        await session._worker
        assert len(client.calls) == 2
        assert all("Own queued task" not in text for text in client.calls)
        assert not store.replay_pending()
        await session.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("notice_status", [201, 409])
def test_overlong_host_answer_fails_once_with_separate_notice_and_no_restart_loop(sdk, notice_status):
    from inkbox_codex.whatsapp import REPLY_REJECTED_NOTICE
    from tests.test_whatsapp import bridge

    async def scenario():
        gateway = bridge(sdk)
        session = gateway_session(gateway)
        client = session._client
        client.reply = "x" * 4001
        sdk.status = notice_status
        await session.handle_inbound("A long answer", "whatsapp", {
            "channel": "whatsapp", "conversation_id": CONVERSATION,
            "conversation_kind": "direct", "sender": "+15550000002",
            "message_id": MESSAGE, "imessage_event_id": "long-answer",
        })
        await finish_burst(session)
        assert len(client.calls) == 1 and session._queue.empty()
        session.on_send_failure.assert_not_called()
        assert len(sdk.calls) == 1
        import json
        assert json.loads(sdk.calls[0].content)["text"] == REPLY_REJECTED_NOTICE
        store = gateway._channel_store("whatsapp")
        assert store.summary()["failed"] == 1
        assert not store.pending_replies() and not store.replay_pending()
        await gateway._recover_imessage_inputs()
        await gateway._recover_imessage_inputs()
        assert len(sdk.calls) == 1 and len(client.calls) == 1
        await session.close()

    asyncio.run(scenario())


def test_foreign_clear_cannot_enter_between_model_completion_and_delivery(monkeypatch):
    async def scenario():
        session, sent, _ = make_session(enabled=False)
        session.resume_session_id = "existing-conversation"
        client = session._client
        started, draining, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def typing_loop():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                draining.set()
                await release.wait()

        async def model(_text):
            await started.wait()
            return "The answer."

        monkeypatch.setattr(session, "_typing_loop", typing_loop)
        monkeypatch.setattr(client, "run", model)
        await session.handle_inbound("Request", "whatsapp", source(1, conversation_kind="group"))
        session._flush_imessage_burst()
        await asyncio.wait_for(draining.wait(), timeout=1)
        await session.handle_inbound("/clear", "whatsapp", source(
            2, raw_text="/clear", sender="+15555550124", conversation_kind="group",
        ))
        assert session.resume_session_id == "existing-conversation"
        assert session._client is client and not sent
        release.set()
        await session._worker
        assert len(sent) == 1 and sent[0][2]["message_id"] == "message-1"

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["pending", "timeout"])
def test_reaction_explicit_send_uncertainty_fences_final_and_preserves_source_receipt(channel, monkeypatch, failure):
    from inkbox_codex.imessage import read_turn_context
    from inkbox_codex.tools import call_inkbox_tool
    import json

    monkeypatch.setenv("INKBOX_BASE_URL", channel.gateway.cfg.base_url)
    original = httpx.MockTransport.handle_request
    posts = []
    expected_receipt = f"reaction:{channel.reaction['id']}:2:applied"

    def request(transport, request):
        if request.method == "POST" and request.url.path == "/api/v1/whatsapp/messages":
            posts.append(request)
            context = read_turn_context("contact-example")
            assert context["route_meta"]["imessage_event_ids"] == [expected_receipt]
            assert context["whatsapp_tool_sends"]["reaction-answer"]["state"] == "sending"
            if failure == "timeout":
                raise httpx.ReadTimeout("Response outcome is unknown", request=request)
            return httpx.Response(409, json={"detail": {"error": "whatsapp_send_pending"}})
        return original(transport, request)

    monkeypatch.setattr(httpx.MockTransport, "handle_request", request)

    async def scenario():
        gateway = channel.gateway
        session = gateway_session(gateway)
        client = session._client

        async def model(text):
            client.calls.append(text)
            from inkbox_codex.turn_authority import HOST_ENV, mcp_request
            for name, value in session.mcp_server_config["env"].items():
                monkeypatch.setenv(name, value)
            monkeypatch.setenv(HOST_ENV, session._host_epoch)
            client.turn_started(client.thread_id, "reaction-turn")
            context = read_turn_context(session.chat_id)
            assert context["admitted_source_ids"] == [MESSAGE]
            assert context["route_meta"]["reaction"] == "👍"
            with mcp_request({"x-codex-turn-metadata": {
                "thread_id": client.thread_id, "turn_id": "reaction-turn",
            }}):
                result = await call_inkbox_tool(channel.client, "agent", "inkbox_send_whatsapp", {
                    "conversation_id": CONVERSATION, "text": "A reaction answer",
                    "idempotency_key": "reaction-answer",
                })
            assert result["isError"]
            assert json.loads(result["content"][0]["text"])["idempotency_key"] == "reaction-answer"
            return "A different automatic answer"

        client.run = model
        await receive_reaction(gateway, {"data": {"reaction": deepcopy(channel.reaction)}})
        await session._worker
        assert len(client.calls) == 1 and session._queue.empty()
        assert posts and {request.headers["Idempotency-Key"] for request in posts} == {"reaction-answer"}
        assert all(json.loads(request.content)["text"] == "A reaction answer" for request in posts)
        session.on_send_failure.assert_not_called()
        store = gateway._channel_store("whatsapp")
        assert store.summary()["uncertain"] == 1
        with store._db() as db:
            row = db.execute("SELECT event_id,meta FROM receipts").fetchone()
        assert row["event_id"] == expected_receipt
        assert json.loads(row["meta"])["imessage_event_ids"] == [expected_receipt]
        assert json.loads(row["meta"])["whatsapp_tool_sends"]["reaction-answer"]["state"] == "sending"
        before = len(posts)
        await gateway._recover_imessage_inputs()
        assert len(posts) == before and len(client.calls) == 1
        await session.close()

    asyncio.run(scenario())



def test_stop_acknowledgment_keeps_its_original_route_while_interrupt_is_held(channel, monkeypatch):
    from inkbox_codex.imessage import read_turn_context

    async def scenario():
        session = gateway_session(channel.gateway)
        session.send_fn = AsyncMock()
        client = session._client
        client.gate = asyncio.Event()
        client.reply = "[SILENT]"
        interrupted, release = asyncio.Event(), asyncio.Event()

        async def interrupt():
            assert read_turn_context(session.chat_id) is None
            interrupted.set()
            await release.wait()
            client.gate.set()

        monkeypatch.setattr(client, "interrupt", interrupt)
        own = {"channel": "whatsapp", "conversation_id": CONVERSATION,
               "conversation_kind": "direct", "participants": ["+15550000002"],
               "sender": "+15550000002", "message_id": MESSAGE, "imessage_event_id": "source-request"}
        await session.handle_inbound("Original request", "whatsapp", own)
        session._flush_imessage_burst()
        await asyncio.wait_for(client.started.wait(), timeout=1)
        stop = asyncio.create_task(session.handle_inbound("/stop", "whatsapp", {
            **own, "message_id": str(UUID(int=30)), "imessage_event_id": "stop-request", "raw_text": "/stop",
        }))
        try:
            await asyncio.wait_for(interrupted.wait(), timeout=1)
            await session.handle_inbound("New email request", "email", {
                "sender": "person@example.test", "conversation_id": "email-conversation",
                "message_id": "email-message",
            })
            assert session.mode == "email" and not stop.done()
        finally:
            release.set()
            client.gate.set()
            await stop
            await session._worker
        acknowledgments = [call.args for call in session.send_fn.call_args_list if call.args[1] == "Stopped."]
        assert len(acknowledgments) == 1 and acknowledgments[0][2] == "whatsapp"
        assert acknowledgments[0][3]["conversation_id"] == CONVERSATION
        assert acknowledgments[0][3]["sender"] == own["sender"]
        assert len(client.calls) == 2 and client.calls[-1].endswith("New email request")
        await session.close()

    asyncio.run(scenario())

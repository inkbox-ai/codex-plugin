"""Gateway tool authority follows submitted sources, including their revocation."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from inkbox_codex.config import BridgeConfig
from inkbox_codex.imessage import clear_turn_context, write_turn_context
from inkbox_codex import turn_authority as authority
from tests.test_whatsapp import sdk


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact")
    cfg = BridgeConfig(identity="agent", base_url="https://example.com")
    host = authority.new_host("contact", cfg)
    monkeypatch.setenv(authority.HOST_ENV, host)
    return cfg, host


def native(source):
    cfg, host = source
    context = write_turn_context("contact", cfg, {
        "channel": "whatsapp", "conversation_id": "conversation", "message_id": "message",
    })
    token = authority.begin("contact", cfg, host, "whatsapp", context["nonce"])
    assert authority.bind(token, "thread", "turn-one")
    return token


def metadata(turn="turn-one"):
    return {"x-codex-turn-metadata": {"thread_id": "thread", "turn_id": turn}}


def test_standalone_is_not_a_revoked_gateway_call(source, monkeypatch):
    token = native(source)
    authority.retire(token)
    monkeypatch.delenv(authority.HOST_ENV)
    assert authority.capture("agent", source[0]) is None
    authority.validate(None)


@pytest.mark.parametrize("retire", [False, True])
def test_cleared_native_context_before_tool_entry_never_becomes_standalone(source, retire):
    token = native(source)
    clear_turn_context("contact")
    if retire:
        authority.retire(token)
    with authority.mcp_request(metadata()), pytest.raises(ValueError, match="(active|available)"):
        authority.capture("agent", source[0])


def test_active_email_source_can_explicitly_use_whatsapp_without_native_context(source):
    cfg, host = source
    token = authority.begin("contact", cfg, host, "email")
    assert authority.bind(token, "thread", "turn-one")
    with authority.mcp_request(metadata()):
        captured = authority.capture("agent", cfg)
        assert captured["channel"] == "email"
        authority.validate(captured)


def test_stale_turn_does_not_acquire_later_email_source(source):
    old = native(source)
    authority.retire(old)
    clear_turn_context("contact")
    current = authority.begin("contact", source[0], source[1], "email")
    assert authority.bind(current, "thread", "turn-two")
    with authority.mcp_request(metadata()), pytest.raises(ValueError, match="does not belong"):
        authority.capture("agent", source[0])
    with authority.mcp_request(metadata("turn-two")):
        assert authority.capture("agent", source[0])["source_epoch"] == current["source_epoch"]


@pytest.mark.parametrize("metadata_value", [{}, {"threadId": "thread"}, metadata("foreign-turn")])
def test_gateway_mcp_requires_actual_turn_metadata(source, metadata_value):
    native(source)
    with authority.mcp_request(metadata_value), pytest.raises(ValueError, match="does not belong"):
        authority.capture("agent", source[0])


@pytest.mark.parametrize("change", ["identity", "environment", "host", "idle"])
def test_gateway_identity_environment_and_host_are_bound(source, monkeypatch, change):
    native(source)
    cfg = source[0]
    identity = "agent"
    if change == "identity":
        identity = "other"
    elif change == "environment":
        cfg = BridgeConfig(identity="agent", base_url="https://different.example.com")
    elif change == "host":
        monkeypatch.setenv(authority.HOST_ENV, "old-host")
    else:
        authority.new_host("contact", cfg)
    with pytest.raises(ValueError, match="No active gateway"):
        authority.capture(identity, cfg)


@pytest.mark.parametrize("change", ["retire", "clear", "next-source", "next-host"])
def test_captured_authority_is_revalidated_before_effect(source, change):
    token = native(source)
    captured = authority.capture("agent", source[0])
    if change == "retire":
        authority.retire(token)
    elif change == "clear":
        clear_turn_context("contact")
    elif change == "next-source":
        authority.begin("contact", source[0], source[1], "email")
    else:
        authority.new_host("contact", source[0])
    with pytest.raises(ValueError, match="(ended|no longer)"):
        authority.validate(captured)


def test_late_start_ack_and_retirement_never_revive_or_close_another_source(source):
    old = native(source)
    authority.retire(old)
    assert not authority.bind(old, "thread", "late-turn")
    current = authority.begin("contact", source[0], source[1], "email")
    assert not authority.bind(old, "thread", "late-turn")
    authority.retire(old)
    assert authority.bind(current, "thread", "fresh-turn")
    with authority.mcp_request(metadata("fresh-turn")):
        authority.validate(authority.capture("agent", source[0]))


def test_old_host_retirement_does_not_close_replacement(source, monkeypatch):
    replacement = authority.new_host("contact", source[0])
    monkeypatch.setenv(authority.HOST_ENV, replacement)
    authority.begin("contact", source[0], replacement, "email")
    authority.retire_host("contact", source[1])
    authority.validate(authority.capture("agent", source[0]))
    authority.retire_host("contact", replacement)
    with pytest.raises(ValueError, match="No active gateway"):
        authority.capture("agent", source[0])


def test_mcp_origin_reaches_threaded_tool_and_is_scoped_per_request(source):
    native(source)

    async def scenario():
        with authority.mcp_request(metadata()):
            assert (await asyncio.to_thread(authority.capture, "agent", source[0]))["turn_id"] == "turn-one"
        with authority.mcp_request(metadata("stale")), pytest.raises(ValueError, match="does not belong"):
            await asyncio.to_thread(authority.capture, "agent", source[0])
        # An independently invoked local tool uses the active host source.
        assert authority.capture("agent", source[0])["turn_id"] == "turn-one"
    asyncio.run(scenario())


def test_client_binds_acknowledgement_to_the_callback_captured_at_submission(source):
    async def scenario():
        client = CodexAppServerClient(source[0], developer_instructions="test")
        client.thread_id = "thread"
        writes = []
        client._proc = SimpleNamespace(stdin=SimpleNamespace(write=lambda data: writes.append(json.loads(data))))
        called = []
        client.turn_started = lambda thread, turn: called.append(("original", thread, turn))
        running = asyncio.create_task(client.run("hello"))
        await asyncio.sleep(0)
        client.turn_started = lambda thread, turn: called.append(("later", thread, turn))
        client._handle_response({"id": writes[0]["id"], "result": {"turn": {"id": "turn-one"}}})
        assert called == [("original", "thread", "turn-one")]
        client._handle_notification({"method": "turn/completed", "params": {"turn": {"id": "turn-one", "status": "completed"}}})
        await asyncio.wait_for(running, 1)
    asyncio.run(scenario())


def test_mcp_stdio_uses_host_metadata_not_model_arguments(source, monkeypatch):
    from inkbox_codex import mcp_stdio
    native(source)

    async def tool(client, identity, name, args):
        authority.capture(identity, source[0])
        return {"content": []}

    monkeypatch.setattr(mcp_stdio, "call_inkbox_tool", tool)
    server = mcp_stdio.InkboxMcpServer()
    server.identity = "agent"
    monkeypatch.setattr(server, "_inkbox", lambda: object())

    async def scenario():
        request = {"id": 1, "method": "tools/call", "params": {
            "name": "inkbox_send_whatsapp", "arguments": {"_meta": metadata()}, "_meta": metadata("stale")}}
        with pytest.raises(ValueError, match="does not belong"):
            await server.handle(request)
        request["params"]["_meta"] = metadata()
        assert (await server.handle(request))["result"] == {"content": []}
    asyncio.run(scenario())


def test_source_binding_failure_settles_the_start_request(source):
    async def scenario():
        client = CodexAppServerClient(source[0], developer_instructions="test")
        client.thread_id = "thread"
        writes = []
        client._proc = SimpleNamespace(stdin=SimpleNamespace(write=lambda data: writes.append(json.loads(data))))

        def failed(*_):
            raise OSError("Local state unavailable")
        client.turn_started = failed
        running = asyncio.create_task(client.run("hello"))
        await asyncio.sleep(0)
        client._handle_response({"id": writes[0]["id"], "result": {"turn": {"id": "turn-one"}}})
        with pytest.raises(CodexAppServerError, match="Could not bind"):
            await asyncio.wait_for(running, 1)
        assert not client._turns and not client._pending
    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["send", "upload", "react", "unreact"])
@pytest.mark.parametrize("new_incoming_channel", [False, True])
def test_actual_session_stop_before_tool_entry_blocks_mutations_while_interrupt_waits(
        sdk, monkeypatch, tmp_path, operation, new_incoming_channel):
    from inkbox_codex.imessage import read_turn_context
    from inkbox_codex.sessions import ContactSession
    from inkbox_codex.tools import call_inkbox_tool
    from tests.test_imessage_sessions import Client
    from tests.test_whatsapp import bridge, CONVERSATION, MESSAGE

    async def scenario():
        gateway = bridge(sdk)
        sent = []

        async def reply(*args):
            sent.append(args)

        session = ContactSession("contact", gateway.cfg, reply, {}, {"handle": "agent"},
            imessage_turn_fn=gateway._imessage_turn_state)
        client = session._client = Client()
        client.gate = asyncio.Event()
        route = {"channel": "whatsapp", "conversation_id": CONVERSATION,
            "sender": "+15550000002", "message_id": MESSAGE, "imessage_event_id": "source-one"}
        await session.handle_inbound("Work", "whatsapp", route)
        session._flush_imessage_burst()
        await asyncio.wait_for(client.started.wait(), 1)
        client.turn_started(client.thread_id, "turn-held")
        for name, value in session.mcp_server_config["env"].items():
            monkeypatch.setenv(name, value)
        monkeypatch.setenv(authority.HOST_ENV, session._host_epoch)
        interrupt_entered, interrupt_release = asyncio.Event(), asyncio.Event()

        async def interrupted():
            interrupt_entered.set()
            await interrupt_release.wait()
            client.gate.set()
        client.interrupt = interrupted
        stop = asyncio.create_task(session.handle_inbound("/stop", "whatsapp", {
            **route, "raw_text": "/stop", "message_id": "control-message", "imessage_event_id": "control-event"}))
        try:
            await asyncio.wait_for(interrupt_entered.wait(), 1)
            assert read_turn_context("contact") is None
            assert not stop.done()
            if new_incoming_channel:
                await session.handle_inbound("Queued email", "email", {
                    "channel": "email", "sender": "person@example.com", "raw_text": "Queued email"})
                assert session.mode == "email"
            attachment = tmp_path / "attachment.txt"
            attachment.write_text("local attachment")
            name, args = {
                "send": ("inkbox_send_whatsapp", {"conversation_id": CONVERSATION, "text": "Do not send"}),
                "upload": ("inkbox_upload_whatsapp_media", {"media_path": str(attachment)}),
                "react": ("inkbox_react_whatsapp", {"message_id": MESSAGE, "reaction": "like", "idempotency_key": "reaction-key"}),
                "unreact": ("inkbox_unreact_whatsapp", {"reaction_id": MESSAGE, "idempotency_key": "reaction-key"}),
            }[operation]
            with authority.mcp_request({"x-codex-turn-metadata": {
                    "thread_id": client.thread_id, "turn_id": "turn-held"}}):
                result = await call_inkbox_tool(sdk.client, "agent", name, args)
            assert result["isError"]
            assert "No active gateway source" in result["content"][0]["text"]
            assert not [request for request in sdk.requests if request.method in {"POST", "PUT", "PATCH", "DELETE"}]
            assert not stop.done(), "No interrupt acknowledgement was available at tool entry"
        finally:
            interrupt_release.set()
            client.gate.set()
            await asyncio.wait_for(stop, 1)
            await asyncio.wait_for(session._worker, 1)
            await session.close()
    asyncio.run(scenario())

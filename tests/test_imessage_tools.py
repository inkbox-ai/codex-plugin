"""Opt-in iMessage tools preserve native targeting and actual message metadata."""

import asyncio
import json
import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import daemon, doctor, tools
from inkbox_codex.config import BridgeConfig, imessage_threading_capability, read_config


@dataclass
class Message:
    id: str = "outbound-1"
    conversation_id: str = "conversation-1"
    status: str = "pending"
    reply_to_message_id: str | None = None
    thread_id: str | None = None
    thread_root_message_id: str | None = None


class Identity:
    def __init__(self):
        self.sent = []
        self.reads = []
        self.target = Message(id="source-1", status="received")
        self.result = Message()
        self.error = None
        self.page = {
            "conversation_id": "conversation-1", "thread_id": None,
            "thread_root_message_id": None, "messages": [self.target],
            "next_cursor": "next-page",
        }

    def send_imessage(self, *, reply_to_message_id=None, plain_reply_fallback=True, idempotency_key=None, **kwargs):
        self.sent.append({
            **kwargs, "reply_to_message_id": reply_to_message_id,
            "plain_reply_fallback": plain_reply_fallback, "idempotency_key": idempotency_key,
        })
        if self.error is not None:
            raise self.error
        return self.result

    def get_imessage(self, message_id):
        self.reads.append(("message", message_id))
        return self.target

    def get_imessage_thread(self, message_id, *, limit=50, cursor=None):
        self.reads.append(("thread", message_id, limit, cursor))
        return self.page

    def get_imessage_conversation_thread(self, conversation_id, thread_id, *, limit=50, cursor=None):
        self.reads.append(("conversation-thread", conversation_id, thread_id, limit, cursor))
        return self.page


@pytest.fixture(autouse=True)
def isolated_tools(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
    monkeypatch.delenv("INKBOX_IMESSAGE_THREADED_REPLIES", raising=False)


def call(identity, name="inkbox_send_imessage", **arguments):
    handles = []

    def get_identity(handle):
        handles.append(handle)
        return identity

    result = asyncio.run(tools.call_inkbox_tool(SimpleNamespace(get_identity=get_identity), "agent", name, arguments))
    assert all(handle == "agent" for handle in handles)
    return result, json.loads(result["content"][0]["text"])


def enable(monkeypatch):
    monkeypatch.setenv("INKBOX_IMESSAGE_THREADED_REPLIES", "true")


def test_flag_defaults_off_and_is_forwarded_to_mcp(monkeypatch):
    assert read_config().imessage_threaded_replies is False
    server, names = tools.build_inkbox_mcp_server_config(BridgeConfig())
    assert server["env"]["INKBOX_IMESSAGE_THREADED_REPLIES"] == "0"
    assert "mcp__inkbox__inkbox_get_imessage_thread" not in names
    enable(monkeypatch)
    server, names = tools.build_inkbox_mcp_server_config(read_config())
    assert server["env"]["INKBOX_IMESSAGE_THREADED_REPLIES"] == "1"
    assert "mcp__inkbox__inkbox_get_imessage_thread" in names


def test_schema_does_not_leak_opt_in_fields_after_flag_is_disabled(monkeypatch):
    before = tools.mcp_tool_list()
    enable(monkeypatch)
    enabled = {tool["name"]: tool for tool in tools.mcp_tool_list()}
    assert "inkbox_get_imessage_conversation_thread" in enabled
    properties = enabled["inkbox_send_imessage"]["inputSchema"]["properties"]
    assert "idempotency_key" in properties
    assert not {"reply_to_message_id", "plain_reply_fallback"} & properties.keys()
    monkeypatch.setenv("INKBOX_IMESSAGE_THREADED_REPLIES", "false")
    assert tools.mcp_tool_list() == before


def test_off_preserves_old_sdk_request_and_result():
    received = []
    identity = SimpleNamespace(send_imessage=lambda **kwargs: received.append(kwargs) or Message())
    result, payload = call(identity, conversation_id="conversation-1", text="hello")
    assert not result.get("isError")
    assert received == [{"conversation_id": "conversation-1", "text": "hello"}]
    assert payload == {"sent": True, "id": "outbound-1"}


@pytest.mark.parametrize("extra", [
    {"reply_to_message_id": "source-1"}, {"plain_reply_fallback": True}, {"idempotency_key": "send-1"},
])
def test_off_rejects_new_arguments_instead_of_silently_flattening(extra):
    identity = Identity()
    result, payload = call(identity, conversation_id="conversation-1", text="hello", **extra)
    assert result["isError"] is True
    assert "Enable INKBOX_IMESSAGE_THREADED_REPLIES" in payload["error"]
    assert identity.sent == []


def test_native_send_preserves_exact_target_policy_and_key(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    identity.result = Message(reply_to_message_id="source-1", thread_id="native-thread", thread_root_message_id="root")
    result, payload = call(identity, conversation_id="conversation-1", text="answer", idempotency_key="stable-output-key")
    assert not result.get("isError")
    assert identity.reads == [("message", "source-1"), ("thread", "source-1", 1, None)]
    assert identity.sent == [{
        "conversation_id": "conversation-1", "text": "answer", "reply_to_message_id": "source-1",
        "plain_reply_fallback": True, "idempotency_key": "stable-output-key",
    }]
    assert payload["status"] == "pending"
    assert payload["reply_to_message_id"] == "source-1"
    assert payload["thread_id"] == "native-thread"


def test_fallback_reports_actual_metadata_without_inventing_native_parent(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    identity.result = Message(thread_id="standalone-thread")
    _, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert identity.sent[0]["plain_reply_fallback"] is True
    assert payload["reply_to_message_id"] is None
    assert payload["thread_root_message_id"] is None
    assert payload["thread_id"] == "standalone-thread"


def test_target_from_another_conversation_never_uploads_or_sends(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    identity.target.conversation_id = "other-conversation"
    upload = []
    monkeypatch.setattr(tools, "_upload_media_url", lambda *args: upload.append(args))
    result, _ = call(identity, conversation_id="conversation-1", text="answer",
                     media_path="example.png")
    assert result["isError"] is True
    assert not identity.sent and not upload


def test_reply_target_cannot_be_used_with_recipients(monkeypatch):
    enable(monkeypatch)
    identity = Identity()
    result, payload = call(identity, to="+15551234567", text="answer", reply_to_message_id="source-1")
    assert result["isError"] is True
    assert "bridge selects iMessage reply routing automatically" in payload["error"]
    assert not identity.reads and not identity.sent


@pytest.mark.parametrize("error", [TimeoutError("outcome unknown"), ValueError("target unavailable")])
def test_failed_targeted_send_is_never_retried_as_plain(monkeypatch, error):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    identity.error = error
    result, _ = call(identity, conversation_id="conversation-1", text="answer")
    assert result["isError"] is True
    assert len(identity.sent) == 1
    assert identity.sent[0]["reply_to_message_id"] == "source-1"


def test_old_sdk_is_actionable_only_when_opted_in(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = SimpleNamespace(send_imessage=lambda **kwargs: pytest.fail("must not send"))
    result, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert result["isError"] is True
    assert "0.7.12 or newer when available" in payload["error"]
    assert "INKBOX_IMESSAGE_THREADED_REPLIES=false" in payload["error"]
    assert imessage_threading_capability(Identity)[0] is True


def active_turn(monkeypatch, **meta):
    from inkbox_codex.imessage import source_metadata, write_turn_context

    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-1")
    return write_turn_context("contact-1", BridgeConfig(identity="agent"), {
        "conversation_id": "conversation-1",
        **source_metadata({"id": meta.get("message_id", "source-1")}), **meta,
    })


def test_active_send_defaults_to_trigger_without_model_selecting_target(monkeypatch):
    from inkbox_codex.imessage import read_turn_context

    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    result, _ = call(identity, conversation_id="conversation-1", text="answer")
    assert not result.get("isError")
    assert identity.sent[0]["reply_to_message_id"] == "source-1"
    assert identity.sent[0]["plain_reply_fallback"] is True
    assert read_turn_context()["sent_outputs"][0]["target"] == "source-1"


@pytest.mark.parametrize("override", [
    {"reply_to_message_id": None}, {"reply_to_message_id": "source-1"},
    {"reply_to_message_id": "different-source"}, {"plain_reply_fallback": False},
    {"plain_reply_fallback": True},
])
def test_model_cannot_override_bridge_reply_routing(monkeypatch, override):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    result, payload = call(identity, conversation_id="conversation-1", text="answer", **override)
    assert result["isError"] is True
    assert "bridge selects iMessage reply routing automatically" in payload["error"]
    assert not identity.sent and not identity.reads


def test_later_scheduled_send_does_not_reuse_previous_trigger(monkeypatch):
    from inkbox_codex.imessage import clear_turn_context

    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    result, _ = call(identity, conversation_id="conversation-1", text="answer")
    assert not result.get("isError")
    clear_turn_context("contact-1")
    result, _ = call(identity, conversation_id="conversation-1", text="Later reminder")
    assert not result.get("isError")
    assert [message["reply_to_message_id"] for message in identity.sent] == ["source-1", None]


def test_backend_thread_capability_failure_does_not_send(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()

    def missing_endpoint(message_id, *, limit=50, cursor=None):
        raise RuntimeError("thread API unavailable")

    identity.get_imessage_thread = missing_endpoint
    result, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert result["isError"] is True
    assert payload["error"] == "thread API unavailable"
    assert identity.sent == []


def test_default_target_must_be_an_admitted_source(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch, imessage_reply_target="unadmitted")
    identity = Identity()
    result, _ = call(identity, conversation_id="conversation-1", text="answer")
    assert result["isError"] is True
    assert not identity.sent and not identity.reads


def test_context_identity_and_environment_cannot_be_reused(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    monkeypatch.setenv("INKBOX_BASE_URL", "https://example.com")
    identity = Identity()
    result, payload = call(identity, "inkbox_get_imessage_thread", message_id="source-1")
    assert result["isError"] is True
    assert "different identity or API environment" in payload["error"]
    assert not identity.sent and not identity.reads


def test_stale_context_after_preflight_cannot_send_into_next_turn(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()

    def preflight(message_id, *, limit=50, cursor=None):
        active_turn(monkeypatch)
        return identity.page

    identity.get_imessage_thread = preflight
    result, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert result["isError"] is True
    assert "matching active iMessage turn" in payload["error"]
    assert not identity.sent


def test_active_tool_send_records_actual_output_and_stable_key(monkeypatch):
    from inkbox_codex.imessage import read_turn_context

    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    arguments = {"conversation_id": "conversation-1", "text": "answer"}
    first, _ = call(identity, **arguments)
    second, _ = call(identity, **arguments)
    assert not first.get("isError") and not second.get("isError")
    assert identity.sent[0]["idempotency_key"] == identity.sent[1]["idempotency_key"]
    recorded = read_turn_context()["sent_outputs"]
    assert len(recorded) == 1
    assert recorded[0]["text"] == "answer"
    assert recorded[0]["target"] == "source-1"
    assert recorded[0]["reply_to_message_id"] is None
    assert recorded[0]["id"] == "outbound-1"


def test_accepted_send_is_not_reported_as_failed_when_correlation_storage_fails(monkeypatch):
    from inkbox_codex import imessage

    enable(monkeypatch)
    active_turn(monkeypatch)

    def unavailable(*args, **kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(imessage, "record_tool_send", unavailable)
    identity = Identity()
    result, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert not result.get("isError")
    assert payload["id"] == "outbound-1"
    assert "Do not resend" in payload["warning"]
    assert len(identity.sent) == 1


def test_accepted_send_after_stop_preserves_original_route_without_touching_next_turn(monkeypatch):
    from inkbox_codex.imessage import IMessageState, read_turn_context

    enable(monkeypatch)
    original = active_turn(monkeypatch)
    identity = Identity()
    identity.result = Message(thread_id="actual-thread")

    def send(*, reply_to_message_id=None, plain_reply_fallback=True, idempotency_key=None, **kwargs):
        active_turn(monkeypatch, message_id="next-source", conversation_id="next-conversation")
        return identity.result

    identity.send_imessage = send
    result, payload = call(identity, conversation_id="conversation-1", text="answer")
    assert not result.get("isError")
    assert payload["id"] == "outbound-1"
    assert "Do not resend" in payload["warning"]
    current = read_turn_context()
    assert current["nonce"] != original["nonce"]
    assert current["sent_outputs"] == []
    route = IMessageState(BridgeConfig(identity="agent")).lookup_outbound("outbound-1")
    assert route["chat_id"] == "contact-1"
    assert route["meta"]["conversation_id"] == "conversation-1"
    assert route["meta"]["imessage_reply_target"] == "source-1"
    assert route["thread_id"] == "actual-thread"
    assert route["reply_to_message_id"] is None


def test_deliberate_plain_send_elsewhere_does_not_suppress_current_answer(monkeypatch):
    from inkbox_codex.imessage import read_turn_context

    enable(monkeypatch)
    active_turn(monkeypatch)
    identity = Identity()
    result, _ = call(identity, conversation_id="other-conversation", text="separate instruction")
    assert not result.get("isError")
    assert len(identity.sent) == 1
    assert read_turn_context()["sent_outputs"] == []


def test_companion_turn_cannot_hydrate_unbounded_native_thread_context(monkeypatch):
    enable(monkeypatch)
    active_turn(monkeypatch, companion=True, companion_scope_id="scope-1")
    identity = Identity()
    result, payload = call(identity, "inkbox_get_imessage_thread", message_id="source-1")
    assert result["isError"] is True
    assert "supplied Companion history" in payload["error"]
    assert not identity.reads


@pytest.mark.parametrize("name,arguments,expected", [
    ("inkbox_get_imessage_thread", {"message_id": "source-1"}, ("thread", "source-1", 50, None)),
    ("inkbox_get_imessage_conversation_thread", {"conversation_id": "conversation-1", "thread_id": "native-thread",
                                               "limit": 200, "cursor": "page-2"},
     ("conversation-thread", "conversation-1", "native-thread", 200, "page-2")),
])
def test_thread_reads_keep_identity_scope_cursor_and_nullable_metadata(monkeypatch, name, arguments, expected):
    enable(monkeypatch)
    identity = Identity()
    result, payload = call(identity, name, **arguments)
    assert not result.get("isError")
    assert identity.reads == [expected]
    assert payload["thread_id"] is None
    assert payload["thread_root_message_id"] is None
    assert payload["messages"][0]["id"] == "source-1"
    assert payload["next_cursor"] == "next-page"


@pytest.mark.parametrize("limit", [0, 201, True, 1.5, "50"])
def test_thread_read_rejects_unbounded_or_invalid_limit(monkeypatch, limit):
    enable(monkeypatch)
    identity = Identity()
    result, _ = call(identity, "inkbox_get_imessage_thread", message_id="source-1", limit=limit)
    assert result["isError"] is True
    assert not identity.reads


def test_thread_read_is_unavailable_when_disabled():
    identity = Identity()
    result, _ = call(identity, "inkbox_get_imessage_thread", message_id="source-1")
    assert result["isError"] is True
    assert not identity.reads


@pytest.mark.parametrize("sdk_class,expected", [(Identity, True), (object, False)])
def test_doctor_distinguishes_sdk_capability_from_backend_readiness(monkeypatch, tmp_path, sdk_class, expected):
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor, "probe_codex", AsyncMock(return_value=(True, "ready")))
    monkeypatch.setattr(doctor, "inbox_summary", lambda _cfg: {
        "blocked_conversations": 0, "oldest_unfinished_age_s": None, "unfinished_count": 0,
    })
    monkeypatch.setattr(doctor, "read_config", lambda: BridgeConfig(imessage_threaded_replies=True, project_dir=str(tmp_path)))
    sdk_module = ModuleType("inkbox.agent_identity")
    sdk_module.AgentIdentity = sdk_class
    monkeypatch.setitem(sys.modules, "inkbox.agent_identity", sdk_module)
    rows = {name: (ok, detail) for name, ok, detail in doctor.run_doctor()}
    assert rows["INKBOX_IMESSAGE_THREADED_REPLIES"] == (True, "true")
    assert rows["iMessage threading SDK"][0] is expected
    if expected:
        assert "backend support is not verified" in rows["iMessage threading SDK"][1]

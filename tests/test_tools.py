import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pytest

from inkbox_codex import tools as tools_mod
from inkbox_codex.config import hosted_sms_turn_context_path
from inkbox_codex.hosted_sms_guard import hosted_sms_attempt_state


@pytest.fixture(autouse=True)
def _run_to_thread_inline(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    async def immediate(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(tools_mod.asyncio, "to_thread", immediate)


@dataclass
class _FakeCall:
    direction: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    local_phone_number: str = "+16614031457"
    remote_phone_number: str = "+15551112222"
    status: str = "completed"
    started_at: datetime = datetime(2026, 6, 18, 4, 0, 0)
    ended_at: datetime = datetime(2026, 6, 18, 4, 1, 0)


@dataclass
class _FakeTranscript:
    party: str
    text: str
    seq: int
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    ts_ms: int = 0


class _FakeIdentity:
    def __init__(self):
        self.id = "identity-1"
        self.phone_number = type(
            "Phone",
            (),
            {"client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws?existing=1"},
        )()
        self.tunnel = type("Tunnel", (), {"public_host": "agent.inkboxwire.com"})()
        self.place_call_kwargs = None
        self.list_calls_kwargs = None
        self.transcript_call_id = None
        self.sent_texts = []
        self.sent_imessages = []
        self.a2a = _FakeA2AClient()
        self.a2a_replies = []
        self.a2a_history_calls = []

    def place_call(self, **kwargs):
        self.place_call_kwargs = kwargs
        return type("Call", (), {"id": "call-123", "status": "queued"})()

    def list_calls(self, **kwargs):
        self.list_calls_kwargs = kwargs
        return [_FakeCall("inbound"), _FakeCall("outbound")]

    def list_transcripts(self, call_id):
        self.transcript_call_id = call_id
        return [
            _FakeTranscript("remote", "hey can you check the build", 1),
            _FakeTranscript("local", "sure, it's green", 2),
        ]

    def send_imessage(self, **kwargs):
        self.sent_imessages.append(kwargs)
        return type("Message", (), {"id": "im-1"})()

    def send_text(self, **kwargs):
        self.sent_texts.append(kwargs)
        return type("Message", (), {"id": "sms-1"})()

    def a2a_client(self):
        return self.a2a

    def a2a_reply(self, task_id, **kwargs):
        self.a2a_replies.append((task_id, kwargs))
        return {"id": task_id, "state": kwargs["intent"]}

    def a2a_tasks(self, **kwargs):
        self.a2a_history_calls.append(("tasks", kwargs))
        return {"items": [{"id": "task-1"}], "next_cursor": "task-next"}

    def a2a_messages(self, **kwargs):
        self.a2a_history_calls.append(("messages", kwargs))
        return {"items": [{"id": "message-1"}], "next_cursor": "message-next"}


class _FakeA2AClient:
    def __init__(self):
        self.calls = []
        self.closed = False

    def fetch_card(self, card_url):
        self.calls.append(("fetch_card", card_url))
        return {"rpc_url": "https://target.example/a2a"}

    def send(self, target, **kwargs):
        self.calls.append(("send", target, kwargs))
        return {
            "kind": "task",
            "task": {"id": "task-1", "context_id": "context-1"},
        }

    def get_task(self, target, task_id):
        self.calls.append(("get_task", target, task_id))
        return {"id": task_id, "state": "TASK_STATE_WORKING"}

    def wait(self, target, task_id):
        self.calls.append(("wait", target, task_id))
        return {"id": task_id, "state": "TASK_STATE_COMPLETED"}

    def close(self):
        self.closed = True


class _FakeContacts:
    def __init__(self):
        self.deleted = []

    def get(self, contact_id):
        return {"id": contact_id, "given_name": "Ada"}

    def delete(self, contact_id):
        self.deleted.append(contact_id)


class _FakeClient:
    def __init__(self):
        self.identity = _FakeIdentity()
        self.contacts = _FakeContacts()

    def get_identity(self, _handle):
        return self.identity


def _call(client, name, arguments):
    result = asyncio.run(
        tools_mod.call_inkbox_tool(client, "codex-agent", name, arguments)
    )
    return json.loads(result["content"][0]["text"])


def test_call_tools_are_registered():
    names = [tool["name"] for tool in tools_mod.mcp_tool_list()]

    assert "inkbox_place_call" in names
    assert "inkbox_list_calls" in names
    assert "inkbox_get_call_transcript" in names


def test_coding_agent_tool_tier_is_registered():
    names = {tool["name"] for tool in tools_mod.mcp_tool_list()}
    expected = {
        "inkbox_whoami",
        "inkbox_send_email",
        "inkbox_send_sms",
        "inkbox_send_imessage",
        "inkbox_place_call",
        "inkbox_list_calls",
        "inkbox_get_call_transcript",
        "inkbox_list_text_conversations",
        "inkbox_get_text_conversation",
        "inkbox_list_imessage_conversations",
        "inkbox_get_imessage_conversation",
        "inkbox_lookup_contact",
        "inkbox_list_contacts",
        "inkbox_get_contact",
        "inkbox_create_contact",
        "inkbox_update_contact",
        "inkbox_delete_contact",
        "inkbox_a2a_call",
        "inkbox_a2a_check",
        "inkbox_a2a_reply",
        "inkbox_list_a2a_tasks",
        "inkbox_list_a2a_messages",
        "inkbox_a2a_complete",
        "inkbox_a2a_ask_caller",
        "inkbox_a2a_fail",
    }

    assert names == expected


def test_get_and_delete_contact_tools():
    client = _FakeClient()

    contact = _call(client, "inkbox_get_contact", {"contact_id": "contact-1"})
    deleted = _call(client, "inkbox_delete_contact", {"contact_id": "contact-1"})

    assert contact["id"] == "contact-1"
    assert deleted["deleted"] == "contact-1"
    assert client.contacts.deleted == ["contact-1"]


def test_a2a_tools_send_check_and_reply():
    client = _FakeClient()
    card_url = "https://target.example/card"

    sent = _call(
        client,
        "inkbox_a2a_call",
        {"card_url": card_url, "text": "Investigate.", "message_id": "msg-1"},
    )
    checked = _call(
        client,
        "inkbox_a2a_check",
        {"card_url": card_url, "task_id": "task-1", "wait": True},
    )
    replied = _call(
        client,
        "inkbox_a2a_reply",
        {
            "card_url": card_url,
            "task_id": "task-1",
            "text": "More context.",
            "message_id": "msg-2",
        },
    )

    assert sent["task"]["id"] == "task-1"
    assert checked["state"] == "TASK_STATE_COMPLETED"
    assert replied["task"]["id"] == "task-1"
    assert (
        "wait",
        {"rpc_url": "https://target.example/a2a"},
        "task-1",
    ) in client.identity.a2a.calls
    assert client.identity.a2a.closed is True


def test_a2a_history_tools_forward_filters_and_pagination():
    client = _FakeClient()
    tasks = _call(
        client,
        "inkbox_list_a2a_tasks",
        {
            "direction": "both",
            "requester_handle": "requester",
            "worker_handle": "worker",
            "state": "completed",
            "context_id": "context-1",
            "query": "summary",
            "since": "2026-07-01T00:00:00Z",
            "cursor": "task-cursor",
            "limit": 3,
        },
    )
    messages = _call(
        client,
        "inkbox_list_a2a_messages",
        {
            "direction": "outbound",
            "requester_handle": "requester",
            "worker_handle": "worker",
            "task_id": "task-1",
            "context_id": "context-1",
            "role": "agent",
            "query": "done",
            "since": "2026-07-01T00:00:00Z",
            "cursor": "message-cursor",
            "limit": 4,
        },
    )

    assert tasks["next_cursor"] == "task-next"
    assert messages["next_cursor"] == "message-next"
    assert client.identity.a2a_history_calls[0] == (
        "tasks",
        {
            "direction": "both",
            "requester_handle": "requester",
            "worker_handle": "worker",
            "context_id": "context-1",
            "q": "summary",
            "since": "2026-07-01T00:00:00Z",
            "cursor": "task-cursor",
            "limit": 3,
            "state": "completed",
        },
    )
    assert client.identity.a2a_history_calls[1] == (
        "messages",
        {
            "direction": "outbound",
            "requester_handle": "requester",
            "worker_handle": "worker",
            "context_id": "context-1",
            "q": "done",
            "since": "2026-07-01T00:00:00Z",
            "cursor": "message-cursor",
            "limit": 4,
            "task_id": "task-1",
            "role": "agent",
        },
    )
    invalid = _call(client, "inkbox_list_a2a_messages", {"limit": 101})
    assert invalid["error"] == "limit must be between 1 and 100"


def test_a2a_intent_tools_require_trusted_turn_context():
    client = _FakeClient()

    outside = _call(client, "inkbox_a2a_complete", {"text": "Done."})
    token = tools_mod.A2A_TURN_CONTEXT.set(
        {
            "task_id": "task-1",
            "message_id": "message-1",
            "context_id": "context-1",
            "reply_intent_committed": False,
        }
    )
    try:
        inside = _call(client, "inkbox_a2a_ask_caller", {"text": "Which region?"})
        context = tools_mod.A2A_TURN_CONTEXT.get()
    finally:
        tools_mod.A2A_TURN_CONTEXT.reset(token)

    assert "only available during an inbound A2A task" in outside["error"]
    assert inside["state"] == "ask_caller"
    assert context["reply_intent_committed"] is True
    assert client.identity.a2a_replies == [
        ("task-1", {"intent": "ask_caller", "text": "Which region?"})
    ]


def test_a2a_intent_tools_share_context_with_the_mcp_process(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "a2a:identity-1:context-1")
    context_path = tools_mod.a2a_turn_context_path(
        "a2a:identity-1:context-1"
    )
    context_path.write_text(
        json.dumps(
            {
                "task_id": "task-1",
                "message_id": "message-1",
                "context_id": "context-1",
                "reply_intent_committed": False,
            }
        )
    )
    client = _FakeClient()

    result = _call(client, "inkbox_a2a_complete", {"text": "Done."})

    assert result["state"] == "complete"
    assert json.loads(context_path.read_text())[
        "reply_intent_committed"
    ] is True


def test_place_call_writes_context_and_tags_websocket_url(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    client = _FakeClient()

    data = _call(
        client,
        "inkbox_place_call",
        {
            "to_number": "+15551112222",
            "purpose": "tell them the build is fixed",
            "opening_message": "Hi, this is Codex with the build update.",
            "context": "The fix landed in PR 12.",
        },
    )

    assert data["placed"] is True
    assert data["id"] == "call-123"
    assert data["to"] == "+15551112222"
    ws_url = client.identity.place_call_kwargs["client_websocket_url"]
    parsed = urlparse(ws_url)
    query = parse_qs(parsed.query)
    assert query["existing"] == ["1"]
    token = query["context_token"][0]
    payload = json.loads((tmp_path / "call_contexts" / f"{token}.json").read_text())
    assert payload["purpose"] == "tell them the build is fixed"
    assert payload["opening_message"] == "Hi, this is Codex with the build update."
    assert payload["context"] == "The fix landed in PR 12."


def test_place_call_accepts_hermes_style_aliases(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    client = _FakeClient()

    data = _call(
        client,
        "inkbox_place_call",
        {
            "toNumber": "+15551112222",
            "purpose": "tell them the build is fixed",
            "openingMessage": "Hi, this is Codex with the build update.",
            "clientWebsocketUrl": "wss://override.inkboxwire.com/phone/media/ws",
        },
    )

    assert data["placed"] is True
    ws_url = client.identity.place_call_kwargs["client_websocket_url"]
    parsed = urlparse(ws_url)
    query = parse_qs(parsed.query)
    assert parsed.netloc == "override.inkboxwire.com"
    token = query["context_token"][0]
    payload = json.loads((tmp_path / "call_contexts" / f"{token}.json").read_text())
    assert payload["to_number"] == "+15551112222"
    assert payload["opening_message"] == "Hi, this is Codex with the build update."


def test_place_call_requires_purpose():
    data = _call(
        _FakeClient(),
        "inkbox_place_call",
        {"to_number": "+15551112222", "purpose": "  "},
    )

    assert "purpose is required" in data["error"]


def test_place_call_forwards_disabled_voicemail_detection(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_VOICEMAIL_DETECTION", "disabled")
    client = _FakeClient()

    data = _call(
        client,
        "inkbox_place_call",
        {
            "to_number": "+15551112222",
            "purpose": "run the CI voice check",
        },
    )

    assert data["placed"] is True
    assert client.identity.place_call_kwargs["voicemail_detection"] == "disabled"


def test_list_calls_passes_pagination_and_returns_rows():
    client = _FakeClient()

    data = _call(client, "inkbox_list_calls", {"limit": 5, "offset": 10})

    assert client.identity.list_calls_kwargs == {"limit": 5, "offset": 10}
    assert [row["direction"] for row in data] == ["inbound", "outbound"]


def test_get_call_transcript_returns_segments():
    client = _FakeClient()

    data = _call(client, "inkbox_get_call_transcript", {"call_id": "call-123"})

    assert client.identity.transcript_call_id == "call-123"
    assert [(seg["party"], seg["text"]) for seg in data] == [
        ("remote", "hey can you check the build"),
        ("local", "sure, it's green"),
    ]


def test_get_call_transcript_requires_call_id():
    data = _call(_FakeClient(), "inkbox_get_call_transcript", {"call_id": "  "})

    assert "call_id is required" in data["error"]


def test_send_sms_rejects_text_over_limit():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_sms",
        {
            "to": "+15551112222",
            "text": "x" * (tools_mod.SMS_MAX_LENGTH + 1),
        },
    )

    assert data["error_code"] == "sms_too_long"
    assert data["char_count"] == tools_mod.SMS_MAX_LENGTH + 1
    assert client.identity.sent_texts == []


def test_hosted_sms_is_exact_target_durable_and_single_use(monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-1")
    context_path = hosted_sms_turn_context_path("contact-1")
    context_path.write_text(json.dumps({
        "call_id": "call-hosted-1",
        "attempt": 1,
        "remote_phone": "+15551112222",
    }) + "\n")
    context_path.chmod(0o600)
    client = _FakeClient()

    first = _call(
        client,
        "inkbox_send_sms",
        {"to": "+15551112222", "text": "release update"},
    )
    duplicate = _call(
        client,
        "inkbox_send_sms",
        {"to": "+15551112222", "text": "release update"},
    )

    assert first["sent"] is True
    assert duplicate["error_code"] == "hosted_sms_duplicate_blocked"
    assert len(client.identity.sent_texts) == 1
    assert hosted_sms_attempt_state("call-hosted-1", 1) == "success"


def test_hosted_sms_wrong_target_is_terminal_before_provider_io(monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-1")
    context_path = hosted_sms_turn_context_path("contact-1")
    context_path.write_text(json.dumps({
        "call_id": "call-hosted-2",
        "attempt": 1,
        "remote_phone": "+15551112222",
    }) + "\n")
    context_path.chmod(0o600)
    client = _FakeClient()

    result = _call(
        client,
        "inkbox_send_sms",
        {"to": "+15559990000", "text": "release update"},
    )

    assert result["error_code"] == "hosted_sms_send_blocked"
    assert client.identity.sent_texts == []
    assert hosted_sms_attempt_state("call-hosted-2", 1) == "terminal"


def test_send_sms_projects_structured_sdk_failure_metadata():
    client = _FakeClient()

    class Rejected(Exception):
        def __init__(self):
            super().__init__("private provider response")
            self.status_code = 422
            self.detail = {
                "error": "message_blocked_spam_filter",
                "rule": "emoji_overload",
                "provider_id": "private-provider-id",
            }

    def reject(**_kwargs):
        raise Rejected()

    client.identity.send_text = reject
    data = _call(
        client,
        "inkbox_send_sms",
        {"to": "+15551112222", "text": "release update"},
    )

    assert data["error_code"] == "message_blocked_spam_filter"
    assert data["rule"] == "emoji_overload"
    assert data["status_code"] == 422
    assert "provider_id" not in data


def test_send_imessage_rejects_text_over_limit():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {
            "conversation_id": "imconv-123",
            "text": "x" * (tools_mod.IMESSAGE_MAX_LENGTH + 1),
        },
    )

    assert data["error_code"] == "imessage_too_long"
    assert data["char_count"] == tools_mod.IMESSAGE_MAX_LENGTH + 1
    assert client.identity.sent_imessages == []


def test_send_imessage_group_requires_dedicated_outbound_line():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {"to": ["+15550000001", "+15550000002"], "text": "hi both"},
    )

    assert "dedicated outbound iMessage" in str(data)
    assert client.identity.sent_imessages == []


def test_send_imessage_rejects_both_target_and_conversation():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {"conversation_id": "imconv-1", "to": ["+15550000001"], "text": "hi"},
    )

    assert "exactly one" in str(data)
    assert client.identity.sent_imessages == []


def test_send_imessage_rejects_too_many_recipients():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {"to": [f"+1555000000{i}" for i in range(9)], "text": "hi"},
    )

    assert "at most 8" in str(data)
    assert client.identity.sent_imessages == []


def test_send_imessage_rejects_duplicate_recipients():
    client = _FakeClient()
    data = _call(
        client,
        "inkbox_send_imessage",
        {"to": ["+15550000001", "+15550000001"], "text": "hi"},
    )

    assert "distinct" in str(data)
    assert client.identity.sent_imessages == []

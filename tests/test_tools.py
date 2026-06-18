import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pytest

from inkbox_codex import tools as tools_mod


@pytest.fixture(autouse=True)
def _run_to_thread_inline(monkeypatch):
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
        self.phone_number = type(
            "Phone",
            (),
            {"client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws?existing=1"},
        )()
        self.tunnel = type("Tunnel", (), {"public_host": "agent.inkboxwire.com"})()
        self.place_call_kwargs = None
        self.list_calls_kwargs = None
        self.transcript_call_id = None

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


class _FakeClient:
    def __init__(self):
        self.identity = _FakeIdentity()

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


def test_place_call_requires_purpose():
    data = _call(
        _FakeClient(),
        "inkbox_place_call",
        {"to_number": "+15551112222", "purpose": "  "},
    )

    assert "purpose is required" in data["error"]


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

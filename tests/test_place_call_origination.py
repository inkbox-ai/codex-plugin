"""Outbound-call line resolution: explicit choice, capability fallback, and
channel-aware defaulting when the identity has BOTH a dedicated number and
iMessage enabled.

Guards against an agent on an iMessage conversation being asked to "call me"
and the call going out over the dedicated number instead of the shared
iMessage line.
"""

import asyncio
import json
import types

import pytest

from inkbox_codex import tools as tools_mod


@pytest.fixture(autouse=True)
def _run_to_thread_inline(monkeypatch):
    async def immediate(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(tools_mod.asyncio, "to_thread", immediate)


def _identity(has_number: bool, imessage: bool):
    return types.SimpleNamespace(
        phone_number=types.SimpleNamespace(number="+15550000000") if has_number else None,
        imessage_enabled=imessage,
    )


def _set_channel(monkeypatch, tmp_path, mode, chat_id="contact-1"):
    # _current_channel_hint reads the session id stamped into the tool env and
    # the hint file the gateway writes on every inbound turn.
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    if mode is None:
        monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)
        return
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", chat_id)
    (tmp_path / "channel_hints.json").write_text(
        json.dumps({chat_id: {"mode": mode, "at": 1.0}})
    )


# --- resolution matrix ----------------------------------------------------

def test_single_line_resolves_unambiguously(monkeypatch, tmp_path):
    _set_channel(monkeypatch, tmp_path, None)
    assert tools_mod._resolve_call_origination(_identity(True, False), "") == "dedicated_number"
    assert tools_mod._resolve_call_origination(_identity(False, True), "") == "shared_imessage_number"
    assert tools_mod._resolve_call_origination(_identity(False, False), "") is None


def test_explicit_choice_wins_over_channel(monkeypatch, tmp_path):
    _set_channel(monkeypatch, tmp_path, "imessage")
    assert tools_mod._resolve_call_origination(_identity(True, True), "dedicated_number") == "dedicated_number"
    _set_channel(monkeypatch, tmp_path, "sms")
    assert tools_mod._resolve_call_origination(_identity(True, True), "shared_imessage_number") == "shared_imessage_number"


def test_both_lines_follow_conversation_channel(monkeypatch, tmp_path):
    both = _identity(True, True)
    _set_channel(monkeypatch, tmp_path, "imessage")
    assert tools_mod._resolve_call_origination(both, "") == "shared_imessage_number"
    _set_channel(monkeypatch, tmp_path, "sms")
    assert tools_mod._resolve_call_origination(both, "") == "dedicated_number"
    _set_channel(monkeypatch, tmp_path, "voice")
    assert tools_mod._resolve_call_origination(both, "") == "dedicated_number"


def test_both_lines_unknown_channel_defaults_dedicated(monkeypatch, tmp_path):
    _set_channel(monkeypatch, tmp_path, None)
    assert tools_mod._resolve_call_origination(_identity(True, True), "") == "dedicated_number"
    # An email turn gives no line preference either.
    _set_channel(monkeypatch, tmp_path, "email")
    assert tools_mod._resolve_call_origination(_identity(True, True), "") == "dedicated_number"


def test_channel_only_breaks_ties(monkeypatch, tmp_path):
    # An iMessage-only identity stays shared even on an SMS-looking turn.
    _set_channel(monkeypatch, tmp_path, "sms")
    assert tools_mod._resolve_call_origination(_identity(False, True), "") == "shared_imessage_number"


def test_hint_for_other_session_is_ignored(monkeypatch, tmp_path):
    # The hint file has an iMessage entry, but for a DIFFERENT session — this
    # tool process serves contact-2, so both-lines still defaults dedicated.
    _set_channel(monkeypatch, tmp_path, "imessage", chat_id="contact-1")
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-2")
    assert tools_mod._resolve_call_origination(_identity(True, True), "") == "dedicated_number"


# --- place-call handler ---------------------------------------------------

class _PlacingIdentity:
    def __init__(self, *, has_number=True, imessage=True, error=None):
        self.phone_number = (
            types.SimpleNamespace(
                number="+15550000000",
                client_websocket_url="wss://agent.inkboxwire.com/phone/media/ws",
            )
            if has_number
            else None
        )
        self.imessage_enabled = imessage
        self.tunnel = types.SimpleNamespace(public_host="agent.inkboxwire.com")
        self.place_call_kwargs = None
        self._error = error

    def place_call(self, **kwargs):
        self.place_call_kwargs = kwargs
        if self._error is not None:
            raise self._error
        return types.SimpleNamespace(id="call-9", status="queued")


class _Client:
    def __init__(self, identity):
        self.identity = identity

    def get_identity(self, _handle):
        return self.identity


def _place(identity, args, monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    result = asyncio.run(
        tools_mod.call_inkbox_tool(
            _Client(identity), "codex-agent", "inkbox_place_call", args
        )
    )
    return json.loads(result["content"][0]["text"])


def test_place_call_passes_resolved_origination_and_echoes_it(monkeypatch, tmp_path):
    identity = _PlacingIdentity(has_number=True, imessage=False)
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "build update"},
        monkeypatch,
        tmp_path,
    )
    assert data["placed"] is True
    assert data["origination"] == "dedicated_number"
    assert identity.place_call_kwargs["origination"] == "dedicated_number"


def test_local_call_uses_gateway_voicemail_policy(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "inkbox_tts_stt")
    monkeypatch.setenv("INKBOX_VOICEMAIL_DETECTION", "disabled")
    identity = _PlacingIdentity(has_number=True, imessage=False)

    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "build update"},
        monkeypatch,
        tmp_path,
    )

    assert data["voicemail_detection"] == "disabled"
    assert identity.place_call_kwargs["mode"] == "client_websocket"
    assert identity.place_call_kwargs["voicemail_detection"] == "disabled"


def test_hosted_call_uses_reason_and_inherits_saved_authority(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "inkbox_voice_ai")
    monkeypatch.setenv("INKBOX_VOICEMAIL_DETECTION", "disabled")
    identity = _PlacingIdentity(has_number=True, imessage=False)
    identity.place_call = lambda **kwargs: (
        setattr(identity, "place_call_kwargs", kwargs)
        or types.SimpleNamespace(
            id="call-hosted", status="queued", mode="hosted_agent",
            hosted_agent_authority_mode="yolo", voicemail_detection="disabled",
        )
    )

    data = _place(
        identity,
        {
            "to_number": "+15551112222",
            "purpose": "Confirm the release timing",
            "opening_message": "Hi, this is Codex.",
            "context": "The release is planned for Friday.",
        },
        monkeypatch,
        tmp_path,
    )

    assert data["placed"] is True
    assert data["mode"] == "hosted_agent"
    assert data["hosted_agent_authority_mode"] == "yolo"
    assert identity.place_call_kwargs == {
        "to_number": "+15551112222",
        "origination": "dedicated_number",
        "mode": "hosted_agent",
        "reason": (
            "Confirm the release timing\n\nOpening guidance: Hi, this is Codex."
            "\n\nContext: The release is planned for Friday."
        ),
        "voicemail_detection": "disabled",
    }
    assert "hosted_agent_authority_mode" not in identity.place_call_kwargs
    assert list((tmp_path / "call_contexts").glob("*.json")) == []


def test_place_call_follows_imessage_channel_when_both_lines(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-1")
    (tmp_path / "channel_hints.json").write_text(
        json.dumps({"contact-1": {"mode": "imessage", "at": 1.0}})
    )
    identity = _PlacingIdentity(has_number=True, imessage=True)
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "call them back"},
        monkeypatch,
        tmp_path,
    )
    assert data["origination"] == "shared_imessage_number"
    assert identity.place_call_kwargs["origination"] == "shared_imessage_number"


def test_place_call_explicit_origination_wins(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "contact-1")
    (tmp_path / "channel_hints.json").write_text(
        json.dumps({"contact-1": {"mode": "imessage", "at": 1.0}})
    )
    identity = _PlacingIdentity(has_number=True, imessage=True)
    data = _place(
        identity,
        {
            "to_number": "+15551112222",
            "purpose": "call them back",
            "origination": "dedicated_number",
        },
        monkeypatch,
        tmp_path,
    )
    assert data["origination"] == "dedicated_number"


def test_place_call_without_any_line_is_a_clear_error(monkeypatch, tmp_path):
    identity = _PlacingIdentity(has_number=False, imessage=False)
    identity.tunnel = None
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "say hi"},
        monkeypatch,
        tmp_path,
    )
    assert "no dedicated phone number" in data["error"]
    assert "iMessage" in data["error"]
    assert identity.place_call_kwargs is None


def test_place_call_no_shared_connection_error_is_legible(monkeypatch, tmp_path):
    identity = _PlacingIdentity(
        has_number=False,
        imessage=True,
        error=RuntimeError("HTTP 409 no_shared_connection"),
    )
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "say hi"},
        monkeypatch,
        tmp_path,
    )
    assert "isn't connected to you over iMessage" in data["error"]
    assert "dedicated_number" in data["error"]


def test_place_call_requires_current_sdk_call_contract(monkeypatch, tmp_path):
    class _LegacyIdentity(_PlacingIdentity):
        def place_call(self, *, to_number, client_websocket_url):
            # Signature without ``origination`` — the first attempt raises
            # TypeError and the handler retries without the kwarg.
            self.place_call_kwargs = {
                "to_number": to_number,
                "client_websocket_url": client_websocket_url,
            }
            return types.SimpleNamespace(id="call-9", status="queued")

    identity = _LegacyIdentity(has_number=True, imessage=False)
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "build update"},
        monkeypatch,
        tmp_path,
    )
    assert "SDK 0.5.9 or newer" in data["error"]


def test_place_call_prefers_identity_scoped_ws_url(monkeypatch, tmp_path):
    identity = _PlacingIdentity(has_number=True, imessage=False)
    identity.get_incoming_call_action = lambda: types.SimpleNamespace(
        client_websocket_url="wss://identity-row.inkboxwire.com/phone/media/ws"
    )
    data = _place(
        identity,
        {"to_number": "+15551112222", "purpose": "build update"},
        monkeypatch,
        tmp_path,
    )
    assert data["placed"] is True
    assert identity.place_call_kwargs["client_websocket_url"].startswith(
        "wss://identity-row.inkboxwire.com/phone/media/ws"
    )


# --- whoami lines block ---------------------------------------------------

def test_whoami_reports_the_two_lines(monkeypatch, tmp_path):
    identity = _PlacingIdentity(has_number=True, imessage=True)
    identity.agent_handle = "codex-agent"
    identity.mailbox = types.SimpleNamespace(email_address="codex@inkbox.ai")
    result = asyncio.run(
        tools_mod.call_inkbox_tool(_Client(identity), "codex-agent", "inkbox_whoami", {})
    )
    data = json.loads(result["content"][0]["text"])
    lines = data["lines"]
    assert lines["dedicated_phone_line"] == "+15550000000"
    assert "origination=dedicated_number" in lines["dedicated_phone_line_note"]
    assert lines["shared_imessage_line"] == "enabled"
    # The shared line's number is managed by Inkbox and never surfaced.
    assert "not shown" in lines["shared_imessage_line_note"]
    assert "origination=shared_imessage_number" in lines["shared_imessage_line_note"]


def test_whoami_lines_without_provisioning(monkeypatch, tmp_path):
    identity = _PlacingIdentity(has_number=False, imessage=False)
    identity.agent_handle = "codex-agent"
    identity.mailbox = None
    result = asyncio.run(
        tools_mod.call_inkbox_tool(_Client(identity), "codex-agent", "inkbox_whoami", {})
    )
    data = json.loads(result["content"][0]["text"])
    assert data["lines"]["dedicated_phone_line"] == "(none provisioned)"
    assert data["lines"]["shared_imessage_line"] == "disabled"


# --- tool schema ----------------------------------------------------------

def test_place_call_schema_names_the_two_lines():
    spec = next(t for t in tools_mod.mcp_tool_list() if t["name"] == "inkbox_place_call")
    assert "two lines" in spec["description"]
    origination = spec["inputSchema"]["properties"]["origination"]
    assert origination["enum"] == ["dedicated_number", "shared_imessage_number"]
    assert "origination" not in spec["inputSchema"]["required"]
    assert "voicemail_detection" not in spec["inputSchema"]["properties"]


def test_hosted_place_call_schema_exposes_task_fields_not_websocket(monkeypatch):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "inkbox_voice_ai")
    spec = next(t for t in tools_mod.mcp_tool_list() if t["name"] == "inkbox_place_call")
    properties = spec["inputSchema"]["properties"]
    assert "Inkbox Voice AI" in spec["description"]
    assert "client_websocket_url" not in properties
    assert "purpose" in properties
    assert spec["inputSchema"]["required"] == ["to_number", "purpose"]

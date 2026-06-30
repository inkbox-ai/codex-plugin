import asyncio
import json
import types

from inkbox_codex import gateway
from inkbox_codex.config import BridgeConfig


class _FakeWS:
    """Stand-in for aiohttp's WebSocketResponse.

    Captures the headers the handler sets before prepare() and yields no
    messages, so the handler arms the socket and then exits cleanly.
    """

    def __init__(self, messages=None):
        self.headers = {}
        self.sent = []
        self._messages = list(messages or [])
        self.prepared = False

    async def prepare(self, _request):
        # Headers must already be set by the time the upgrade is committed.
        self.prepared = True

    async def send_str(self, data):
        self.sent.append(json.loads(data))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class _FakeRequest:
    def __init__(self, *, headers=None, query=None):
        self.headers = headers or {}  # no X-Call-Context; signature check is off
        self.query = query or {}  # no context_token; inbound (no outbound place-call ctx)


class _FakeMsg:
    def __init__(self, data):
        self.type = "text"
        self.data = json.dumps(data)


def _write_context(tmp_path, token="ctx"):
    contexts = tmp_path / "call_contexts"
    contexts.mkdir()
    (contexts / f"{token}.json").write_text(json.dumps({
        "purpose": "talk about soccer and the World Cup",
        "opening_message": "Hey Dima, it's Codex calling about soccer and the World Cup.",
        "context": "The operator asked by iMessage for this call.",
        "to_number": "+15167251294",
    }))
    return token


def test_call_ws_declares_inkbox_stt_tts_headers(monkeypatch):
    """The WS upgrade must advertise platform-side STT/TTS so Inkbox sends us
    transcripts and speaks our text frames — without these it defaults to raw
    media and voice is silent both ways."""
    fake_ws = _FakeWS()
    # gateway.web is None when aiohttp isn't installed, so swap in a tiny
    # stand-in namespace rather than patching an attribute on it.
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    cfg = BridgeConfig(require_signature=False)
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.prepared is True
    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "true"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "true"


class _FakeBridge:
    def __init__(self):
        self.ran = False
        self.closed = False

    async def run(self, *, inkbox_ws, on_agent_consult, on_post_call_actions, on_call_ended):
        self.ran = True

    async def close(self):
        self.closed = True


def test_call_ws_realtime_path_sets_rawmedia_headers_and_runs_bridge(monkeypatch):
    """With Realtime enabled and OpenAI reachable, accept in raw-media mode
    (STT/TTS off) and hand the call to the bridge."""
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))
    bridge = _FakeBridge()

    async def fake_open(*, config, meta):
        return bridge

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_codex.realtime import RealtimeConfig
    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(enabled=True, api_key="sk-x"))
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "false"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "false"
    assert bridge.ran is True and bridge.closed is True


def test_call_ws_realtime_falls_back_to_stt_tts_on_connect_failure(monkeypatch):
    """If OpenAI can't be reached and fallback is allowed, accept the call on
    the Inkbox STT/TTS path (headers back to true) instead of dropping it."""
    fake_ws = _FakeWS()
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    async def fake_open(*, config, meta):
        raise gateway.RealtimeBridgeConnectError("no key")

    monkeypatch.setattr(gateway, "open_inkbox_realtime_bridge", fake_open)

    from inkbox_codex.realtime import RealtimeConfig
    cfg = BridgeConfig(require_signature=False, realtime=RealtimeConfig(
        enabled=True, api_key="sk-x", fallback_to_inkbox_stt_tts=True,
    ))
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest()))

    assert fake_ws.headers.get("x-use-inkbox-speech-to-text") == "true"
    assert fake_ws.headers.get("x-use-inkbox-text-to-speech") == "true"


def test_call_ws_fallback_uses_outbound_opening_on_start(monkeypatch, tmp_path):
    token = _write_context(tmp_path)
    context_path = tmp_path / "call_contexts" / f"{token}.json"
    fake_ws = _FakeWS([_FakeMsg({"event": "start"})])
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(gateway, "WSMsgType", types.SimpleNamespace(TEXT="text"))
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    cfg = BridgeConfig(require_signature=False)
    gw = gateway.InkboxGateway(cfg)

    asyncio.run(gw._handle_call_ws(_FakeRequest(query={"context_token": token})))

    assert fake_ws.sent[0] == {
        "event": "text",
        "delta": "Hey Dima, it's Codex calling about soccer and the World Cup.",
        "turn_id": "greeting",
    }
    assert not context_path.exists()


class _FakeSession:
    def __init__(self):
        self.calls = []

    async def handle_inbound(self, text, mode, meta):
        self.calls.append((text, mode, meta))


class _FakeSessions:
    def __init__(self):
        self.by_chat = {}

    def get(self, chat_id):
        session = _FakeSession()
        self.by_chat[chat_id] = session
        return session


def test_call_ws_fallback_passes_outbound_context_to_voice_turn(monkeypatch, tmp_path):
    token = _write_context(tmp_path)
    fake_ws = _FakeWS([_FakeMsg({
        "event": "transcript",
        "is_final": True,
        "text": "Do you know why you're calling me?",
    })])
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(gateway, "WSMsgType", types.SimpleNamespace(TEXT="text"))
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(WebSocketResponse=lambda: fake_ws))

    cfg = BridgeConfig(require_signature=False)
    gw = gateway.InkboxGateway(cfg)
    gw.sessions = _FakeSessions()

    asyncio.run(gw._handle_call_ws(_FakeRequest(query={"context_token": token})))

    session = gw.sessions.by_chat["+15167251294"]
    assert session.calls == [(
        "Do you know why you're calling me?",
        "voice",
        {
            "call_id": "",
            "sender": "+15167251294",
            "to": "+15167251294",
            "outbound_purpose": "talk about soccer and the World Cup",
            "outbound_opening": "Hey Dima, it's Codex calling about soccer and the World Cup.",
            "outbound_context": "The operator asked by iMessage for this call.",
        },
    )]

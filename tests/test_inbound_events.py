import asyncio
import json
import types

import pytest

from inkbox_codex import gateway, inbound_events
from inkbox_codex.config import BridgeConfig, read_config
from inkbox_codex.inbound_events import InboundEvents, agent_text, run_script


class _Response:
    def __init__(self, *, status=200, text=""):
        self.status = status
        self.text = text


class _Request:
    def __init__(self, envelope, *, signature="sha256=x"):
        self._body = json.dumps(envelope).encode()
        self.headers = {"X-Inkbox-Request-Id": "req", "X-Inkbox-Signature": signature}
        self.url = "http://127.0.0.1/webhook"

    async def read(self):
        return self._body


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(
        Response=_Response,
        json_response=lambda payload, status=200: _Response(status=status, text=json.dumps(payload)),
    ))


def _data(handler=None, event_id="inbev_1", delivery_id="inbdel_1", **request):
    return {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "source": {"id": "inbsrc_1", "name": "billing", "instructions": "Record each paid invoice."},
        "handler": handler or {"type": "agent"},
        "signature_verified": True,
        "request": {"headers": {"x-a": "1"}, "body": '{"paid": true}', "body_encoding": "utf8", **request},
    }


def _gateway(tmp_path, monkeypatch, *, mode="local", verified=True):
    gw = gateway.InkboxGateway(BridgeConfig(allow_all_users=True, delivery_mode=mode))
    gw._inbound_path = tmp_path / "inbound.json"
    provider = types.SimpleNamespace(name="inkbox", verify=lambda **_kw: verified)
    monkeypatch.setattr(gateway, "match_provider", lambda headers: provider)
    monkeypatch.setattr(gw, "_provider_secret", lambda _name: "secret")
    return gw


def _envelope(data):
    return {"event_type": "inbound.received", "timestamp": "2026-01-01T00:00:00Z", "data": data}


def test_local_mode_queues_and_dedupes(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)

    async def scenario():
        first = await gw._handle_webhook(_Request(_envelope(_data())))
        second = await gw._handle_webhook(_Request(_envelope(_data())))
        return first, second

    first, second = asyncio.run(scenario())
    assert json.loads(first.text)["deduped"] is False
    assert json.loads(second.text)["deduped"] is True
    stored = json.loads((tmp_path / "inbound.json").read_text())
    assert stored["inbdel_1"]["state"] == "pending"


def test_outside_local_mode_is_ignored(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch, mode="")
    response = asyncio.run(gw._handle_webhook(_Request(_envelope(_data()))))
    assert response.status == 200 and json.loads(response.text)["ignored"] == "inbound.received"
    assert not (tmp_path / "inbound.json").exists()


def test_requires_inkbox_signature(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch, verified=False)
    response = asyncio.run(gw._handle_webhook(_Request(_envelope(_data()))))
    assert response.status == 401


def test_agent_turn_uses_inbound_session(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)
    calls = []

    class Session:
        async def handle_inbound(self, text, mode, meta):
            calls.append((text, mode, meta))

    gw.sessions = types.SimpleNamespace(get=lambda chat_id: calls.append(chat_id) or Session())

    async def scenario():
        await gw._handle_webhook(_Request(_envelope(_data())))
        gw._inbound_events().recover()
        await asyncio.gather(*gw._inbound.tasks)

    asyncio.run(scenario())
    assert calls[0] == "inbound:billing"
    text, mode, meta = calls[1]
    assert mode == "external" and meta["event_key"] == "inbdel_1"
    assert text.index("UNTRUSTED") < text.index("Record each paid invoice.") < text.index('{"paid": true}')
    stored = json.loads((tmp_path / "inbound.json").read_text())
    assert stored["inbdel_1"]["state"] == "done"


def test_binary_body_is_not_inlined():
    text = agent_text(_data(body="AAEC", body_encoding="base64"))
    assert "binary body" in text and "AAEC" not in text


def _run(events):
    async def scenario():
        events.recover()
        await asyncio.gather(*events.tasks)
    asyncio.run(scenario())


def test_script_empty_stdout_needs_no_turn(tmp_path, monkeypatch):
    turns = []

    async def run_agent(data, body):
        turns.append(body)

    events = InboundEvents(tmp_path / "e.json", run_agent, None)
    events.accept(_data({"type": "script", "command": "cat > received.json"}))
    monkeypatch.setenv("HOME", str(tmp_path))
    _run(events)
    assert turns == []
    stdin = json.loads((tmp_path / "received.json").read_text())
    assert stdin == {"source": "billing", "event_id": "inbev_1", "headers": {"x-a": "1"},
                     "body": '{"paid": true}', "body_encoding": "utf8"}


def test_script_stdout_becomes_agent_body(tmp_path):
    turns = []

    async def run_agent(data, body):
        turns.append(body)

    events = InboundEvents(tmp_path / "e.json", run_agent, None)
    events.accept(_data({"type": "script", "command": "echo summarized"}))
    _run(events)
    assert turns == ["summarized\n"]


def test_script_failure_retries_then_reports(tmp_path, monkeypatch):
    sleeps, reports = [], []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        sleeps.append(delay)
        await real_sleep(0)

    async def report(data, error):
        reports.append((data["event_id"], error))

    monkeypatch.setattr(inbound_events.asyncio, "sleep", fake_sleep)
    events = InboundEvents(tmp_path / "e.json", None, report)
    events.accept(_data({"type": "script", "command": "echo bad >&2; exit 3"}))
    _run(events)
    assert sleeps == [0, 10, 30, 120, 600]
    assert reports == [("inbev_1", "bad\n")]
    assert json.loads((tmp_path / "e.json").read_text())["inbdel_1"]["state"] == "done"


def test_replay_with_new_delivery_id_runs_again(tmp_path):
    events = InboundEvents(tmp_path / "e.json", None, None)
    assert events.accept(_data()) is True
    assert events.accept(_data()) is False  # retry of the same delivery
    assert events.accept(_data(delivery_id="inbdel_2")) is True  # replay


def test_dedupe_falls_back_to_event_id(tmp_path):
    events = InboundEvents(tmp_path / "e.json", None, None)
    assert events.accept(_data(delivery_id=None)) is True
    assert events.accept(_data(delivery_id=None)) is False
    assert "inbev_1" in json.loads((tmp_path / "e.json").read_text())


def test_script_runs_five_times(tmp_path, monkeypatch):
    runs, sleeps, reports = [], [], []

    async def fake_sleep(delay):
        sleeps.append(delay)

    async def fake_run(command, stdin):
        runs.append(len(sleeps))
        return False, "", "bad"

    async def report(data, error):
        reports.append(error)

    monkeypatch.setattr(inbound_events.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(inbound_events, "run_script", fake_run)
    events = InboundEvents(tmp_path / "e.json", None, report)
    events.accept(_data({"type": "script", "command": "x"}))
    _run(events)
    assert runs == [1, 2, 3, 4, 5]
    assert sleeps == [0, 10, 30, 120, 600]
    assert reports == ["bad"]


def test_body_capped_at_64_kib_on_char_boundary():
    body = "a" + "\u00e9" * 40000  # the 65536-byte cut lands inside a 2-byte char
    shown = agent_text(_data(), body).split("Body:\n", 1)[1]
    assert shown.endswith("\n(truncated)")
    kept = shown[: -len("\n(truncated)")]
    assert len(kept.encode()) == 65535 and kept == body[: len(kept)]
    assert agent_text(_data(), "x" * 65536).endswith("Body:\n" + "x" * 65536)


def test_script_timeout(tmp_path):
    ok, _out, error = asyncio.run(run_script("sleep 5", b"", timeout=0.2))
    assert not ok and "timed out" in error


def test_pending_events_survive_restart(tmp_path):
    InboundEvents(tmp_path / "e.json", None, None).accept(_data())
    turns = []

    async def run_agent(data, body):
        turns.append(data["event_id"])

    restarted = InboundEvents(tmp_path / "e.json", run_agent, None)
    assert restarted.accept(_data()) is False
    _run(restarted)
    assert turns == ["inbev_1"]


def test_failure_report_without_sdk_support(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)
    gw._identity = types.SimpleNamespace(agent_handle="agent")
    gw._inkbox = object()
    data = {"event_id": "inbev_1", "source": {"id": "inbsrc_1"}}
    asyncio.run(gw._report_inbound_failure(data, "bad"))

    seen = []
    gw._inkbox = types.SimpleNamespace(inbound=types.SimpleNamespace(
        report_inbound_event_result=lambda *a, **kw: seen.append((a, kw))))
    asyncio.run(gw._report_inbound_failure(data, "bad"))
    assert seen == [(("agent", "inbsrc_1", "inbev_1"), {"status": "failed", "error": "bad"})]


def test_local_mode_skips_identity_registration(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)
    gw._inkbox = None  # any registration call would fail
    gw._patch_identity_objects()


def test_delivery_mode_config(monkeypatch):
    monkeypatch.setenv("INKBOX_DELIVERY_MODE", "LOCAL")
    assert read_config().delivery_mode == "local"
    monkeypatch.setenv("INKBOX_DELIVERY_MODE", "tunnel")
    with pytest.raises(ValueError):
        read_config()


def test_local_listener_binds_loopback_with_large_bodies(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)
    gw.cfg.host, gw.cfg.port = "0.0.0.0", 0
    seen = {}

    class App:
        def __init__(self, **kwargs):
            seen["app"] = kwargs
            self.cleanup_ctx = []
            self.router = types.SimpleNamespace(add_get=lambda *a: None, add_post=lambda *a: None)

    class Runner:
        def __init__(self, app):
            pass

        async def setup(self):
            pass

    class Site:
        def __init__(self, runner, host, port):
            seen["host"] = host

        async def start(self):
            pass

    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(
        Application=App, AppRunner=Runner, TCPSite=Site))
    asyncio.run(gw._start_http_server())
    assert seen == {"app": {"client_max_size": 2 * 1024 * 1024}, "host": "127.0.0.1"}

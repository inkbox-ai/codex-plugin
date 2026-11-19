import asyncio
import hashlib
import json
import sqlite3
import types
from uuid import uuid4

import pytest

from inkbox_codex import gateway, inbound_events
from inkbox_codex.config import BridgeConfig, read_config
from inkbox_codex.inbound_events import InboundEvents, agent_text, run_script
from inkbox_codex.managed_work import TurnQueue
from inkbox_codex.sessions import _Turn


class _Response:
    def __init__(self, *, status=200, text=""):
        self.status = status
        self.text = text


class _Request:
    def __init__(self, envelope, *, signature="sha256=x"):
        # Same serialization the server signs and sends.
        self._body = json.dumps(envelope, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
        self.headers = {"X-Inkbox-Request-Id": "req", "X-Inkbox-Signature": signature}
        self.url = "http://127.0.0.1/webhook"

    async def read(self):
        return self._body


def _json_response(payload, status=200):
    return _Response(status=status, text=json.dumps(payload))


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(Response=_Response, json_response=_json_response))


@pytest.fixture
def clock(monkeypatch):
    """Virtual time: sleeps advance the clock instead of waiting."""
    now = [1_000_000.0]
    sleeps = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *args):
        if delay > 0:
            sleeps.append(round(delay))
            now[0] += delay
        await real_sleep(0)

    monkeypatch.setattr(inbound_events.time, "time", lambda: now[0])
    monkeypatch.setattr(inbound_events.asyncio, "sleep", fake_sleep)
    return types.SimpleNamespace(now=now, sleeps=sleeps)


def _data(handler=None, event_id="inbev_1", delivery_id="5b0e2f4c-1d1e-4a57-9a43-2b6a1f0c7d10", **request):
    return {
        "event_id": event_id,
        "delivery_id": delivery_id,
        "received_at": "2026-01-01T00:00:00+00:00",
        "source": {"id": "inbsrc_1", "name": "billing", "instructions": "Record each paid invoice."},
        "handler": handler or {"type": "agent"},
        "signature_verified": True,
        "request": {"method": "POST", "path": "/", "query": "", "headers": {"x-a": "1"},
                    "body": '{"paid": true}', "body_encoding": "utf8", **request},
    }


def _envelope(data):
    return {"event_type": "inbound.received", "timestamp": "2026-01-01T00:00:00+00:00", "data": data}


def _hosted(data, activation):
    """The envelope the server delivers to a hosted agent."""
    event_id = "evt_" + hashlib.sha256(f"inbound:{data['delivery_id']}".encode()).hexdigest()
    return {**_envelope(data), "id": event_id, "hosted_delivery": {
        "version": 1, "activation_id": activation, "event_id": event_id, "starts_agent": True}}


def _gateway(tmp_path, monkeypatch, *, mode="local", verified=True):
    gw = gateway.InkboxGateway(BridgeConfig(allow_all_users=True, delivery_mode=mode))
    gw._inbound_path = tmp_path / "inbound.sqlite3"
    provider = types.SimpleNamespace(name="inkbox", verify=lambda **_kw: verified)
    monkeypatch.setattr(gateway, "match_provider", lambda headers: provider)
    monkeypatch.setattr(gw, "_provider_secret", lambda _name: "secret")
    return gw


def _rows(path):
    with sqlite3.connect(path) as db:
        return {key: (state, runs) for key, state, runs in db.execute("SELECT delivery_id, state, runs FROM events")}


async def _drain(events):
    while events.tasks:
        await asyncio.gather(*list(events.tasks))


def _run(events):
    async def scenario():
        events.recover()
        await _drain(events)
    asyncio.run(scenario())


class _Session:
    """Queues turns like a real session so delivery-scope leaks would surface."""

    def __init__(self, calls):
        self.calls = calls
        self.queue = TurnQueue()

    async def handle_inbound(self, text, mode, meta):
        self.queue.put_nowait(_Turn(text=text))
        self.calls.append((text, mode, meta))


def test_hosted_delivery_is_persisted_then_run(tmp_path, monkeypatch):
    from inkbox_codex.managed_ingress import ManagedIngress

    activation = str(uuid4())
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_HOSTED_ACTIVATION_ID", activation)
    gw = _gateway(tmp_path, monkeypatch)
    calls, chats = [], []
    session = _Session(calls)
    gw.sessions = types.SimpleNamespace(get=lambda chat_id: chats.append(chat_id) or session)
    envelope = _hosted(_data(), activation)

    async def scenario():
        ingress = ManagedIngress(gw, activation)
        gw._managed_control = types.SimpleNamespace(ingress=ingress)
        ingress.start()
        try:
            gw._inbound_events().recover()
            response = await gw._handle_webhook(_Request(envelope))
            assert response.status == 202
            receipt = json.loads(response.text)
            assert receipt["event_id"] == envelope["id"]
            for _ in range(200):
                if calls and ingress.inbox.summary()["pending"] + ingress.inbox.summary()["active"] == 0:
                    break
                await asyncio.sleep(0.02)
            await _drain(gw._inbound)
            assert ingress.inbox.summary() == {"pending": 0, "active": 0, "unknown": 0, "review_required": 0}
            repeat = await gw._handle_webhook(_Request(envelope))
            assert json.loads(repeat.text)["state"] == "completed"
        finally:
            await ingress.close()
            await gw._inbound.close()

    async def bounded():
        # The managed delivery must complete without waiting for the inbound turn.
        await asyncio.wait_for(scenario(), 10)

    asyncio.run(bounded())
    assert chats == ["inbound:billing"] and len(calls) == 1
    text, mode, meta = calls[0]
    assert mode == "external" and meta["event_key"] == envelope["data"]["delivery_id"]
    assert text.index("UNTRUSTED") < text.index("Record each paid invoice.") < text.index('{"paid": true}')
    assert _rows(tmp_path / "inbound.sqlite3") == {envelope["data"]["delivery_id"]: ("done", 0)}


def test_hosted_delivery_requires_inkbox_signature(tmp_path, monkeypatch):
    activation = str(uuid4())
    monkeypatch.setenv("INKBOX_HOSTED_ACTIVATION_ID", activation)
    gw = _gateway(tmp_path, monkeypatch, verified=False)
    gw.cfg.require_signature = False
    assert asyncio.run(gw._handle_webhook(_Request(_hosted(_data(), activation)))).status == 401
    assert asyncio.run(gw._handle_webhook(_Request(_envelope(_data())))).status == 401


def test_unmanaged_inbound_event_is_ignored_when_hosted(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_HOSTED_ACTIVATION_ID", str(uuid4()))
    gw = _gateway(tmp_path, monkeypatch)
    response = asyncio.run(gw._handle_webhook(_Request(_envelope(_data()))))
    assert json.loads(response.text)["ignored"] == "managed-delivery-required"
    assert not (tmp_path / "inbound.sqlite3").exists()


def test_local_mode_queues_and_dedupes(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)

    async def scenario():
        first = await gw._handle_webhook(_Request(_envelope(_data())))
        second = await gw._handle_webhook(_Request(_envelope(_data())))
        missing = await gw._handle_webhook(_Request(_envelope(_data(delivery_id=None))))
        return first, second, missing

    first, second, missing = asyncio.run(scenario())
    assert json.loads(first.text)["deduped"] is False
    assert json.loads(second.text)["deduped"] is True
    assert missing.status == 400
    assert gw._inbound.pending() == 1


def test_outside_local_mode_is_ignored(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch, mode="")
    response = asyncio.run(gw._handle_webhook(_Request(_envelope(_data()))))
    assert response.status == 200 and json.loads(response.text)["ignored"] == "inbound.received"
    assert not (tmp_path / "inbound.sqlite3").exists()


def test_binary_body_is_not_inlined():
    text = agent_text(_data(body="AAEC", body_encoding="base64"))
    assert "binary body" in text and "AAEC" not in text


def test_script_empty_stdout_needs_no_turn(tmp_path, monkeypatch):
    turns = []

    async def run_agent(data, body):
        turns.append(body)

    events = InboundEvents(tmp_path / "e.sqlite3", run_agent, None)
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

    events = InboundEvents(tmp_path / "e.sqlite3", run_agent, None)
    events.accept(_data({"type": "script", "command": "echo summarized"}))
    _run(events)
    assert turns == ["summarized\n"]
    assert events.pending() == 0


def test_script_runs_five_times_then_reports(tmp_path, clock):
    reports = []

    async def report(data, error):
        reports.append((data["delivery_id"], error))

    events = InboundEvents(tmp_path / "e.sqlite3", None, report)
    events.accept(_data({"type": "script", "command": "echo run >> runs.txt; echo bad >&2; exit 3"}))
    _run(events)
    assert clock.sleeps == [10, 30, 120, 600]
    assert reports == [(_data()["delivery_id"], "bad\n")]
    assert _rows(tmp_path / "e.sqlite3") == {_data()["delivery_id"]: ("done", 5)}


def test_restart_resumes_remaining_runs_and_backoff(tmp_path, clock, monkeypatch):
    runs, reports = [], []

    async def fake_run(command, stdin):
        runs.append(clock.now[0])
        return False, "", "bad"

    async def report(data, error):
        reports.append(error)

    monkeypatch.setattr(inbound_events, "run_script", fake_run)
    path = tmp_path / "e.sqlite3"
    first = InboundEvents(path, None, report)
    first.accept(_data({"type": "script", "command": "x"}))

    async def stop_after_two_runs():
        first.recover()
        while len(runs) < 2 or clock.sleeps[-1:] != [30]:
            await asyncio.sleep(0)
        await first.close()

    asyncio.run(stop_after_two_runs())
    assert _rows(path) == {_data()["delivery_id"]: ("pending", 2)}
    # Restart before the 30 s backoff elapsed: the persisted due time is kept.
    clock.now[0] -= 30
    due = clock.now[0] + 30
    clock.sleeps.clear()
    _run(InboundEvents(path, None, report))
    assert len(runs) == 5 and reports == ["bad"]
    assert clock.sleeps == [30, 120, 600] and runs[2] == due


def test_failure_report_retries_without_rerunning_script(tmp_path, clock, monkeypatch):
    attempts = []

    async def report(data, error):
        attempts.append(error)
        if len(attempts) < 3:
            raise RuntimeError("server unavailable")

    events = InboundEvents(tmp_path / "e.sqlite3", None, report)
    events.accept(_data({"type": "script", "command": "echo run >> runs.txt; exit 1"}))
    monkeypatch.setenv("HOME", str(tmp_path))
    _run(events)
    assert len(attempts) == 3
    assert (tmp_path / "runs.txt").read_text().count("run") == 5
    assert clock.sleeps[-2:] == [30, 60]


def test_failure_report_gives_up_after_a_day(tmp_path, clock):
    tries = []

    async def report(data, error):
        tries.append(clock.now[0])
        raise RuntimeError("down")

    async def agent(data, body):
        raise RuntimeError("sessions are not ready")

    events = InboundEvents(tmp_path / "e.sqlite3", agent, report)
    events.accept(_data())
    _run(events)
    assert tries and tries[-1] - tries[0] <= 86400
    assert _rows(tmp_path / "e.sqlite3") == {_data()["delivery_id"]: ("done", 0)}


def test_agent_handler_failure_is_reported_once_without_rerun(tmp_path, clock):
    turns, reports = [], []

    async def agent(data, body):
        turns.append(body)
        raise RuntimeError("sessions are not ready")

    async def report(data, error):
        reports.append(error)

    events = InboundEvents(tmp_path / "e.sqlite3", agent, report)
    events.accept(_data())
    _run(events)
    assert turns == [None] and reports == ["RuntimeError: sessions are not ready"]
    assert clock.sleeps == []


def test_interrupted_agent_turn_is_reported_not_repeated(tmp_path):
    path = tmp_path / "e.sqlite3"
    started, reports = asyncio.Event(), []

    async def blocking_agent(data, body):
        started.set()
        await asyncio.Event().wait()

    async def stop_mid_turn():
        first = InboundEvents(path, blocking_agent, None)
        first.accept(_data())
        first.recover()
        await started.wait()
        await first.close()

    asyncio.run(stop_mid_turn())

    async def agent(data, body):
        raise AssertionError("turn repeated")

    async def report(data, error):
        reports.append(error)

    _run(InboundEvents(path, agent, report))
    assert reports == ["agent turn was interrupted by a restart"]


def test_script_output_turn_failure_reports_without_rerun(tmp_path, clock, monkeypatch):
    runs, reports = [], []

    async def fake_run(command, stdin):
        runs.append(command)
        return True, "summary", ""

    async def agent(data, body):
        raise RuntimeError("no session")

    async def report(data, error):
        reports.append(error)

    monkeypatch.setattr(inbound_events, "run_script", fake_run)
    events = InboundEvents(tmp_path / "e.sqlite3", agent, report)
    events.accept(_data({"type": "script", "command": "x"}))
    _run(events)
    assert runs == ["x"] and reports == ["RuntimeError: no session"] and clock.sleeps == []


def test_replay_with_new_delivery_id_runs_again(tmp_path):
    events = InboundEvents(tmp_path / "e.sqlite3", None, None)
    assert events.accept(_data()) is True
    assert events.accept(_data()) is False  # retry of the same delivery
    assert events.accept(_data(delivery_id="inbdel_2")) is True  # replay
    with pytest.raises(ValueError):
        events.accept(_data(delivery_id=None))
    assert events.pending() == 2


def test_body_capped_at_64_kib_on_char_boundary():
    body = "a" + "é" * 40000  # the 65536-byte cut lands inside a 2-byte char
    shown = agent_text(_data(), body).split("Body:\n", 1)[1]
    assert shown.endswith("\n(truncated)")
    kept = shown[: -len("\n(truncated)")]
    assert len(kept.encode()) == 65535 and kept == body[: len(kept)]
    assert agent_text(_data(), "x" * 65536).endswith("Body:\n" + "x" * 65536)


def test_script_timeout():
    ok, _out, error = asyncio.run(run_script("sleep 5", b"", timeout=0.2))
    assert not ok and "timed out" in error


def test_script_output_is_drained_and_capped():
    stdin = b"x" * (2 * 1024 * 1024)
    # Output larger than the pipe buffers on both streams while stdin is still being written.
    command = "head -c 300000 /dev/zero | tr '\\0' o; head -c 300000 /dev/zero | tr '\\0' e >&2; cat > /dev/null"
    ok, out, err = asyncio.run(run_script(command, stdin, timeout=20))
    assert ok and out == "o" * 65536 + "\n(truncated)" and err == "e" * 4096
    ok, _out, err = asyncio.run(run_script("true", stdin, timeout=20))
    assert ok and err == ""


def test_pending_events_survive_restart(tmp_path):
    InboundEvents(tmp_path / "e.sqlite3", None, None).accept(_data())
    turns = []

    async def run_agent(data, body):
        turns.append(data["event_id"])

    restarted = InboundEvents(tmp_path / "e.sqlite3", run_agent, None)
    assert restarted.pending() == 1 and restarted.accept(_data()) is False
    _run(restarted)
    assert turns == ["inbev_1"] and restarted.pending() == 0


def _report_gateway(tmp_path, monkeypatch, report):
    gw = _gateway(tmp_path, monkeypatch)
    gw._identity = types.SimpleNamespace(agent_handle="agent")
    gw._inkbox = types.SimpleNamespace(inbound=types.SimpleNamespace(report_inbound_event_result=report))
    return gw


class _ApiError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


def test_failure_report_sends_delivery_id(tmp_path, monkeypatch):
    seen = []
    gw = _report_gateway(tmp_path, monkeypatch, lambda *a, **kw: seen.append((a, kw)))
    asyncio.run(gw._report_inbound_failure(_data(), "bad"))
    assert seen == [(("agent", "inbsrc_1", "inbev_1"),
                     {"status": "failed", "error": "bad", "delivery_id": _data()["delivery_id"]})]

    gw._inkbox = object()
    asyncio.run(gw._report_inbound_failure(_data(), "bad"))  # older SDK: logged, done


@pytest.mark.parametrize(("status", "retried"), [(409, False), (404, False), (429, True), (500, True), (None, True)])
def test_failure_report_outcomes(tmp_path, monkeypatch, status, retried):
    def report(*_a, **_kw):
        raise _ApiError(status) if status else OSError("network")

    gw = _report_gateway(tmp_path, monkeypatch, report)
    if retried:
        with pytest.raises(Exception):
            asyncio.run(gw._report_inbound_failure(_data(), "bad"))
    else:
        asyncio.run(gw._report_inbound_failure(_data(), "bad"))


def test_pending_work_counts_as_activity_but_pending_reports_do_not(tmp_path, monkeypatch):
    gw = _gateway(tmp_path, monkeypatch)
    events = gw._inbound_events()
    events.accept(_data())
    events.accept(_data({"type": "script", "command": "x"}, delivery_id="retry-wait"))
    events._update("retry-wait", runs=1, due=10**12)
    events.accept(_data(delivery_id="report"))
    events._fail("report", "bad")

    async def readiness():
        return {"companion": {"readable": True}, "codex": {"startup_ok": True}}

    monkeypatch.setattr(gw, "_readiness", readiness)
    assert asyncio.run(gw.managed_status())["active"] == 2


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

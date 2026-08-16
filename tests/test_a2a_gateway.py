import asyncio
import json
import threading
import types

import pytest

from inkbox_codex import gateway as gateway_mod
from inkbox_codex import tools as tools_mod
from inkbox_codex.a2a_progress_gate import (
    acquire_a2a_progress_gate,
    fence_a2a_progress,
    release_a2a_progress_gate,
)
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway


@pytest.fixture(autouse=True)
def fake_web(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(
        gateway_mod,
        "web",
        types.SimpleNamespace(
            json_response=lambda payload, status=200: types.SimpleNamespace(
                status=status,
                text=json.dumps(payload),
            )
        ),
    )


class _Session:
    def __init__(self):
        self.calls = []
        self.inbound = []

    async def run_consult(
        self,
        prompt,
        *,
        a2a_context=None,
        activity_handler=None,
    ):
        self.calls.append((prompt, dict(a2a_context or {})))
        return "Completed."

    async def handle_inbound(self, prompt, mode, meta):
        self.inbound.append((prompt, mode, meta))


class _Sessions:
    def __init__(self):
        self.session = _Session()
        self.keys = []

    def get(self, key):
        self.keys.append(key)
        return self.session


def _gateway(tmp_path):
    gateway = object.__new__(InkboxGateway)
    gateway._a2a_registry_path = tmp_path / "a2a.json"
    gateway._a2a_jobs = {}
    gateway._a2a_progress_jobs = {}
    gateway._a2a_progress_stop_events = {}
    gateway._a2a_identifiers = {}
    gateway.cfg = BridgeConfig(a2a_progress_interval_seconds=0)
    gateway._identity = types.SimpleNamespace(
        id="identity-1",
        a2a_task=lambda _task_id: types.SimpleNamespace(
            state="submitted",
            messages=[],
        ),
        a2a_reply=lambda task_id, **kwargs: gateway.replies.append(
            (task_id, kwargs)
        ),
    )
    gateway.replies = []
    gateway.sessions = _Sessions()
    return gateway


def _event():
    return {
        "id": "evt-1",
        "event_type": "a2a.task.created",
        "data": {
            "task_id": "task-1",
            "context_id": "context-1",
            "message_id": "message-1",
            "caller": {
                "identity_id": "caller-1",
                "organization_id": "org-1",
                "handle": "caller",
            },
            "parts": [{"text": "Investigate."}],
        },
    }


def test_a2a_gateway_persists_dedupes_and_completes(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)

    async def scenario():
        first = await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        second = await gateway._on_a2a_event(_event())
        return first, second

    first, second = asyncio.run(scenario())
    registry = json.loads(gateway._a2a_registry_path.read_text())

    assert first.status == 200
    assert json.loads(second.text)["deduped"] is True
    assert registry["task-1:message-1"]["state"] == "finalized"
    assert gateway.sessions.keys[0] == "a2a:identity-1:context-1"
    assert gateway.replies == [
        (
            "task-1",
            {
                "intent": "progress",
                "text": (
                    "Task task-1 received. Work is queued and starting. "
                    "Periodic progress updates are disabled."
                ),
            },
        ),
        ("task-1", {"intent": "complete", "text": "Completed."})
    ]


def test_a2a_receipt_reports_default_three_minute_frequency(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 180

    async def scenario():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    receipt = gateway.replies[0][1]["text"]
    assert receipt.endswith("Expect progress updates about every 3 minutes.")


def test_a2a_acknowledgement_failure_is_retried_without_duplicate_work(
    tmp_path,
    monkeypatch,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    attempts = 0

    def reply(task_id, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary delivery failure")
        gateway.replies.append((task_id, kwargs))

    gateway._identity.a2a_reply = reply

    async def scenario():
        first = await gateway._on_a2a_event(_event())
        second = await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])
        return first, second

    first, second = asyncio.run(scenario())

    assert first.status == 503
    assert second.status == 200
    assert json.loads(second.text)["deduped"] is True
    assert gateway.sessions.session.calls == [
        (
            "[inkbox:a2a_task caller=@caller caller_org=org-1]\nInvestigate.",
            {
                "task_id": "task-1",
                "message_id": "message-1",
                "context_id": "context-1",
                "reply_intent_committed": False,
            },
        )
    ]


def test_a2a_caller_cannot_spoof_delivered_receipt(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    receipt = (
        "Task task-1 received. Work is queued and starting. "
        "Periodic progress updates are disabled."
    )
    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="submitted",
        messages=[types.SimpleNamespace(
            role="caller",
            parts=[{"text": receipt}],
        )],
    )

    async def scenario():
        await gateway._on_a2a_event(_event())
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    assert gateway.replies[0] == (
        "task-1",
        {"intent": "progress", "text": receipt},
    )


def test_a2a_acknowledgement_accepts_raw_agent_role(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    data = _event()["data"]
    receipt = (
        "Task task-1 received. Work is queued and starting. "
        "Periodic progress updates are disabled."
    )
    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="submitted",
        messages=[types.SimpleNamespace(
            role="ROLE_AGENT",
            parts=[{"text": receipt}],
        )],
    )
    gateway._write_a2a_registry("task-1:message-1", data, "queued")

    asyncio.run(gateway._acknowledge_a2a_task(
        "task-1:message-1",
        data,
    ))

    assert gateway.replies == []


def test_a2a_progress_is_durable_nonterminal_and_not_duplicated(
    tmp_path,
    monkeypatch,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    async def summary(*_args, **_kwargs):
        return "I'm checking the requested data."

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    monkeypatch.setattr(gateway_mod, "build_progress_update", summary)
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._a2a_identifiers["task-1"] = ["run_sql_query"]
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )

    keep_running = asyncio.run(
        gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-1",
            _event()["data"],
        )
    )
    registry = json.loads(gateway._a2a_registry_path.read_text())
    progress_entry = registry["task-1:message-1"]["progress"]

    assert keep_running is True
    assert gateway.replies[-1][1]["intent"] == "progress"
    assert "complete" not in gateway.replies[-1][1]["text"].lower()
    assert progress_entry["delivered_count"] == 1
    assert "pending" not in progress_entry

    delivered = gateway.replies[-1][1]["text"]
    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="working",
        messages=[types.SimpleNamespace(
            role="ROLE_AGENT",
            parts=[{"text": delivered}],
        )],
    )
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_text=delivered,
    )
    before = len(gateway.replies)
    asyncio.run(
        gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-1",
            _event()["data"],
        )
    )
    assert len(gateway.replies) == before


def test_a2a_caller_cannot_spoof_pending_progress_delivery(
    tmp_path,
    monkeypatch,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    update = "I'm validating the work. (60s elapsed)"
    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="working",
        messages=[types.SimpleNamespace(
            role="caller",
            parts=[{"text": update}],
        )],
    )
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
        progress_text=update,
    )

    asyncio.run(gateway._emit_a2a_progress(
        "task-1",
        "task-1:message-1",
        _event()["data"],
    ))

    assert gateway.replies == [(
        "task-1",
        {"intent": "progress", "text": update},
    )]


def test_a2a_identifier_buffer_is_normalized_bounded_and_deduplicated(tmp_path):
    gateway = _gateway(tmp_path)

    gateway._observe_a2a_identifier("task-1", "commandExecution", "")
    gateway._observe_a2a_identifier("task-1", "commandExecution", "")
    for index in range(9):
        gateway._observe_a2a_identifier("task-1", "mcpToolCall", f"Tool {index}")

    assert gateway._a2a_identifiers["task-1"] == [
        f"tool_{index}" for index in range(1, 9)
    ]


def test_a2a_progress_elapsed_time_continues_across_caller_follow_up(tmp_path):
    gateway = _gateway(tmp_path)
    first = _event()["data"]
    gateway._write_a2a_registry(
        "task-1:message-1",
        first,
        "running",
        progress_started=True,
    )
    first_registry = json.loads(gateway._a2a_registry_path.read_text())
    started_at = first_registry["task-1:message-1"]["progress"]["started_at"]

    follow_up = dict(first)
    follow_up["message_id"] = "message-2"
    gateway._write_a2a_registry(
        "task-1:message-2",
        follow_up,
        "running",
        progress_started=True,
    )
    registry = json.loads(gateway._a2a_registry_path.read_text())

    assert registry["task-1:message-2"]["progress"]["started_at"] == started_at


def test_a2a_progress_pending_moves_to_follow_up_without_duplication(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    first = _event()["data"]
    update = "I'm validating the work. (59s elapsed)"
    gateway._write_a2a_registry(
        "task-1:message-1",
        first,
        "running",
        progress_started=True,
        progress_text=update,
    )
    follow_up = dict(first)
    follow_up["message_id"] = "message-2"
    gateway._write_a2a_registry(
        "task-1:message-2",
        follow_up,
        "running",
        progress_started=True,
    )
    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="working",
        messages=[types.SimpleNamespace(
            role="ROLE_AGENT",
            parts=[{"text": update}],
        )],
    )
    original_emit = gateway._emit_a2a_progress

    async def no_sleep(_delay):
        pytest.fail("inherited pending progress must reconcile before sleeping")

    async def reconcile_once(*args):
        result = await original_emit(*args)
        assert result is True
        return False

    gateway._emit_a2a_progress = reconcile_once
    monkeypatch.setattr(gateway_mod.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(
        gateway_mod,
        "build_progress_update",
        lambda *_args, **_kwargs: pytest.fail(
            "inherited pending text must not be regenerated"
        ),
    )

    asyncio.run(gateway._run_a2a_progress(
        "task-1",
        "task-1:message-2",
        follow_up,
    ))

    registry = json.loads(gateway._a2a_registry_path.read_text())
    progress = registry["task-1:message-2"]["progress"]
    assert progress["delivered_count"] == 1
    assert progress["last_delivered_text"] == update
    assert "pending" not in progress


def test_a2a_progress_runner_preserves_restart_phase(monkeypatch, tmp_path):
    now = 1_000.0
    monkeypatch.setattr(gateway_mod.time, "time", lambda: now)
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )
    now = 1_059.0
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    async def stop_after_one(*_args):
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "sleep", fake_sleep)
    gateway._emit_a2a_progress = stop_after_one

    asyncio.run(gateway._run_a2a_progress(
        "task-1",
        "task-1:message-1",
        _event()["data"],
    ))

    assert sleeps == [1]


def test_a2a_progress_runner_preserves_follow_up_phase(monkeypatch, tmp_path):
    now = 2_000.0
    monkeypatch.setattr(gateway_mod.time, "time", lambda: now)
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )
    now = 2_059.0
    follow_up = dict(_event()["data"])
    follow_up["message_id"] = "message-2"
    gateway._write_a2a_registry(
        "task-1:message-2",
        follow_up,
        "running",
        progress_started=True,
    )
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    async def stop_after_one(*_args):
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "sleep", fake_sleep)
    gateway._emit_a2a_progress = stop_after_one

    asyncio.run(gateway._run_a2a_progress(
        "task-1",
        "task-1:message-2",
        follow_up,
    ))

    assert sleeps == [1]


def test_a2a_progress_runner_retries_pending_delivery_immediately(
    monkeypatch,
    tmp_path,
):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
        progress_text="I'm validating the work. (59s elapsed)",
    )

    async def unexpected_sleep(_delay):
        pytest.fail("a pending delivery must be retried before sleeping")

    async def stop_after_one(*_args):
        return False

    monkeypatch.setattr(gateway_mod.asyncio, "sleep", unexpected_sleep)
    gateway._emit_a2a_progress = stop_after_one

    asyncio.run(gateway._run_a2a_progress(
        "task-1",
        "task-1:message-1",
        _event()["data"],
    ))


def test_a2a_progress_runner_retries_active_delivery_after_five_seconds(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )
    reply_attempts = 0

    def reply(task_id, **kwargs):
        nonlocal reply_attempts
        reply_attempts += 1
        if reply_attempts == 1:
            raise OSError("delivery unavailable")
        gateway.replies.append((task_id, kwargs))

    gateway._identity.a2a_reply = reply
    summary_calls = 0

    async def summary(*_args, **_kwargs):
        nonlocal summary_calls
        summary_calls += 1
        return "I'm validating the work."

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(gateway_mod, "build_progress_update", summary)
    monkeypatch.setattr(gateway_mod.asyncio, "sleep", fake_sleep)
    original_emit = gateway._emit_a2a_progress

    async def stop_after_retry(*args):
        result = await original_emit(*args)
        return False if reply_attempts == 2 else result

    gateway._emit_a2a_progress = stop_after_retry

    asyncio.run(gateway._run_a2a_progress(
        "task-1",
        "task-1:message-1",
        _event()["data"],
    ))

    assert sleeps[0] == pytest.approx(60, abs=0.1)
    assert sleeps[1:] == [5]
    assert summary_calls == 1
    assert reply_attempts == 2


def test_a2a_terminal_tool_waits_for_inflight_progress(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )
    progress_entered = asyncio.Event()
    release_progress = asyncio.Event()
    terminal_replies = []

    async def paused_summary(*_args, **_kwargs):
        progress_entered.set()
        await release_progress.wait()
        return "I'm validating the work."

    class Identity:
        def a2a_reply(self, task_id, **kwargs):
            terminal_replies.append((task_id, kwargs))
            return {"id": task_id, "state": kwargs["intent"]}

    client = types.SimpleNamespace(
        get_identity=lambda _handle: Identity(),
    )
    monkeypatch.setattr(gateway_mod, "build_progress_update", paused_summary)

    async def scenario():
        token = tools_mod.A2A_TURN_CONTEXT.set({
            "task_id": "task-1",
            "message_id": "message-1",
            "context_id": "context-1",
            "reply_intent_committed": False,
        })
        try:
            progress_task = asyncio.create_task(gateway._emit_a2a_progress(
                "task-1",
                "task-1:message-1",
                _event()["data"],
            ))
            await progress_entered.wait()
            terminal_task = asyncio.create_task(tools_mod.call_inkbox_tool(
                client,
                "agent",
                "inkbox_a2a_complete",
                {"text": "Done."},
            ))
            await asyncio.sleep(0.05)
            assert terminal_replies == []
            release_progress.set()
            await progress_task
            await terminal_task
        finally:
            tools_mod.A2A_TURN_CONTEXT.reset(token)

    asyncio.run(scenario())

    assert gateway.replies[-1][1]["intent"] == "progress"
    assert terminal_replies == [(
        "task-1",
        {"intent": "complete", "text": "Done."},
    )]


def test_a2a_progress_gate_wait_is_cancellation_safe(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    held = acquire_a2a_progress_gate("task-1")

    async def scenario():
        blocked = asyncio.create_task(gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-1",
            _event()["data"],
        ))
        await asyncio.sleep(0.01)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked

    try:
        asyncio.run(scenario())
    finally:
        release_a2a_progress_gate(held)

    available = acquire_a2a_progress_gate("task-1")
    release_a2a_progress_gate(available)


def test_a2a_terminal_failure_keeps_progress_fenced(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
    )

    class Identity:
        def a2a_reply(self, *_args, **_kwargs):
            raise OSError("ambiguous delivery")

    client = types.SimpleNamespace(get_identity=lambda _handle: Identity())

    async def scenario():
        token = tools_mod.A2A_TURN_CONTEXT.set({
            "task_id": "task-1",
            "message_id": "message-1",
            "context_id": "context-1",
            "reply_intent_committed": False,
        })
        try:
            result = await tools_mod.call_inkbox_tool(
                client,
                "agent",
                "inkbox_a2a_fail",
                {"reason": "Cannot continue."},
            )
        finally:
            tools_mod.A2A_TURN_CONTEXT.reset(token)
        keep_running = await gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-1",
            _event()["data"],
        )
        return result, keep_running

    result, keep_running = asyncio.run(scenario())

    assert "ambiguous delivery" in result["content"][0]["text"]
    assert keep_running is False
    assert gateway.replies == []


def test_a2a_ask_caller_follow_up_reacquires_progress(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    first = _event()["data"]
    gateway._write_a2a_registry(
        "task-1:message-1",
        first,
        "finalized",
        progress_started=True,
        preserve_progress_pending=True,
    )
    gate = acquire_a2a_progress_gate("task-1")
    try:
        fence_a2a_progress("task-1", "message-1")
    finally:
        release_a2a_progress_gate(gate)
    follow_up = dict(first)
    follow_up["message_id"] = "message-2"

    async def scenario():
        await gateway._start_a2a_progress(
            "task-1",
            "task-1:message-2",
            follow_up,
        )
        await gateway._stop_a2a_progress("task-1", "task-1:message-2")
        return await gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-2",
            follow_up,
        )

    assert asyncio.run(scenario()) is True
    assert gateway.replies[-1][1]["intent"] == "progress"


def test_a2a_same_key_restart_with_older_sibling_keeps_fence(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    first = _event()["data"]
    second = dict(first)
    second["message_id"] = "message-2"
    gateway._write_a2a_registry(
        "task-1:message-1",
        first,
        "finalized",
        progress_started=True,
    )
    gateway._write_a2a_registry(
        "task-1:message-2",
        second,
        "running",
        progress_started=True,
    )
    gate = acquire_a2a_progress_gate("task-1")
    try:
        fence_a2a_progress("task-1", "message-2")
    finally:
        release_a2a_progress_gate(gate)

    async def scenario():
        await gateway._start_a2a_progress(
            "task-1",
            "task-1:message-2",
            second,
        )
        await gateway._stop_a2a_progress("task-1", "task-1:message-2")
        return await gateway._emit_a2a_progress(
            "task-1",
            "task-1:message-2",
            second,
        )

    assert asyncio.run(scenario()) is False
    assert gateway.replies == []


def test_a2a_progress_update_does_not_wake_requester_session(tmp_path):
    gateway = _gateway(tmp_path)
    event = _event()
    event["event_type"] = "a2a.sent_task.updated"
    event["data"]["state"] = "working"
    event["data"]["parts"] = [{"text": "Still working."}]

    asyncio.run(gateway._on_a2a_event(event))

    assert gateway.sessions.keys == []
    assert gateway.sessions.session.inbound == []


def test_a2a_terminal_update_still_wakes_requester_session(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(
        gateway_mod,
        "find_a2a_delegation",
        lambda _task_id: {
            "session_key": "contact-1",
            "card_url": "https://target.example/card",
        },
    )
    event = _event()
    event["event_type"] = "a2a.sent_task.updated"
    event["data"]["state"] = "completed"
    event["data"]["parts"] = [{"text": "Finished."}]

    asyncio.run(gateway._on_a2a_event(event))

    assert gateway.sessions.keys == ["contact-1"]
    assert "state=completed" in gateway.sessions.session.inbound[0][0]


def test_a2a_cancel_stops_progress_child(tmp_path):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    event = _event()
    event["event_type"] = "a2a.task.canceled"

    async def scenario():
        stop_event = asyncio.Event()
        child = asyncio.create_task(stop_event.wait())
        gateway._a2a_progress_jobs["task-1"] = (
            "task-1:message-1",
            child,
        )
        gateway._a2a_progress_stop_events["task-1"] = stop_event
        gateway._a2a_identifiers["task-1"] = ["command_execution"]
        await gateway._on_a2a_event(event)
        return child

    child = asyncio.run(scenario())

    assert child.done() and not child.cancelled()
    assert gateway._a2a_progress_jobs == {}
    assert gateway._a2a_identifiers == {}


def test_a2a_cleanup_waits_for_inflight_reply_thread(tmp_path):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    gateway._hosted_call_jobs = {}
    gateway._runner = None
    gateway._tunnel = None
    gateway.sessions = None
    entered = threading.Event()
    release = threading.Event()
    replies = []

    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="working",
        messages=[],
    )

    def reply(task_id, **kwargs):
        entered.set()
        release.wait(timeout=5)
        replies.append((task_id, kwargs))

    gateway._identity.a2a_reply = reply
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
        progress_text="I'm validating the work. (60s elapsed)",
    )

    async def scenario():
        await gateway._start_a2a_progress(
            "task-1",
            "task-1:message-1",
            _event()["data"],
        )
        while not entered.is_set():
            await asyncio.sleep(0.01)
        cleanup = asyncio.create_task(gateway._cleanup())
        await asyncio.sleep(0.05)
        assert not cleanup.done()
        release.set()
        await cleanup
        await asyncio.sleep(0.05)

    asyncio.run(scenario())

    assert len(replies) == 1
    assert gateway._a2a_progress_jobs == {}


def test_a2a_default_completion_drains_and_fences_progress(tmp_path):
    gateway = _gateway(tmp_path)
    gateway.cfg.a2a_progress_interval_seconds = 60
    entered = threading.Event()
    release = threading.Event()
    replies = []

    gateway._identity.a2a_task = lambda _task_id: types.SimpleNamespace(
        state="working",
        messages=[],
    )

    def reply(task_id, **kwargs):
        if kwargs.get("text") == "I'm validating the work. (60s elapsed)":
            entered.set()
            release.wait(timeout=5)
        replies.append((task_id, kwargs))

    gateway._identity.a2a_reply = reply
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
        progress_started=True,
        progress_text="I'm validating the work. (60s elapsed)",
    )

    class Session(_Session):
        async def run_consult(self, prompt, *, a2a_context=None, activity_handler=None):
            self.calls.append((prompt, dict(a2a_context or {})))
            while not entered.is_set():
                await asyncio.sleep(0.01)
            return "Completed."

    gateway.sessions.session = Session()

    async def scenario():
        turn = asyncio.create_task(gateway._run_a2a_turn(
            "task-1:message-1",
            _event()["data"],
        ))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert "complete" not in [
            kwargs["intent"] for _task_id, kwargs in replies
        ]
        release.set()
        await turn
        await asyncio.sleep(0.05)

    asyncio.run(scenario())

    assert [kwargs["intent"] for _task_id, kwargs in replies] == [
        "progress",
        "progress",
        "complete",
    ]


def test_a2a_gateway_resumes_nonfinal_registry_entries(tmp_path, monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state="working",
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                message_id="message-1",
                role="ROLE_CALLER",
                parts=[{"text": "SDK copy must not replace persisted input."}],
            ),
            types.SimpleNamespace(
                message_id="receipt-1",
                role="ROLE_AGENT",
                parts=[{"text": (
                    "Task task-1 received. Work is queued and starting. "
                    "Periodic progress updates are disabled."
                )}],
            ),
            types.SimpleNamespace(
                message_id="progress-1",
                role="agent",
                parts=[{"text": "I'm continuing the requested work. (60s elapsed)"}],
            ),
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter((task,))
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
    )

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())
    registry = json.loads(gateway._a2a_registry_path.read_text())

    assert registry["task-1:message-1"]["state"] == "finalized"
    assert list(registry) == ["task-1:message-1"]
    assert gateway.sessions.session.calls[0][0].endswith("Investigate.")


@pytest.mark.parametrize("settled_state", ["input_required", "auth_required"])
def test_a2a_catch_up_finalizes_settled_task_without_rerun(
    tmp_path,
    monkeypatch,
    settled_state,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    task = types.SimpleNamespace(state=settled_state)
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
    )

    asyncio.run(gateway._catch_up_a2a_tasks())

    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert registry["task-1:message-1"]["state"] == "finalized"
    assert gateway.sessions.session.calls == []
    assert gateway._a2a_jobs == {}


def test_a2a_new_caller_follow_up_runs_after_settled_recovery(
    tmp_path,
    monkeypatch,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    authoritative = types.SimpleNamespace(state="input_required")
    gateway._identity.a2a_task = lambda _task_id: authoritative
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())
    gateway._write_a2a_registry(
        "task-1:message-1",
        _event()["data"],
        "running",
    )

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        authoritative.state = "working"
        follow_up = _event()
        follow_up["data"] = dict(follow_up["data"])
        follow_up["data"]["message_id"] = "message-2"
        await gateway._on_a2a_event(follow_up)
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert "task-1:message-2" in registry
    assert len(gateway.sessions.session.calls) == 1


def test_a2a_catch_up_new_task_selects_latest_caller_message(
    tmp_path,
    monkeypatch,
):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(gateway_mod.asyncio, "to_thread", inline)
    gateway = _gateway(tmp_path)
    task = types.SimpleNamespace(
        id="task-1",
        context_id="context-1",
        state="submitted",
        caller=types.SimpleNamespace(
            identity_id="caller-1",
            organization_id="org-1",
            handle="caller",
        ),
        messages=[
            types.SimpleNamespace(
                message_id="message-1",
                role="ROLE_CALLER",
                parts=[{"text": "Use this caller request."}],
            ),
            types.SimpleNamespace(
                message_id="progress-1",
                role="ROLE_AGENT",
                parts=[{"text": "Ignore this worker progress."}],
            ),
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter((task,))

    async def scenario():
        await gateway._catch_up_a2a_tasks()
        await asyncio.gather(*gateway._a2a_jobs["task-1"])

    asyncio.run(scenario())

    assert len(gateway.sessions.session.calls) == 1
    assert gateway.sessions.session.calls[0][0].endswith(
        "Use this caller request."
    )
    registry = json.loads(gateway._a2a_registry_path.read_text())
    assert list(registry) == ["task-1:message-1"]


def test_a2a_sent_update_returns_to_the_delegating_session(
    tmp_path,
    monkeypatch,
):
    gateway = _gateway(tmp_path)
    monkeypatch.setattr(
        gateway_mod,
        "find_a2a_delegation",
        lambda _task_id: {
            "session_key": "contact-1",
            "card_url": "https://target.example/card",
        },
    )
    event = _event()
    event["event_type"] = "a2a.sent_task.updated"
    event["data"]["state"] = "input_required"
    event["data"]["parts"] = [{"text": "Which region?"}]

    asyncio.run(gateway._on_a2a_event(event))

    assert gateway.sessions.keys[0] == "contact-1"
    prompt, mode, meta = gateway.sessions.session.inbound[0]
    assert "Which region?" in prompt
    assert mode == "external"
    assert meta["a2a_task_id"] == "task-1"

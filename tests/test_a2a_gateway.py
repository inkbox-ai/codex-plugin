import asyncio
import json
import types

import pytest

from inkbox_codex import gateway as gateway_mod
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
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
        self.calls.append((prompt, a2a_context))
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
    gateway._a2a_activities = {}
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
    gateway._a2a_activities["task-1"] = ["checking the requested data"]
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
        messages=[types.SimpleNamespace(parts=[{"text": delivered}])],
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
        child = asyncio.create_task(asyncio.sleep(60))
        gateway._a2a_progress_jobs["task-1"] = (
            "task-1:message-1",
            child,
        )
        gateway._a2a_activities["task-1"] = ["working through the task"]
        await gateway._on_a2a_event(event)
        return child

    child = asyncio.run(scenario())

    assert child.cancelled()
    assert gateway._a2a_progress_jobs == {}
    assert gateway._a2a_activities == {}


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
                parts=[{"text": "Resume this."}],
            )
        ],
    )
    gateway._identity.a2a_task = lambda _task_id: task
    gateway._identity.iter_a2a_tasks = lambda **_kwargs: iter(())
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
    assert gateway.sessions.session.calls[0][0].endswith("Resume this.")


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

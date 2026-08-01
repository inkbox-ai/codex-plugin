import asyncio
import json
import os
import types

from inkbox_codex import gateway as gateway_module
from inkbox_codex.codex_client import CodexTurnResult, McpToolCallResult
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.hosted_sms_guard import (
    reserve_hosted_sms_attempt,
    settle_hosted_sms_attempt,
)


class _Identity:
    def list_transcripts(self, call_id):
        assert call_id == "call-1"
        return [
            types.SimpleNamespace(
                party="remote",
                text="After we hang up, send me one SMS with the result.",
            ),
            types.SimpleNamespace(party="local", text="I will do that."),
        ]


class _NonSmsIdentity:
    def list_transcripts(self, _call_id):
        return [
            types.SimpleNamespace(
                party="remote", text="Please update the release checklist.",
            ),
        ]


def _sms_result(
    *,
    status="completed",
    to="+15167251294",
    sent=True,
    error_kind="unknown",
    aborted=False,
):
    return CodexTurnResult(
        text="This plaintext must not be delivered.",
        mcp_tool_calls=(McpToolCallResult(
            server="inkbox",
            tool="inkbox_send_sms",
            status=status,
            arguments={"to": to, "text": "release update"},
            sent=sent,
            error_kind=error_kind,
        ),),
        aborted=aborted,
    )


class _Session:
    def __init__(self, gate=None, error=None, results=None):
        self.prompts = []
        self.gate = gate
        self.error = error
        self.results = list(results or [_sms_result()])
        self.hosted_contexts = []

    async def run_consult(self, prompt):
        self.prompts.append(prompt)
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return "This plaintext must not be delivered."

    async def run_consult_detailed(self, prompt, **kwargs):
        self.prompts.append(prompt)
        self.hosted_contexts.append(kwargs.get("hosted_sms_context"))
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return self.results.pop(0)


class _Sessions:
    def __init__(self, session):
        self.session = session

    def get(self, chat_id):
        assert chat_id == "contact-1"
        return self.session


def _payload():
    return {
        "id": "event-1",
        "event_type": "call.ended",
        "data": {
            "call": {
                "id": "call-1",
                "mode": "hosted_agent",
                "direction": "outbound",
                "remote_phone_number": "+15167251294",
                "status": "completed",
                "hangup_reason": "remote_hangup",
                "reason": "Confirm the release timing",
            },
            "outcome": "completed",
            "post_call_action_items": [{
                "status": "open",
                "action": "Send the requested release update",
                "details": "by SMS",
            }],
            "contacts": [{
                "id": "contact-1",
                "preferred_name": "Dima",
                "phone_numbers": ["+19999999999"],
                "memories": [
                    "  Prefers concise release updates.  ",
                    "",
                    "Prefers concise release updates.",
                ],
            }],
        },
    }


def _non_sms_payload():
    payload = _payload()
    payload["data"]["post_call_action_items"] = [{
        "status": "open",
        "action": "Update the release checklist",
    }]
    return payload


def _gateway(tmp_path, monkeypatch, session):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = InkboxGateway(BridgeConfig(identity="agent"))
    gateway._inkbox = types.SimpleNamespace(get_identity=lambda _handle: _Identity())
    gateway.sessions = _Sessions(session)

    async def resolve(_call, _remote):
        return gateway._contact_summary({
            "id": "contact-1", "preferred_name": "Dima",
        })

    gateway._resolve_call_contact = resolve
    return gateway


async def _drain(gateway):
    while gateway._hosted_call_jobs:
        await asyncio.gather(*list(gateway._hosted_call_jobs.values()))


def test_hosted_completion_fetches_transcript_and_suppresses_plaintext(tmp_path, monkeypatch):
    async def scenario():
        session = _Session()
        gateway = _gateway(tmp_path, monkeypatch, session)
        response = await gateway._on_hosted_call_ended(_payload())
        assert response.status == 200
        await _drain(gateway)
        assert len(session.prompts) == 1
        prompt = session.prompts[0]
        assert "+15167251294" in prompt
        assert "+19999999999" not in prompt
        assert "inkbox_send_sms" in prompt
        assert "`to` set to that exact remote number" in prompt
        assert "Every safe, still-needed commitment must be attempted" in prompt
        assert "Mark it complete only after the required tool reports success" in prompt
        assert "After a terminal error or a failed second attempt" in prompt
        assert "stop without claiming success or duplicating the send" in prompt
        assert "After we hang up, send me one SMS with the result." in prompt
        assert "Send the requested release update" in prompt
        assert "Prefers concise release updates." in prompt
        assert prompt.count("Prefers concise release updates.") == 1
        registry = json.loads(gateway._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "completed"
        assert "payload" not in registry["call-1"]
        assert gateway._hosted_call_registry_path.stat().st_mode & 0o777 == 0o600

    asyncio.run(scenario())


def test_hosted_completion_dedupes_same_process_inflight(tmp_path, monkeypatch):
    async def scenario():
        gate = asyncio.Event()
        session = _Session(gate)
        gateway = _gateway(tmp_path, monkeypatch, session)
        first = await gateway._on_hosted_call_ended(_payload())
        await asyncio.sleep(0)
        second = await gateway._on_hosted_call_ended(_payload())
        assert first.status == 200
        assert json.loads(second.text)["deduped"] is True
        gate.set()
        await _drain(gateway)
        assert len(session.prompts) == 1

    asyncio.run(scenario())


def test_hosted_sms_recoverable_failure_gets_one_correction(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[
            _sms_result(status="failed", sent=False, error_kind="recoverable"),
            _sms_result(),
        ])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 2
        assert "only correction attempt" in session.prompts[1]
        assert "Do not return [SILENT], skip, or defer" in session.prompts[1]
        assert "+15167251294" in session.prompts[1]
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert session.hosted_contexts == [
            {
                "call_id": "call-1",
                "attempt": 1,
                "remote_phone": "+15167251294",
            },
            {
                "call_id": "call-1",
                "attempt": 2,
                "remote_phone": "+15167251294",
            },
        ]

    asyncio.run(scenario())


def test_hosted_sms_failed_twice_stays_terminally_failed(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[
            _sms_result(status="failed", sent=False, error_kind="recoverable"),
            _sms_result(status="failed", sent=False, error_kind="recoverable"),
        ])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 2
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False
        assert "payload" not in entry

        restarted = _gateway(tmp_path, monkeypatch, _Session())
        await restarted._recover_hosted_call_completions()
        assert restarted._hosted_call_jobs == {}

    asyncio.run(scenario())


def test_hosted_sms_turn_exception_is_terminal_and_not_replayed(
    tmp_path, monkeypatch,
):
    async def scenario():
        session = _Session(error=RuntimeError("capture transport failed"))
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False
        assert "payload" not in entry

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        await restarted._recover_hosted_call_completions()
        assert restarted._hosted_call_jobs == {}
        assert restarted_session.prompts == []

    asyncio.run(scenario())


def test_hosted_sms_turn_cancellation_is_terminal_and_not_replayed(
    tmp_path, monkeypatch,
):
    async def scenario():
        gate = asyncio.Event()
        session = _Session(gate=gate)
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        while not session.prompts:
            await asyncio.sleep(0)
        task = next(iter(gateway._hosted_call_jobs.values()))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        await restarted._recover_hosted_call_completions()
        assert restarted._hosted_call_jobs == {}
        assert restarted_session.prompts == []

    asyncio.run(scenario())


def test_hosted_sms_terminal_failure_is_not_retried(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[
            _sms_result(status="failed", sent=False, error_kind="terminal"),
        ])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_missing_tool_gets_only_one_correction(tmp_path, monkeypatch):
    async def scenario():
        empty = CodexTurnResult(text="", mcp_tool_calls=())
        session = _Session(results=[empty, empty])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 2
        assert "tool was not called" in session.prompts[1]
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_requires_positive_sent_result(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[_sms_result(sent=False)])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_completed_to_wrong_target_is_not_retried(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[_sms_result(to="+19999999999")])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_duplicate_completed_side_effects_are_not_retried(tmp_path, monkeypatch):
    async def scenario():
        duplicate = CodexTurnResult(
            text="",
            mcp_tool_calls=_sms_result().mcp_tool_calls * 2,
        )
        session = _Session(results=[duplicate])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_blocked_duplicate_preserves_single_success():
    result = CodexTurnResult(
        text="",
        mcp_tool_calls=(
            _sms_result().mcp_tool_calls[0],
            McpToolCallResult(
                server="inkbox",
                tool="inkbox_send_sms",
                status="failed",
                arguments={"to": "+15167251294"},
                sent=False,
                error_kind="duplicate_blocked",
            ),
        ),
    )

    assert gateway_module._hosted_sms_settlement(
        result, "+15167251294"
    ) == "success"


def test_hosted_sms_failure_then_success_in_one_turn_is_terminal(tmp_path, monkeypatch):
    async def scenario():
        mixed = CodexTurnResult(
            text="",
            mcp_tool_calls=(
                _sms_result(
                    status="failed",
                    sent=False,
                    error_kind="recoverable",
                ).mcp_tool_calls[0],
                _sms_result().mcp_tool_calls[0],
            ),
        )
        session = _Session(results=[mixed])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_sms_aborted_capture_is_terminal_without_replay(tmp_path, monkeypatch):
    async def scenario():
        session = _Session(results=[_sms_result(aborted=True)])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        await restarted._recover_hosted_call_completions()
        assert restarted._hosted_call_jobs == {}
        assert restarted_session.prompts == []

    asyncio.run(scenario())


def test_hosted_sms_correction_requires_one_clean_success(tmp_path, monkeypatch):
    async def scenario():
        mixed = CodexTurnResult(
            text="",
            mcp_tool_calls=(
                _sms_result().mcp_tool_calls[0],
                _sms_result(
                    status="failed",
                    sent=False,
                    error_kind="recoverable",
                ).mcp_tool_calls[0],
            ),
        )
        session = _Session(results=[
            _sms_result(status="failed", sent=False, error_kind="recoverable"),
            mixed,
        ])
        gateway = _gateway(tmp_path, monkeypatch, session)
        await gateway._on_hosted_call_ended(_payload())
        await _drain(gateway)

        assert len(session.prompts) == 2
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_hosted_transcript_sms_commitment_requires_settlement(tmp_path, monkeypatch):
    async def scenario():
        empty = CodexTurnResult(text="", mcp_tool_calls=())
        session = _Session(results=[empty, _sms_result()])
        gateway = _gateway(tmp_path, monkeypatch, session)
        payload = _payload()
        payload["data"]["post_call_action_items"] = []

        await gateway._on_hosted_call_ended(payload)
        await _drain(gateway)

        assert len(session.prompts) == 2
        assert "tool was not called" in session.prompts[1]
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"

    asyncio.run(scenario())


def test_hosted_transcript_generic_text_references_do_not_require_sms(
    tmp_path, monkeypatch,
):
    class _ReferenceOnlyIdentity:
        def list_transcripts(self, _call_id):
            return [
                types.SimpleNamespace(party="remote", text="I got your text yesterday."),
                types.SimpleNamespace(
                    party="remote", text="Can you see my earlier SMS messages?",
                ),
                types.SimpleNamespace(
                    party="local", text="We discussed texting during the call.",
                ),
            ]

    async def scenario():
        session = _Session()
        gateway = _gateway(tmp_path, monkeypatch, session)
        gateway._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: _ReferenceOnlyIdentity(),
        )
        payload = _payload()
        payload["data"]["post_call_action_items"] = []

        await gateway._on_hosted_call_ended(payload)
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"

    asyncio.run(scenario())


def test_hosted_transcript_sms_commitment_classifier_is_narrow():
    assert gateway_module._hosted_requires_sms(
        [], [("remote", "After we hang up, text Alex with the release status.")]
    )
    assert gateway_module._hosted_requires_sms(
        [], [("remote", "When this call ends, could you text her the confirmation?")]
    )
    assert gateway_module._hosted_requires_sms(
        [], [("remote", "Please text Alex the confirmation once the call is over.")]
    )
    assert gateway_module._hosted_requires_sms(
        [], [("local", "I'll send you a text message after we hang up.")]
    )
    assert gateway_module._hosted_requires_sms(
        [], [("local", "After I hang up, text the release status to Dima.")]
    )
    assert gateway_module._hosted_requires_sms(
        [],
        [("remote", "Don't text me now; after this call ends, text me the code.")],
    )
    assert gateway_module._hosted_requires_sms(
        [], [("remote", "After this call ends; text me release-ready.")]
    )
    assert gateway_module._hosted_requires_sms(
        [], [("remote", "Text me release-ready. Do it once this call is over.")]
    )
    assert not gateway_module._hosted_requires_sms(
        [],
        [
            ("remote", "I got your text yesterday."),
            ("remote", "Can you see my earlier SMS messages?"),
            ("local", "We discussed texting during the call."),
            ("local", "I will send her the confirmation."),
            ("remote", "After we hang up, send me the summary."),
            ("remote", "Please text me the status."),
            ("local", "I'll text you the final result."),
            ("remote", "After we hang up, review the text conversation."),
            ("remote", "Review the text exchange after we hang up."),
            ("remote", "After we hang up, do not send me an SMS."),
            ("remote", "After the call ends, don't text me."),
            ("remote", "Never text me after the call is over."),
        ],
    )
    assert not gateway_module._hosted_requires_sms(
        [{"status": "open", "action": "Do not send me an SMS."}], []
    )
    assert not gateway_module._hosted_requires_sms(
        [
            {"status": "open", "action": "Review the text exchange."},
            {"status": "open", "action": "Search SMS history for the release."},
            {"status": "open", "action": "Summarize the text conversation."},
            {"status": "open", "action": "Read the SMS thread."},
        ],
        [],
    )
    assert gateway_module._hosted_requires_sms(
        [
            {"status": "open", "action": "Do not send me an SMS."},
            {"status": "open", "action": "Text the operator by SMS."},
        ],
        [],
    )
    assert gateway_module._hosted_requires_sms(
        [{"status": "open", "action": "Send Alex a text message with the result."}],
        [],
    )
    assert gateway_module._hosted_requires_sms(
        [{
            "status": "open",
            "action": "Do not text now; text the operator after the call.",
        }],
        [],
    )


def test_hosted_non_sms_action_does_not_require_sms_tool(tmp_path, monkeypatch):
    async def scenario():
        session = _Session()
        gateway = _gateway(tmp_path, monkeypatch, session)
        gateway._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: types.SimpleNamespace(
                list_transcripts=lambda _call_id: [
                    types.SimpleNamespace(
                        party="remote", text="Please update the release checklist.",
                    ),
                ],
            ),
        )
        payload = _payload()
        payload["data"]["post_call_action_items"] = [{
            "status": "open",
            "action": "Update the release checklist",
        }]
        await gateway._on_hosted_call_ended(payload)
        await _drain(gateway)

        assert len(session.prompts) == 1
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"

    asyncio.run(scenario())


def test_hosted_completion_recovers_after_restart_without_redelivery(tmp_path, monkeypatch):
    async def scenario():
        original = _gateway(
            tmp_path,
            monkeypatch,
            _Session(error=RuntimeError("Codex unavailable")),
        )
        original._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: _NonSmsIdentity(),
        )
        await original._on_hosted_call_ended(_non_sms_payload())
        await _drain(original)
        registry = json.loads(original._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "failed"

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        restarted._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: _NonSmsIdentity(),
        )
        await restarted._recover_hosted_call_completions()
        await _drain(restarted)
        assert len(restarted_session.prompts) == 1
        registry = json.loads(restarted._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "completed"

    asyncio.run(scenario())


def test_hosted_reserved_sms_attempt_is_not_replayed_after_restart(
    tmp_path, monkeypatch,
):
    async def scenario():
        gateway = _gateway(tmp_path, monkeypatch, _Session())
        gateway._write_hosted_call_registry(
            "call-1",
            event_id="event-1",
            state="running",
            payload=_payload(),
            outcome="completed",
        )
        assert reserve_hosted_sms_attempt(
            "call-1", 1, "+15167251294"
        ) is True

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        await restarted._recover_hosted_call_completions()

        assert restarted._hosted_call_jobs == {}
        assert restarted_session.prompts == []
        entry = json.loads(
            restarted._hosted_call_registry_path.read_text()
        )["call-1"]
        assert entry["state"] == "failed"
        assert entry["retryable"] is False

    asyncio.run(scenario())


def test_restart_resumes_only_recoverable_sms_correction(tmp_path, monkeypatch):
    async def scenario():
        session = _Session()
        gateway = _gateway(tmp_path, monkeypatch, session)
        gateway._write_hosted_call_registry(
            "call-1",
            event_id="event-1",
            state="running",
            payload=_payload(),
            outcome="completed",
        )
        assert reserve_hosted_sms_attempt(
            "call-1", 1, "+15167251294"
        ) is True
        settle_hosted_sms_attempt("call-1", 1, "recoverable")

        await gateway._recover_hosted_call_completions()
        await _drain(gateway)

        assert len(session.prompts) == 1
        assert "[hosted_post_call_sms_correction]" in session.prompts[0]
        assert "Review the outcome, transcript" not in session.prompts[0]
        assert session.hosted_contexts == [{
            "call_id": "call-1",
            "attempt": 2,
            "remote_phone": "+15167251294",
        }]
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert "payload" not in entry

    asyncio.run(scenario())


def test_hosted_completion_registry_bounds_private_replay_data(tmp_path, monkeypatch):
    async def scenario():
        gate = asyncio.Event()
        gateway = _gateway(tmp_path, monkeypatch, _Session(gate=gate))
        payload = _payload()
        payload["data"]["contacts"][0]["memories"] = ["private" * 20_000]
        payload["data"]["transcript"] = {
            "entries": [{"party": "remote", "text": "secret" * 100_000}],
        }
        payload["data"]["post_call_action_items"] = [
            {
                "id": f"action-{index}",
                "action": "a" * 10_000,
                "details": "d" * 20_000,
                "status": "open",
            }
            for index in range(150)
        ]

        await gateway._on_hosted_call_ended(payload)
        await asyncio.sleep(0)
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        replay = entry["payload"]["data"]
        assert "transcript" not in replay
        assert "memories" not in replay["contacts"][0]
        assert len(replay["post_call_action_items"]) == 100
        assert len(replay["post_call_action_items"][0]["action"]) == 4_000
        assert len(replay["post_call_action_items"][0]["details"]) == 8_000
        assert gateway._hosted_call_registry_path.stat().st_mode & 0o777 == 0o600

        gate.set()
        await _drain(gateway)
        entry = json.loads(gateway._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert "payload" not in entry

    asyncio.run(scenario())


def test_private_registry_mode_is_set_before_content_write(tmp_path, monkeypatch):
    original_fdopen = os.fdopen
    observed_modes = []

    def checked_fdopen(fd, *args, **kwargs):
        observed_modes.append(os.fstat(fd).st_mode & 0o777)
        return original_fdopen(fd, *args, **kwargs)

    monkeypatch.setattr(os, "fdopen", checked_fdopen)
    path = tmp_path / "private.json"
    InkboxGateway._write_private_json(path, {"secret": "bounded"})

    assert observed_modes == [0o600]
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == {"secret": "bounded"}


def test_recovery_retries_when_authoritative_transcript_is_unavailable(
    tmp_path, monkeypatch,
):
    class _TranscriptFailureIdentity:
        def list_transcripts(self, _call_id):
            raise RuntimeError("transcript endpoint not settled")

    async def scenario():
        first = _gateway(
            tmp_path,
            monkeypatch,
            _Session(error=RuntimeError("Codex unavailable")),
        )
        first._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: _NonSmsIdentity(),
        )
        await first._on_hosted_call_ended(_non_sms_payload())
        await _drain(first)

        unsettled_session = _Session()
        unsettled = _gateway(tmp_path, monkeypatch, unsettled_session)
        unsettled._inkbox = types.SimpleNamespace(
            get_identity=lambda _handle: _TranscriptFailureIdentity(),
        )
        await unsettled._recover_hosted_call_completions()
        await _drain(unsettled)
        entry = json.loads(unsettled._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "failed"
        assert unsettled_session.prompts == []

        settled_session = _Session()
        settled = _gateway(tmp_path, monkeypatch, settled_session)
        await settled._recover_hosted_call_completions()
        await _drain(settled)
        entry = json.loads(settled._hosted_call_registry_path.read_text())["call-1"]
        assert entry["state"] == "completed"
        assert len(settled_session.prompts) == 1

    asyncio.run(scenario())


def test_local_call_completion_is_ignored(tmp_path, monkeypatch):
    async def scenario():
        session = _Session()
        gateway = _gateway(tmp_path, monkeypatch, session)
        payload = _payload()
        payload["data"]["call"]["mode"] = "client_websocket"
        response = await gateway._on_hosted_call_ended(payload)
        assert json.loads(response.text)["ignored"] == "client_websocket"
        assert session.prompts == []

    asyncio.run(scenario())

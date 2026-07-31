import asyncio
import json
import types

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway


class _Identity:
    def list_transcripts(self, call_id):
        assert call_id == "call-1"
        return [
            types.SimpleNamespace(party="remote", text="Please text me the result."),
            types.SimpleNamespace(party="local", text="I will do that after we hang up."),
        ]


class _Session:
    def __init__(self, gate=None, error=None):
        self.prompts = []
        self.gate = gate
        self.error = error

    async def run_consult(self, prompt):
        self.prompts.append(prompt)
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return "This plaintext must not be delivered."


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
        assert "Please text me the result." in prompt
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


def test_hosted_completion_recovers_after_restart_without_redelivery(tmp_path, monkeypatch):
    async def scenario():
        original = _gateway(
            tmp_path,
            monkeypatch,
            _Session(error=RuntimeError("Codex unavailable")),
        )
        await original._on_hosted_call_ended(_payload())
        await _drain(original)
        registry = json.loads(original._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "failed"

        restarted_session = _Session()
        restarted = _gateway(tmp_path, monkeypatch, restarted_session)
        await restarted._recover_hosted_call_completions()
        await _drain(restarted)
        assert len(restarted_session.prompts) == 1
        registry = json.loads(restarted._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "completed"

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
        await first._on_hosted_call_ended(_payload())
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

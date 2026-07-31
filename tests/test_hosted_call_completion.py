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
            }],
        },
    }


def _gateway(tmp_path, monkeypatch, session):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = InkboxGateway(BridgeConfig(identity="agent"))
    gateway._inkbox = types.SimpleNamespace(get_identity=lambda _handle: _Identity())
    gateway.sessions = _Sessions(session)

    async def resolve(_call, _remote):
        return {"id": "contact-1", "name": "Dima", "memories": []}

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
        registry = json.loads(gateway._hosted_call_registry_path.read_text())
        assert registry["call-1"]["state"] == "completed"
        assert registry["call-1"]["payload"]["event_type"] == "call.ended"
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

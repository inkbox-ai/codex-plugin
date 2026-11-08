import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from inkbox_codex.chat_adapter import ChatAdapter


def test_native_chat_maps_permission_scope_and_structured_questions():
    asyncio.run(native_chat())


async def native_chat():
    public = []
    requests = []

    async def run_chat(prompt, event, request, before_submit, model=None):
        assert model == "gpt-6.1-sol"
        await before_submit()
        event("item/reasoning/textDelta", {"delta": "private reasoning"})
        event("item/agentMessage/delta", {"delta": "Working"})
        event(
            "item/started", {"item": {"type": "commandExecution", "id": "native-tool"}}
        )
        grant = await request(
            "item/permissions/requestApproval",
            {"permissions": {"network": {"enabled": True}}, "itemId": "native-tool"},
        )
        assert grant == {"permissions": {"network": {"enabled": True}}, "scope": "turn"}
        answer = await request(
            "item/tool/requestUserInput",
            {
                "questions": [
                    {
                        "id": "format",
                        "question": "Format?",
                        "options": [{"label": "CSV"}, {"label": "JSON"}],
                        "isOther": False,
                    }
                ]
            },
        )
        assert answer == {"answers": {"format": {"answers": ["CSV"]}}}
        with pytest.raises(ValueError):
            await request(
                "item/tool/requestUserInput",
                {"questions": [{"id": "key", "isSecret": True}]},
            )
        event(
            "item/completed",
            {
                "item": {
                    "type": "commandExecution",
                    "id": "native-tool",
                    "exitCode": 1,
                }
            },
        )
        return SimpleNamespace(text="Finished", aborted=False)

    session = SimpleNamespace(run_chat=run_chat, resume_session_id="native-session")
    manager = SimpleNamespace(get=lambda _: session, sessions={})
    adapter = ChatAdapter(manager)

    async def ask(value):
        requests.append(value)
        return (
            {"kind": "approval", "decision": "approve_once"}
            if value["kind"] == "approval"
            else {"kind": "question", "answers": {"format": "0"}}
        )

    result = await adapter.run(
        {"run_id": str(uuid4()), "session_id": str(uuid4()), "prompt": "Example", "model": "gpt-6.1-sol"},
        lambda k, v: public.append((k, v)),
        ask,
    )
    assert result["text"] == "Finished"
    assert requests[0]["effective_scope"] == "turn"
    assert requests[0]["tool_call_id"] == public[1][1]["id"]
    assert public[-1][1]["id"] == public[1][1]["id"]
    assert public[-1][1]["status"] == "failed"
    assert "private reasoning" not in str(public)

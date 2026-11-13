import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest

from inkbox_codex.chat_adapter import ChatAdapter


@pytest.mark.parametrize("decision,expected", [
    ("approve_once", "accept"), ("deny", "decline"),
    ("cancel", "cancel"), ("cancel_after_approval", "cancel"),
])
def test_mcp_approval_becomes_a_card_without_persistent_permission(decision, expected):
    async def scenario():
        cards = []
        spec = {"run_id": str(uuid4()), "session_id": str(uuid4()), "prompt": "Example"}

        async def run_chat(prompt, event, request, **kwargs):
            event("item/started", {"item": {"type": "mcpToolCall", "id": "native-tool"}})
            response = await request("mcpServer/elicitation/request", {
                "serverName": "example", "itemId": "native-tool",
                "message": "Allow example to call create_schedule?",
                "requestedSchema": {"type": "object", "properties": {}},
                "_meta": {"persist": ["session", "always"]},
            })
            assert response == {"action": expected, "content": None}
            return SimpleNamespace(text="Finished", aborted=False)

        async def ask(card):
            cards.append(card)
            if decision == "cancel_after_approval":
                adapter.cancelled.add(spec["session_id"])
                return {"decision": "approve_once"}
            return {"decision": decision, "cancelled": decision == "cancel"}

        session = SimpleNamespace(run_chat=run_chat, resume_session_id="native")
        adapter = ChatAdapter(SimpleNamespace(get=lambda _: session, sessions={}))
        await adapter.run(spec, lambda *_: None, ask)
        assert len(cards) == 1
        assert cards[0]["decisions"] == ["approve_once", "deny"]
        assert cards[0]["effective_scope"] == "operation"
        assert cards[0]["tool_call_id"]
        assert cards[0]["detail"] == "Allow example to call create_schedule?"

    asyncio.run(scenario())


@pytest.mark.parametrize("params", [
    {"mode": "url", "url": "https://example.com/authorize"},
    {"requestedSchema": {"type": "object", "properties": {"token": {"type": "string"}}}},
])
def test_mcp_forms_are_not_mistaken_for_approval(params):
    async def scenario():
        async def run_chat(prompt, event, request, **kwargs):
            return await request("mcpServer/elicitation/request", params)

        async def ask(_):
            pytest.fail("Unsupported forms must not be offered as approvals")

        session = SimpleNamespace(run_chat=run_chat, resume_session_id="native")
        adapter = ChatAdapter(SimpleNamespace(get=lambda _: session, sessions={}))
        with pytest.raises(ValueError, match="Unsupported MCP input form"):
            await adapter.run({"run_id": str(uuid4()), "session_id": str(uuid4()), "prompt": "Example"}, lambda *_: None, ask)

    asyncio.run(scenario())


def test_explicit_yolo_accepts_mcp_permission_without_persistent_grant():
    async def scenario():
        async def run_chat(prompt, event, request, **kwargs):
            response = await request("mcpServer/elicitation/request", {
                "serverName": "example", "message": "Allow this tool?",
                "requestedSchema": {"type": "object", "properties": {}},
            })
            assert response == {"action": "accept", "content": None}
            return SimpleNamespace(text="Finished", aborted=False)

        async def ask(_):
            pytest.fail("Explicit YOLO should not request approval")

        session = SimpleNamespace(run_chat=run_chat, resume_session_id="native")
        adapter = ChatAdapter(SimpleNamespace(get=lambda _: session, sessions={}))
        await adapter.run({"run_id": str(uuid4()), "session_id": str(uuid4()), "prompt": "Example",
                           "options": {"approval_policy": "never", "sandbox": "danger-full-access"}}, lambda *_: None, ask)

    asyncio.run(scenario())


def test_native_chat_maps_permission_scope_and_structured_questions():
    asyncio.run(native_chat())


async def native_chat():
    public = []
    requests = []

    async def run_chat(prompt, event, request, before_submit, model=None, permission_mode="ask"):
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


@pytest.mark.parametrize('failed', [False, True])
def test_native_items_keep_order_phase_and_partial_text_across_journal_restart(tmp_path, failed):
    from inkbox_codex.codex_client import CodexTurnFailed
    from inkbox_codex.managed_chat import ChatRuntime, Journal
    import time

    async def scenario():
        async def run_chat(prompt, event, request, **kwargs):
            event('item/started', {'item': {'type': 'agentMessage', 'id': 'progress', 'phase': 'commentary'}})
            event('item/agentMessage/delta', {'itemId': 'progress', 'delta': 'Checking the schedule.'})
            event('item/completed', {'item': {'type': 'agentMessage', 'id': 'progress', 'phase': 'commentary', 'text': 'Checking the schedule.'}})
            event('item/started', {'item': {'type': 'mcpToolCall', 'id': 'lookup'}})
            event('item/completed', {'item': {'type': 'mcpToolCall', 'id': 'lookup'}})
            event('item/started', {'item': {'type': 'agentMessage', 'id': 'answer', 'phase': 'final_answer'}})
            event('item/agentMessage/delta', {'itemId': 'answer', 'delta': 'The registration needs attention.'})
            if failed:
                raise CodexTurnFailed({'message': 'Synthetic execution failure'})
            event('item/completed', {'item': {'type': 'agentMessage', 'id': 'answer', 'phase': 'final_answer', 'text': 'The registration needs attention.'}})
            return SimpleNamespace(text='The registration needs attention.', aborted=False)

        session = SimpleNamespace(run_chat=run_chat, resume_session_id='native')
        adapter = ChatAdapter(SimpleNamespace(get=lambda _: session, sessions={}))
        path = tmp_path / 'chat.sqlite'
        runtime = ChatRuntime(path, 'owner', adapter.run, adapter.cancel)
        run = str(uuid4())
        await runtime.control({'action': 'chat_start', 'spec': {
            'run_id': run, 'session_id': str(uuid4()), 'harness': 'codex', 'prompt': 'Example',
            'deadline_unix_ms': int(time.time() * 1000) + 60000,
        }})
        await asyncio.gather(*list(runtime.tasks.values()))
        events = runtime.journal.events(run, 0)
        assert runtime.journal.get(run)['state'] == ('failed' if failed else 'succeeded')
        await runtime.close()
        journal = Journal(path, 'owner')
        assert journal.events(run, 0) == events
        journal.close()
        created = [e for e in events if e['kind'] == 'message.created']
        assert len(created) == 2
        assert created[0]['payload']['phase'] == 'commentary'
        assert created[1]['payload']['phase'] == 'final_answer'
        assert created[0]['payload']['id'] != created[1]['payload']['id']
        tool = next(e for e in events if e['kind'] == 'tool.started')
        assert created[0]['sequence'] < tool['sequence'] < created[1]['sequence']
        texts = {e['payload']['id']: '' for e in created}
        for e in events:
            if e['kind'] == 'message.delta':
                texts[e['payload']['id']] += e['payload']['text']
        assert list(texts.values()) == ['Checking the schedule.', 'The registration needs attention.']
        assert events[-1]['kind'] == 'turn.updated' and events[-1]['payload']['terminal']
        if failed:
            assert 'model access' not in events[-1]['payload']['blocker']['message']
    asyncio.run(scenario())


@pytest.mark.parametrize('options,mode', [({}, 'ask'),
    ({'approval_policy': 'never', 'sandbox': 'danger-full-access'}, 'yolo'),
    ({'approval_policy': 'never'}, 'ask')])
def test_browser_permission_is_explicit_per_turn(options, mode):
    async def scenario():
        async def run_chat(prompt, event, request, **kwargs):
            assert kwargs['permission_mode'] == mode
            return SimpleNamespace(text='done', aborted=False)
        session = SimpleNamespace(run_chat=run_chat, resume_session_id='native')
        adapter = ChatAdapter(SimpleNamespace(get=lambda _: session, sessions={}))
        await adapter.run({'run_id': str(uuid4()), 'session_id': str(uuid4()),
                           'prompt': 'Example', 'options': options}, lambda *_: None, None)
    asyncio.run(scenario())

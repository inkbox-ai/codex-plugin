import asyncio
import types

from inkbox_codex import a2a_progress as progress
from inkbox_codex.config import BridgeConfig


def test_activity_mapping_and_fallback_are_sanitized():
    activities = [
        progress.activity_for_item("mcpToolCall", "list_directory_users"),
        progress.activity_for_item("mcpToolCall", "run_sql_query"),
    ]

    assert activities == [
        "reviewing the requested records",
        "checking the requested data",
    ]
    assert progress.fallback_update(activities) == (
        "I'm reviewing the requested records and checking the requested data."
    )


def test_progress_summary_is_isolated_short_and_nonterminal(monkeypatch):
    calls = []

    class Client:
        def __init__(self, cfg, **kwargs):
            calls.append((cfg, kwargs))

        async def run(self, prompt):
            calls.append(prompt)
            return "Completed the task and found everything."

        async def disconnect(self):
            calls.append("disconnected")

    monkeypatch.setattr(progress, "CodexAppServerClient", Client)
    update = asyncio.run(
        progress.build_progress_update(
            BridgeConfig(codex_sandbox="workspace-write", codex_approval_policy="on-request"),
            task_text="Inspect the requested records.",
            activities=["reviewing the requested records"],
        )
    )

    auxiliary_cfg = calls[0][0]
    assert auxiliary_cfg.codex_sandbox == "read-only"
    assert auxiliary_cfg.codex_approval_policy == "never"
    assert update == "I'm reviewing the requested records."
    assert calls[-1] == "disconnected"


def test_progress_summary_enforces_word_limit(monkeypatch):
    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def run(self, _prompt):
            return " ".join(f"word{index}" for index in range(30))

        async def disconnect(self):
            pass

    monkeypatch.setattr(progress, "CodexAppServerClient", Client)
    update = asyncio.run(
        progress.build_progress_update(
            BridgeConfig(),
            task_text="Work.",
            activities=[],
        )
    )

    assert len(update.split()) == progress.A2A_PROGRESS_MAX_WORDS
    assert update.endswith("…")


def test_activity_observer_receives_no_tool_arguments_or_results():
    observed = []

    def handler(item_type, tool_name):
        observed.append((item_type, tool_name))

    capture = types.SimpleNamespace(
        future=types.SimpleNamespace(done=lambda: False),
        activity_handler=handler,
        messages=[],
        deltas=[],
        mcp_tool_calls=[],
    )
    client = object.__new__(progress.CodexAppServerClient)
    client._turns = {"turn-1": capture}
    client._handle_notification(
        {
            "method": "item/started",
            "params": {
                "turnId": "turn-1",
                "item": {
                    "type": "mcpToolCall",
                    "tool": "run_sql_query",
                    "arguments": {"secret": "must-not-be-retained"},
                    "result": {"private": "must-not-be-retained"},
                },
            },
        }
    )

    assert observed == [("mcpToolCall", "run_sql_query")]

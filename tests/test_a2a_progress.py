import asyncio
import types

import pytest

from inkbox_codex import a2a_progress as progress
from inkbox_codex.config import BridgeConfig


def test_item_identifiers_are_normalized_without_classification():
    assert (
        progress.safe_item_identifier("mcpToolCall", " List Directory Users ")
        == "list_directory_users"
    )
    assert progress.safe_item_identifier("commandExecution") == "command_execution"
    assert (
        progress.safe_item_identifier("mcpToolCall", "run/sql query\n")
        == "run_sql_query"
    )
    assert len(progress.safe_item_identifier("mcpToolCall", "x" * 100)) == 80
    assert progress.fallback_update() == "I'm continuing the requested work."


def test_progress_summary_is_isolated_short_and_nonterminal(monkeypatch):
    calls = []

    class Client:
        def __init__(self, cfg, **kwargs):
            calls.append((cfg, kwargs))

        async def run(self, prompt):
            calls.append(prompt)
            return "I'm reviewing the requested records."

        async def disconnect(self):
            calls.append("disconnected")

    monkeypatch.setattr(progress, "CodexAppServerClient", Client)
    update = asyncio.run(
        progress.build_progress_update(
            BridgeConfig(
                codex_sandbox="workspace-write", codex_approval_policy="on-request"
            ),
            task_text="Inspect the requested records.",
            identifiers=["list_directory_users"],
            previous_update="I'm checking the request.",
        )
    )

    auxiliary_cfg = calls[0][0]
    assert auxiliary_cfg.codex_sandbox == "read-only"
    assert auxiliary_cfg.codex_approval_policy == "never"
    assert update == "I'm reviewing the requested records."
    assert "list_directory_users" in calls[1]
    assert "I'm checking the request." in calls[1]
    assert calls[-1] == "disconnected"


@pytest.mark.parametrize(
    "claim",
    [
        "Done — the task is complete.",
        "The final answer is ready.",
        "I successfully resolved the request.",
        "I cannot continue.",
        "I'm waiting for your input.",
    ],
)
def test_progress_summary_rejects_terminal_claim(claim):
    assert (
        progress.clean_update(claim, ["run_tests"])
        == "I'm continuing the requested work."
    )


@pytest.mark.parametrize(
    "result",
    [
        "I'm using browser_search to investigate.",
        "I'm using browser search to investigate.",
    ],
)
def test_progress_summary_rejects_echoed_item_identifier(monkeypatch, result):
    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def run(self, _prompt):
            return result

        async def disconnect(self):
            pass

    monkeypatch.setattr(progress, "CodexAppServerClient", Client)
    update = asyncio.run(
        progress.build_progress_update(
            BridgeConfig(),
            task_text="Research the requested topic.",
            identifiers=["browser_search"],
        )
    )

    assert update == "I'm continuing the requested work."


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
            identifiers=[],
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
    client._handle_notification(
        {
            "method": "item/completed",
            "params": {
                "turnId": "turn-1",
                "item": {
                    "type": "mcpToolCall",
                    "tool": "run_sql_query",
                    "result": {"private": "must-not-be-retained"},
                },
            },
        }
    )

    assert observed == [("mcpToolCall", "run_sql_query")]

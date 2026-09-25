"""Text decisions cross a real app-server and reach an isolated MCP peer."""

import asyncio
import json
from pathlib import Path
import shutil
import sys

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig
from inkbox_codex.sessions import ContactSession


CODEX_BIN = shutil.which("codex")
pytestmark = pytest.mark.skipif(CODEX_BIN is None, reason="contract suite: needs the codex CLI on PATH")


@pytest.mark.parametrize("case,reply,action,content,persist", [
    ("form", '{"confirmed": true}', "accept", {"confirmed": True}, None),
    ("approval", "yes", "accept", {}, None),
    ("approval", "no", "decline", None, None),
    ("approval", "2", "decline", None, None),
    ("persist-session", "2", "accept", {}, "session"),
    ("persist-session", "3", "decline", None, None),
    ("persist-always", "2", "decline", None, None),
    ("persist-always", "3", "accept", {}, "always"),
    ("persist", "3", "decline", None, None),
    ("persist", "4", "accept", {}, "always"),
    ("persist", "session", "accept", {}, "session"),
    ("persist", "always", "accept", {}, "always"),
])
def test_text_elicitation_round_trips_real_host(tmp_path, monkeypatch, case, reply, action, content, persist):
    """A valid response must execute the peer's tool, not merely sound approved."""
    home = tmp_path / "codex-home"
    home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "bridge-home"))
    # These requests exercise tools directly; no model request or login is needed.
    (home / "config.toml").write_text(
        'model = "contract-model"\nmodel_provider = "contract"\n'
        '[model_providers.contract]\nname = "Local contract"\n'
        'base_url = "http://127.0.0.1:1/v1"\nwire_api = "responses"\n'
    )
    cfg = BridgeConfig(project_dir=str(tmp_path), codex_bin=CODEX_BIN,
                       codex_sandbox="read-only", permission_timeout_s=5)
    server = {"command": sys.executable, "args": [str(Path(__file__).parents[1] / "fixtures" / "elicitation_server.py")]}
    prompts, requests = [], []

    async def scenario():
        async def send(_chat, text, mode, meta):
            prompts.append(text)
            await session.handle_inbound(reply, mode, meta)

        session = ContactSession(chat_id="contract-contact", cfg=cfg, send_fn=send,
                                 mcp_server_config={}, identity_info={"handle": "contract-agent"})

        async def approve(method, params):
            requests.append((method, params))
            return await session._handle_codex_request(method, params)

        client = CodexAppServerClient(cfg, developer_instructions="Local elicitation contract.",
                                      mcp_server_config=server, approval_handler=approve)
        try:
            thread = await asyncio.wait_for(client.connect(), 20)
            result = await asyncio.wait_for(client._request("mcpServer/tool/call", {
                "threadId": thread, "server": "inkbox", "tool": "lookup_example", "arguments": {"case": case},
            }), 20)
            payload = json.loads(next(item["text"] for item in result["content"] if item.get("type") == "text"))
            assert payload["response"]["action"] == action
            # Codex normalizes accepted empty-form content to an empty object.
            assert payload["response"].get("content") == content
            assert payload["executed"] is (action == "accept")
            assert payload["response"].get("_meta", {}).get("persist") == persist
            assert len(prompts) == len(requests) == 1
            assert requests[0][0] == "mcpServer/elicitation/request"
            if case == "form":
                assert requests[0][1]["requestedSchema"]["properties"]["confirmed"]["type"] == "boolean"
            if persist:
                assert requests[0][1]["_meta"]["persist"] == (["session", "always"] if case == "persist" else [persist])
        finally:
            await client.disconnect()
            await session.close()

    asyncio.run(scenario())

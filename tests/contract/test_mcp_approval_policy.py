"""Thread overrides must preserve the real host's other MCP approval policies."""

import asyncio
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import shutil
import sys
import threading

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig


CODEX_BIN = shutil.which("codex")
pytestmark = pytest.mark.skipif(CODEX_BIN is None, reason="contract suite: needs the codex CLI on PATH")


@pytest.mark.parametrize("resume", [False, True], ids=["new-thread", "resumed-thread"])
@pytest.mark.parametrize("decision", ["accept", "decline"])
@pytest.mark.parametrize("policy_source", ["config-file", "launcher"])
def test_inkbox_override_preserves_other_server_tool_approval(tmp_path, monkeypatch, resume, decision, policy_source):
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "tests" / "live"))
    import mock_openai

    model_requests = []

    class ToolCallHandler(mock_openai.Handler):
        def _respond_responses(self, request):
            model_requests.append(request)
            if len(model_requests) != (2 if resume else 1):
                return super()._respond_responses(request)
            item = {
                "type": "function_call", "id": "fc_example", "call_id": "call_example",
                "name": "lookup_example", "namespace": "mcp__example", "arguments": "{}", "status": "completed",
            }
            response = {
                "id": "resp_example", "object": "response", "status": "completed",
                "model": "mock-model", "output": [item],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            }
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            for name, payload in [
                ("response.created", {"response": {**response, "status": "in_progress", "output": []}}),
                ("response.output_item.done", {"output_index": 0, "item": item}),
                ("response.completed", {"response": response}),
            ]:
                self.wfile.write(f"event: {name}\ndata: {json.dumps({'type': name, **payload})}\n\n".encode())
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), ToolCallHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    fixture = root / "tests" / "fixtures" / "approval_policy_server.py"
    executed = tmp_path / "example-executed"
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model = "mock-model"\nmodel_provider = "mock"\n'
        '[model_providers.mock]\nname = "Mock"\n'
        f'base_url = "http://127.0.0.1:{server.server_port}/v1"\nwire_api = "responses"\n'
        '[mcp_servers.example]\n'
        f'command = {json.dumps(sys.executable)}\n'
        f'args = {json.dumps([str(fixture), str(executed)])}\n'
        + ('[mcp_servers.example.tools.lookup_example]\napproval_mode = "prompt"\n'
           if policy_source == "config-file" else "")
    )
    codex_bin = CODEX_BIN
    if policy_source == "launcher":
        # Match a service launcher that supplies per-tool policy with `codex -c`.
        # A broad thread override can hide this runtime override even when the
        # underlying config.toml still supplies the server's connection details.
        launcher = tmp_path / "codex-policy"
        launcher.write_text(
            f"#!{sys.executable}\nimport os, sys\n"
            f"os.execv({CODEX_BIN!r}, [{CODEX_BIN!r}, '-c', "
            "'mcp_servers.example.tools.lookup_example.approval_mode=\"prompt\"', *sys.argv[1:]])\n"
        )
        launcher.chmod(0o755)
        codex_bin = str(launcher)
    monkeypatch.setenv("CODEX_HOME", str(home))
    approvals = []

    async def approve(method, params):
        approvals.append((method, params))
        return {"action": decision, "content": {} if decision == "accept" else None}

    cfg = BridgeConfig(project_dir=str(tmp_path), codex_bin=codex_bin,
                       codex_model="mock-model", codex_approval_policy="on-request",
                       codex_sandbox="read-only")
    client = CodexAppServerClient(
        cfg, developer_instructions="Local MCP policy contract.", approval_handler=approve,
        mcp_server_config={"command": sys.executable,
                           "args": [str(fixture), str(tmp_path / "inkbox-executed")]},
    )

    async def scenario():
        try:
            if resume:
                thread = await asyncio.wait_for(client.connect(), timeout=10)
                await asyncio.wait_for(client.run("Record a seed turn for resumption."), timeout=30)
                await client.disconnect()
                assert await asyncio.wait_for(client.connect(resume_thread_id=thread), timeout=10) == thread
            await asyncio.wait_for(client.run("Look up the example."), timeout=30)
        finally:
            await client.disconnect()

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()

    assert len(model_requests) == (3 if resume else 2)
    assert len(approvals) == 1, "The host did not enforce the other server's prompt policy"
    assert approvals[0][0] == "mcpServer/elicitation/request"
    assert approvals[0][1]["serverName"] == "example"
    assert executed.exists() is (decision == "accept"), "Tool execution did not honor the approval decision"
    assert not (tmp_path / "inkbox-executed").exists(), "Called the wrong MCP server"

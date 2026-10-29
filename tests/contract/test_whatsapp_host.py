"""The real host loads the candidate SDK inside its MCP tool subprocess."""

import asyncio
from importlib.metadata import version
import json
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig
from inkbox_codex.tools import build_inkbox_mcp_server_config
from tests.test_whatsapp import IDENTITY, NOW


@pytest.mark.skipif(shutil.which("codex") is None, reason="needs the codex CLI on PATH")
def test_real_host_resolves_candidate_and_reads_dynamic_whatsapp_capability(tmp_path, monkeypatch):
    assert version("inkbox") == "0.7.24"
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == "/api/v1/identities/agent":
                value = {"id": IDENTITY, "organization_id": "org_example", "agent_handle": "agent",
                         "whatsapp_enabled": False, "created_at": NOW, "updated_at": NOW}
            elif self.path == "/api/v1/identities/agent/channel-status":
                value = {"whatsapp": {"enabled": True, "number": "+15550000001", "status": "ready"}}
            elif self.path == f"/api/v1/whatsapp/limits?agent_identity_id={IDENTITY}":
                value = {"agent_identity_id": IDENTITY, "limits": {"synthetic": 123}, "usage": {"whatsapp.texts_per_agent_identity_per_24h": 7}}
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(value).encode())

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text('model="contract-model"\nmodel_provider="contract"\n[model_providers.contract]\nname="Local contract"\nbase_url="http://127.0.0.1:1/v1"\nwire_api="responses"\n')
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "bridge-home"))
    cfg = BridgeConfig(api_key="ApiKey_synthetic_test_only", identity="agent",
                       base_url=f"http://127.0.0.1:{http.server_port}", project_dir=str(tmp_path),
                       codex_bin=shutil.which("codex"), codex_sandbox="read-only", auto_approve_inkbox_tools=True)
    server, names = build_inkbox_mcp_server_config(cfg)
    assert "mcp__inkbox__inkbox_get_whatsapp_status" in names
    wrapper = tmp_path / "checked_mcp.py"
    wrapper.write_text("from importlib.metadata import version\nfrom inkbox.agent_identity import AgentIdentity\nassert version('inkbox') == '0.7.24'\nassert callable(AgentIdentity.send_whatsapp)\nimport runpy\nrunpy.run_module('inkbox_codex.mcp_stdio', run_name='__main__')\n")
    server["args"] = [str(wrapper)]

    async def scenario():
        host = CodexAppServerClient(cfg, developer_instructions="contract test", mcp_server_config=server)
        try:
            thread = await asyncio.wait_for(host.connect(), 25)
            response = await asyncio.wait_for(host._request("mcpServer/tool/call", {
                "threadId": thread, "server": "inkbox", "tool": "inkbox_get_whatsapp_status", "arguments": {},
            }), 25)
            assert not response.get("isError"), response
            result = json.loads(response["content"][0]["text"])
            assert result["enabled"] and result["status"] == "ready"
            assert set(result["limits"]) == {"agent_identity_id", "limits", "usage"}
            assert result["limits"]["limits"] == {"synthetic": 123}
            assert result["limits"]["usage"] == {"whatsapp.texts_per_agent_identity_per_24h": 7}
            assert requests == ["/api/v1/identities/agent", "/api/v1/identities/agent/channel-status",
                                f"/api/v1/whatsapp/limits?agent_identity_id={IDENTITY}"]
        finally:
            await host.disconnect()
    try:
        asyncio.run(scenario())
    finally:
        http.shutdown()
        http.server_close()

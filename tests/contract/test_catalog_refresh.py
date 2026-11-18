"""Catalog refresh through the pinned native app-server."""

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
from inkbox_codex.tools import build_inkbox_mcp_server_config


@pytest.mark.skipif(shutil.which("codex") is None, reason="requires the Codex CLI")
@pytest.mark.parametrize("surface", ["local", "remote"])
def test_native_refresh_add_change_remove_preserves_thread(tmp_path, monkeypatch, surface):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "live"))
    import mock_openai

    requests = []

    class Handler(mock_openai.Handler):
        def do_GET(self):
            if self.path == "/api/whoami":
                self._send_json(200, {"auth_type": "api_key", "auth_subtype": "api_key.agent_scoped.claimed"})
            elif self.path == "/api/v1/identities/agent":
                self._send_json(200, {"id": "00000000-0000-0000-0000-000000000001",
                                      "organization_id": "00000000-0000-0000-0000-000000000002",
                                      "agent_handle": "agent", "created_at": "2026-01-01T00:00:00Z",
                                      "updated_at": "2026-01-01T00:00:00Z"})
            else:
                self.send_error(404)

        def do_POST(self):
            if self.path != "/mcp":
                return super().do_POST()
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert self.headers["X-Api-Key"] == "synthetic-key"
            assert self.headers["Mcp-Method"] == body["method"]
            if body["method"] == "server/discover":
                result = {"supportedVersions": ["2026-07-28"]}
            elif body["method"] == "tools/list":
                result = {"tools": json.loads(catalog.read_text())}
            else:
                raise AssertionError("Unexpected remote method")
            self._send_json(200, {"jsonrpc": "2.0", "id": body["id"], "result": result})

        def _respond_responses(self, request):
            requests.append(request)
            return super()._respond_responses(request)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    home = tmp_path / "native-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="mock"\n[model_providers.mock]\nname="Local"\n'
        f'base_url="http://127.0.0.1:{server.server_port}/v1"\nwire_api="responses"\n')
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "plugin-home"))
    catalog = tmp_path / "catalog.json"
    fixture = Path(__file__).resolve().parents[1] / "fixtures/dynamic_catalog_server.py"
    cfg = BridgeConfig(project_dir=str(tmp_path), codex_bin=shutil.which("codex"), codex_model="mock-model", codex_sandbox="read-only",
                       api_key="synthetic-key", identity="agent", base_url=f"http://127.0.0.1:{server.server_port}")
    native_server = build_inkbox_mcp_server_config(cfg)[0] if surface == "remote" else {
        "command": sys.executable, "args": [str(fixture), str(catalog)],
        "default_tools_approval_mode": "prompt",
    }
    client = CodexAppServerClient(cfg, developer_instructions="Reply briefly.", mcp_server_config=native_server)

    def publish(name, description):
        catalog.write_text(json.dumps([{"name": name, "description": description,
                                       "inputSchema": {"type": "object", "properties": {}}}]))

    async def scenario():
        try:
            publish("example_first", "Initial description")
            thread = await client.connect()
            await client.run("Reply briefly.")
            assert "example_first" in json.dumps(requests[-1]["tools"])
            publish("example_second", "Updated description")
            await client.disconnect()
            await client.connect(resume_thread_id=thread)
            await client.run("Reply briefly again.")
            current = json.dumps(requests[-1]["tools"])
            assert "example_second" in current and "example_first" not in current
            assert "Updated description" in current
            catalog.write_text("[]")
            await client.disconnect()
            await client.connect(resume_thread_id=thread)
            await client.run("Reply one final time.")
            assert "example_second" not in json.dumps(requests[-1]["tools"])
            assert client.thread_id == thread
        finally:
            await client.disconnect()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40))
    finally:
        server.shutdown()
        server.server_close()

"""Vault reads cross the real Codex host and Inkbox MCP subprocess."""

import asyncio
import json
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig
from inkbox_codex.daemon import _maybe_load_env_file
from inkbox_codex.tools import build_inkbox_mcp_server_config
from tests.fixtures.vault_api import LOGIN_ID, TOTP_SEED, VAULT_KEY, VaultAPI


@pytest.mark.skipif(shutil.which("codex") is None, reason="needs the codex CLI on PATH")
@pytest.mark.parametrize("key", [VAULT_KEY, "Wrong-example-key-42!"])
def test_host_loads_vault_key_from_env_file_and_isolates_unlock_errors(tmp_path, monkeypatch, key):
    api = VaultAPI()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            response = api.respond(httpx.Request("GET", f"http://127.0.0.1{self.path}", headers=dict(self.headers)))
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model = "contract-model"\nmodel_provider = "contract"\n'
        '[model_providers.contract]\nname = "Local contract"\n'
        'base_url = "http://127.0.0.1:1/v1"\nwire_api = "responses"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("INKBOX_VAULT_KEY", raising=False)
    monkeypatch.delenv("INKBOX_CODEX_VAULT_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(f"INKBOX_CODEX_VAULT_KEY={key}\n")
    monkeypatch.setenv("INKBOX_CODEX_ENV_FILE", str(env_file))
    _maybe_load_env_file()
    cfg = BridgeConfig(api_key="synthetic-agent-key", identity="example-agent",
                       base_url=f"http://127.0.0.1:{http.server_port}",
                       project_dir=str(tmp_path), codex_bin=shutil.which("codex"),
                       codex_sandbox="read-only", auto_approve_inkbox_tools=True)
    server_config, _ = build_inkbox_mcp_server_config(cfg)

    async def scenario():
        client = CodexAppServerClient(cfg, developer_instructions="Use the requested Vault tool.",
                                      mcp_server_config=server_config)
        try:
            thread = await asyncio.wait_for(client.connect(), 20)
            for tool, arguments in (
                ("inkbox_list_contacts", {}),
                ("inkbox_list_vault_secrets", {"secret_type": "login"}),
                ("inkbox_get_totp_code", {"secret_id": LOGIN_ID}),
                ("inkbox_list_contacts", {}),
            ):
                result = await asyncio.wait_for(client._request("mcpServer/tool/call", {
                    "threadId": thread, "server": "inkbox", "tool": tool, "arguments": arguments,
                }), 20)
                data = json.loads(result["content"][0]["text"])
                if tool == "inkbox_get_totp_code" and key != VAULT_KEY:
                    assert result["isError"]
                    assert "No vault key matched" in data["error"]
                else:
                    assert not result.get("isError"), result
                    if tool == "inkbox_list_vault_secrets":
                        assert [s["id"] for s in data] == [LOGIN_ID]
                        assert all(not r.url.path.endswith("/unlock") for r in api.requests)
                    elif tool == "inkbox_get_totp_code":
                        assert len(data["code"]) == 8 and data["code"].isdigit()
                        assert 0 < data["seconds_remaining"] <= 30
                    else:
                        assert data == []
                for value in (key, VAULT_KEY, TOTP_SEED, "synthetic-password"):
                    assert value not in json.dumps(result)
        finally:
            await client.disconnect()

    try:
        asyncio.run(scenario())
    finally:
        http.shutdown()
        http.server_close()
        worker.join(timeout=5)

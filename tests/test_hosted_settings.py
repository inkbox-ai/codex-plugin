import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig


def test_standalone_configuration_unchanged(monkeypatch):
    monkeypatch.delenv("INKBOX_HOSTED_MODE", raising=False)
    monkeypatch.delenv("INKBOX_HOSTED_INSTRUCTIONS", raising=False)
    client = CodexAppServerClient(BridgeConfig(), developer_instructions="routing")
    params = client._thread_params()
    assert params["developerInstructions"] == "routing"
    assert "mcp_servers.inkbox_connected" not in (params.get("config") or {})


def test_hosted_prompt_and_connections_keep_token_out_of_config(monkeypatch):
    monkeypatch.setenv("INKBOX_HOSTED_MODE", "1")
    monkeypatch.setenv("INKBOX_HOSTED_INSTRUCTIONS", "Be helpful")
    monkeypatch.setenv("INKBOX_API_KEY", "synthetic-secret")
    client = CodexAppServerClient(BridgeConfig(identity="example", base_url="https://api.example.test"), developer_instructions="routing")
    params = client._thread_params()
    assert params["developerInstructions"] == "Be helpful\n\nrouting"
    server = params["config"]["mcp_servers.inkbox_connected"]
    assert server["url"] == "https://api.example.test/api/v1/identities/example/hosting/mcp"
    assert server["env_http_headers"] == {"X-Service-Token": "INKBOX_API_KEY"}
    assert "synthetic-secret" not in str(params)


@pytest.mark.parametrize("origin", ["http://api.example.test", "https://user@api.example.test", "https://api.example.test/other"])
def test_hosted_connections_reject_ambiguous_origin(monkeypatch, origin):
    monkeypatch.setenv("INKBOX_HOSTED_MODE", "1")
    client = CodexAppServerClient(BridgeConfig(identity="example", base_url=origin), developer_instructions="")
    with pytest.raises(ValueError):
        client._thread_params()

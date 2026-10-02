"""Vault tools exercised through MCP with real SDK decryption and TOTP."""

import asyncio
import json

import httpx
import pytest

from inkbox_codex import __version__
from inkbox_codex.config import BridgeConfig
from inkbox_codex.mcp_stdio import InkboxMcpServer
from inkbox_codex.tools import build_inkbox_mcp_server_config
from tests.fixtures.vault_api import LOGIN_ID, TOKEN_ID, TOTP_SEED, VAULT_KEY, VaultAPI


@pytest.fixture
def vault(monkeypatch, tmp_path):
    api = VaultAPI()
    monkeypatch.setenv("INKBOX_API_KEY", "synthetic-agent-key")
    monkeypatch.setenv("INKBOX_IDENTITY", "example-agent")
    monkeypatch.setenv("INKBOX_BASE_URL", "https://api.example.com")
    monkeypatch.delenv("INKBOX_VAULT_KEY", raising=False)
    monkeypatch.setattr("inkbox._config._CONFIG_PATH", tmp_path / "missing-config")
    monkeypatch.setattr("inkbox._http.httpx.HTTPTransport", lambda **kw: httpx.MockTransport(api.respond))
    server = InkboxMcpServer()
    yield server, api
    if server._client is not None:
        server._client.close()


def call(server, name, **arguments):
    response = asyncio.run(server.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }))
    result = response["result"]
    return result, json.loads(result["content"][0]["text"])


def test_metadata_listing_without_unlock_key(vault):
    server, api = vault
    result, secrets = call(server, "inkbox_list_vault_secrets", secret_type="login")
    assert not result.get("isError")
    assert [s["id"] for s in secrets] == [LOGIN_ID]
    assert len(api.requests) == 1
    assert api.requests[0].url.params["secret_type"] == "login"
    assert all("payload" not in key for s in secrets for key in s)
    assert TOTP_SEED not in json.dumps(result)


@pytest.mark.parametrize("name", ["inkbox_get_vault_secret", "inkbox_get_totp_code"])
def test_locked_vault_explains_local_configuration(vault, name):
    server, api = vault
    result, data = call(server, name, secret_id=LOGIN_ID)
    assert result["isError"]
    assert "INKBOX_VAULT_KEY" in data["error"]
    assert "restart" in data["error"]
    assert not api.requests


def test_totp_returns_rfc_code_and_expiry_without_credentials(vault, monkeypatch):
    server, api = vault
    monkeypatch.setenv("INKBOX_VAULT_KEY", VAULT_KEY)
    monkeypatch.setattr("inkbox.vault.totp.time.time", lambda: 1111111109)
    result, data = call(server, "inkbox_get_totp_code", secret_id=LOGIN_ID)
    assert not result.get("isError")
    assert data == {"code": "07081804", "period_start": 1111111080,
                    "period_end": 1111111110, "seconds_remaining": 1}
    for value in (VAULT_KEY, TOTP_SEED, "synthetic-password", "synthetic-api-token"):
        assert value not in json.dumps(result)
    monkeypatch.setattr("inkbox.vault.totp.time.time", lambda: 1111111111)
    _, refreshed = call(server, "inkbox_get_totp_code", secret_id=LOGIN_ID)
    assert refreshed["code"] == "14050471"
    assert refreshed["seconds_remaining"] == 29
    assert sum(r.url.path.endswith("/unlock") for r in api.requests) == 1
    assert sum(r.url.path.endswith(f"/secrets/{LOGIN_ID}") for r in api.requests) == 2


@pytest.mark.parametrize("secret_id,payload,has_totp", [
    (LOGIN_ID, {"username": "agent@example.com", "password": "synthetic-password",
                "email": None, "url": None, "notes": None}, True),
    (TOKEN_ID, {"api_key": "synthetic-api-token", "endpoint": None, "notes": None}, None),
])
def test_get_one_credential_omits_totp_seed(vault, monkeypatch, secret_id, payload, has_totp):
    server, _ = vault
    monkeypatch.setenv("INKBOX_VAULT_KEY", VAULT_KEY)
    result, secret = call(server, "inkbox_get_vault_secret", secret_id=secret_id)
    assert not result.get("isError")
    assert secret["id"] == secret_id
    assert secret["payload"] == payload
    assert secret.get("has_totp") is has_totp
    assert TOTP_SEED not in json.dumps(result)
    assert VAULT_KEY not in json.dumps(result)


@pytest.mark.parametrize("name", ["inkbox_get_vault_secret", "inkbox_get_totp_code"])
@pytest.mark.parametrize("failure", ["denied", "deleted"])
def test_reads_refetch_instead_of_using_unlock_snapshot(vault, monkeypatch, name, failure):
    server, api = vault
    monkeypatch.setenv("INKBOX_VAULT_KEY", VAULT_KEY)
    result, _ = call(server, name, secret_id=LOGIN_ID)
    assert not result.get("isError")
    if failure == "denied":
        api.denied = True
    else:
        del api.details[LOGIN_ID]
    result, data = call(server, name, secret_id=LOGIN_ID)
    assert result["isError"]
    assert data["status_code"] == (403 if failure == "denied" else 404)
    assert "payload" not in data and "code" not in data


@pytest.mark.parametrize("secret_id,error", [(TOKEN_ID, "only login"), (LOGIN_ID, "no TOTP")])
def test_totp_rejects_wrong_type_and_missing_configuration(vault, monkeypatch, secret_id, error):
    server, api = vault
    api.set_secret(LOGIN_ID, "login", {"username": "agent@example.com", "password": "synthetic-password"})
    monkeypatch.setenv("INKBOX_VAULT_KEY", VAULT_KEY)
    result, data = call(server, "inkbox_get_totp_code", secret_id=secret_id)
    assert result["isError"]
    assert error in data["error"]


@pytest.mark.parametrize("secret_id", ["", "../keys", "not-a-uuid"])
def test_invalid_secret_id_never_reaches_api(vault, secret_id):
    server, api = vault
    result, data = call(server, "inkbox_get_vault_secret", secret_id=secret_id)
    assert result["isError"]
    assert "UUID" in data["error"]
    assert not api.requests


@pytest.mark.parametrize("failure,error", [("wrong_key", "No vault key matched"), ("uninitialized", "not been initialized")])
def test_sdk_unlock_errors_are_actionable(vault, monkeypatch, failure, error):
    server, api = vault
    monkeypatch.setenv("INKBOX_VAULT_KEY", "Wrong-example-key-42!" if failure == "wrong_key" else VAULT_KEY)
    api.initialized = failure != "uninitialized"
    with pytest.raises(ValueError, match=error):
        call(server, "inkbox_get_totp_code", secret_id=LOGIN_ID)


def test_mcp_forwards_vault_key_by_environment_name(monkeypatch):
    monkeypatch.setenv("INKBOX_VAULT_KEY", VAULT_KEY)
    config, names = build_inkbox_mcp_server_config(BridgeConfig())
    assert "INKBOX_VAULT_KEY" in config["env_vars"]
    assert VAULT_KEY not in json.dumps(config)
    assert "mcp__inkbox__inkbox_get_totp_code" in names


def test_mcp_reports_current_plugin_version(vault):
    server, _ = vault
    response = asyncio.run(server.handle({"id": 1, "method": "initialize"}))
    assert response["result"]["serverInfo"]["version"] == __version__

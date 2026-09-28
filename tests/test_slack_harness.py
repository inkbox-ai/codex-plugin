"""Harness identity checks and signed HTTP-to-reply acceptance without live sends."""

import asyncio
import json
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex import slack_harness
from inkbox_codex.config import BridgeConfig
from inkbox_codex.sessions import SessionManager
from inkbox_codex.slack import inbound_message
from tests.test_slack import CONNECTION, IDENTITY, event
from tests.test_webhook_providers import _sign


def test_env_file_is_literal_and_does_not_execute(tmp_path):
    path = tmp_path / "credentials.env"
    path.write_text("export EXAMPLE_KEY='literal$(false)'\n# comment\nOTHER=\"a b\"\n")
    assert slack_harness.read_env_file(path) == {"EXAMPLE_KEY": "literal$(false)", "OTHER": "a b"}


def test_preflight_rejects_another_identity_before_slack_access():
    client = Mock()
    client.whoami.return_value = NS(scope="agent_identity:another")
    client.get_identity.return_value = NS(id=IDENTITY)
    with pytest.raises(ValueError, match="scoped"):
        slack_harness.preflight(client, "agent")
    client.slack.list_connections.assert_not_called()


def test_read_only_preflight_uses_only_selected_key_and_does_not_write(tmp_path, monkeypatch):
    import inkbox

    path = tmp_path / "credentials.env"
    path.write_text("SELECTED_KEY=test_selected\nINKBOX_API_KEY=test_other\n")
    monkeypatch.setenv("INKBOX_API_KEY", "ambient_other")
    client = Mock()
    client.whoami.return_value = NS(scope=f"agent_identity:{IDENTITY}")
    client.get_identity.return_value = NS(id=IDENTITY)
    client.slack.list_connections.return_value = NS(connections=[])
    factory = Mock(return_value=client)
    monkeypatch.setattr(inkbox, "Inkbox", factory)
    state = tmp_path / "state"
    assert slack_harness.main([
        "--credentials-file", str(path), "--api-key-env", "SELECTED_KEY",
        "--identity", "agent", "--base-url", "https://api.example",
        "--state-dir", str(state),
    ]) == 0
    assert factory.call_args.kwargs["api_key"] == "test_selected"
    assert factory.call_args.kwargs["base_url"] == "https://api.example"
    assert not state.exists()
    client.webhooks.subscriptions.create.assert_not_called()


def test_harness_only_reconciles_slack(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    gateway = slack_harness.SlackHarness(BridgeConfig(slack_enabled=True))
    gateway._reconcile_slack = Mock()
    gateway._inkbox = Mock()
    gateway._patch_identity_objects()
    gateway._reconcile_slack.assert_called_once()
    assert not gateway._inkbox.mock_calls


def test_signed_http_event_runs_session_and_posts_one_threaded_reply(tmp_path, monkeypatch):
    from aiohttp import ClientSession, web
    from inkbox_codex.webhook_providers import inkbox as verifier
    from inkbox import verify_webhook

    monkeypatch.setattr(verifier, "verify_webhook", verify_webhook)
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def scenario():
        cfg = BridgeConfig(slack_enabled=True, signing_key="whsec_test", identity="agent")
        gateway = slack_harness.SlackHarness(cfg)
        gateway._identity = NS(id=IDENTITY)
        client = gateway._inkbox = Mock()
        client.slack.send_message.return_value = NS(id="action-1", status="sent")
        gateway.sessions = SessionManager(cfg, gateway.send_to_contact, {}, {"handle": "agent"})
        session = gateway.sessions.get(inbound_message(event(), IDENTITY)[0])
        prompts = []

        class Codex:
            thread_id = "test-thread"

            async def run(self, prompt):
                prompts.append(prompt)
                return "*Hello from Codex*"

        session._client = Codex()
        app = web.Application()
        app.router.add_post("/webhook", gateway._handle_webhook)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with ClientSession() as http:
                url = f"http://127.0.0.1:{port}/webhook"
                body = json.dumps(event()).encode()
                headers = _sign(body, "whsec_test", timestamp=str(int(time.time())))
                async with http.post(url, data=body, headers={**headers, "X-Inkbox-Signature": "bad"}) as response:
                    assert response.status == 401
                for request_id in ("request-1", "request-2"):
                    headers = _sign(body, "whsec_test", request_id=request_id, timestamp=str(int(time.time())))
                    async with http.post(url, data=body, headers=headers) as response:
                        assert response.status == 200
                await session._worker
                assert len(prompts) == 1 and "inkbox:slack" in prompts[0]
                call = client.slack.send_message.call_args
                assert client.slack.send_message.call_count == 1
                assert call.args == (CONNECTION,)
                assert call.kwargs["conversation_id"] == "C_TEST"
                assert call.kwargs["thread_ts"] == "1234567890.000001"
                assert call.kwargs["text"] == "*Hello from Codex*"
                unrelated = json.dumps({"event_type": "text.received", "companion": {"test": True}}).encode()
                async with http.post(url, data=unrelated) as response:
                    assert (await response.json())["ignored"] == "non-slack"
                assert len(prompts) == 1
        finally:
            await runner.cleanup()

    asyncio.run(scenario())

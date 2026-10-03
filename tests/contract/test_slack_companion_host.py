"""The real host persists Slack ride-along context without running a model."""

import asyncio
import json
import shutil
import threading
from http.server import ThreadingHTTPServer

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from tests.contract.test_host_interface import _free_port
from tests.live import mock_openai
from tests.test_slack_companion import fixture, gateway, live, session_for


CODEX_BIN = shutil.which("codex")
pytestmark = pytest.mark.skipif(CODEX_BIN is None, reason="requires real codex CLI")


def test_slack_quiet_context_survives_real_host_restart_then_one_mentioned_turn(tmp_path, monkeypatch):
    requests = []

    class Handler(mock_openai.Handler):
        def _respond_responses(self, request):
            requests.append(request)
            super()._respond_responses(request)

    server = ThreadingHTTPServer(("127.0.0.1", _free_port()), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="mock"\n'
        '[model_providers.mock]\nname="Mock"\n'
        f'base_url="http://127.0.0.1:{server.server_port}/v1"\nwire_api="responses"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(home))

    async def scenario():
        initial = fixture()
        current = live(initial, access="sponsored", actor="UBOB", text="This is a background progress note.")
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        gw.cfg.project_dir = str(tmp_path)
        gw.cfg.codex_bin, gw.cfg.codex_model, gw.cfg.codex_sandbox = CODEX_BIN, "mock-model", "read-only"
        receiver = gw._companion()
        session = session_for(gw, current)
        host = CodexAppServerClient(gw.cfg, developer_instructions="contract-test")
        try:
            thread_id = await host.connect()
            session._client = host
            await receiver.accept(current)
            await asyncio.wait_for(asyncio.gather(*receiver.tasks.values()), 30)
            assert not requests
            slack.send_message.assert_not_called()
            await host.disconnect()
            host = CodexAppServerClient(gw.cfg, developer_instructions="contract-test")
            await host.connect(resume_thread_id=thread_id)
            session._client = host
            next_event = live(initial, sequence=3, text="<@UBOT> Summarize the current progress.")
            await receiver.accept(next_event)
            await asyncio.wait_for(asyncio.gather(*receiver.tasks.values()), 30)
            assert len(requests) == 1
            assert slack.send_message.call_count == 1
            model_input = json.dumps(requests[0]["input"])
            for entry in initial["companion"]["history"]:
                assert model_input.count(entry["text"]) == 1
                assert entry["author"] in model_input
            assert model_input.count(current["data"]["event"]["text"]) == 1
            assert "sender_access=sponsored" in model_input
            assert "sender_access=direct" in model_input
            call = slack.send_message.call_args
            assert call.kwargs["conversation_id"] == "CEXAMPLE"
            assert call.kwargs["thread_ts"] == initial["data"]["thread_ts"]
        finally:
            await receiver.close()
            await host.disconnect()

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()

"""Real host MCP metadata binds WhatsApp mutations to the submitted source."""

import asyncio
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import shutil
import sys
import threading
from urllib.parse import urlsplit

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.config import BridgeConfig
from inkbox_codex.imessage import clear_turn_context, write_turn_context
from inkbox_codex.tools import build_inkbox_mcp_server_config
from inkbox_codex import turn_authority as authority
from tests.test_whatsapp import CONVERSATION, IDENTITY, MESSAGE, NOW, OUTBOUND, message


@pytest.mark.skipif(shutil.which("codex") is None, reason="needs the codex CLI on PATH")
@pytest.mark.parametrize("origin", ["native-stopped", "native-active", "email", "standalone"])
def test_actual_host_source_metadata_and_late_cancelled_tool(tmp_path, monkeypatch, origin):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "live"))
    import mock_openai

    model_requests, sends = [], []

    class Handler(mock_openai.Handler):
        def do_GET(self):
            if self.path != "/api/v1/identities/agent":
                self.send_error(404)
                return
            self._send_json(200, {"id": IDENTITY, "organization_id": "org_example", "agent_handle": "agent",
                "whatsapp_enabled": True, "created_at": NOW, "updated_at": NOW})

        def do_POST(self):
            if urlsplit(self.path).path == "/api/v1/whatsapp/messages":
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                sends.append(body)
                self._send_json(201, {"message": message(id=OUTBOUND, direction="outbound", status="queued")})
                return
            super().do_POST()

        def _respond_responses(self, request):
            model_requests.append(request)
            if len(model_requests) != 1:
                return super()._respond_responses(request)
            item = {
                "type": "function_call", "id": "fc_source", "call_id": "call_source",
                "name": "inkbox_send_whatsapp", "namespace": "mcp__inkbox",
                "arguments": json.dumps({"conversation_id": CONVERSATION, "text": "Local test reply"}),
                "status": "completed",
            }
            response = {"id": "resp_source", "object": "response", "status": "completed", "model": "mock-model",
                "output": [item], "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}
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

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model="mock-model"\nmodel_provider="mock"\n[model_providers.mock]\nname="Local contract"\n'
        f'base_url="http://127.0.0.1:{http.server_port}/v1"\nwire_api="responses"\n')
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path / "bridge-home"))
    monkeypatch.delenv(authority.HOST_ENV, raising=False)
    cfg = BridgeConfig(api_key="ApiKey_synthetic_test_only", identity="agent",
        base_url=f"http://127.0.0.1:{http.server_port}", project_dir=str(tmp_path),
        codex_bin=shutil.which("codex"), codex_model="mock-model", codex_sandbox="read-only",
        codex_approval_policy="never", auto_approve_inkbox_tools=True)
    server, _ = build_inkbox_mcp_server_config(cfg)
    source = None
    if origin != "standalone":
        epoch = authority.new_host("contact", cfg)
        server["env"].update({authority.HOST_ENV: epoch, "INKBOX_CODEX_CHAT_ID": "contact"})
        context = write_turn_context("contact", cfg, {
            "channel": "whatsapp", "conversation_id": CONVERSATION, "message_id": MESSAGE,
            "whatsapp_auto_reply_target": MESSAGE,
            "imessage_event_ids": ["source-event"],
        }) if origin.startswith("native") else None
        source = authority.begin("contact", cfg, epoch,
            "whatsapp" if context else "email", context["nonce"] if context else None)

    dispatched = tmp_path / "dispatched.json"
    release = tmp_path / "release-tool"
    if origin != "native-stopped":
        release.touch()
    wrapper = tmp_path / "held_mcp.py"
    wrapper.write_text(
        "import asyncio,json\nfrom pathlib import Path\nfrom inkbox_codex import mcp_stdio,turn_authority\n"
        "original=mcp_stdio.call_inkbox_tool\n"
        "async def held(*args):\n"
        f"    Path({str(dispatched)!r}).write_text(json.dumps(turn_authority._request_metadata.get()))\n"
        "    async def wait():\n"
        f"        while not Path({str(release)!r}).exists(): await asyncio.sleep(0.01)\n"
        "    await asyncio.wait_for(wait(),10)\n"
        "    return await original(*args)\n"
        "mcp_stdio.call_inkbox_tool=held\nmcp_stdio.main()\n")
    server["args"] = [str(wrapper)]

    async def scenario():
        host = CodexAppServerClient(cfg, developer_instructions="Local source-lifetime contract.",
            mcp_server_config=server,
            turn_started=(lambda thread, turn: authority.bind(source, thread, turn)) if source else None)
        stop = running = None
        interrupt_entered, release_interrupt = asyncio.Event(), asyncio.Event()
        request = host._request

        async def held_request(method, params, **kwargs):
            if method == "turn/interrupt":
                interrupt_entered.set()
                await release_interrupt.wait()
            return await request(method, params, **kwargs)

        try:
            await asyncio.wait_for(host.connect(), 10)
            running = asyncio.create_task(host.run_detailed("Send the requested WhatsApp reply."))

            async def tool_dispatched():
                while not dispatched.exists():
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(tool_dispatched(), 10)
            actual_origin = json.loads(dispatched.read_text())["x-codex-turn-metadata"]
            assert actual_origin["thread_id"] == host.thread_id
            assert actual_origin["turn_id"] == host._current_turn_id
            if origin == "native-stopped":
                authority.retire(source)
                clear_turn_context("contact")
                host._request = held_request
                stop = asyncio.create_task(host.interrupt())
                await asyncio.wait_for(interrupt_entered.wait(), 1)
                assert not stop.done()
                release.touch()
            result = await asyncio.wait_for(running, 15)
            assert len(result.mcp_tool_calls) == 1
            assert len(model_requests) == 2
            tool_outputs = [item for item in model_requests[-1]["input"] if item.get("type") == "function_call_output"]
            assert tool_outputs
            output = json.dumps(tool_outputs)
            assert len(sends) == (0 if origin == "native-stopped" else 1), output
            if sends:
                assert sends[0] == {
                    "conversation_id": CONVERSATION, "text": "Local test reply",
                    **({"reply_to_message_id": MESSAGE} if origin == "native-active" else {}),
                }
            if origin == "native-stopped":
                assert "No active gateway source" in output
                assert not stop.done(), "Tool must be rejected before interrupt completion"
            else:
                assert "Local test reply" == sends[0]["text"]
                assert OUTBOUND in output
        finally:
            release.touch()
            release_interrupt.set()
            if stop is not None:
                await asyncio.wait_for(asyncio.gather(stop, return_exceptions=True), 5)
            if running is not None and not running.done():
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            await host.disconnect()
            authority.retire(source)
    try:
        asyncio.run(scenario())
    finally:
        http.shutdown()
        http.server_close()

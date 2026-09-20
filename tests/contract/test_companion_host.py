"""The real Codex host gets one initialization input, never one turn per entry."""
import asyncio
import json
import shutil
import threading
from http.server import ThreadingHTTPServer

import pytest

from inkbox_codex.codex_client import CodexAppServerClient
from tests.contract.test_host_interface import _free_port
from tests.live import mock_openai
from tests.test_companion import fixture, harness, live, isolated

CODEX_BIN = shutil.which('codex')
pytestmark = pytest.mark.skipif(CODEX_BIN is None, reason='requires real codex CLI')


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
@pytest.mark.parametrize('reply_mode', ['auto', 'mention'])
def test_companion_one_host_turn_or_quiet_context_then_resume(tmp_path, monkeypatch, channel, reply_mode):
    requests = []
    class Handler(mock_openai.Handler):
        def _respond_responses(self, request):
            requests.append(request)
            super()._respond_responses(request)
    server = ThreadingHTTPServer(('127.0.0.1', _free_port()), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    home = tmp_path / 'codex-home'
    home.mkdir()
    (home / 'config.toml').write_text(
        'model="mock-model"\nmodel_provider="mock"\n'
        '[model_providers.mock]\nname="Mock"\n'
        f'base_url="http://127.0.0.1:{server.server_port}/v1"\nwire_api="responses"\n'
    )
    monkeypatch.setenv('CODEX_HOME', str(home))
    async def scenario():
        e = fixture(channel)
        r, sdk, s, sent = harness(e, reply_mode=reply_mode)
        s.cfg.project_dir = str(tmp_path)
        s.cfg.codex_bin, s.cfg.codex_model, s.cfg.codex_sandbox = CODEX_BIN, 'mock-model', 'read-only'
        host = CodexAppServerClient(s.cfg, developer_instructions='contract-test')
        s._client = host
        try:
            thread_id = await host.connect()
            await r.accept(e)
            await asyncio.wait_for(asyncio.gather(*r.tasks.values()), 30)
            if reply_mode == 'mention':
                assert not requests
                assert not sent
                # A fresh app-server resumes the quiet context from disk.
                await host.disconnect()
                host = CodexAppServerClient(s.cfg, developer_instructions='contract-test')
                await host.connect(resume_thread_id=thread_id)
                s._client = host
                await r.accept(live(e))
                await asyncio.wait_for(asyncio.gather(*r.tasks.values()), 30)
            assert len(requests) == 1
            assert len(sent) == 1
            model_input = json.dumps(requests[0]['input'])
            for entry in e['companion']['history']:
                assert model_input.count(entry['text']) == 1
                assert entry['author'] in model_input
        finally:
            await r.close()
            await host.disconnect()
    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()

"""Companion queue conformance against a local Codex app-server."""

import asyncio
import copy
import shutil
import threading
from http.server import ThreadingHTTPServer

import pytest

from inkbox_codex import sessions
from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.companion import CompanionInbox
from tests.live.mock_openai import Handler
from tests.test_companion import Request, event, snapshot, harness as harness

pytestmark = pytest.mark.skipif(shutil.which("codex") is None, reason="Requires local Codex")


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_companion_native_turn_and_restart(harness, tmp_path, monkeypatch, channel):
    sdk = pytest.importorskip("inkbox.companion")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model = "mock-model"\nmodel_provider = "mock"\n'
        '[model_providers.mock]\nname = "Mock"\n'
        f'base_url = "http://127.0.0.1:{server.server_port}/v1"\nwire_api = "responses"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    submissions = []
    failures = []
    initial_complete, release = asyncio.Event(), asyncio.Event()

    class Client(CodexAppServerClient):
        async def connect(self, resume_thread_id=None):
            self.mcp_server_config = {}
            try:
                return await super().connect(resume_thread_id)
            except Exception as exc:
                failures.append(str(exc))
                raise

        async def _request(self, method, params):
            if method == "turn/start":
                submissions.append(params)
            return await super()._request(method, params)

        async def run_detailed(self, text, **kwargs):
            result = await super().run_detailed(text, **kwargs)
            if len(submissions) == 1:
                initial_complete.set()
                await release.wait()
            return result

    monkeypatch.setattr(sessions, "CodexAppServerClient", Client)
    h = harness
    h.gw.cfg.codex_model = "mock-model"
    h.gw.cfg.codex_sandbox = "read-only"
    h.gw.cfg.codex_turn_timeout_s = 30
    envelope = event(channel)
    h.snapshot.value = snapshot(envelope)
    cursors = []

    class Transport:
        def get(self, path, *, params):
            assert "/companion/activations/" in path
            cursor = params.get("cursor")
            cursors.append(cursor)
            fixture = h.snapshot.value
            return copy.deepcopy({
                **{key: fixture[key] for key in (
                    "scope_id", "activation_id", "conversation_id", "channel", "reply_context",
                    "notices",
                )},
                "items": fixture["entries"][:2] if cursor is None else fixture["entries"][1:],
                "history_complete": cursor is not None,
                "next_cursor": "page-two" if cursor is None else None,
            })

    h.gw._inkbox.companion = sdk.CompanionResource(Transport())

    async def drain():
        await asyncio.wait_for(asyncio.gather(*h.gw._companion.tasks.values()), 45)

    async def run():
        try:
            await h.gw._handle_webhook(Request(envelope))
            await asyncio.wait_for(initial_complete.wait(), 30)
            await h.gw._handle_webhook(Request(event(channel, "live", 4, 2), "queued-live"))
            assert len(submissions) == 1
            assert h.gw._companion.db.execute(
                "SELECT count(*) FROM jobs WHERE status='pending'"
            ).fetchone()[0] == 1
            release.set()
            await drain()
            assert len(submissions) == 2, failures
            assert len(submissions[0]["input"]) == 1
            text = submissions[0]["input"][0]["text"]
            assert all(value in text for value in ("/clear", "YES", "Please join", "agenda.txt"))
            assert text.count('"is_trigger":true') == 1
            assert "page-two" in cursors
            assert "REPLY_OK" in h.replies[0][1]
            row = dict(h.gw._companion.db.execute("SELECT * FROM jobs").fetchone())
            assert row["status"] == "completed" and row["turn_id"]
            thread_id = row["thread_id"]
            await h.gw._companion.close()
            h.gw.sessions.sessions.clear()
            h.gw.sessions._session_ids.clear()
            h.gw._companion = CompanionInbox(h.gw)
            await h.gw._handle_webhook(Request(envelope, "duplicate"))
            await drain()
            assert len(submissions) == 2
            await h.gw._handle_webhook(Request(event(channel, "live", 5, 3), "live"))
            await drain()
            assert len(submissions) == 3
            assert submissions[1]["threadId"] == thread_id
            assert submissions[2]["threadId"] == thread_id
            assert "REPLY_OK" in h.replies[2][1]
        finally:
            if h.gw._companion is not None:
                await h.gw._companion.close()

    try:
        asyncio.run(run())
    finally:
        server.shutdown()
        server.server_close()

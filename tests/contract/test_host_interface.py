"""Contract tests against the REAL Codex host.

The bridge's entire host interface is the ``codex app-server`` stdio JSON-RPC
protocol: the ``initialize`` handshake, ``thread/start``/``thread/resume`` with
the exact parameter shape the bridge sends, ``turn/start`` + the notification
stream it consumes (``item/agentMessage/delta``, ``item/completed``,
``turn/completed``), and the ``account/*`` usage endpoints. A renamed method, a
moved result field, or a dropped notification breaks users silently — this suite
catches that drift by exercising a real installed ``codex`` binary.

The turn-level test drives the bridge's own ``CodexAppServerClient`` against a
local deterministic mock model (tests/live/mock_openai.py) via a custom provider
in an isolated ``CODEX_HOME`` — full protocol coverage, no account auth, no
tokens. (``thread/start`` and the handshake need no auth at all; the ``account/*``
endpoints answer unauthenticated requests with a distinctive auth-required error,
which is itself asserted — an unknown method would error differently.)

Skipped when no ``codex`` binary is on PATH (e.g. the offline unit lane).
"""

from __future__ import annotations

import asyncio
import json
import queue
import shutil
import socket
import subprocess
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

CODEX_BIN = shutil.which("codex")

pytestmark = pytest.mark.skipif(
    CODEX_BIN is None,
    reason="contract suite: needs the codex CLI on PATH",
)


class _RawAppServer:
    """Minimal line-delimited JSON-RPC session with ``codex app-server``.

    Deliberately independent of the bridge's client so protocol-shape tests
    stay meaningful even if the client has a bug.
    """

    def __init__(self, env: dict):
        self.proc = subprocess.Popen(
            [CODEX_BIN, "app-server"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        )
        self._q: "queue.Queue[str]" = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()
        self._next_id = 1

    def _pump(self):
        for line in self.proc.stdout:
            self._q.put(line)

    def request(self, method: str, params: dict, timeout: float = 30.0) -> dict:
        """Send one request and return its full response message ({result} or {error})."""
        mid = self._next_id
        self._next_id += 1
        self.proc.stdin.write(json.dumps({"id": mid, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        while True:
            try:
                msg = json.loads(self._q.get(timeout=timeout))
            except queue.Empty:
                pytest.fail(f"no response to {method} within {timeout:.0f}s")
            if msg.get("id") == mid and ("result" in msg or "error" in msg):
                return msg
            # everything else is a notification / unrelated message — keep reading

    def notify(self, method: str, params: dict) -> None:
        self.proc.stdin.write(json.dumps({"method": method, "params": params}) + "\n")
        self.proc.stdin.flush()

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()


@pytest.fixture()
def raw(tmp_path, monkeypatch):
    # Isolated CODEX_HOME: no user config, no login — proves what works auth-free.
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex-home"))
    (tmp_path / "codex-home").mkdir()
    import os
    session = _RawAppServer(env=dict(os.environ))
    session.request("initialize", {
        "clientInfo": {"name": "inkbox_codex", "title": "Inkbox Codex Bridge", "version": "0.2.8"},
        "capabilities": {"experimentalApi": True},
    })
    session.notify("initialized", {})
    yield session
    session.close()


def _thread_params(cwd: str) -> dict:
    # The exact shape CodexAppServerClient._thread_params sends.
    return {
        "cwd": cwd,
        "model": None,
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "developerInstructions": "contract-test",
        "sandbox": "read-only",
        "config": None,
        "serviceName": "inkbox-codex",
    }


def test_cli_present_and_versioned():
    out = subprocess.run([CODEX_BIN, "--version"], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip(), "codex --version printed nothing"


def test_thread_start_and_resume_route(raw, tmp_path):
    """thread/start accepts the bridge's params and returns result.thread.id.
    thread/resume must still route — a fresh no-turn thread has no rollout to
    reopen (that's fine; the real resume is proven turn-first in the mock-turn
    test), but a dropped method would be method-not-found."""
    started = raw.request("thread/start", _thread_params(str(tmp_path)))
    assert "result" in started, f"thread/start errored: {started.get('error')}"
    thread_id = str((started["result"].get("thread") or {}).get("id") or "")
    assert thread_id, f"no thread id in: {started['result']!r}"

    params = _thread_params(str(tmp_path))
    params["threadId"] = thread_id
    resumed = raw.request("thread/resume", params)
    if "result" in resumed:
        resumed_id = str((resumed["result"].get("thread") or {}).get("id") or "")
        assert resumed_id == thread_id, f"resume returned a different thread: {resumed_id!r}"
    else:
        err = resumed["error"]
        assert err.get("code") != -32601, f"thread/resume is gone from the host: {err}"
        assert "method not found" not in str(err.get("message", "")).lower(), err


def test_turn_interrupt_method_exists(raw, tmp_path):
    """turn/interrupt must still be a routable method (bogus ids may error, but
    never with method-not-found)."""
    started = raw.request("thread/start", _thread_params(str(tmp_path)))
    thread_id = str((started["result"].get("thread") or {}).get("id") or "")
    resp = raw.request("turn/interrupt", {"threadId": thread_id, "turnId": "not-a-real-turn"})
    err = resp.get("error") or {}
    assert err.get("code") != -32601, f"turn/interrupt is gone from the host: {err}"
    assert "method not found" not in str(err.get("message", "")).lower(), err


def test_account_usage_methods_exist(raw):
    """The /usage command reads account/rateLimits/read + account/usage/read.
    Unauthenticated they must answer with an auth-required error — an unknown
    method would be method-not-found instead."""
    for method in ("account/rateLimits/read", "account/usage/read"):
        resp = raw.request(method, {})
        if "result" in resp:
            continue  # runner happens to be logged in — even better
        err = resp["error"]
        assert err.get("code") != -32601, f"{method} is gone from the host: {err}"
        assert "auth" in str(err.get("message", "")).lower(), \
            f"{method} failed for a non-auth reason: {err}"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_bridge_client_full_mock_turn(tmp_path, monkeypatch):
    """The bridge's own CodexAppServerClient completes a full turn against a
    real app-server thinking on the local mock model.

    Covers everything the raw probes can't: turn/start's parameter shape, the
    item/agentMessage delta + completed notifications, turn/completed handling,
    and final-message assembly — the whole reply path the gateway relies on.
    """
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "tests" / "live"))
    import mock_openai  # noqa: E402

    port = _free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), mock_openai.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    home = tmp_path / "codex-home"
    home.mkdir()
    (home / "config.toml").write_text(
        'model = "mock-model"\n'
        'model_provider = "mock"\n'
        "\n"
        "[model_providers.mock]\n"
        'name = "Mock"\n'
        f'base_url = "http://127.0.0.1:{port}/v1"\n'
        'wire_api = "responses"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(home))

    from inkbox_codex.codex_client import CodexAppServerClient
    from inkbox_codex.config import BridgeConfig

    cfg = BridgeConfig(
        project_dir=str(tmp_path),
        codex_model="mock-model",
        codex_bin=CODEX_BIN,
        codex_sandbox="read-only",
    )
    client = CodexAppServerClient(cfg, developer_instructions="contract-test")

    async def _run() -> tuple[str, str, str]:
        try:
            thread_id = await client.connect()
            assert thread_id
            reply = await asyncio.wait_for(client.run("ping smoke-c0ffee42"), timeout=60)
        finally:
            await client.disconnect()
        # Resume the SAME thread from a fresh client — the bridge does exactly
        # this on restart (session ids persisted in sessions.json).
        client2 = CodexAppServerClient(cfg, developer_instructions="contract-test")
        try:
            resumed_id = await client2.connect(resume_thread_id=thread_id)
        finally:
            await client2.disconnect()
        return reply, thread_id, resumed_id

    try:
        reply, thread_id, resumed_id = asyncio.run(_run())
    finally:
        server.shutdown()
    assert "REPLY_OK" in reply, f"mock reply did not round-trip: {reply!r}"
    assert "smoke-c0ffee42" in reply, f"nonce lost in the turn pipeline: {reply!r}"
    assert resumed_id == thread_id, f"thread/resume reopened {resumed_id!r}, wanted {thread_id!r}"

"""Live end-to-end coverage for GitHub-signed external webhooks.

A forged signature must be rejected before Codex wakes; a valid signature must
be accepted and start the expected external-event session. Real outbound call
placement is covered independently by ``test_voice.py``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import pytest

GITHUB_SECRET = os.environ.get("INKBOX_WEBHOOK_SECRET_GITHUB")
WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "")
SESSION_START_TIMEOUT_S = float(os.environ.get("LIVE_GITHUB_SESSION_TIMEOUT", "45"))
POLL_EVERY_S = 0.5

pytestmark = pytest.mark.skipif(
    not (
        GITHUB_SECRET
        and GATEWAY_LOG
        and os.environ.get("LIVE_REAL_MODEL") == "1"
    ),
    reason="github external-event suite: needs webhook secret + gateway log + LIVE_REAL_MODEL=1",
)


def _sign_github(payload: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def _post_github_event(
    envelope: dict,
    *,
    secret: str | None = None,
    signature: str | None = None,
) -> tuple[int, str]:
    payload = json.dumps(envelope).encode()
    if secret is not None:
        signature = _sign_github(payload, secret)
    assert signature is not None
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "GitHub-Hookshot/live-test",
            "X-GitHub-Event": "workflow_run",
            "X-GitHub-Delivery": str(uuid.uuid4()),
            "X-Inkbox-Request-Id": str(uuid.uuid4()),
            "X-Hub-Signature-256": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 -- local gateway
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _workflow_run_envelope() -> dict:
    repository = os.environ.get("GITHUB_REPOSITORY", "inkbox-ai/codex-plugin")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_id = str(uuid.uuid4().int % 10**17)
    return {
        "action": "completed",
        "workflow_run": {
            "id": run_id,
            "name": "CI",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "failure",
            "head_branch": "main",
            "html_url": f"{server_url}/{repository}/actions/runs/{run_id}",
        },
        "repository": {
            "name": repository.rsplit("/", 1)[-1],
            "full_name": repository,
            "html_url": f"{server_url}/{repository}",
        },
    }


def _session_marker(envelope: dict) -> str:
    repository = envelope["repository"]["full_name"]
    return f"[session external:{repository}] Codex session started"


def _gateway_log() -> str:
    try:
        with open(GATEWAY_LOG, encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return ""


def _wait_for_log(marker: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if marker in _gateway_log():
            return True
        time.sleep(POLL_EVERY_S)
    return False


def test_forged_github_signature_is_rejected_before_agent_wakes():
    envelope = _workflow_run_envelope()
    marker = _session_marker(envelope)
    status, body = _post_github_event(envelope, signature="sha256=deadbeef")
    assert status == 401, f"forged signature should be rejected, got {status} {body!r}"
    time.sleep(2)
    assert marker not in _gateway_log(), "forged GitHub event unexpectedly woke Codex"


def test_valid_github_signature_wakes_agent_session():
    envelope = _workflow_run_envelope()
    marker = _session_marker(envelope)
    status, body = _post_github_event(envelope, secret=GITHUB_SECRET)
    assert status == 200 and json.loads(body).get("ok") is True, \
        f"valid webhook not accepted: {status} {body!r}"
    assert _wait_for_log(marker, SESSION_START_TIMEOUT_S), \
        f"valid GitHub webhook never started the expected Codex session: {marker}"

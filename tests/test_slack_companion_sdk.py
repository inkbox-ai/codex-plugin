"""Real Slack/Companion SDK wire contracts without external requests."""

import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
from unittest.mock import patch

import httpx
import pytest

from inkbox import Inkbox
from inkbox_codex.slack_companion import require_sdk_support
from tests.test_companion import drained
from tests.test_companion_sdk import HTTP
from tests.test_slack_companion import fixture, gateway, session_for, source_id


try:
    require_sdk_support()
except ValueError:
    pytest.skip("requires the Slack Companion SDK", allow_module_level=True)


class SlackWire:
    def __init__(self, envelope, failure=None):
        self.envelope = deepcopy(envelope)
        self.history = HTTP(envelope)
        self.requests = []
        self.failure = failure

    def __call__(self, request):
        self.requests.append(request)
        path = request.url.path
        data = self.envelope["data"]
        if path.endswith("/slack/connections"):
            assert request.method == "GET"
            assert request.url.params["identity_id"] == data["identity_id"]
            body = {"connections": [{
                "id": data["connection_id"], "identity_id": data["identity_id"],
                "workspace_id": data["workspace_id"], "workspace_name": "Example",
                "bot_user_id": "UBOT", "status": "connected", "scopes": [],
                "created_at": "2026-01-01T00:00:00Z",
            }], "installation_available": False}
        elif path.endswith("/archive/messages"):
            assert request.method == "GET"
            assert request.url.params["conversation_id"] == data["conversation_id"]
            assert request.url.params["limit"] == "2"
            body = {"messages": [{
                "id": source_id(self.envelope), "connection_id": data["connection_id"],
                "conversation_id": data["conversation_id"], "message_ts": data["message_ts"],
                "thread_ts": data["thread_ts"], "user_id": data["actor_id"],
                "text": data["event"]["text"], "files": [], "mentioned": True,
                "source": "event", "captured_at": "2026-01-01T12:02:00Z",
            }], "next_cursor": None}
        elif "/users/" in path:
            assert request.method == "GET"
            assert path.endswith("/users/" + data["actor_id"])
            body = {"id": data["actor_id"], "team_id": "THOME"}
        elif "/companion/activations/" in path:
            assert request.method == "GET"
            assert path.endswith("/" + self.envelope["companion"]["activation_id"] + "/messages")
            body = self.history.get(path, dict(request.url.params))
            if self.failure == "wrong-thread":
                body["reply_context"]["thread_ts"] = "1767268801.000100"
            elif self.failure == "wrong-connection":
                body["reply_context"]["connection_id"] = "40000000-0000-4000-8000-000000000099"
            elif self.failure == "revoked":
                return httpx.Response(403, json={"detail": "Companion access unavailable"})
        elif request.method == "POST" and path.endswith("/messages"):
            assert path == f"/api/v1/slack/connections/{data['connection_id']}/messages"
            body = {
                "id": "40000000-0000-4000-8000-000000000090", "connection_id": data["connection_id"],
                "status": "sent", "conversation_id": data["conversation_id"],
                "message_ts": "1767268999.999999", "thread_ts": data["thread_ts"],
            }
        else:
            pytest.fail(f"Unexpected SDK request: {request.method} {path}")
        return httpx.Response(200, json=body)


def client_for(envelope, monkeypatch, wire):
    with patch("inkbox._http.httpx.HTTPTransport", return_value=httpx.MockTransport(wire)):
        client = Inkbox(api_key="synthetic-key", base_url="https://api.example.com")
    monkeypatch.setattr(client, "get_identity", lambda handle: NS(id=envelope["data"]["identity_id"]))
    return client


@pytest.mark.parametrize("thread", [None, "1767268800.000100"])
def test_real_sdk_source_pagination_and_exact_slack_send(tmp_path, monkeypatch, thread):
    async def scenario():
        initial = fixture()
        initial["data"]["thread_ts"] = thread
        initial["companion"]["reply_context"]["thread_ts"] = thread
        wire = SlackWire(initial)
        gw, _, _ = gateway(monkeypatch, tmp_path, initial, normalize=False)
        with client_for(initial, monkeypatch, wire) as client:
            gw._inkbox = client
            receiver = gw._companion()
            try:
                await receiver.accept(initial)
                await drained(receiver)
                events = session_for(gw, initial)._client.events
                assert len(events) == 1 and events[0][0] == "run"
                for entry in initial["companion"]["history"]:
                    assert events[0][1].count(entry["text"]) == 1
                assert [params.get("cursor") for _, params in wire.history.calls[:3]] == [None, "opaque/+==?cursor", None]
                assert all(params.get("limit") == "1" for _, params in wire.history.calls[3:])
                sends = [request for request in wire.requests if request.method == "POST"]
                assert len(sends) == 1
                assert json.loads(sends[0].content) == {
                    "conversation_id": "CEXAMPLE", "text": "Answer",
                    **({"thread_ts": thread} if thread is not None else {}),
                }
                assert sends[0].headers["Idempotency-Key"].startswith("codex:")
                assert not await receiver.accept(initial)
                await drained(receiver)
                assert len([r for r in wire.requests if r.method == "POST"]) == 1
            finally:
                await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["wrong-thread", "wrong-connection", "revoked"])
def test_sdk_snapshot_cannot_cross_route_or_admit_unavailable_history(tmp_path, monkeypatch, failure):
    async def scenario():
        initial = fixture()
        wire = SlackWire(initial, failure=failure)
        gw, _, _ = gateway(monkeypatch, tmp_path, initial, normalize=False)
        with client_for(initial, monkeypatch, wire) as client:
            gw._inkbox = client
            receiver = gw._companion()
            try:
                await receiver.accept(initial)
                await drained(receiver)
                assert all(session._client is None for session in gw.sessions.sessions.values())
                assert not any(request.method == "POST" for request in wire.requests)
            finally:
                await receiver.close()
    asyncio.run(scenario())

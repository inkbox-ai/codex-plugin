"""Automatic email replies retain their original message's audience and thread."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import httpx
from inkbox import Inkbox
from inkbox.exceptions import InkboxAPIError

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from tests.test_sessions import make_session


def _gateway():
    identity = Mock(spec=["reply_all_email", "send_email"])
    gw = InkboxGateway(BridgeConfig(identity="test-agent", allow_all_users=True))
    gw._inkbox = SimpleNamespace(get_identity=Mock(return_value=identity))
    gw._resolve_contact_full = AsyncMock(return_value=None)
    session = SimpleNamespace(handle_inbound=AsyncMock())
    gw.sessions = SimpleNamespace(get=Mock(return_value=session))
    return gw, identity, session


def test_inbound_email_reply_uses_stored_message_for_recipient_and_thread_resolution():
    gw, identity, session = _gateway()
    envelope = {"data": {"message": {
        "id": "mail-in-1",
        "thread_id": "thread-1",
        "wire_message_id": "<original@example.com>",
        "from_address": "author@example.com",
        "reply_to": "replies@example.com",
        "to_addresses": ["test-agent@inkboxmail.com", "teammate@example.com"],
        "cc_addresses": ["reviewer@example.com"],
        "subject": "Planning",
        "body": "Please review this with everyone copied.",
        "body_state": "complete",
    }}}

    async def exercise():
        await gw._on_mail_received(envelope)
        _, mode, meta = session.handle_inbound.call_args.args
        await gw.send_to_contact("email:thread-1", "Looks good.", mode, meta)

    asyncio.run(exercise())

    # The reply-all API resolves Reply-To, To/CC, self exclusion, and threading
    # from the stored message, not a reconstructed or sender-only address list.
    identity.reply_all_email.assert_called_once_with("mail-in-1", body_text="Looks good.")
    identity.send_email.assert_not_called()


@pytest.mark.parametrize("message_id", [None, "", "   "])
def test_missing_original_message_never_falls_back_to_sender_only(message_id):
    gw, identity, _ = _gateway()

    with pytest.raises(ValueError, match="original email message ID"):
        asyncio.run(gw.send_to_contact("contact-1", "Reply", "email", {
            "to": "author@example.com", "message_id": message_id,
        }))

    gw._inkbox.get_identity.assert_not_called()
    identity.reply_all_email.assert_not_called()
    identity.send_email.assert_not_called()


def test_reply_all_error_does_not_trigger_another_send():
    gw, identity, _ = _gateway()
    identity.reply_all_email.side_effect = RuntimeError("reply unavailable")

    with pytest.raises(RuntimeError, match="reply unavailable"):
        asyncio.run(gw.send_to_contact("contact-1", "Reply", "email", {
            "to": "author@example.com", "message_id": "mail-in-1",
        }))

    identity.reply_all_email.assert_called_once()
    identity.send_email.assert_not_called()


def test_silent_email_does_not_send():
    gw, identity, _ = _gateway()

    asyncio.run(gw.send_to_contact("contact-1", "[SILENT]", "email", {}))

    identity.reply_all_email.assert_not_called()
    identity.send_email.assert_not_called()


@pytest.mark.parametrize("first_fails", [False, True])
@pytest.mark.parametrize("second_mode", ["email", "sms"])
def test_queued_email_results_keep_their_original_reply_audience(
    tmp_path, monkeypatch, first_fails, second_mode,
):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def exercise():
        sent = []
        session = make_session(sent)

        class Client:
            thread_id = "codex-thread"

            async def run(self, text):
                if first_fails and "First request" in text:
                    raise RuntimeError("first turn failed")
                return "First answer" if "First request" in text else "Second answer"

        session._client = Client()
        # Same contact, different reply targets; both arrive before the worker runs.
        await session.handle_inbound("First request", "email", {
            "sender": "author@example.com", "message_id": "original-1",
        })
        await session.handle_inbound("Second request", second_mode, {
            "sender": "author@example.com", "message_id": "original-2",
        })
        await session._worker

        assert [item[3]["message_id"] for item in sent] == ["original-1", "original-2"]
        assert [item[2] for item in sent] == ["email", second_mode]
        assert sent[1][1] == "Second answer"
        if first_fails:
            assert "hit an error" in sent[0][1]
        else:
            assert sent[0][1] == "First answer"

    asyncio.run(exercise())


@pytest.mark.parametrize("status_code", [201, 403, 404, 422])
def test_reply_all_through_real_sdk_http_boundary(monkeypatch, tmp_path, status_code):
    original_id = "11111111-1111-4111-8111-111111111111"
    mailbox_id = "22222222-2222-4222-8222-222222222222"
    identity_id = "33333333-3333-4333-8333-333333333333"
    timestamp = "2026-01-01T00:00:00Z"
    requests = []

    def respond(request):
        requests.append((request.method, request.url.path, request.content))
        if request.method == "GET":
            assert request.url.path == "/api/v1/identities/test-agent"
            return httpx.Response(200, json={
                "id": identity_id, "organization_id": "test-org",
                "agent_handle": "test-agent", "created_at": timestamp,
                "updated_at": timestamp, "mailbox": {
                    "id": mailbox_id, "email_address": "agent@example.com",
                    "agent_identity_id": identity_id, "created_at": timestamp,
                    "updated_at": timestamp,
                },
            })
        assert request.method == "POST"
        assert request.url.path == f"/api/v1/mail/mailboxes/agent@example.com/messages/{original_id}/reply-all"
        # Recipients/subject/threading must be resolved from the original by the
        # reply-all endpoint, not replaced with webhook guesses by this plugin.
        assert json.loads(request.content) == {"body_text": "Reply for everyone."}
        if status_code != 201:
            return httpx.Response(status_code, json={"detail": "reply unavailable"})
        return httpx.Response(201, json={
            "id": "44444444-4444-4444-8444-444444444444", "mailbox_id": mailbox_id,
            "message_id": "<reply@example.com>", "from_address": "agent@example.com",
            "to_addresses": ["reply-to@example.com"], "cc_addresses": ["reviewer@example.com"],
            "subject": "Re: Planning", "direction": "outbound", "status": "queued",
            "is_read": True, "is_starred": False, "has_attachments": False,
            "created_at": timestamp,
        })

    http_client = httpx.Client

    def offline_client(*args, **kwargs):
        kwargs.update(transport=httpx.MockTransport(respond), trust_env=False)
        return http_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", offline_client)
    monkeypatch.delenv("INKBOX_VAULT_KEY", raising=False)
    monkeypatch.setattr("inkbox._config._CONFIG_PATH", tmp_path / "sdk-config")
    with Inkbox(api_key="test-key", base_url="https://example.com") as sdk:
        gw, _, _ = _gateway()
        gw._inkbox = sdk
        operation = gw.send_to_contact("contact-1", "Reply for everyone.", "email", {
            "message_id": original_id, "to": "wrong-recipient@example.com",
            "subject": "Do not override the stored subject",
        })
        if status_code == 201:
            asyncio.run(operation)
        else:
            with pytest.raises(InkboxAPIError):
                asyncio.run(operation)

    assert [method for method, _, _ in requests] == ["GET", "POST"]


@pytest.mark.parametrize("shared_budget", [False, True])
def test_email_send_recovery_keeps_original_audience(tmp_path, monkeypatch, shared_budget):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def exercise():
        sent = []
        session = make_session(sent)
        original_send = session.send_fn
        rejected = False

        async def send(chat_id, text, mode, meta):
            nonlocal rejected
            if meta["message_id"] == "original-1" and not rejected:
                rejected = True
                raise RuntimeError("send rejected")
            await original_send(chat_id, text, mode, meta)

        class Client:
            thread_id = "codex-thread"

            async def run(self, text):
                return "Answer"

        session.send_fn = send
        session._client = Client()
        if shared_budget:
            def on_failure(chat_id, mode, meta, text, reason):
                assert mode == "email" and meta["message_id"] == "original-1"
                return "Recover the first reply"

            session.on_send_failure = on_failure
        for message_id in ["original-1", "original-2"]:
            await session.handle_inbound("Request", "email", {"message_id": message_id})
        await session._worker

        assert rejected
        assert [item[3]["message_id"] for item in sent] == ["original-2", "original-1"]

    asyncio.run(exercise())


def test_email_approval_and_answer_keep_original_audience(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def exercise():
        sent = []
        session = make_session(sent)
        prompted = asyncio.Event()
        original_send = session.send_fn

        async def send(*args):
            await original_send(*args)
            if args[1] == "Allow?":
                prompted.set()

        class Client:
            thread_id = "codex-thread"

            async def run(self, text):
                if "First request" in text:
                    assert await session._escalate("permission", "Allow?") == "YES"
                return "Answer"

        session.send_fn = send
        session._client = Client()
        await session.handle_inbound("First request", "email", {"message_id": "original-1"})
        await session.handle_inbound("Second request", "email", {"message_id": "original-2"})
        await asyncio.wait_for(prompted.wait(), timeout=1)
        await session.handle_inbound("YES", "email", {"message_id": "approval-message"})
        await session._worker

        assert [item[3]["message_id"] for item in sent] == ["original-1", "original-1", "original-2"]

    asyncio.run(exercise())

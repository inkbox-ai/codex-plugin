"""Automatic email replies retain their original message's audience and thread."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway


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

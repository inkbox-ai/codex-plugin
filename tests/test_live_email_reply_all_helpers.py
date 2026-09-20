"""Ensure the live reply-all assertions cannot accept sender-only delivery."""

from types import SimpleNamespace

import pytest

from tests.live.mock_openai import _reply_text
from tests.live.test_email_reply_all import _assert_reply, _find_message


def _reply(**changes):
    fields = dict(
        from_address="agent@example.com", to_addresses=["sender@example.com"],
        cc_addresses=["copy@example.com"], bcc_addresses=None,
        in_reply_to="<original@example.com>", references=["<original@example.com>"],
        body_text="REPLY_OK smoke-abcdef",
    )
    fields.update(changes)
    return SimpleNamespace(**fields)


def _check(reply):
    _assert_reply(
        reply, sender="agent@example.com", recipient="sender@example.com",
        cc="copy@example.com", original_wire_id="<original@example.com>", nonce="smoke-abcdef",
    )


def test_live_reply_all_accepts_correct_audience_and_wire_thread():
    _check(_reply())


@pytest.mark.parametrize("changes", [
    {"cc_addresses": []},
    {"cc_addresses": ["copy@example.com", "agent@example.com"]},
    {"to_addresses": ["copy@example.com"]},
    {"bcc_addresses": ["hidden@example.com"]},
    {"in_reply_to": None},
    {"references": []},
    {"body_text": "REPLY_OK smoke-123456"},
    {"body_text": "REPLY_OK smoke-abcdef; hit an error"},
])
def test_live_reply_all_rejects_incomplete_or_wrong_replies(changes):
    with pytest.raises(AssertionError):
        _check(_reply(**changes))


def test_live_message_match_requires_sender_and_nonce():
    wanted = SimpleNamespace(id="wanted", from_address="Agent <agent@example.com>", subject="smoke-abcdef")
    messages = [
        SimpleNamespace(id="wrong-sender", from_address="other@example.com", subject="smoke-abcdef"),
        SimpleNamespace(id="old", from_address="agent@example.com", subject="smoke-123456"),
        wanted,
    ]

    def list_messages(mailbox, **kwargs):
        assert mailbox == "copy@example.com"
        assert kwargs == {"direction": "inbound", "start_datetime": "boundary"}
        return iter(messages)

    client = SimpleNamespace(messages=SimpleNamespace(
        list=list_messages, get=lambda mailbox, message_id: message_id,
    ))
    assert _find_message(
        client, "copy@example.com", since="boundary", sender="agent@example.com",
        nonce="smoke-abcdef", direction="inbound",
    ) == "wanted"


def test_mock_model_uses_latest_nonce_in_a_resumed_session():
    result = _reply_text({"input": [
        {"role": "user", "content": "smoke-111111"},
        {"role": "assistant", "content": "REPLY_OK smoke-111111"},
        {"role": "user", "content": "smoke-222222"},
    ]})
    assert "REPLY_OK smoke-222222" in result
    assert "smoke-111111" not in result


def test_email_only_live_checks_do_not_resolve_or_send_sms_resets():
    from tests.live.conftest import _reset_conversation_health

    def unexpected_fixture(name):
        raise AssertionError(f"email-only test must not initialize {name}")

    request = SimpleNamespace(
        node=SimpleNamespace(get_closest_marker=lambda name: name == "no_sms_reset"),
        getfixturevalue=unexpected_fixture,
    )
    fixture = _reset_conversation_health.__wrapped__(request)
    next(fixture)
    with pytest.raises(StopIteration):
        next(fixture)

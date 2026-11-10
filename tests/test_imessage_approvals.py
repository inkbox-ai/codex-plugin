"""Native reply metadata binds permission answers to one displayed request."""
import asyncio
import inspect

import pytest

from inkbox_codex.imessage import IMessageState
from inkbox_codex.sessions import _Turn
from tests.test_approval_controls import REQUEST, wait_until
from tests.test_imessage_gateway import bridge, metadata
from tests.test_imessage_sessions import make_session


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


def setup():
    gateway, identity = bridge()
    gateway.cfg.permission_timeout_s = 5
    send = identity.send_imessage

    def numbered(**kwargs):
        result = send(**kwargs)
        result.id = f"outbound-{len(identity.calls)}"
        return result

    identity.send_imessage = numbered
    # Keep the real signature visible to the runtime capability check.
    numbered.__signature__ = inspect.signature(send)
    session, _, _ = make_session(hook=gateway._imessage_turn_state, send=gateway.send_to_contact)
    session.cfg = gateway.cfg
    session._current_turn = _Turn(text="Request", reply_mode="imessage", reply_meta=metadata("source"))
    return gateway, identity, session


def answer(parent=None, *, source="answer", **changes):
    return {**metadata(source, parent=parent), **changes}


def test_simultaneous_permissions_require_the_current_prompt_and_original_author():
    async def run():
        gateway, identity, session = setup()
        first = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        second = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        try:
            await wait_until(lambda: gateway._threaded_imessage_state().lookup_outbound("outbound-1"))
            assert identity.calls[0]["reply_to_message_id"] == "source"
            assert "Reply directly" in identity.calls[0]["text"]
            assert not session._imessage_permission_matches(answer("outbound-1", sender="different-sender"))
            assert not session._imessage_permission_matches(answer("outbound-1", conversation_id="other"))
            assert not session._imessage_permission_matches(answer("outbound-1", companion_activation_id="other"))
            await session.handle_inbound("4", "imessage", answer())
            assert not first.done() and not second.done()
            await session.handle_inbound("4", "imessage", answer("outbound-1", source="answer-2"))
            assert (await first)["_meta"] == {"persist": "always"}
            await wait_until(lambda: gateway._threaded_imessage_state().lookup_outbound("outbound-3"))
            pending = session.pending
            await session.handle_inbound("4", "imessage", answer("outbound-1", source="answer-3"))
            assert session.pending is pending and not pending.future.done()
            await session.handle_inbound("1", "imessage", answer("outbound-3", source="answer-4"))
            assert (await second)["action"] == "accept"
            assert not session._client.calls
        finally:
            await session.close()
            await asyncio.gather(first, second, return_exceptions=True)
    asyncio.run(run())


def test_restarted_session_rejects_answer_to_a_saved_old_prompt():
    async def run():
        gateway, _, session = setup()
        task = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        await wait_until(lambda: gateway._threaded_imessage_state().lookup_outbound("outbound-1"))
        await session.close()
        await task
        # A new store/session sees persisted correlation, never a pending grant.
        assert IMessageState(gateway.cfg).lookup_outbound("outbound-1")["meta"]["imessage_permission_id"]
        restarted, sent, _ = make_session(send=gateway.send_to_contact)
        restarted.cfg = gateway.cfg
        try:
            await restarted.handle_inbound("4", "imessage", answer("outbound-1"))
            assert "no longer active" in sent[-1][0]
            assert restarted.pending is None and restarted._queue.empty()
            assert restarted._imessage_burst is None and not restarted._client.calls
        finally:
            await restarted.close()
    asyncio.run(run())


def test_failed_permission_prompt_is_not_retried_or_approved():
    async def run():
        gateway, identity, session = setup()
        identity.error = TimeoutError("Unknown send outcome")
        try:
            result = await session._handle_codex_request("mcpServer/elicitation/request", REQUEST)
            assert result["action"] == "cancel"
            assert len(identity.calls) == 1
            assert identity.calls[0]["reply_to_message_id"] == "source"
            assert session.pending is None
            assert gateway._threaded_imessage_state().lookup_outbound("outbound-1") is None
        finally:
            await session.close()
    asyncio.run(run())

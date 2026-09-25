"""Approval scopes, cancellation, and fresh work survive text-channel routing."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from tests.test_companion import drained, fixture, harness, live
from tests.test_companion_response_modes import set_input
from tests.test_sessions import make_session


REQUEST = {
    "serverName": "example",
    "message": 'Allow Example to run tool "lookup"?',
    "mode": "form",
    "requestedSchema": {"type": "object", "properties": {}},
    "_meta": {"codex_approval_kind": "mcp_tool_call", "persist": ["session", "always"]},
}


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


async def wait_until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(wait(), 2)


def enforce_reply_route(receiver, session):
    send = session.send_fn

    async def checked(chat_id, text, mode, meta):
        receiver.check_reply_route(meta)
        await send(chat_id, text, mode, meta)

    session.send_fn = checked


@pytest.mark.parametrize("channel", ["sms", "imessage", "email"])
@pytest.mark.parametrize("answer,persist", [("1", None), ("2", "session"), ("4", "always"), ("Yes, you are allowed to proceed", None)])
def test_numbered_scopes_reach_host_over_each_channel(channel, answer, persist):
    async def scenario():
        sent = []
        session = make_session(sent)
        session.mode = channel
        task = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        try:
            await wait_until(lambda: sent)
            await session.handle_inbound(answer, channel, {})
            result = await task
            assert result == {"action": "accept", "content": None, **({"_meta": {"persist": persist}} if persist else {})}
            assert "2 — Allow for this session" in sent[0][1]
            assert "4 — Always allow across sessions" in sent[0][1]
            assert session._queue.empty()
        finally:
            await session.close()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["sms", "imessage", "email"])
@pytest.mark.parametrize("scopes,answer,action,persist", [
    ([], "2", "decline", None),
    (["always"], "2", "decline", None),
    (["always"], "3", "accept", "always"),
])
def test_renumbered_options_reach_host_over_each_channel(channel, scopes, answer, action, persist):
    async def scenario():
        sent = []
        session = make_session(sent)
        session.mode = channel
        request = deepcopy(REQUEST)
        request["_meta"]["persist"] = scopes
        task = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", request))
        try:
            await wait_until(lambda: sent)
            assert "2 — Deny this request" in sent[0][1]
            await session.handle_inbound(answer, channel, {})
            result = await asyncio.wait_for(task, 2)
            assert result == {"action": action, "content": None, **({"_meta": {"persist": persist}} if persist else {})}
            assert session._queue.empty()
        finally:
            await session.close()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["sms", "imessage", "email"])
@pytest.mark.parametrize("text", ["/stop", "Please cancel my request", "What is the weather today?"])
def test_control_or_new_task_is_not_an_approval_answer(channel, text):
    async def scenario():
        sent, results, turns = [], [], []
        session = make_session(sent)
        client = AsyncMock()
        client.thread_id = "thread-example"
        session._client = client

        async def run(prompt):
            turns.append(prompt)
            if len(turns) == 1:
                results.append(await session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
                return "Old task output"
            return "New task output"

        client.run.side_effect = run
        try:
            await session.handle_inbound("Start a task", channel, {})
            await wait_until(lambda: sent)
            await session.handle_inbound(text, channel, {})
            await asyncio.wait_for(session._worker, 2)
            assert results == [{"action": "cancel", "content": None}]
            client.interrupt.assert_awaited_once()
            assert "Old task output" not in [message[1] for message in sent]
            if "weather" in text:
                assert len(turns) == 2 and text in turns[1]
                assert sent[-1][1] == "New task output"
            else:
                assert len(turns) == 1 and sent[-1][1] == "Stopped."
        finally:
            await session.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["phone", "imessage", "mail"])
@pytest.mark.parametrize("text", ["/stop", "Please cancel my request", "What is the weather today?"])
def test_companion_controls_unblock_receipts_without_losing_new_work(channel, text):
    async def scenario():
        event = set_input(fixture(channel), "direct", "@agent Start a task")
        receiver, _, session, sent = harness(event, reply_mode="mention")
        enforce_reply_route(receiver, session)
        turns, responses = [], []
        session._client.interrupt = AsyncMock()

        async def run(prompt):
            turns.append(prompt)
            if len(turns) == 1:
                responses.append(await session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
                return "Old task output"
            return "New task output"

        session._client.run = run
        try:
            await receiver.accept(event)
            await wait_until(lambda: sent)
            incoming = set_input(live(event), "direct", "@agent " + text)
            await receiver.accept(incoming)
            await drained(receiver)
            assert responses == [{"action": "cancel", "content": None}]
            session._client.interrupt.assert_awaited_once()
            assert [row[0] for row in receiver.inbox.db.execute("SELECT state FROM events")] == ["done", "done"]
            assert "Old task output" not in [message[1] for message in sent]
            if "weather" in text:
                assert len(turns) == 2 and text in turns[1]
                assert sent[-1][1] == "New task output"
            else:
                assert len(turns) == 1 and sent[-1][1] == "Stopped."
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_simultaneous_requests_are_displayed_and_answered_in_order():
    async def scenario():
        sent = []
        session = make_session(sent)
        second_request = {**REQUEST, "message": 'Allow Example to run tool "search"?'}
        first = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        second = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", second_request))
        try:
            await wait_until(lambda: sent)
            assert len(sent) == 1
            await session.handle_inbound("1", "imessage", {})
            assert await first == {"action": "accept", "content": None}
            await wait_until(lambda: len(sent) == 2)
            assert '"search"' in sent[1][1]
            await session.handle_inbound("3", "imessage", {})
            assert await second == {"action": "decline", "content": None}
            assert session.pending is None
        finally:
            await session.close()
            await asyncio.gather(first, second, return_exceptions=True)
    asyncio.run(scenario())


def test_stop_invalidates_queued_dialog_even_after_first_answer_resolves():
    async def scenario():
        sent = []
        session = make_session(sent)
        first = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        second = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
        try:
            await wait_until(lambda: sent)
            await session.handle_inbound("1", "imessage", {})
            await session.handle_inbound("/stop", "imessage", {})
            assert await second == {"action": "cancel", "content": None}
            await first
            assert len([message for message in sent if "Allow Example" in message[1]]) == 1
        finally:
            await session.close()
            await asyncio.gather(first, second, return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["phone", "imessage", "mail"])
@pytest.mark.parametrize("control", ["/stop", "What is the weather today?"])
def test_companion_answer_then_immediate_control_cancels_queued_question(channel, control):
    async def scenario():
        event = set_input(fixture(channel), "direct", "@agent Start a task")
        receiver, _, session, sent = harness(event, reply_mode="mention")
        enforce_reply_route(receiver, session)
        responses, turns = [], []
        session._client.interrupt = AsyncMock()

        async def run(prompt):
            turns.append(prompt)
            if len(turns) == 1:
                responses.extend(await asyncio.gather(
                    session._handle_codex_request("mcpServer/elicitation/request", REQUEST),
                    session._handle_codex_request("mcpServer/elicitation/request", {
                        **REQUEST, "message": 'Allow Example to run tool "second"?',
                    }),
                ))
                return "Old task output"
            return "New task output"

        session._client.run = run
        try:
            await receiver.accept(event)
            await wait_until(lambda: sent)
            await receiver.accept(set_input(live(event, sequence=2), "direct", "@agent 1"))
            await receiver.accept(set_input(live(event, sequence=3), "direct", "@agent " + control))
            await drained(receiver)
            assert responses[1] == {"action": "cancel", "content": None}
            assert not any('"second"' in message[1] for message in sent)
            session._client.interrupt.assert_awaited_once()
            assert all(state == "done" for (state,) in receiver.inbox.db.execute("SELECT state FROM events"))
            if "weather" in control:
                assert len(turns) == 2 and control in turns[1]
            else:
                assert len(turns) == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_stop_settles_a_queued_companion_waiter():
    from inkbox_codex.sessions import _Turn

    async def scenario():
        session = make_session([])
        completed = asyncio.get_running_loop().create_future()
        await session._queue.put(_Turn("queued", completion=completed))
        await session.handle_inbound("/stop", "imessage", {})
        assert completed.done() and completed.result() is None
    asyncio.run(scenario())


def test_unavailable_scope_never_downgrades_to_once():
    async def scenario():
        sent = []
        session = make_session(sent)
        request = deepcopy(REQUEST)
        request["_meta"]["persist"] = ["session"]
        task = asyncio.create_task(session._handle_codex_request("mcpServer/elicitation/request", request))
        try:
            await wait_until(lambda: sent)
            await session.handle_inbound("always", "sms", {})
            assert not task.done() and not session.pending.future.done()
            assert "4 —" not in sent[0][1]
            assert "not one of the supported choices" in sent[-1][1]
            await session.handle_inbound("2", "sms", {})
            assert (await task)["_meta"] == {"persist": "session"}
        finally:
            await session.close()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_trusting_inkbox_never_auto_approves_another_server():
    async def scenario():
        session = make_session([])
        session.cfg.auto_approve_inkbox_tools = True
        session._escalate = AsyncMock(return_value="no")
        result = await session._handle_codex_request("mcpServer/elicitation/request", {
            **REQUEST, "message": 'Allow the inkbox MCP server to run tool "inkbox_lookup"?',
        })
        assert result == {"action": "decline", "content": None}
        session._escalate.assert_awaited_once()
    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["phone", "imessage", "mail"])
def test_companion_clear_removes_durable_thread_before_next_input(channel, monkeypatch):
    from inkbox_codex import sessions
    from tests.test_companion import ReconnectingClient

    resumes = []

    class RecordingClient(ReconnectingClient):
        async def connect(self, resume=None):
            resumes.append(resume)
            self.thread_id = resume or "fresh-thread"
            return self.thread_id

    monkeypatch.setattr(sessions, "CodexAppServerClient", RecordingClient)

    async def scenario():
        event = set_input(fixture(channel), "direct", "@agent First request")
        receiver, _, session, sent = harness(event, reply_mode="mention")
        original_send = session.send_fn

        async def checked_send(chat_id, text, mode, meta):
            receiver.check_reply_route(meta)
            await original_send(chat_id, text, mode, meta)

        session.send_fn = checked_send
        try:
            await receiver.accept(event)
            await drained(receiver)
            old_thread = session._client.thread_id
            assert receiver.inbox.db.execute("SELECT thread_id FROM threads").fetchall() == [(old_thread,)]

            await receiver.accept(set_input(live(event, sequence=2), "direct", "@agent /clear"))
            await drained(receiver)
            assert session._client is None and session.resume_session_id is None
            assert not receiver.inbox.db.execute("SELECT 1 FROM threads").fetchone()
            assert sent[-1][1] == "Started a fresh conversation — previous context cleared."

            await receiver.accept(set_input(live(event, sequence=3), "direct", "@agent Next request"))
            await drained(receiver)
            assert resumes == [None], "The cleared conversation must never be passed to thread/resume"
            assert session._client.thread_id == "fresh-thread" != old_thread
            assert receiver.inbox.db.execute("SELECT thread_id FROM threads").fetchall() == [("fresh-thread",)]
        finally:
            await receiver.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("channel", ["phone", "imessage", "mail"])
@pytest.mark.parametrize("case", ["sponsored", "missing_access", "unmentioned", "other_author"])
def test_companion_stop_requires_the_prompted_author_access_and_mention(channel, case):
    async def scenario():
        event = set_input(fixture(channel), "direct", "@agent Start a task")
        receiver, _, session, sent = harness(event, reply_mode="mention", response_mode="safe")
        enforce_reply_route(receiver, session)
        session._client.interrupt = AsyncMock()
        responses, turns = [], []

        async def run(prompt):
            turns.append(prompt)
            if len(turns) == 1:
                responses.append(await session._handle_codex_request("mcpServer/elicitation/request", REQUEST))
            return "[SILENT]"

        session._client.run = run
        try:
            await receiver.accept(event)
            await wait_until(lambda: sent)
            pending = session.pending
            access = "sponsored" if case == "sponsored" else None if case == "missing_access" else "direct"
            text = "/stop" if case == "unmentioned" else "@agent /stop"
            other = ("other@example.com" if channel == "mail" else "+12025550199") if case == "other_author" else None
            await receiver.accept(set_input(live(event, sequence=2, author=other), access, text))
            await asyncio.sleep(0)
            assert session.pending is pending and not pending.future.done()
            session._client.interrupt.assert_not_awaited()
            assert len(sent) == 1

            await receiver.accept(set_input(live(event, sequence=3), "direct", "@agent /stop"))
            await drained(receiver)
            assert responses == [{"action": "cancel", "content": None}]
            session._client.interrupt.assert_awaited_once()
            assert "Stopped." in [message[1] for message in sent]
            assert all(state == "done" for (state,) in receiver.inbox.db.execute("SELECT state FROM events"))
        finally:
            await receiver.close()

    asyncio.run(scenario())

"""A host that dies between messages is reconnected before submission."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from inkbox_codex import sessions as sessions_mod
from inkbox_codex.codex_client import CodexAppServerClient, CodexAppServerError
from tests.test_companion import drained, fixture, harness, live
from tests.test_sessions import make_session


@pytest.mark.parametrize("failure", ["exited", "reader_stopped"])
def test_cached_dead_client_reconnects_without_poisoning_receipt(monkeypatch, tmp_path, failure):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def scenario():
        event = fixture()
        receiver, _, session, sent = harness(event)
        await receiver.accept(event)
        await drained(receiver)
        sent.clear()
        dead = CodexAppServerClient(session.cfg, developer_instructions="")
        saved_thread = session._client.thread_id
        dead.thread_id = saved_thread
        dead._initialized = True
        dead._proc = SimpleNamespace(returncode=1 if failure == "exited" else None)
        dead._reader_task = asyncio.get_running_loop().create_future()
        if failure == "reader_stopped":
            dead._reader_task.set_result(None)
        dead.disconnect = AsyncMock()
        session._client = dead
        session.resume_session_id = None
        resumes, inputs = [], []

        class Reconnected:
            thread_id = None
            is_alive = True

            def __init__(self, *args, **kwargs):
                pass

            async def connect(self, resume):
                resumes.append(resume)
                if len(resumes) == 1:
                    raise CodexAppServerError("temporary initialize failure")
                self.thread_id = resume
                return resume

            async def disconnect(self):
                pass

            async def run(self, text):
                inputs.append(text)
                return "Recovered response"

        monkeypatch.setattr(sessions_mod, "CodexAppServerClient", Reconnected)
        following = live(event)
        try:
            await receiver.accept(following)
            await drained(receiver)
            state = receiver.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (following["id"],)).fetchone()[0]
            assert state == "pending"
            assert session._client is None
            assert not inputs and not sent
            dead.disconnect.assert_awaited_once()
            scope = following["companion"]["scope_id"]
            receiver.retries.pop(scope).cancel()
            await receiver._drain(scope)
            assert resumes == [saved_thread, saved_thread]
            assert len(inputs) == len(sent) == 1
            assert receiver.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (following["id"],)).fetchone()[0] == "done"
        finally:
            await receiver.close()

    asyncio.run(scenario())


def test_close_during_dead_client_cleanup_cannot_reopen_session(monkeypatch):
    async def scenario():
        cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event()
        disconnects = []

        async def disconnect():
            disconnects.append(True)
            if len(disconnects) == 1:
                cleanup_started.set()
                await release_cleanup.wait()

        session = make_session([])
        session._client = SimpleNamespace(is_alive=False, thread_id="saved-thread", disconnect=disconnect)
        client_factory = AsyncMock(side_effect=AssertionError("closed session must not reconnect"))
        monkeypatch.setattr(sessions_mod, "CodexAppServerClient", client_factory)
        reconnect = asyncio.create_task(session._ensure_client())
        await asyncio.wait_for(cleanup_started.wait(), 1)
        await session.close()
        release_cleanup.set()
        with pytest.raises(CodexAppServerError, match="closed during reconnect"):
            await reconnect
        assert session._client is None
        client_factory.assert_not_called()

    asyncio.run(scenario())

"""Slack turn indicators follow admission, completion, and failure, not every message."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex.slack_activity import SlackActivity
from tests.test_sessions import make_session


def route(event="event-1", message="1234567890.000002"):
    return dict(connection_id="00000000-0000-4000-8000-000000000001",
                conversation_id="C123", message_ts=message, thread_ts="1234567890.000001",
                source_event_id=event, conversation_kind="group", slack_mentioned=True, sender="T123:U123")


def resource():
    sdk = Mock()
    sdk.add_reaction.return_value = sdk.remove_reaction.return_value = NS(status="succeeded")
    return sdk


def changes(sdk):
    return [(call[0], call.args[2], call.args[3]) for call in sdk.mock_calls]


@pytest.mark.parametrize("outcome,terminal", [("completed", "remove_reaction"),
                                              ("failed", "add_reaction"), ("cancelled", "remove_reaction")])
def test_activity_lifecycle_targets_incoming_message_not_thread_root(tmp_path, outcome, terminal):
    async def scenario():
        sdk = resource()
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        await tracker.notify("chat", "slack", route(), "accepted")
        await tracker.flush()
        assert changes(sdk) == [("remove_reaction", "1234567890.000002", "x"),
                                ("add_reaction", "1234567890.000002", "eyes")]
        await tracker.notify("chat", "slack", route(), outcome)
        await tracker.flush()
        assert changes(sdk)[-2:] == [("remove_reaction", "1234567890.000002", "eyes"),
                                    (terminal, "1234567890.000002", "x")]
        assert json.loads(tracker.state_path.read_text()) == {}
        keys = [call.kwargs["idempotency_key"] for call in sdk.mock_calls]
        assert len(set(keys)) == 4 and all(len(key) <= 128 for key in keys)
    asyncio.run(scenario())


def test_overlapping_turns_and_duplicate_admission_do_not_clear_busy_message(tmp_path):
    async def scenario():
        sdk = resource()
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        await tracker.notify("chat", "slack", route(), "accepted")
        await tracker.notify("chat", "slack", route(), "accepted")
        await tracker.notify("chat", "slack", route("event-2"), "accepted")
        await tracker.notify("chat", "slack", route(), "cancelled")
        await tracker.flush()
        assert len(sdk.mock_calls) == 2
        await tracker.notify("chat", "slack", route("event-2"), "completed")
        await tracker.flush()
        assert changes(sdk)[-1] == ("remove_reaction", "1234567890.000002", "x")
    asyncio.run(scenario())


def test_messages_in_same_thread_have_independent_work_and_failure_indicators(tmp_path):
    async def scenario():
        sdk = resource()
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        first = route()
        second = route("event-2", "1234567890.000003")
        await tracker.notify("chat", "slack", first, "accepted")
        await tracker.notify("chat", "slack", second, "accepted")
        await tracker.flush()
        assert {call.args[2] for call in sdk.add_reaction.call_args_list} == {
            first["message_ts"], second["message_ts"],
        }
        sdk.reset_mock()
        await tracker.notify("chat", "slack", first, "failed")
        await tracker.flush()
        assert changes(sdk) == [("remove_reaction", first["message_ts"], "eyes"),
                                ("add_reaction", first["message_ts"], "x")]
        records = json.loads(tracker.state_path.read_text())
        assert [record["message_ts"] for record in records.values()] == [second["message_ts"]]
        sdk.reset_mock()
        await tracker.notify("chat", "slack", second, "completed")
        await tracker.flush()
        assert changes(sdk) == [("remove_reaction", second["message_ts"], "eyes"),
                                ("remove_reaction", second["message_ts"], "x")]
        assert json.loads(tracker.state_path.read_text()) == {}
    asyncio.run(scenario())


def test_process_restart_marks_unfinished_work_failed_and_new_work_clears_failure(tmp_path):
    async def scenario():
        sdk = resource()
        path = tmp_path / "activity.json"
        original = SlackActivity(sdk, path)
        await original.notify("chat", "slack", route(), "accepted")
        await original.flush()
        restarted = SlackActivity(sdk, path)
        await restarted.recover()
        await restarted.flush()
        assert changes(sdk)[-2:] == [("remove_reaction", "1234567890.000002", "eyes"),
                                    ("add_reaction", "1234567890.000002", "x")]
        await restarted.notify("chat", "slack", route("event-2"), "accepted")
        await restarted.flush()
        assert changes(sdk)[-2:] == [("remove_reaction", "1234567890.000002", "x"),
                                    ("add_reaction", "1234567890.000002", "eyes")]
        await restarted.close()
        assert json.loads(path.read_text()) == {}
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["exception", "unknown"])
def test_reaction_failure_is_best_effort_and_retains_cleanup_record(tmp_path, failure):
    async def scenario():
        sdk = resource()
        if failure == "exception":
            sdk.add_reaction.side_effect = RuntimeError("private-token")
        else:
            sdk.add_reaction.return_value = NS(status="unknown")
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        await tracker.notify("chat", "slack", route(), "accepted")
        await tracker.notify("chat", "slack", route(), "failed")
        await tracker.flush()
        assert len(sdk.mock_calls) == 4
        assert len(json.loads(tracker.state_path.read_text())) == 1
    asyncio.run(scenario())


def test_dm_indicator_uses_triggering_message_and_other_channels_are_untouched(tmp_path):
    async def scenario():
        sdk = resource()
        tracker = SlackActivity(sdk, tmp_path / "activity.json")
        meta = {**route(), "thread_ts": None}
        await tracker.notify("chat", "email", meta, "accepted")
        await tracker.notify("chat", "slack", meta, "accepted")
        await tracker.flush()
        assert len(sdk.mock_calls) == 2
        assert all(call.args[2] == meta["message_ts"] for call in sdk.mock_calls)
        await tracker.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [None, "model", "delivery"])
def test_session_reports_acceptance_and_final_outcome_after_reply(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    async def scenario():
        session = make_session([])
        events = []
        async def notify(_chat, _mode, meta, state):
            events.append((meta["source_event_id"], state))
        session.turn_activity_fn = notify
        class Client:
            thread_id = None
            async def run(self, _text):
                assert events == [("event-1", "accepted")]
                if failure == "model":
                    raise RuntimeError("turn crashed")
                return "done"
        async def ensure():
            return Client()
        async def send(*args):
            if failure == "delivery":
                raise RuntimeError("delivery failed")
            assert events[-1][1] == "accepted"
        session._ensure_client = ensure
        session.send_fn = send
        session.on_send_failure = lambda *args: None
        await session.handle_inbound("hello", "slack", route())
        await session._worker
        assert events == [("event-1", "accepted"), ("event-1", "failed" if failure else "completed")]
    asyncio.run(scenario())


def test_mention_context_and_controls_do_not_get_working_reactions(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    async def scenario():
        session = make_session([])
        session.cfg.group_reply_mode = "mention"
        events = []
        async def notify(*args):
            events.append(args)
        async def flush():
            pass
        session._flush_context = flush
        session.turn_activity_fn = notify
        await session.handle_inbound("context", "slack", {**route(), "slack_mentioned": False})
        await session._worker
        await session.handle_inbound("/stop", "slack", route("event-2"))
        assert events == []
    asyncio.run(scenario())


def test_new_message_interruption_keeps_separate_turn_outcomes(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    async def scenario():
        sent, events = [], []
        session = make_session(sent)
        started, interrupted = asyncio.Event(), asyncio.Event()
        class Client:
            thread_id = None
            calls = 0
            async def run(self, text):
                self.calls += 1
                if self.calls == 1:
                    started.set()
                    await interrupted.wait()
                    return "discarded partial reply"
                return "second reply"
        client = Client()
        session._client = client
        async def ensure():
            return client
        async def interrupt():
            interrupted.set()
        async def notify(_chat, _mode, meta, state):
            events.append((meta["source_event_id"], state))
        session._ensure_client = ensure
        session._interrupt_client = interrupt
        session.turn_activity_fn = notify
        await session.handle_inbound("first", "slack", route())
        await started.wait()
        await session.handle_inbound("second", "slack", route("event-2"))
        await session._worker
        assert events == [("event-1", "accepted"), ("event-2", "accepted"),
                          ("event-1", "cancelled"), ("event-2", "completed")]
        assert [item[1] for item in sent] == ["second reply"]
    asyncio.run(scenario())


def test_stop_clears_indicators_for_queued_work(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    async def scenario():
        session = make_session([])
        events = []
        async def notify(_chat, _mode, meta, state):
            events.append((meta["source_event_id"], state))
        session.turn_activity_fn = notify
        session._worker = asyncio.create_task(asyncio.sleep(60))
        await session.handle_inbound("queued", "slack", route())
        await session.handle_inbound("/stop", "slack", route("event-2"))
        assert events == [("event-1", "accepted"), ("event-1", "cancelled")]
        session._worker.cancel()
        await asyncio.gather(session._worker, return_exceptions=True)
    asyncio.run(scenario())


def test_reaction_lifecycle_through_actual_sdk_http_transport(tmp_path, monkeypatch):
    import httpx
    from inkbox import Inkbox
    slack = pytest.importorskip("inkbox.slack")
    if not hasattr(slack.SlackResource, "add_reaction"):
        pytest.skip("SDK predates Slack reactions")
    requests = []
    def handle(request):
        assert request.headers["X-API-Key"] == "test-key"
        assert request.headers["Idempotency-Key"].startswith("codex:activity:")
        prefix = f"/api/v1/slack/connections/{route()['connection_id']}/conversations/C123/messages/1234567890.000002/reactions"
        requests.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
        assert request.url.path in (prefix, prefix + "/eyes", prefix + "/x")
        return httpx.Response(200, json={
            "id": "00000000-0000-4000-8000-000000000002", "connection_id": route()["connection_id"],
            "operation": "reaction_add" if request.method == "POST" else "reaction_remove",
            "status": "succeeded", "conversation_id": "C123", "message_ts": "1234567890.000002",
        })
    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    async def scenario():
        with Inkbox(api_key="test-key", base_url="https://example.com") as client:
            tracker = SlackActivity(client.slack, tmp_path / "activity.json")
            await tracker.notify("chat", "slack", route(), "accepted")
            await tracker.notify("chat", "slack", route(), "failed")
            await tracker.flush()
            assert [method for method, _, _ in requests] == ["DELETE", "POST", "DELETE", "POST"]
            assert [body for method, _, body in requests if method == "POST"] == [{"name": "eyes"}, {"name": "x"}]
    asyncio.run(scenario())

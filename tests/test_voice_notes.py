"""Voice-note admission, completion ordering, fallback and restart contracts."""

import asyncio
from copy import deepcopy
import types
from unittest.mock import AsyncMock, Mock
import time

import pytest

from inkbox_codex.voice_notes import (
    VoiceNotes,
    merge_results,
    unconfirmed_ids,
    message_for,
    pending,
    store_message,
    transcript_note,
)
from inkbox_codex import gateway


def envelope(
    channel="imessage", status="pending", *, event_id="received-event", count=1
):
    message = {
        "id": "message-1",
        "conversation_id": "conversation-1",
        "direction": "inbound",
        "content": "Typed caption",
        "text": "Typed caption",
        "media": [
            {
                "id": f"attachment-{index}",
                "url": "https://example.com/voice.wav",
                "content_type": "audio/wav",
                "transcription": {
                    "status": status,
                    "text": "Move the review to Thursday."
                    if status == "completed"
                    else None,
                },
            }
            for index in range(count)
        ],
    }
    return {
        "id": event_id,
        "event_type": f"{channel}.received",
        "data": {"text_message" if channel == "text" else "message": message},
    }


def completion(original, status="completed"):
    result = deepcopy(original)
    result["id"] = "separate-completion-event"
    result["data"].update(channel=original["event_type"].split(".")[0], message_id=message_for(original)["id"])
    result["event_type"] = "message.transcription_finished"
    for item in message_for(result)["media"]:
        item["transcription"] = {
            "status": status,
            "text": "Move the review to Thursday." if status == "completed" else None,
        }
    return result


def inbox(tmp_path, deliver, *, wait=60, client=None):
    identity = types.SimpleNamespace(
        id="identity-1", phone_number=types.SimpleNamespace(id="number-1")
    )
    return VoiceNotes(
        tmp_path / "voice.sqlite3", identity, client, deliver, wait_seconds=wait
    )


@pytest.mark.parametrize("channel", ["text", "imessage", "whatsapp"])
def test_pending_receipt_is_immediate_and_matching_completion_wakes_once(
    tmp_path, channel
):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = envelope(channel)
        assert state.defer(original)  # No await: the HTTP handler can ACK now.
        assert state.defer(deepcopy(original))
        assert delivered == []
        mismatched = completion(original)
        mismatched["data"]["message_id"] = "different-source"
        state.complete(mismatched)
        assert state.db.execute("SELECT COUNT(*) FROM voice_notes WHERE completion IS NOT NULL").fetchone()[0] == 0
        state.complete(completion(original))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        assert message_for(delivered[0])["content"] == "Typed caption"
        assert (
            message_for(delivered[0])["media"][0]["url"]
            == "https://example.com/voice.wav"
        )
        assert "[Voice note 1 — automatically transcribed]" in transcript_note(
            message_for(delivered[0])["media"]
        )
        state.complete(completion(original))
        assert state.defer(original)
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_completion_before_original_is_cached_without_waking(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = envelope()
        state.complete(completion(original))
        assert not state.tasks and not delivered
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1 and not pending(message_for(delivered[0]))
        await state.close()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["completed", "pending", "failed", "error"])
def test_deadline_polls_once_then_delivers_original_attachment_path(tmp_path, outcome):
    async def run():
        delivered, reads = [], []
        original = envelope()

        def get(path, **kwargs):
            reads.append((path, kwargs))
            if outcome == "error":
                raise OSError("read unavailable")
            return message_for(envelope(status=outcome))

        async def deliver(item):
            delivered.append(item)

        state = inbox(
            tmp_path,
            deliver,
            wait=0,
            client=types.SimpleNamespace(_imessage_http=types.SimpleNamespace(get=get)),
        )
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert len(reads) == 1 and reads[0][1]["timeout"] == 10
        assert reads[0][1]["params"] == {"agent_identity_id": "identity-1"}
        assert len(delivered) == 1
        body = message_for(delivered[0])
        assert (
            body["content"] == "Typed caption"
            and body["media"][0]["url"] == message_for(original)["media"][0]["url"]
        )
        assert (
            "[Voice note 1 — automatically transcribed]" in transcript_note(body["media"])
            if outcome == "completed"
            else "transcript unavailable" in transcript_note(body["media"])
        )
        state.complete(completion(original))
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_shutdown_retains_original_and_does_not_restart_sixty_second_deadline(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        first = inbox(tmp_path, deliver)
        first.defer(envelope())
        deadline = first.db.execute("SELECT deadline FROM voice_notes").fetchone()[0]
        await first.close()
        second = inbox(tmp_path, deliver)
        assert (
            second.db.execute("SELECT deadline FROM voice_notes").fetchone()[0]
            == deadline
        )
        second.complete(completion(envelope()))
        second.recover()
        await next(iter(second.tasks.values()))
        assert len(delivered) == 1
        await second.close()

    asyncio.run(run())


def test_multiple_attachments_and_result_merge_preserve_original_routing():
    original = message_for(envelope(count=2))
    current = deepcopy(original)
    current["content"] = "Replacement text must not be used"
    current["media"].reverse()
    current["media"][0]["transcription"] = {
        "status": "completed",
        "text": "Second recording",
    }
    result = merge_results(original, current)
    assert pending(result)
    assert result["media"][1]["transcription"]["text"] == "Second recording"
    assert result["content"] == "Typed caption"
    current["conversation_id"] = "another-conversation"
    assert merge_results(original, current) == original


def test_terminal_and_legacy_messages_do_not_wait_and_silence_is_not_failure(tmp_path):
    async def run():
        async def deliver(item):
            raise AssertionError("No deferred turn")

        state = inbox(tmp_path, deliver)
        assert not state.defer(envelope(status="completed"))
        original = envelope()
        message_for(original)["media"][0].pop("transcription")
        assert not state.defer(original)
        assert not state.defer({"event_type": "message.received", "data": {}})
        assert "no speech detected" in transcript_note(
            [{"transcription": {"status": "completed", "text": ""}}]
        )
        await state.close()

    asyncio.run(run())


def test_subscription_catalogs_register_completion_events():
    from inkbox_codex.whatsapp import EVENTS

    assert gateway.TRANSCRIPTION_EVENTS == ["message.transcription_finished"]
    assert not set(gateway.TRANSCRIPTION_EVENTS).intersection(gateway.TEXT_EVENTS + gateway.IMESSAGE_EVENTS + list(EVENTS))


@pytest.mark.parametrize("channel", ["text", "imessage", "whatsapp"])
def test_configured_hosted_runtime_does_not_wait_again(tmp_path, monkeypatch, channel):
    monkeypatch.setenv("INKBOX_HOSTED_MODE", "1")

    async def run():
        async def deliver(item):
            raise AssertionError("The normal consumer owns this turn")

        state = inbox(tmp_path, deliver)
        original = envelope(channel)
        item = message_for(original)
        try:
            assert not state.defer(original)
            assert await asyncio.wait_for(state.resolve_message(channel, item), 0.1) is item
            assert not state.tasks
            assert "transcript unavailable" in transcript_note(item["media"])
            state.complete(completion(original))
            state.recover()
            assert not state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_payload_cannot_claim_hosted_transcription_ownership(tmp_path, monkeypatch):
    monkeypatch.delenv("INKBOX_HOSTED_MODE", raising=False)

    async def run():
        async def deliver(item):
            raise AssertionError("Pending original should still wait")

        state = inbox(tmp_path, deliver)
        original = envelope()
        original["hosted_delivery"] = {"version": 1}
        try:
            assert state.defer(original)
            assert state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_payload_cannot_disable_the_standalone_wait(tmp_path, monkeypatch):
    monkeypatch.delenv("INKBOX_HOSTED_MODE", raising=False)

    async def run():
        async def deliver(item):
            raise AssertionError("The pending message must wait")

        state = inbox(tmp_path, deliver)
        original = envelope()
        original["hosted_delivery"] = {"starts_agent": True}
        try:
            assert state.defer(original)
            assert state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_non_audio_metadata_is_not_labelled_as_a_voice_note():
    media = [{"transcription": {"status": "skipped", "error_code": "not_audio"}}]
    assert transcript_note(media) == ""
    media[0]["transcription"]["error_code"] = "unsupported_audio"
    assert "transcript unavailable" in transcript_note(media)


def test_startup_prunes_old_resolved_companion_rows_but_keeps_unresolved_work(tmp_path):
    async def run():
        async def deliver(_item):
            raise AssertionError("Cleanup must not dispatch")

        state = inbox(tmp_path, deliver)
        old = int(time.time()) - 8 * 86400
        for key, direct, result, created in [
            ("old-resolved", 0, "{}", old),
            ("old-unresolved", 0, None, old),
            ("old-direct", 1, "{}", old),
            ("recent-resolved", 0, "{}", int(time.time())),
        ]:
            state.db.execute("INSERT INTO voice_notes(key,original,direct,result,created) VALUES(?,?,?,?,?)",
                             (key, "{}", direct, result, created))
        state.db.commit()
        await state.close()
        reopened = inbox(tmp_path, deliver)
        try:
            assert {row[0] for row in reopened.db.execute("SELECT key FROM voice_notes")} == {
                "old-unresolved", "old-direct", "recent-resolved",
            }
        finally:
            await reopened.close()

    asyncio.run(run())


def test_ambiguous_handler_failure_is_not_replayed_after_restart(tmp_path):
    async def run():
        admitted = []

        async def deliver(item):
            admitted.append(item)
            raise OSError("connection lost after handoff")

        state = inbox(tmp_path, deliver)
        original = envelope()
        state.complete(completion(original))
        state.defer(original)
        await next(iter(state.tasks.values()))
        await state.close()
        assert unconfirmed_ids(tmp_path / "voice.sqlite3") == ["imessage:message-1"]
        recovered = inbox(tmp_path, deliver)
        recovered.recover()
        refreshed = deepcopy(original)
        message_for(refreshed)["media"] = []
        assert recovered.defer(refreshed) and not recovered.tasks
        assert len(admitted) == 1
        await recovered.close()
    asyncio.run(run())


def test_gateway_retries_saved_message_after_sessions_start(tmp_path, monkeypatch):
    async def run():
        bridge = object.__new__(gateway.InkboxGateway)
        bridge._identity = types.SimpleNamespace(id="agent-1")
        bridge._inkbox = None
        bridge.sessions = None
        monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
        state = bridge._voice_note_inbox()
        original = envelope("text")
        message_for(original)["sender_phone_number"] = "+15555550123"
        state.complete(completion(original))
        state.defer(original)
        await asyncio.sleep(0)
        result, retry_at = state.db.execute("SELECT result,retry_at FROM voice_notes").fetchone()
        assert result and retry_at > time.time()
        await state.close()
        bridge._voice_note_state = None
        native = AsyncMock()
        bridge.sessions = Mock(get=Mock(return_value=native))
        bridge._recent_request_ids = {}
        bridge._inflight_request_ids = {}
        bridge.cfg = types.SimpleNamespace(contact_memories_enabled=False)
        bridge._sender_allowed = lambda _: True
        bridge._with_media = AsyncMock(return_value="caption and transcript")
        bridge._lookup_text_conversation_summary = AsyncMock(return_value=None)
        bridge._resolve_contact_full = AsyncMock(return_value=None)
        bridge._chat_key = Mock(return_value="sms:conversation-1")
        bridge._clear_outbound_failures = Mock()
        recovered = bridge._voice_note_inbox()
        recovered.db.execute("UPDATE voice_notes SET retry_at=0")
        recovered.db.commit()
        recovered.recover()
        await next(iter(recovered.tasks.values()))
        native.handle_inbound.assert_awaited_once()
        body, mode, meta = native.handle_inbound.await_args.args
        assert mode == "sms" and meta["conversation_id"] == "conversation-1"
        assert bridge._with_media.await_args.args[1] == message_for(completion(original))["media"]
        assert recovered.db.execute("SELECT done FROM voice_notes").fetchone() == (1,)
        await recovered.close()
    asyncio.run(run())


SLACK_MESSAGE = "00000000-0000-4000-8000-0000000000aa"
SLACK_CONNECTION = "00000000-0000-4000-8000-000000000002"


def slack_envelope(status="pending", *, event_id="evt_slack", count=1):
    from tests.test_slack import event

    result = event(message_id=SLACK_MESSAGE, event={"type": "message", "text": "<@U_AGENT> typed caption", "files": [
        {
            "id": f"F_AUDIO_{index}",
            "name": "memo.m4a",
            "mimetype": "audio/mp4",
            "transcription": {
                "status": status,
                "text": "Move the review to Thursday." if status == "completed" else None,
                "error_code": "transcription_failed" if status == "failed" else None,
            },
        }
        for index in range(count)
    ]})
    result["id"] = event_id
    return result


def slack_completion(original, status="completed"):
    result = deepcopy(original)
    result["id"] = "separate-slack-completion"
    result["event_type"] = "message.transcription_finished"
    result["data"]["channel"] = "slack"
    for item in result["data"]["event"]["files"]:
        item["transcription"] = {
            "status": status,
            "text": "Move the review to Thursday." if status == "completed" else None,
            "error_code": "transcription_failed" if status == "failed" else None,
        }
    return result


def test_slack_message_is_keyed_by_retained_message_id_and_written_back():
    original = slack_envelope()
    message = message_for(original)
    assert message["id"] == SLACK_MESSAGE and message["connection_id"] == SLACK_CONNECTION
    assert pending(message)
    assert message_for(slack_completion(original))["id"] == SLACK_MESSAGE
    assert VoiceNotes.key("slack", message) == f"slack:{SLACK_MESSAGE}"
    resolved = merge_results(message, message_for(slack_completion(original)))
    store_message(original, resolved)
    assert original["data"]["event"]["text"] == "<@U_AGENT> typed caption"
    assert original["data"]["event"]["files"][0]["transcription"]["status"] == "completed"
    legacy = slack_envelope()
    legacy["data"].pop("message_id")
    assert message_for(legacy) == {}


def test_slack_pending_receipt_waits_for_completion_and_wakes_once(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = slack_envelope()
        assert state.defer(original)
        assert state.defer(deepcopy(original))
        assert delivered == []
        mismatched = slack_completion(original)
        mismatched["data"]["message_id"] = "00000000-0000-4000-8000-0000000000bb"
        state.complete(mismatched)
        assert state.db.execute(
            "SELECT COUNT(*) FROM voice_notes WHERE key=? AND completion IS NOT NULL", (f"slack:{SLACK_MESSAGE}",)
        ).fetchone()[0] == 0
        other_connection = slack_completion(original)
        other_connection["data"]["connection_id"] = "00000000-0000-4000-8000-0000000000cc"
        state.complete(other_connection)
        await asyncio.sleep(0)
        assert delivered == []
        state.complete(slack_completion(original))
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1
        assert delivered[0]["event_type"] == "slack.mention_received"
        assert delivered[0]["data"]["event"]["text"] == "<@U_AGENT> typed caption"
        assert "[Voice note 1 — automatically transcribed]\nMove the review to Thursday." in transcript_note(
            delivered[0]["data"]["event"]["files"]
        )
        state.complete(slack_completion(original))
        assert state.defer(original)
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_slack_completion_before_original_is_cached_without_waking(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = slack_envelope()
        state.complete(slack_completion(original))
        assert not state.tasks and not delivered
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert len(delivered) == 1 and not pending(message_for(delivered[0]))
        assert delivered[0]["data"]["event"]["text"] == "<@U_AGENT> typed caption"
        await state.close()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["completed", "pending", "failed", "error", "missing"])
def test_slack_deadline_polls_archive_once_then_delivers_original(tmp_path, outcome):
    async def run():
        delivered, reads = [], []
        original = slack_envelope()

        def get(path, **kwargs):
            reads.append((path, kwargs))
            if outcome == "error":
                raise OSError("read unavailable")
            if outcome == "missing":
                return {"messages": []}
            current = message_for(slack_envelope(outcome))
            return {"messages": [{
                "id": SLACK_MESSAGE, "connection_id": SLACK_CONNECTION, "conversation_id": "C_TEST",
                "text": "Replacement text must not be used", "files": current["media"],
            }]}

        async def deliver(item):
            delivered.append(item)

        state = inbox(
            tmp_path, deliver, wait=0,
            client=types.SimpleNamespace(_api_http=types.SimpleNamespace(get=get)),
        )
        state.defer(original)
        await next(iter(state.tasks.values()))
        assert reads == [(
            f"/slack/connections/{SLACK_CONNECTION}/archive/messages",
            {"params": {"message_id": SLACK_MESSAGE}, "timeout": 10},
        )]
        assert len(delivered) == 1
        event = delivered[0]["data"]["event"]
        assert event["text"] == "<@U_AGENT> typed caption"
        assert (
            "[Voice note 1 — automatically transcribed]" in transcript_note(event["files"])
            if outcome == "completed"
            else "transcript unavailable" in transcript_note(event["files"])
        )
        state.complete(slack_completion(original))
        assert len(delivered) == 1
        await state.close()

    asyncio.run(run())


def test_slack_shutdown_retains_original_and_does_not_restart_sixty_second_deadline(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        first = inbox(tmp_path, deliver)
        first.defer(slack_envelope())
        deadline = first.db.execute("SELECT deadline FROM voice_notes").fetchone()[0]
        await first.close()
        second = inbox(tmp_path, deliver)
        assert second.db.execute("SELECT deadline FROM voice_notes").fetchone()[0] == deadline
        second.complete(slack_completion(slack_envelope()))
        second.recover()
        await next(iter(second.tasks.values()))
        assert len(delivered) == 1
        assert delivered[0]["event_type"] == "slack.mention_received"
        assert delivered[0]["data"]["event"]["files"][0]["transcription"]["status"] == "completed"
        await second.close()

    asyncio.run(run())


def test_slack_failed_completion_delivers_original_attachment_notice(tmp_path):
    async def run():
        delivered = []

        async def deliver(item):
            delivered.append(item)

        state = inbox(tmp_path, deliver)
        original = slack_envelope()
        state.defer(original)
        state.complete(slack_completion(original, "failed"))
        await next(iter(state.tasks.values()))
        note = transcript_note(delivered[0]["data"]["event"]["files"])
        assert "[Voice note 1: transcript unavailable; process the original attachment normally" in note
        await state.close()

    asyncio.run(run())


def test_slack_hosted_runtime_does_not_wait_again(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_HOSTED_MODE", "1")

    async def run():
        async def deliver(item):
            raise AssertionError("The normal consumer owns this turn")

        state = inbox(tmp_path, deliver)
        try:
            original = slack_envelope()
            assert not state.defer(original)
            item = message_for(original)
            assert await asyncio.wait_for(state.resolve_message("slack", item), 0.1) is item
            assert not state.tasks
        finally:
            await state.close()

    asyncio.run(run())


def test_slack_label_matches_other_channels():
    from inkbox_codex.slack import inbound_message
    from tests.test_slack import IDENTITY

    _, body, meta = inbound_message(slack_envelope("completed", count=2), IDENTITY)
    assert body.startswith("<@U_AGENT> typed caption\n\n[Voice note 1 — automatically transcribed]\n"
                           "Move the review to Thursday.\n\n[Voice note 2 — automatically transcribed]")
    assert meta["raw_text"] == "<@U_AGENT> typed caption"
    assert "Move the review" not in body.split("Attachment references", 1)[1]
    _, plain, _ = inbound_message(slack_envelope("completed") | {"data": {
        **slack_envelope()["data"], "event": {"type": "message", "text": "hello", "files": [{"id": "F_DOC"}]}}}, IDENTITY)
    assert "Voice note" not in plain


def test_gateway_defers_pending_slack_voice_note_and_wakes_once(tmp_path, monkeypatch):
    from tests.test_slack import IDENTITY, Sessions
    from inkbox_codex.config import BridgeConfig

    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    downloads = []

    async def with_attachments(_client, text, meta):
        downloads.append(meta["slack_files"])
        return text

    monkeypatch.setattr("inkbox_codex.slack_media.with_attachments", with_attachments)

    async def run():
        bridge = gateway.InkboxGateway(BridgeConfig(slack_enabled=True))
        bridge._identity = types.SimpleNamespace(id=IDENTITY)
        bridge.sessions = Sessions()
        original = slack_envelope()

        def route(item):
            return bridge._route_inkbox_event(item["event_type"], item)

        try:
            first = await route(original)
            assert b'"pending_transcription": true' in first.body
            again = await route(deepcopy(original))
            assert b'"pending_transcription": true' in again.body
            assert not bridge.sessions.turns and not downloads
            bridge._voice_note_receipt("message.transcription_finished", slack_completion(original))
            await next(iter(bridge._voice_note_state.tasks.values()))
            assert len(bridge.sessions.turns) == 1
            _, body, mode, meta = bridge.sessions.turns[0]
            assert mode == "slack" and meta["raw_text"] == "<@U_AGENT> typed caption"
            assert "[Voice note 1 — automatically transcribed]\nMove the review to Thursday." in body
            assert len(downloads) == 1
            bridge._voice_note_receipt("message.transcription_finished", slack_completion(original))
            await route(deepcopy(original))
            await asyncio.sleep(0)
            assert len(bridge.sessions.turns) == 1
            transcribed = slack_envelope("completed", event_id="evt_ready")
            transcribed["data"]["message_id"] = "00000000-0000-4000-8000-0000000000dd"
            await route(transcribed)
            assert len(bridge.sessions.turns) == 2
        finally:
            if getattr(bridge, "_voice_note_state", None) is not None:
                await bridge._voice_note_state.close()

    asyncio.run(run())


def test_disabled_slack_does_not_hold_pending_voice_note(tmp_path, monkeypatch):
    from tests.test_slack import IDENTITY, Sessions
    from inkbox_codex.config import BridgeConfig

    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))

    async def run():
        bridge = gateway.InkboxGateway(BridgeConfig(slack_enabled=False))
        bridge._identity = types.SimpleNamespace(id=IDENTITY)
        bridge.sessions = Sessions()
        original = slack_envelope()
        response = await bridge._route_inkbox_event(original["event_type"], original)
        assert b'"ignored": "slack-disabled"' in response.body
        assert getattr(bridge, "_voice_note_state", None) is None
        assert not bridge.sessions.turns

    asyncio.run(run())


def test_slack_subscription_selects_completions_and_tolerates_older_servers():
    from inkbox_codex.slack import SLACK_SUBSCRIPTION_EVENTS, reconcile_subscription
    from inkbox_codex.voice_notes import FINISHED

    assert FINISHED == "message.transcription_finished" and FINISHED not in SLACK_SUBSCRIPTION_EVENTS
    client = Mock()
    rejected = Exception("unsupported")
    rejected.status_code = 422
    rejected.detail = [{"type": "enum", "loc": ["body", "event_types", 6],
                        "input": "message.transcription_finished"}]
    client.webhooks.subscriptions.list.return_value = []
    client.webhooks.subscriptions.create.side_effect = [rejected, None]
    reconcile_subscription(client, "identity-1", "https://agent.example/webhook?channel=slack")
    attempts = [call.kwargs["event_types"] for call in client.webhooks.subscriptions.create.call_args_list]
    assert attempts == [[*SLACK_SUBSCRIPTION_EVENTS, FINISHED], list(SLACK_SUBSCRIPTION_EVENTS)]
    client.webhooks.subscriptions.create.side_effect = Exception("server error")
    with pytest.raises(Exception, match="server error"):
        reconcile_subscription(client, "identity-1", "https://agent.example/webhook?channel=slack")


class _UniqueSubscriptions:
    """One active subscription per URL may select a given event, as the API enforces."""

    def __init__(self, rows):
        self.rows = {row.id: row for row in rows}
        self.writes = []

    def _check(self, sub_id, url, events):
        for row in self.rows.values():
            if row.id != sub_id and row.url == url and row.status == "active" and set(row.event_types) & set(events):
                error = Exception("conflict")
                error.status_code = 409
                raise error

    def list(self, **kwargs):
        return [types.SimpleNamespace(**vars(row)) for row in self.rows.values() if row.url == kwargs["url"]]

    def create(self, *, agent_identity_id, url, event_types):
        self._check(None, url, event_types)
        sub_id = f"created-{len(self.rows)}"
        self.rows[sub_id] = types.SimpleNamespace(id=sub_id, url=url, status="active", event_types=list(event_types))
        self.writes.append(("create", sub_id))

    def update(self, sub_id, *, event_types, scope):
        assert scope == "identity"
        self._check(sub_id, self.rows[sub_id].url, event_types)
        self.rows[sub_id].event_types = list(event_types)
        self.writes.append(("update", sub_id))

    def delete(self, sub_id):
        del self.rows[sub_id]
        self.writes.append(("delete", sub_id))


@pytest.mark.parametrize("case", ["fresh", "standalone_completion", "split_message_rows"])
def test_slack_completion_moves_onto_one_message_subscription(case):
    from inkbox_codex.slack import SLACK_INCOMING_EVENTS, SLACK_SUBSCRIPTION_EVENTS, reconcile_subscription

    url = "https://agent.example/webhook?channel=slack"
    finished = "message.transcription_finished"

    def row(sub_id, events):
        return types.SimpleNamespace(id=sub_id, url=url, status="active", event_types=list(events))

    rows = {
        "fresh": [],
        # A completion-only receiver never gets Slack completions; the message row must hold it.
        "standalone_completion": [
            row("messages", SLACK_SUBSCRIPTION_EVENTS),
            row("completion", [finished]),
        ],
        "split_message_rows": [
            row("dm", ["slack.dm_received", finished]),
            row("messages", [event for event in SLACK_SUBSCRIPTION_EVENTS if event != "slack.dm_received"]),
        ],
    }[case]
    subscriptions = _UniqueSubscriptions(rows)
    client = types.SimpleNamespace(slack=object(), webhooks=types.SimpleNamespace(subscriptions=subscriptions))
    reconcile_subscription(client, "identity-1", url)
    holders = [sub for sub in subscriptions.rows.values() if finished in sub.event_types]
    assert len(holders) == 1 and set(holders[0].event_types) & set(SLACK_INCOMING_EVENTS)
    assert {event for sub in subscriptions.rows.values() for event in sub.event_types} == {*SLACK_SUBSCRIPTION_EVENTS, finished}
    if case == "standalone_completion":
        assert subscriptions.writes == [("delete", "completion"), ("update", "messages")]
    if case == "split_message_rows":
        assert subscriptions.writes == []
    writes = list(subscriptions.writes)
    reconcile_subscription(client, "identity-1", url)  # Restart changes nothing.
    assert subscriptions.writes == writes

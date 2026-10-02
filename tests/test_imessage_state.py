"""Receipt recovery and cross-process reply scoping use real local storage."""

import json
import sqlite3
from types import SimpleNamespace

import pytest

from inkbox_codex.config import BridgeConfig
from inkbox_codex.imessage import (
    IMessageState,
    auto_reply_kwargs,
    clear_identity_turn_contexts,
    clear_turn_context,
    imessage_turn_context_path,
    read_turn_context,
    record_accepted_tool_send,
    record_tool_send,
    source_metadata,
    validate_tool_target,
    write_turn_context,
)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.delenv("INKBOX_CODEX_CHAT_ID", raising=False)


def metadata(message_id="message-1", **kwargs):
    return {"conversation_id": "conversation-1", **source_metadata({
        "id": message_id, "content": "A synthetic question", **kwargs,
    }, event_id=f"event-{message_id}")}


def test_durable_duplicate_receipts_preserve_first_admitted_route():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    meta = metadata()
    assert state.admit("chat-1", "original text", meta)
    restarted = IMessageState(cfg)
    assert not restarted.admit("chat-2", "changed text", meta)
    assert restarted.replay_pending() == [{"chat_id": "chat-1", "text": "original text", "meta": meta}]


def test_overlap_anchors_survive_restart_without_combining_unsubmitted_fragments():
    state = IMessageState(BridgeConfig(identity="test-agent"))
    first, second = metadata("first"), metadata("second")
    state.admit("chat-1", "first task", first)
    state.admit("chat-1", "second task", second)
    batch = {**first, "imessage_event_ids": first["imessage_event_ids"] + second["imessage_event_ids"]}
    state.anchor_overlapping(batch, "other-chat")
    assert all(not row["meta"].get("imessage_reply_target") for row in state.replay_pending())
    state.anchor_overlapping(batch, "chat-1")
    replay = state.replay_pending()
    assert [row["text"] for row in replay] == ["first task", "second task"]
    assert [row["meta"]["imessage_reply_target"] for row in replay] == ["first", "second"]
    assert [row["meta"]["imessage_sources"][0]["id"] for row in replay] == ["first", "second"]


def test_overlap_cannot_retarget_checkpointed_answer():
    state = IMessageState(BridgeConfig(identity="test-agent"))
    meta = metadata()
    state.admit("chat-1", "task", meta)
    state.mark(meta, "reply_pending", reply="answer")
    state.anchor_overlapping(meta, "chat-1")
    assert not state.pending_replies()[0]["meta"].get("imessage_reply_target")


def test_restart_replays_only_unstarted_work_and_never_uncertain_sends():
    state = IMessageState(BridgeConfig(identity="test-agent"))
    for index, phase in enumerate(("pending", "running", "sending", "done", "cancelled", "failed")):
        meta = metadata(f"message-{index}")
        state.admit("chat-1", phase, meta)
        state.mark(meta, phase)
    assert [row["text"] for row in state.replay_pending()] == ["pending"]
    assert state.summary()["uncertain"] == 2
    assert state.summary()["unfinished"] == 4
    assert state.pending_replies() == []
    state.mark(metadata("message-3"), "running")
    state.mark(metadata("message-4"), "reply_pending", reply="must not revive")
    assert state.summary()["done"] == 1
    assert state.summary()["cancelled"] == 1
    assert state.pending_replies() == []


def test_completed_burst_recovers_one_saved_reply_without_model_work():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    first, second = metadata("message-1"), metadata("message-2")
    state.admit("chat-1", "first", first)
    state.admit("chat-1", "second", second)
    batch = {
        **first,
        "imessage_event_ids": first["imessage_event_ids"] + second["imessage_event_ids"],
        "imessage_sources": first["imessage_sources"] + second["imessage_sources"],
    }
    state.mark(batch, "running", chat_id="chat-1")
    state.mark(batch, "reply_pending", reply="One completed answer", chat_id="chat-1")
    restarted = IMessageState(cfg)
    assert restarted.replay_pending() == []
    replies = restarted.pending_replies()
    assert len(replies) == 1
    assert replies[0]["reply"] == "One completed answer"
    assert replies[0]["meta"] == batch
    restarted.mark(replies[0]["meta"], "sending")
    restarted.mark(replies[0]["meta"], "done")
    assert restarted.pending_replies() == []
    assert restarted.summary()["done"] == 2


def test_read_preflight_failure_retains_answer_but_send_uncertainty_cannot_replay():
    cfg = BridgeConfig(identity="test-agent")
    state = IMessageState(cfg)
    meta = metadata()
    state.admit("chat-1", "question", meta)
    state.mark(meta, "running")
    state.mark(meta, "reply_pending", reply="Completed answer")
    state.mark(meta, "uncertain")
    restarted = IMessageState(cfg)
    assert restarted.replay_pending() == []
    assert restarted.pending_replies()[0]["reply"] == "Completed answer"
    restarted.mark(meta, "sending")
    restarted.mark(meta, "uncertain")
    assert restarted.pending_replies() == []
    assert restarted.summary()["uncertain"] == 1


def test_identity_and_environment_have_isolated_receipts(tmp_path):
    configurations = [
        BridgeConfig(identity="one", base_url="https://example.test/api"),
        BridgeConfig(identity="two", base_url="https://example.test/api"),
        BridgeConfig(identity="one", base_url="https://other.example.test/api"),
    ]
    states = [IMessageState(cfg) for cfg in configurations]
    assert len({state.path for state in states}) == 3
    assert all(state.admit("chat-1", "same event", metadata()) for state in states)
    for state in states:
        assert state.path.stat().st_mode & 0o777 == 0o600
        assert state.path.parent.stat().st_mode & 0o777 == 0o700
        assert state.path.is_relative_to(tmp_path)


def test_wrong_chat_cannot_update_receipt():
    state = IMessageState(BridgeConfig(identity="one"))
    state.admit("chat-1", "question", metadata())
    state.mark(metadata(), "done", chat_id="chat-2")
    assert state.summary()["pending"] == 1


def test_outbound_ids_and_native_nulls_are_not_inferred_or_rerouted():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    meta = metadata(reply_to_message_id="earlier-message")
    accepted = SimpleNamespace(id="sent-1", status="pending", reply_to_message_id=None,
                               thread_id=None, thread_root_message_id=None)
    state.record_outbound(accepted, meta, "chat-1")
    state.record_outbound({"id": "sent-1", "status": "delivered"}, metadata("other"), "chat-2")
    route = IMessageState(cfg).lookup_outbound("sent-1")
    assert route == {
        "chat_id": "chat-1", "meta": meta, "message_id": "sent-1", "status": "pending",
        "reply_to_message_id": None, "thread_id": None, "thread_root_message_id": None,
    }
    assert state.lookup_outbound("unknown") is None


def test_late_failure_keeps_original_route_and_completed_model_receipt():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    meta = metadata()
    state.admit("chat-1", "question", meta)
    state.mark(meta, "done")
    state.record_outbound({"id": "sent-1", "status": "pending"}, meta, "chat-1")
    state.mark_delivery_failed("sent-1")
    restarted = IMessageState(cfg)
    assert restarted.lookup_outbound("sent-1")["meta"] == meta
    assert restarted.lookup_outbound("sent-1")["status"] == "failed"
    assert restarted.summary()["done"] == 1
    assert restarted.summary()["outbound_failed_count"] == 1
    assert restarted.summary()["unfinished"] == 0


def test_callback_before_send_response_keeps_failure_and_later_fills_real_route():
    cfg = BridgeConfig(identity="one")
    state = IMessageState(cfg)
    state.mark_delivery_failed("sent-1")
    assert state.lookup_outbound("sent-1") == {
        "chat_id": None, "meta": {}, "message_id": "sent-1", "status": "failed", "unknown_route": True,
    }
    meta = metadata(reply_to_message_id="earlier-message")
    state.record_outbound({"id": "sent-1", "status": "pending", "reply_to_message_id": "message-1"}, meta, "chat-1")
    restarted = IMessageState(cfg)
    route = restarted.lookup_outbound("sent-1")
    assert route["status"] == "failed"
    assert route["chat_id"] == "chat-1"
    assert route["meta"] == meta
    assert route["reply_to_message_id"] == "message-1"
    assert "unknown_route" not in route
    assert restarted.summary()["outbound_failed_count"] == 1
    restarted.record_outbound({"id": "sent-1", "status": "delivered"}, metadata("other"), "wrong-chat")
    assert restarted.lookup_outbound("sent-1") == route


def test_only_explicit_replies_select_an_automatic_target():
    ordinary = source_metadata({"id": "message-1", "thread_id": "standalone-thread"})
    assert ordinary["imessage_event_id"] == "message-1"
    assert ordinary["imessage_sources"][0]["thread_root_message_id"] is None
    assert auto_reply_kwargs(ordinary) == {}
    reply = source_metadata({"id": "message-2", "reply_to_message_id": "message-1"})
    assert reply["reply_to_message_id"] == "message-1"
    assert ordinary["thread_id"] == "standalone-thread"
    assert auto_reply_kwargs(reply) == {"reply_to_message_id": "message-2", "plain_reply_fallback": True}


def test_visible_root_can_confirm_reply_without_inventing_hidden_parent():
    root = source_metadata({"id": "message-1", "thread_root_message_id": "message-1"})
    assert auto_reply_kwargs(root) == {}
    descendant = source_metadata({"id": "message-2", "thread_root_message_id": "message-1"})
    assert descendant["reply_to_message_id"] is None
    assert descendant["imessage_sources"][0]["reply_to_message_id"] is None
    assert auto_reply_kwargs(descendant) == {"reply_to_message_id": "message-2", "plain_reply_fallback": True}


def test_active_context_is_private_scoped_and_records_actual_sends(monkeypatch):
    cfg = BridgeConfig(identity="one", api_key="synthetic-secret-not-for-context")
    context = write_turn_context("../chat-1", cfg, metadata())
    path = imessage_turn_context_path("../chat-1")
    assert path.parent.name == "imessage_turn_contexts"
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert cfg.api_key not in path.read_text()
    assert read_turn_context() is None
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "../chat-1")
    assert read_turn_context() == context
    validate_tool_target(context, "conversation-1", "message-1")
    validate_tool_target(context, "conversation-1", None)
    for conversation, target in (("other", "message-1"), (None, "message-1"), ("conversation-1", "not-admitted")):
        with pytest.raises(ValueError):
            validate_tool_target(context, conversation, target)
    sent = record_tool_send(context, {"id": "sent-1", "status": "pending"}, "request-1", "answer", "message-1")
    assert sent["sent_outputs"] == [{
        "id": "sent-1", "status": "pending", "reply_to_message_id": None,
        "thread_id": None, "thread_root_message_id": None,
        "idempotency_key": "request-1", "text": "answer", "target": "message-1",
    }]
    record_tool_send(context, {"id": "sent-2"}, "request-1")
    assert len(read_turn_context()["sent_outputs"]) == 1
    clear_turn_context("../chat-1")
    assert read_turn_context() is None
    route = IMessageState(cfg).lookup_outbound("sent-1")
    assert route["chat_id"] == "../chat-1"
    assert route["meta"]["conversation_id"] == "conversation-1"
    assert route["meta"]["imessage_reply_target"] == "message-1"
    assert route["reply_to_message_id"] is None


def test_stale_tool_cannot_record_into_next_turn_or_validate_against_it():
    cfg = BridgeConfig(identity="one")
    old = write_turn_context("chat-1", cfg, metadata())
    current = write_turn_context("chat-1", cfg, metadata("message-2"))
    assert old["nonce"] != current["nonce"]
    with pytest.raises(ValueError, match="active iMessage"):
        record_tool_send(old, {"id": "sent-1"}, "request-1")
    with pytest.raises(ValueError, match="active iMessage"):
        validate_tool_target(old, "conversation-1", "message-1")
    assert json.loads(imessage_turn_context_path("chat-1").read_text())["sent_outputs"] == []


def test_startup_cleanup_retires_only_matching_identity_and_environment():
    current = BridgeConfig(identity="one", base_url="https://example.test/api")
    other_identity = BridgeConfig(identity="two", base_url=current.base_url)
    other_environment = BridgeConfig(identity="one", base_url="https://other.example.test/api")
    stale = write_turn_context("stale-chat", current, metadata())
    second_identity = write_turn_context("other-identity-chat", other_identity, metadata())
    second_environment = write_turn_context("other-environment-chat", other_environment, metadata())
    clear_identity_turn_contexts(BridgeConfig(identity=current.identity, base_url=current.base_url + "/"))
    assert read_turn_context("stale-chat") is None
    assert read_turn_context("other-identity-chat") == second_identity
    assert read_turn_context("other-environment-chat") == second_environment
    with pytest.raises(ValueError, match="active iMessage"):
        validate_tool_target(stale, "conversation-1", "message-1")
    with pytest.raises(ValueError, match="active iMessage"):
        record_tool_send(stale, {"id": "sent-1"}, "request-1")


def test_accepted_tool_send_after_stop_keeps_original_route_without_touching_next_turn():
    cfg = BridgeConfig(identity="one")
    before_send = write_turn_context("chat-1", cfg, metadata())
    clear_turn_context("chat-1")
    next_turn = write_turn_context("chat-1", cfg, metadata("message-2"))
    record_accepted_tool_send(before_send, {"id": "sent-1", "status": "pending"}, "request-1", target="message-1")
    route = IMessageState(cfg).lookup_outbound("sent-1")
    assert route["chat_id"] == "chat-1"
    assert route["meta"]["message_id"] == "message-1"
    assert route["meta"]["imessage_reply_target"] == "message-1"
    assert read_turn_context("chat-1") == next_turn
    with pytest.raises(ValueError, match="active iMessage"):
        record_tool_send(before_send, {"id": "sent-1"}, "request-1")


def test_context_exposes_companion_boundary_without_message_history():
    meta = {**metadata(), "companion": True, "companion_scope_id": "scope-1",
            "companion_activation_id": "activation-1"}
    context = write_turn_context("chat-1", BridgeConfig(identity="one"), meta)
    assert context["companion"] is True
    assert context["companion_scope_id"] == "scope-1"
    assert context["companion_activation_id"] == "activation-1"
    assert "text" not in context
    assert read_turn_context("chat-1") == context


def test_route_context_does_not_copy_arbitrary_metadata_or_secrets():
    context = write_turn_context("chat-1", BridgeConfig(identity="one"), {
        **metadata(), "api_key": "synthetic-api-secret", "authorization": "synthetic-token",
        "contact_memories": ["Not needed for send correlation"],
    })
    assert "api_key" not in context["route_meta"]
    assert "authorization" not in context["route_meta"]
    assert "contact_memories" not in context["route_meta"]


def test_empty_receipt_ids_fail_closed_and_unknown_states_are_rejected():
    state = IMessageState(BridgeConfig(identity="one"))
    with pytest.raises(ValueError, match="stable event"):
        state.admit("chat-1", "question", {})
    with pytest.raises(ValueError, match="Unknown"):
        state.mark(metadata(), "not-a-state")
    with sqlite3.connect(state.path) as db:
        assert db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0

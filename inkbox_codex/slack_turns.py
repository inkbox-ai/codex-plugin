"""Source-bound Slack attachment authority shared with the tool process."""

from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy

from .imessage import _context_lock, _private_dir, _read_context, _write_context


def _path(chat_id, source):
    key = hashlib.sha256(json.dumps([chat_id, source]).encode()).hexdigest()
    return _private_dir("slack_turns") / f"{key}.json"


def _current_path(chat_id):
    return _private_dir("slack_current") / f"{hashlib.sha256(chat_id.encode()).hexdigest()}.json"


def _is_current(context):
    return _read_context(_current_path(context["chat_id"])) == {"source": context["meta"]["source_event_id"]}


def begin(chat_id, cfg, meta):
    required = ("source_event_id", "identity_id", "connection_id", "workspace_id", "conversation_id", "message_ts")
    if not all(isinstance(meta.get(key), str) and meta[key] for key in required):
        return None
    path = _path(chat_id, meta["source_event_id"])
    record = {"chat_id": chat_id, "identity": str(cfg.identity), "base_url": str(cfg.base_url or ""),
              "project_dir": str(cfg.project_dir), "meta": deepcopy(meta), "state": "active", "uploads": {}}
    with _context_lock(_current_path(chat_id)), _context_lock(path):
        if path.exists():
            raise ValueError("This Slack source already has a turn receipt; it cannot be replayed")
        _write_context(path, record)
        _write_context(_current_path(chat_id), {"source": meta["source_event_id"]})
    return record


def end(context):
    if context is None:
        return
    path = _path(context["chat_id"], context["meta"]["source_event_id"])
    with _context_lock(_current_path(context["chat_id"])), _context_lock(path):
        if _is_current(context):
            _current_path(context["chat_id"]).unlink(missing_ok=True)
        record = _read_context(path)
        if record is not None:
            record["state"] = "closed"
            _write_context(path, record)


def retire(cfg):
    """Invalidate old tool authority after exclusive gateway ownership is acquired."""
    for path in _private_dir("slack_turns").glob("*.json"):
        with _context_lock(path):
            record = _read_context(path)
            if record and record.get("identity") == str(cfg.identity) and record.get("base_url") == str(cfg.base_url or ""):
                record["state"] = "closed"
                _write_context(path, record)


def capture(identity, cfg, args):
    chat_id = os.getenv("INKBOX_CODEX_CHAT_ID")
    source = args.get("source_event_id")
    if not chat_id or not isinstance(source, str) or not source:
        raise ValueError("Upload requires the original Slack source_event_id from the active request")
    record = _read_context(_path(chat_id, source))
    if (not record or record.get("state") != "active" or not _is_current(record) or record.get("identity") != identity
            or str(record.get("base_url") or "").rstrip("/") != str(cfg.base_url or "").rstrip("/")):
        raise ValueError("No matching active Slack request is available for this attachment")
    meta = record["meta"]
    if any(args.get(key) != meta.get(key) for key in ("connection_id", "conversation_id", "thread_ts")):
        raise ValueError("The attachment destination must match its original Slack request")
    return record


def upload(client, context, args, payload):
    from .slack import operation_summary, validate_connection

    meta = context["meta"]
    validate_connection(client.slack, meta["identity_id"], meta)
    if meta.get("companion"):
        page = client.companion.activation_messages(context["identity"], meta["companion_activation_id"], limit=1)
        if (str(page.scope_id), str(page.activation_id), str(page.conversation_id), page.channel) != (
                meta["companion_scope_id"], meta["companion_activation_id"], meta["companion_conversation_id"], "slack"):
            raise ValueError("The attachment's original Companion activation is no longer available")
        reply = page.reply_context
        def field(name):
            return reply.get(name) if isinstance(reply, dict) else getattr(reply, name, None)
        if (str(field("connection_id")), field("slack_conversation_id")) != (meta["connection_id"], meta["conversation_id"]):
            raise ValueError("The attachment's original Companion destination no longer matches")
    path = _path(context["chat_id"], meta["source_event_id"])
    key = args["idempotency_key"]
    operation_key = "codex:upload:" + hashlib.sha256(json.dumps([
        meta["identity_id"], meta["connection_id"], meta["conversation_id"], meta.get("thread_ts"),
        meta["source_event_id"], key,
    ]).encode()).hexdigest()
    fingerprint = hashlib.sha256(json.dumps([meta["source_event_id"], payload], sort_keys=True).encode()).hexdigest()
    with _context_lock(_current_path(context["chat_id"])), _context_lock(path):
        record = _read_context(path)
        if not record or record.get("state") != "active" or not _is_current(record) or record.get("meta") != meta:
            raise ValueError("The Slack request ended before the attachment was submitted")
        existing = record["uploads"].get(key)
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise ValueError("An attachment idempotency key cannot be reused for different content")
            return existing["result"]
        if any(row["result"].get("status") not in {"succeeded", "failed"} for row in record["uploads"].values()):
            raise ValueError("An earlier attachment outcome is unresolved; inspect its operation before sending again")
        receipt = {"operation": "file_upload", "status": "unknown", "connection_id": meta["connection_id"],
                   "conversation_id": meta["conversation_id"], "thread_ts": meta.get("thread_ts"), "idempotency_key": operation_key}
        record["uploads"][key] = {"fingerprint": fingerprint, "result": receipt}
        _write_context(path, record)
    try:
        operation = client.slack.upload_file(meta["connection_id"], conversation_id=meta["conversation_id"],
            thread_ts=meta.get("thread_ts"), idempotency_key=operation_key, **payload)
        receipt = {**receipt, **operation_summary(operation)}
        if (receipt.get("operation") != "file_upload" or receipt.get("connection_id") != meta["connection_id"]
                or receipt.get("conversation_id") != meta["conversation_id"]
                or receipt.get("thread_ts") != meta.get("thread_ts")
                or receipt.get("status") not in {"succeeded", "failed", "unknown", "in_progress"}):
            raise ValueError("The attachment operation could not be correlated with its original request")
    except Exception:
        # The durable unknown receipt prohibits a new upload even if the response was lost.
        return {**record["uploads"][key]["result"], "inspection": (
            "Use inkbox_slack_get_operation with this idempotency_key when the SDK supports by-key lookup. "
            "An unknown outcome is not failure; do not upload again with a new key.")}
    with _context_lock(path):
        record = _read_context(path)
        if record is None:
            raise RuntimeError("The attachment receipt could not be saved; do not resend")
        record["uploads"][key]["result"] = receipt
        _write_context(path, record)
    return receipt


def reconcile(identity, cfg, connection_id, result, *, operation_id=None, idempotency_key=None):
    """A read-only inspection may settle only a matching original upload receipt."""
    chat = os.getenv("INKBOX_CODEX_CHAT_ID")
    if not chat:
        return
    current = _read_context(_current_path(chat))
    if not current or not current.get("source"):
        return
    path = _path(chat, current["source"])
    with _context_lock(path):
        record = _read_context(path)
        if (not record or record.get("identity") != identity
                or record.get("base_url") != str(cfg.base_url or "")
                or record["meta"].get("connection_id") != connection_id
                or result.get("connection_id") != connection_id
                or result.get("conversation_id") != record["meta"].get("conversation_id")
                or result.get("operation") != "file_upload"
                or result.get("status") not in {"succeeded", "failed"}):
            return
        for upload in record["uploads"].values():
            prior = upload["result"]
            if ((operation_id and prior.get("id") == operation_id)
                    or (idempotency_key and prior.get("idempotency_key") == idempotency_key)):
                upload["result"] = {**prior, **result}
        _write_context(path, record)

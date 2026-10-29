"""Host and source lifetimes shared with gateway tool subprocesses."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import os
import secrets
from typing import Any, Iterator

from .imessage import _context_lock, _private_dir, _read_context, _write_context, read_turn_context


HOST_ENV = "INKBOX_CODEX_HOST_EPOCH"
_request_metadata: ContextVar[dict[str, Any] | None] = ContextVar("inkbox_tool_origin", default=None)


def _path(chat_id: str):
    return _private_dir("turn_authority") / f"{hashlib.sha256(chat_id.encode()).hexdigest()}.json"


def _base_url(cfg: Any) -> str:
    return str(cfg.base_url or "").rstrip("/")


@contextmanager
def mcp_request(metadata: Any) -> Iterator[None]:
    """Retain the host's request metadata separately from model arguments."""
    token = _request_metadata.set(metadata if isinstance(metadata, dict) else {})
    try:
        yield
    finally:
        _request_metadata.reset(token)


def new_host(chat_id: str, cfg: Any) -> str:
    epoch = secrets.token_hex(16)
    path = _path(chat_id)
    with _context_lock(path):
        _write_context(path, {
            "chat_id": chat_id, "identity": str(cfg.identity), "base_url": _base_url(cfg),
            "host_epoch": epoch, "state": "idle",
        })
    return epoch


def begin(chat_id: str, cfg: Any, host_epoch: str, channel: str,
          native_nonce: str | None = None) -> dict[str, Any]:
    """Publish only a source actually submitted to this host, never an arrival hint."""
    path = _path(chat_id)
    with _context_lock(path):
        record = _read_context(path)
        if (not record or record.get("host_epoch") != host_epoch
                or record.get("identity") != str(cfg.identity) or record.get("base_url") != _base_url(cfg)):
            raise ValueError("The gateway host is no longer current")
        record = {
            **record, "source_epoch": secrets.token_hex(16), "state": "active",
            "channel": channel, "native_nonce": native_nonce, "thread_id": None, "turn_id": None,
        }
        _write_context(path, record)
        return record


def _same_source(record: dict[str, Any] | None, token: dict[str, Any]) -> bool:
    return bool(record and all(record.get(key) == token.get(key) for key in
        ("chat_id", "identity", "base_url", "host_epoch", "source_epoch")))


def bind(token: dict[str, Any], thread_id: str, turn_id: str) -> bool:
    """Bind the acknowledged host turn without reviving a stopped source."""
    path = _path(token["chat_id"])
    with _context_lock(path):
        record = _read_context(path)
        if not _same_source(record, token) or record.get("state") != "active":
            return False
        record.update(thread_id=thread_id, turn_id=turn_id)
        _write_context(path, record)
        return True


def retire(token: dict[str, Any] | None) -> None:
    if token is None:
        return
    path = _path(token["chat_id"])
    with _context_lock(path):
        record = _read_context(path)
        if _same_source(record, token):
            record["state"] = "closed"
            _write_context(path, record)


def retire_host(chat_id: str, host_epoch: str | None) -> None:
    path = _path(chat_id)
    with _context_lock(path):
        record = _read_context(path)
        if record and record.get("host_epoch") == host_epoch:
            record["state"] = "closed"
            _write_context(path, record)


def capture(identity_handle: str, cfg: Any) -> dict[str, Any] | None:
    """Capture a gateway source, while keeping standalone calls independent."""
    epoch = os.getenv(HOST_ENV)
    if not epoch:
        return None
    chat_id = os.getenv("INKBOX_CODEX_CHAT_ID")
    record = _read_context(_path(chat_id)) if chat_id else None
    if (not record or record.get("host_epoch") != epoch or record.get("identity") != identity_handle
            or record.get("base_url") != _base_url(cfg) or record.get("state") != "active"):
        raise ValueError("No active gateway source is available for this operation")
    metadata = _request_metadata.get()
    if metadata is not None:
        origin = metadata.get("x-codex-turn-metadata")
        if (not isinstance(origin, dict) or not origin.get("thread_id") or not origin.get("turn_id")
                or origin["thread_id"] != record.get("thread_id") or origin["turn_id"] != record.get("turn_id")):
            raise ValueError("The tool call does not belong to the active gateway source")
    validate(record)
    return record


def validate(token: dict[str, Any] | None) -> None:
    """Recheck the captured lifetime immediately before an external mutation."""
    if token is None:
        return
    current = _read_context(_path(token["chat_id"]))
    if (not _same_source(current, token) or current.get("state") != "active"
            or any(current.get(key) != token.get(key) for key in ("thread_id", "turn_id"))):
        raise ValueError("The gateway source ended before this operation was submitted")
    nonce = current.get("native_nonce")
    if current.get("channel") == "whatsapp" and not nonce:
        raise ValueError("No active WhatsApp source is available for this operation")
    if nonce:
        context = read_turn_context(token["chat_id"])
        if (not context or context.get("nonce") != nonce or context.get("identity") != token["identity"]
                or str(context.get("base_url") or "").rstrip("/") != token["base_url"]):
            raise ValueError("The native messaging source is no longer active")

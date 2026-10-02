"""Durable iMessage receipts and narrowly scoped active-turn reply context."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import tempfile
from types import SimpleNamespace
from typing import Any, Iterator


_STATES = {"pending", "running", "reply_pending", "sending", "done", "cancelled", "uncertain", "failed"}
_TERMINAL = {"done", "cancelled"}
_ROUTE_FIELDS = {
    "conversation_id", "conversation_kind", "sender", "to", "message_id", "source_message_id",
    "reply_to_message_id", "thread_id", "thread_root_message_id",
    "imessage_event_id", "imessage_event_ids", "imessage_sources", "imessage_reply_target",
    "companion", "companion_scope_id", "companion_activation_id", "companion_sequence",
    "companion_reply_context", "companion_initialization", "companion_context_only",
}


def _private_dir(name: str) -> Path:
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    path = root / name
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _field(value: Any, name: str) -> Any:
    return value.get(name) if isinstance(value, dict) else getattr(value, name, None)


def _id(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def source_metadata(message: Any, event_id: str | None = None) -> dict[str, Any]:
    """Keep native IDs nullable; a standalone thread ID is not a reply request."""
    message_id = _id(_field(message, "id"))
    source = {
        "id": message_id,
        "text": _field(message, "content") or _field(message, "text") or "",
        **{name: _id(_field(message, name)) for name in (
            "reply_to_message_id", "thread_id", "thread_root_message_id",
        )},
    }
    receipt_id = _id(event_id) or message_id
    is_reply = bool(source["reply_to_message_id"] or (
        source["thread_root_message_id"] and source["thread_root_message_id"] != message_id
    ))
    return {
        "message_id": message_id,
        "imessage_event_id": receipt_id,
        "imessage_event_ids": [receipt_id] if receipt_id else [],
        "imessage_sources": [source] if message_id else [],
        "imessage_reply_target": message_id if is_reply else None,
        **{name: source[name] for name in ("reply_to_message_id", "thread_id", "thread_root_message_id")},
    }


def auto_reply_kwargs(meta: dict[str, Any]) -> dict[str, Any]:
    target = meta.get("imessage_reply_target")
    return {"reply_to_message_id": target, "plain_reply_fallback": True} if target else {}


def _event_ids(meta: dict[str, Any]) -> list[str]:
    ids = meta.get("imessage_event_ids") or [meta.get("imessage_event_id") or meta.get("message_id")]
    return list(dict.fromkeys(str(value) for value in ids if value))


def _outbound(message: Any) -> dict[str, Any]:
    return {name: _id(_field(message, name)) for name in (
        "id", "status", "reply_to_message_id", "thread_id", "thread_root_message_id",
    )}


class IMessageState:
    """One identity/environment's admitted work, with no automatic uncertain replay."""

    def __init__(self, cfg: Any):
        scope = _json([str(cfg.base_url or "").rstrip("/"), str(cfg.identity)])
        self.path = _private_dir("imessage_state") / f"{hashlib.sha256(scope.encode()).hexdigest()}.sqlite3"
        fd = os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)
        self.path.chmod(0o600)
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS receipts (
                    event_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL,
                    text TEXT NOT NULL, meta TEXT NOT NULL,
                    state TEXT NOT NULL, reply TEXT, batch_anchor TEXT
                );
                CREATE TABLE IF NOT EXISTS outbound (
                    message_id TEXT PRIMARY KEY, route TEXT NOT NULL
                );
            """)

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def admit(self, chat_id: str, text: str, meta: dict[str, Any]) -> bool:
        event_id = meta.get("imessage_event_id") or meta.get("message_id")
        if not event_id:
            raise ValueError("An iMessage receipt requires a stable event or message ID")
        with self._db() as db:
            result = db.execute(
                "INSERT OR IGNORE INTO receipts(event_id,chat_id,text,meta,state) VALUES(?,?,?,?, 'pending')",
                (str(event_id), chat_id, text, _json(meta)),
            )
            return result.rowcount == 1

    @staticmethod
    def _receipt(row: sqlite3.Row) -> dict[str, Any]:
        result = {"chat_id": row["chat_id"], "text": row["text"], "meta": json.loads(row["meta"])}
        if row["reply"] is not None:
            result["reply"] = json.loads(row["reply"])
        return result

    def replay_pending(self) -> list[dict[str, Any]]:
        """Call once at startup: interrupted work needs review, not a second model run."""
        with self._db() as db:
            db.execute("UPDATE receipts SET state='uncertain' WHERE state IN ('running','sending')")
            return [self._receipt(row) for row in db.execute(
                "SELECT * FROM receipts WHERE state='pending' ORDER BY rowid"
            )]

    def pending_replies(self) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute("SELECT * FROM receipts WHERE state='reply_pending' ORDER BY rowid").fetchall()
        seen: set[str] = set()
        result = []
        for row in rows:
            anchor = row["batch_anchor"] or row["event_id"]
            if anchor not in seen and row["reply"] is not None:
                seen.add(anchor)
                result.append(self._receipt(row))
        return result

    def anchor_overlapping(self, meta: dict[str, Any], chat_id: str) -> None:
        """Retain own-source anchors without advancing work or merging receipts.

        Pending fragments can replay separately after a restart, so each
        receipt must keep an anchor from its own admitted sources. Started
        batches already share the full source list and therefore first source.
        Never change a saved/sending answer's route.
        """
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            for event_id in _event_ids(meta):
                row = db.execute(
                    "SELECT meta FROM receipts WHERE event_id=? AND chat_id=? AND state IN ('pending','running')",
                    (event_id, chat_id),
                ).fetchone()
                if row is None:
                    continue
                saved = json.loads(row["meta"])
                sources = saved.get("imessage_sources") or []
                target = sources[0].get("id") if sources else None
                if target and not saved.get("imessage_reply_target"):
                    saved["imessage_reply_target"] = target
                    db.execute("UPDATE receipts SET meta=? WHERE event_id=?", (_json(saved), event_id))

    def mark(self, meta: dict[str, Any], state: str, reply: Any = None, chat_id: str | None = None) -> None:
        if state not in _STATES:
            raise ValueError("Unknown iMessage receipt state")
        ids = _event_ids(meta)
        if not ids:
            return
        with self._db() as db:
            # Serialize batch selection and updates across the gateway and tool processes.
            db.execute("BEGIN IMMEDIATE")
            rows = [db.execute("SELECT * FROM receipts WHERE event_id=?", (event_id,)).fetchone() for event_id in ids]
            rows = [row for row in rows if row is not None and row["state"] not in _TERMINAL]
            if chat_id is not None:
                rows = [row for row in rows if row["chat_id"] == chat_id]
            anchor = next((row["batch_anchor"] for row in rows if row["batch_anchor"]), ids[0])
            for row in rows:
                merged = {**json.loads(row["meta"]), **meta}
                # A completed answer remains safely retryable until the send
                # checkpoint; a failed read-only preflight cannot have sent it.
                next_state = "reply_pending" if state == "uncertain" and row["state"] == "reply_pending" else state
                db.execute(
                    "UPDATE receipts SET meta=?,state=?,reply=?,batch_anchor=? WHERE event_id=?",
                    (_json(merged), next_state, _json(reply) if reply is not None else row["reply"], anchor, row["event_id"]),
                )

    def record_outbound(self, message: Any, meta: dict[str, Any], chat_id: str) -> None:
        sent = _outbound(message)
        if not sent["id"]:
            return
        route = {"chat_id": chat_id, "meta": meta, "message_id": sent.pop("id"), **sent}
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (route["message_id"],)).fetchone()
            if row is None:
                db.execute("INSERT INTO outbound(message_id,route) VALUES(?,?)", (route["message_id"], _json(route)))
            elif json.loads(row["route"]).get("unknown_route"):
                # Delivery callbacks can win the race with the accepted-send
                # response. Fill its missing route without undoing that failure.
                route["status"] = json.loads(row["route"])["status"]
                db.execute("UPDATE outbound SET route=? WHERE message_id=?", (_json(route), route["message_id"]))

    def lookup_outbound(self, message_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (str(message_id),)).fetchone()
        return json.loads(row["route"]) if row else None

    def mark_delivery_failed(self, message_id: str) -> None:
        """Record late failure without changing the completed model work or its route."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT route FROM outbound WHERE message_id=?", (str(message_id),)).fetchone()
            if row is not None:
                route = {**json.loads(row["route"]), "status": "failed"}
                db.execute("UPDATE outbound SET route=? WHERE message_id=?", (_json(route), str(message_id)))
            else:
                route = {"chat_id": None, "meta": {}, "message_id": str(message_id),
                         "status": "failed", "unknown_route": True}
                db.execute("INSERT INTO outbound(message_id,route) VALUES(?,?)", (str(message_id), _json(route)))

    def summary(self) -> dict[str, int]:
        counts = dict.fromkeys(sorted(_STATES), 0)
        with self._db() as db:
            counts.update({row[0]: row[1] for row in db.execute("SELECT state,COUNT(*) FROM receipts GROUP BY state")})
        counts["unfinished"] = sum(count for state, count in counts.items() if state not in _TERMINAL)
        with self._db() as db:
            counts["outbound_failed_count"] = db.execute(
                "SELECT COUNT(*) FROM outbound WHERE json_extract(route, '$.status') IN ('failed', 'delivery_failed')"
            ).fetchone()[0]
        return counts


def imessage_turn_context_path(chat_id: str) -> Path:
    return _private_dir("imessage_turn_contexts") / f"{hashlib.sha256(chat_id.encode()).hexdigest()}.json"


@contextmanager
def _context_lock(path: Path) -> Iterator[None]:
    fd = os.open(path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _read_context(path: Path) -> dict[str, Any] | None:
    try:
        result = json.loads(path.read_text())
        return result if isinstance(result, dict) else None
    except (OSError, ValueError):
        return None


def _write_context(path: Path, context: dict[str, Any]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=path.stem + "-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(_json(context) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_turn_context(chat_id: str, cfg: Any, meta: dict[str, Any]) -> dict[str, Any]:
    sources = meta.get("imessage_sources") or [{"id": meta.get("message_id")}]
    context = {
        "chat_id": chat_id, "identity": str(cfg.identity), "base_url": str(cfg.base_url or ""),
        "conversation_id": _id(meta.get("conversation_id")),
        "admitted_source_ids": list(dict.fromkeys(str(source["id"]) for source in sources if source.get("id"))),
        "nonce": secrets.token_hex(16), "sent_outputs": [],
        "companion": bool(meta.get("companion")),
        "companion_scope_id": meta.get("companion_scope_id"),
        "companion_activation_id": meta.get("companion_activation_id"),
        "route_meta": {key: value for key, value in meta.items() if key in _ROUTE_FIELDS},
    }
    path = imessage_turn_context_path(chat_id)
    with _context_lock(path):
        _write_context(path, context)
    return context


def read_turn_context(chat_id: str | None = None) -> dict[str, Any] | None:
    chat_id = chat_id or os.getenv("INKBOX_CODEX_CHAT_ID")
    return _read_context(imessage_turn_context_path(chat_id)) if chat_id else None


def clear_turn_context(chat_id: str) -> None:
    path = imessage_turn_context_path(chat_id)
    with _context_lock(path):
        path.unlink(missing_ok=True)


def clear_identity_turn_contexts(cfg: Any) -> None:
    """Retire stale tool contexts after acquiring exclusive gateway ownership."""
    identity, base_url = str(cfg.identity), str(cfg.base_url or "").rstrip("/")
    for path in _private_dir("imessage_turn_contexts").glob("*.json"):
        with _context_lock(path):
            current = _read_context(path)
            if (
                current and current.get("identity") == identity
                and str(current.get("base_url") or "").rstrip("/") == base_url
            ):
                path.unlink(missing_ok=True)


def _active_context(context: dict[str, Any]) -> dict[str, Any]:
    current = _read_context(imessage_turn_context_path(str(context.get("chat_id") or "")))
    if not current or not context.get("nonce") or current.get("nonce") != context["nonce"]:
        raise ValueError("No matching active iMessage turn")
    return current


def validate_tool_target(context: dict[str, Any], conversation_id: str | None, target: str | None) -> None:
    current = _active_context(context)
    if not conversation_id or str(conversation_id) != current.get("conversation_id"):
        raise ValueError("The iMessage conversation must match the active turn")
    if target is not None and str(target) not in current.get("admitted_source_ids", []):
        raise ValueError("The iMessage reply target must be an admitted message in the active turn")


def record_accepted_tool_send(
    context: dict[str, Any], message: Any, idempotency_key: str,
    text: str | None = None, target: str | None = None,
) -> None:
    """Correlate an accepted send with its pre-send validated context snapshot.

    A concurrent stop can close that turn before the response returns. This
    records historical delivery metadata only; it cannot authorize a send or
    modify another turn's active context.
    """
    route_meta = {**context["route_meta"], "imessage_reply_target": target,
                  "imessage_idempotency_key": idempotency_key}
    state = IMessageState(SimpleNamespace(identity=context["identity"], base_url=context["base_url"]))
    state.record_outbound(message, route_meta, context["chat_id"])


def record_tool_send(
    context: dict[str, Any], message: Any, idempotency_key: str,
    text: str | None = None, target: str | None = None,
) -> dict[str, Any]:
    path = imessage_turn_context_path(str(context.get("chat_id") or ""))
    with _context_lock(path):
        current = _active_context(context)
        if not any(item.get("idempotency_key") == idempotency_key for item in current["sent_outputs"]):
            record_accepted_tool_send(current, message, idempotency_key, text=text, target=target)
            current["sent_outputs"].append({
                **_outbound(message), "idempotency_key": idempotency_key,
                "text": text, "target": target,
            })
            _write_context(path, current)
        return current

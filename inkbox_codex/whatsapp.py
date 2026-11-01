"""WhatsApp direct messages, native replies, reactions and durable receipt routing."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import mimetypes
import re
from pathlib import Path
import uuid
from datetime import datetime, timezone
from typing import Any

from .whatsapp_v2 import V2_OPERATIONS, run_v2, specifications as v2_specifications, send_selector

logger = logging.getLogger(__name__)


class WhatsAppReplyRejected(ValueError):
    """The automatic answer failed local validation without a send attempt."""


REPLY_REJECTED_NOTICE = (
    "The answer was not sent because WhatsApp requires nonempty text of at most 4000 characters. "
    "Please ask for a shorter text answer."
)


EVENTS = ["whatsapp.received", "whatsapp.sent", "whatsapp.delivered", "whatsapp.delivery_failed", "whatsapp.delivery_unconfirmed", "whatsapp.reaction_received"]
_REJECTED_SEND_CODES = {
    403: {"whatsapp_not_eligible", "whatsapp_access_required", "whatsapp_contact_not_allowed", "whatsapp_media_source_not_allowed"},
    413: {"whatsapp_media_too_large"},
    422: {"whatsapp_groups_not_supported", "whatsapp_invalid_recipient", "whatsapp_text_too_long", "whatsapp_unanswered_followup_limit", "whatsapp_opening_message_too_long", "whatsapp_reply_required_for_media", "whatsapp_reply_required_for_links", "whatsapp_reply_required_for_phone_numbers", "whatsapp_reply_required_for_email_addresses"},
    429: {"whatsapp_daily_limit_exceeded", "whatsapp_unanswered_message_limit", "whatsapp_conversation_rate_limited", "whatsapp_new_contact_rate_limited", "whatsapp_new_contact_spacing", "whatsapp_follow_up_rate_limited"},
}


def known_send_rejection(error):
    detail = getattr(error, "detail", None)
    code = detail.get("error") if isinstance(detail, dict) else None
    return isinstance(code, str) and code in _REJECTED_SEND_CODES.get(getattr(error, "status_code", None), set())

def field(value: Any, name: str, default=None):
    return (
        value.get(name, default)
        if isinstance(value, dict)
        else getattr(value, name, default)
    )

def unsupported(exc):
    """An older API may reject the new subscription family without blocking other channels."""
    detail = str(getattr(exc, "detail", exc)).lower()
    return (
        (isinstance(exc, ValueError) or getattr(exc, "status_code", None) in {400, 422})
        and "whatsapp" in detail
        and any(
            word in detail for word in ("event", "prefix", "unknown", "unsupported")
        )
    )

def require_method(identity, name):
    method = getattr(identity, name, None)
    if not callable(method):
        raise RuntimeError("WhatsApp requires Inkbox SDK 0.7.24 or newer")
    return method


def observation_time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError):
        return None


def route_allowed(gateway, meta):
    """Apply the same local participant rule to fresh and retained input."""
    return _route_allowed(gateway._sender_allowed, meta)


def unsupported_shape(value):
    """Reject unsupported legacy routes without inferring a direct recipient."""
    data = value if isinstance(value, dict) else vars(value)
    for attempt in (data.get("whatsapp_tool_sends") or {}).values():
        payload = attempt.get("payload") or {}
        if unsupported_shape(payload) or isinstance(payload.get("to"), list) or attempt.get("independent_group"):
            return True
    return (bool(data.get("is_group") or data.get("isGroup"))
            or data.get("conversation_kind") == "group"
            or any((name.startswith("group_") or name == "groupName") and item is not None
                   for name, item in data.items())
            or bool(data.get("unresolved_participant_count"))
            or any(isinstance(data.get(name), list) and len(data[name]) > 1
                   for name in ("participants", "recipients")))


def _route_allowed(sender_allowed, meta):
    return not unsupported_shape(meta) and sender_allowed(str(meta.get("sender") or ""))


async def current_route_allowed(gateway, meta):
    return route_allowed(gateway, meta)


async def receive(gateway, envelope):
    from aiohttp import web

    data = envelope.get("data") or {}
    message = data.get("message") or {}
    message_id = str(message.get("id") or "")
    event_id = str(envelope.get("id") or message_id)
    conversation = str(message.get("conversation_id") or "")
    sender = str(message.get("sender_number") or "")
    identity = gateway._identity
    participants = message.get("participants") or []
    if (
        unsupported_shape(message)
        or not message_id
        or not event_id
        or not conversation
        or message.get("direction") != "inbound"
        or not isinstance(participants, list)
        or not participants
        or any(not isinstance(number, str) or re.fullmatch(r"\+[1-9][0-9]{6,14}", number) is None for number in participants)
        or sender not in participants
        or len(participants) != 1 or sender != message.get("remote_number")
        or sender == message.get("local_number")
        or str(message.get("agent_identity_id") or "") != str(identity.id)
    ):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    if not gateway._sender_allowed(sender):
        return web.json_response({"ok": True, "ignored": "sender-not-allowed"})
    text = str(message.get("content") or "")
    media = message.get("media") or []
    if len(media) > 1 or (not text.strip() and not media):
        return web.json_response({"ok": True, "ignored": "unsupported-message"})
    key = f"whatsapp:{message_id}"
    if gateway._dedup_begin(key):
        return web.json_response({"ok": True, "deduped": True})
    try:
        refreshed = []
        for item in media:
            if item.get("status") == "unavailable":
                text += "\n[WhatsApp attachment unavailable]"
                continue
            fresh = await asyncio.to_thread(
                require_method(identity, "get_whatsapp_media"), item["id"]
            )
            if field(fresh, "status") == "ready" and field(fresh, "url"):
                refreshed.append(
                    {
                        "url": field(fresh, "url"),
                        "content_type": field(fresh, "content_type"),
                        "size": field(fresh, "size"),
                    }
                )
            else:
                text += "\n[WhatsApp attachment unavailable]"
        body = await gateway._with_media(
            text, refreshed, prefix=f"whatsapp-{message_id}"
        )
        if refreshed and body == text:
            body += "\n[WhatsApp attachment could not be downloaded; its media ID remains available]"
        contact = await gateway._resolve_contact_full(kind="phone", value=sender)
        payload_contact = gateway._matched_payload_contact(
            gateway._webhook_list(data, "contacts", "contact_list"),
            resolved_id=gateway._contact_id(contact),
        )
        memories = gateway._webhook_contact_memories(payload_contact)
        chat_id = gateway._chat_key(
            data,
            sender,
            gateway._thread_key("whatsapp", conversation),
            contact=contact,
            allow_webhook_contact=False,
        )
        meta = {
            "channel": "whatsapp",
            "raw_text": str(message.get("content") or ""),
            "message_id": message_id,
            "source_event_id": event_id,
            "imessage_event_id": event_id,
            "imessage_event_ids": [event_id],
            "conversation_id": conversation,
            "sender": sender,
            "contact": contact,
            "agent_identity": None
            if contact
            else gateway._single_agent_identity(
                gateway._webhook_list(
                    data, "agent_identities", "agentIdentities", "identity_agents"
                )
            ),
            "contact_memories": memories,
            "conversation_kind": "direct",
            "participants": participants,
            "reply_to_message_id": message.get("reply_to_message_id"),
            "thread_id": message.get("thread_id"),
            "thread_root_message_id": message.get("thread_root_message_id"),
            "whatsapp_reply_target": message_id,
            "whatsapp_auto_reply_target": message_id,
            "media": bool(media),
        }
        await gateway.sessions.get(chat_id).handle_inbound(body, "whatsapp", meta)
    except BaseException:
        gateway._dedup_rollback(key)
        raise
    gateway._dedup_commit(key)
    return web.json_response({"ok": True})

def record_status(gateway, envelope):
    """Correlate content-free delivery facts without starting or retrying model work."""
    if envelope.get("event_type") not in EVENTS[1:5]:
        return
    message = (envelope.get("data") or {}).get("message") or {}
    if (
        not message.get("id")
        or str(message.get("agent_identity_id") or "") != str(gateway._identity.id)
        or message.get("direction") != "outbound"
    ):
        return
    store = gateway._channel_store("whatsapp")
    with store._db() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT route FROM outbound WHERE message_id=?", (str(message["id"]),)
        ).fetchone()
        route = (
            json.loads(row["route"])
            if row
            else {
                "chat_id": None,
                "meta": {},
                "message_id": str(message["id"]),
                "unknown_route": True,
            }
        )
        observed = observation_time(message.get("updated_at"))
        previous = observation_time(route.get("updated_at"))
        if observed is None or (previous is not None and observed < previous):
            return
        route.update({"status": message.get("status"), "delivery_state": message.get("delivery_state"),
                      "updated_at": message["updated_at"]})
        db.execute(
            "INSERT OR REPLACE INTO outbound(message_id,route) VALUES(?,?)",
            (str(message["id"]), json.dumps(route)),
        )


def _specifications():
    def string(description):
        return {"type": "string", "description": description}
    integer = {"type": "integer", "minimum": 1, "maximum": 200}
    specs = [
        (
            "inkbox_lookup_whatsapp_send",
            "Recover an ambiguous WhatsApp send's original message ID using its preserved request key. Read-only: this never sends or retries delivery.",
            {"idempotency_key": string("Original logical-send key")},
            ["idempotency_key"],
            "lookup_whatsapp_send",
        ),
        (
            "inkbox_get_whatsapp_status",
            "Read fresh WhatsApp availability. Access is approved in the console, not enabled by this tool.",
            {},
            [],
            "get_whatsapp_status",
        ),
        (
            "inkbox_send_whatsapp",
            "Send a requested direct WhatsApp message, automatically quoting the active ordinary source when sending to its conversation only if idempotency_key is omitted. With an explicit key and no explicit quote, only a known retained quote is restored; an unknown key preserves legacy plain-send behavior. An explicit quote can select another visible message in that conversation. Your normal final response already replies automatically. Use one recipient or conversation; text and one owned media ID are optional individually. Preserve the returned request key and media ID after uncertainty; never retry as a new message.",
            {
                "to": {**string("One E.164 recipient; arrays are not supported"), "pattern": r"^\+[1-9][0-9]{6,14}$"},
                "reply_to_message_id": string("Optional visible message ID to quote; defaults to the active ordinary source for its conversation only when idempotency_key is omitted. With an explicit key, omission restores only a known retained quote; an unknown key stays plain"),
                "conversation_id": string(
                    "Existing conversation; mutually exclusive with to"
                ),
                "text": {**string("Message text"), "maxLength": 4000},
                "media_id": string("Owned ID returned by upload"),
            },
            [],
            "send_whatsapp",
        ),
        (
            "inkbox_list_whatsapp_conversations",
            "List WhatsApp conversations with local unread counts.",
            {"limit": integer, "offset": {"type": "integer", "minimum": 0}},
            [],
            "list_whatsapp_conversations",
        ),
        (
            "inkbox_get_whatsapp_conversation",
            "Read metadata for one WhatsApp conversation.",
            {"conversation_id": string("Conversation ID")},
            ["conversation_id"],
            "get_whatsapp_conversation",
        ),
        (
            "inkbox_list_whatsapp_messages",
            "List WhatsApp messages in one conversation, newest first.",
            {
                "conversation_id": string("Conversation ID"),
                "limit": integer,
                "offset": {"type": "integer", "minimum": 0},
            },
            ["conversation_id"],
            "list_whatsapp_messages",
        ),
        (
            "inkbox_get_whatsapp_message",
            "Read one WhatsApp message and its current sending and delivery state.",
            {"message_id": string("Message ID")},
            ["message_id"],
            "get_whatsapp_message",
        ),
        (
            "inkbox_upload_whatsapp_media",
            "Upload one local attachment (at most 20 MiB) and retain its owned ID for sending. Do not upload again to recover an uncertain send.",
            {"media_path": string("Local file path")},
            ["media_path"],
            "upload_whatsapp_media",
        ),
        (
            "inkbox_get_whatsapp_media",
            "Renew temporary access to an owned WhatsApp attachment by ID; unavailable media retains metadata.",
            {"media_id": string("Owned attachment ID")},
            ["media_id"],
            "get_whatsapp_media",
        ),
        (
            "inkbox_mark_whatsapp_read",
            "Mark local WhatsApp history read through an exact message. This sends no remote read receipt.",
            {
                "conversation_id": string("Conversation ID"),
                "through_message_id": string("Last visible message ID"),
            },
            ["conversation_id", "through_message_id"],
            "mark_whatsapp_read",
        ),
    ]
    return specs + v2_specifications()


TOOLS = [{"name": name, "description": description, "inputSchema": {
    "type": "object", "properties": properties, "required": required,
    "additionalProperties": False,
}} for name, description, properties, required, operation in _specifications()]
OPERATIONS = {name: operation for name, description, properties, required, operation in _specifications()}
next(tool for tool in TOOLS if tool["name"] == "inkbox_send_whatsapp")["inputSchema"]["properties"]["idempotency_key"] = {
    "type": "string", "description": "Preserve the original key when recovering a send; never choose a new key after uncertainty."
}


def _send_arguments(args):
    recipient, conversation, reply_target = send_selector(args)
    text = args.get("text")
    if text is not None and (not isinstance(text, str) or len(text) > 4000):
        raise ValueError("WhatsApp text must be at most 4000 characters")
    media_id = args.get("media_id")
    if media_id is not None:
        if not isinstance(media_id, str):
            raise ValueError("media_id must be a UUID")
        media_id = str(uuid.UUID(media_id))
    if not (text and text.strip()) and not media_id:
        raise ValueError("Provide text or one media ID")
    key = args.get("idempotency_key")
    if key is not None and (not isinstance(key, str) or not key.strip() or len(key) > 255
                            or not key.isascii() or not key.isprintable()):
        raise ValueError("idempotency_key must contain 1–255 printable ASCII characters")
    return recipient, conversation, reply_target, text, media_id, key


def _same_uuid(left, right):
    try:
        return uuid.UUID(str(left)) == uuid.UUID(str(right))
    except (ValueError, TypeError):
        return False


def auto_reply_kwargs(meta):
    """Only newly admitted ordinary sources opt in; retained plain intents stay plain."""
    target = meta.get("whatsapp_auto_reply_target")
    if not target or meta.get("reaction") or meta.get("reaction_id"):
        return {}
    sources = meta.get("imessage_sources") or [{"id": meta.get("message_id")}]
    if not any(_same_uuid(source.get("id"), target) for source in sources):
        raise ValueError("The WhatsApp reply target must belong to the admitted source")
    return {"reply_to_message_id": str(uuid.UUID(str(target)))}


def _source_bound(owner, recipient, conversation):
    route = (owner or {}).get("route_meta") or {}
    if not owner or route.get("channel") != "whatsapp":
        return False
    if conversation:
        return _same_uuid(owner.get("conversation_id"), conversation)
    if route.get("conversation_kind") == "group":
        return False
    return recipient == route.get("sender")


def _legacy_payload_matches(owner, key, previous, payload):
    """Prove a missing selector from the quote contract or exact historical hash."""
    if not payload or unsupported_shape(owner.get("route_meta") or {}) or isinstance(payload.get("to"), list) or unsupported_shape(payload):
        return False
    payload = {k: v for k, v in payload.items() if k != "idempotency_key"}
    expected = {"text": previous.get("text")}
    media_id = previous.get("media_id")
    if media_id:
        expected["media_ids"] = [str(uuid.UUID(media_id))]
    quote = previous.get("quote_target")
    if quote:
        # A quote required conversation_id, and only the source conversation
        # could acquire this checkpoint. Recipients cannot carry a quote.
        return payload == {**expected, "conversation_id": owner["conversation_id"],
                           "reply_to_message_id": quote}
    recipient, conversation = payload.get("to"), payload.get("conversation_id")
    expected["conversation_id" if conversation else "to"] = conversation or recipient
    if payload != expected:
        return False
    if conversation and conversation != owner.get("conversation_id"):
        return False
    if not conversation:
        # The old scalar-recipient hash omitted `to`; source ownership supplies
        # that value only for a direct sender, never for a group or other peer.
        if not _source_bound(owner, recipient, None):
            return False
    material = [owner["nonce"], conversation, previous.get("text"), media_id]
    return key == "codex-wa-" + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()


def _legacy_recovery_error(key, previous):
    error = ValueError(
        "Cannot prove the original message request from this legacy checkpoint. "
        "Preserve its idempotency_key and media_id; use inkbox_lookup_whatsapp_send for read-only lookup. "
        "Do not send a replacement or change keys. Recover the original request record before retrying; "
        "a delivered message does not establish the original selector."
    )
    error.idempotency_key = key
    error.whatsapp_media_id = previous.get("media_id")
    return error


def _retained_legacy_key(owner, payload, key):
    sends = owner.get("whatsapp_tool_sends") or {}
    if (sends.get(key) or {}).get("payload") is not None:
        return key
    matches, unproven = [], []
    for saved_key, previous in sends.items():
        if previous.get("payload") is not None:
            continue
        if key in sends and saved_key != key:
            continue
        saved_media = previous.get("media_id")
        if saved_media:
            saved_media = str(uuid.UUID(saved_media))
        fields_match = (previous.get("text") == payload.get("text")
                        and saved_media == next(iter(payload.get("media_ids") or []), None))
        if saved_key != key and (not fields_match or previous.get("quote_target")):
            continue
        if previous.get("state") == "sending":
            error = ValueError("An earlier WhatsApp send is unconfirmed; use its original key for read-only lookup")
            error.idempotency_key = saved_key
            error.whatsapp_media_id = previous.get("media_id")
            raise error
        candidate = dict(payload)
        if saved_key == key and previous.get("quote_target") and "reply_to_message_id" not in candidate:
            candidate["reply_to_message_id"] = previous["quote_target"]
        if not _legacy_payload_matches(owner, saved_key, previous, candidate):
            unproven.append((saved_key, previous))
        elif saved_key == key or (key is None and previous.get("state") in {"accepted", "rejected"}):
            matches.append(saved_key)
    if len(matches) == 1:
        return matches[0]
    if unproven:
        raise _legacy_recovery_error(*unproven[0])
    if len(matches) > 1:
        raise ValueError("Multiple retained WhatsApp requests match; use the original idempotency_key")
    return key


def run_tool(client, identity_handle, name, args):
    """Resolve fresh capability and delegate policy to the identity-scoped SDK."""
    from .tools import _imessage_active_context, _json_safe
    from .turn_authority import capture, validate
    from .config import read_config

    operation = OPERATIONS[name]
    if any(name.startswith("group_") or name in {"groupName", "is_group", "isGroup", "unresolved_participant_count", "include_groups"} for name in args):
        raise ValueError("WhatsApp supports direct messages only")
    if operation == "send_whatsapp":
        recipient, conversation, reply_target, text, media_id, key = _send_arguments(args)
    mutation = operation in {"send_whatsapp", "upload_whatsapp_media", "send_whatsapp_reaction", "remove_whatsapp_reaction"}
    origin = capture(identity_handle, read_config()) if mutation else None
    owner = _imessage_active_context(identity_handle)
    if mutation and owner:
        route = owner.get("route_meta") or {}
        if (route.get("channel") == "whatsapp" and unsupported_shape(route)) or unsupported_shape(owner):
            for saved_key, previous in (owner.get("whatsapp_tool_sends") or {}).items():
                if unsupported_shape({"whatsapp_tool_sends": {saved_key: previous}}):
                    raise _legacy_recovery_error(saved_key, previous)
            raise ValueError("Retained WhatsApp group routes cannot send; use read-only lookup")
    identity = client.get_identity(identity_handle)
    if operation in V2_OPERATIONS:
        def authorize_v2():
            validate(origin)
            if owner and (owner.get("route_meta") or {}).get("channel") == "whatsapp":
                from .imessage import validate_tool_target
                validate_tool_target(owner, owner["conversation_id"], None)
        return _json_safe(run_v2(identity, operation, args, authorize=authorize_v2))
    if operation == "lookup_whatsapp_send":
        require_method(identity, "get_whatsapp_status")
        return {"message_id": str(client.message_sends.lookup(
            sender_kind="whatsapp_identity", sender_id=identity.id,
            operation="whatsapp.send", idempotency_key=args["idempotency_key"],
        ))}
    if operation == "get_whatsapp_status":
        status = _json_safe(require_method(identity, operation)(**args))
        try:
            limits = _json_safe(require_method(identity, "get_whatsapp_limits")())
        except Exception:
            # A not-yet-available limits endpoint must not hide channel readiness/history.
            return status
        return {**status, "limits": limits} if isinstance(status, dict) else status
    if operation == "send_whatsapp":
        if owner:
            from .imessage import _active_context
            owner = _active_context(owner)
            if ((owner.get("route_meta") or {}).get("channel") == "whatsapp"
                    and unsupported_shape(owner.get("route_meta") or {})) or unsupported_shape(owner):
                raise ValueError("Retained WhatsApp group routes cannot send; use read-only lookup")
            legacy_payload = {"text": text, "conversation_id" if conversation else "to": conversation or recipient}
            if media_id:
                legacy_payload["media_ids"] = [media_id]
            if reply_target is not None:
                legacy_payload["reply_to_message_id"] = reply_target
            key = _retained_legacy_key(owner, legacy_payload, key)
        context = owner if _source_bound(owner, recipient, conversation) else None
        if owner and key in (owner.get("whatsapp_tool_sends") or {}) and context is None:
            raise ValueError("A retained WhatsApp request key must keep its original message")
        if context and conversation and reply_target is None:
            if key is None:
                reply_target = auto_reply_kwargs(context.get("route_meta") or {}).get("reply_to_message_id")
            else:
                previous = (context.get("whatsapp_tool_sends") or {}).get(key)
                if previous:
                    if previous.get("text") != text or previous.get("media_id") != args.get("media_id"):
                        raise ValueError("A retained WhatsApp request key must keep its original message")
                    reply_target = previous.get("quote_target")
                # An unrecognized caller key may recover an older plain send.
                # Only an exact retained intent can restore its quote.
        if key is None:
            # Preserve the historical quoted-send hash, including its final null.
            key = "codex-wa-" + (hashlib.sha256(json.dumps([
                context["nonce"], conversation, text, args.get("media_id"),
                *([recipient, reply_target, None] if reply_target else []),
            ], sort_keys=True).encode()).hexdigest() if context else uuid.uuid4().hex)
        kwargs = {"text": text, "idempotency_key": key}
        if reply_target is not None:
            kwargs["reply_to_message_id"] = reply_target
        kwargs["conversation_id" if conversation else "to"] = conversation or recipient
        if media_id:
            kwargs["media_ids"] = [media_id]
        method = require_method(identity, operation)
        if owner and (owner.get("route_meta") or {}).get("channel") == "whatsapp":
            from .imessage import validate_tool_target
            validate_tool_target(owner, owner["conversation_id"], None)
            if context is None:
                current = _active_context(owner)
                pending = next(((saved_key, send) for saved_key, send in
                    (current.get("whatsapp_tool_sends") or {}).items() if send.get("state") == "sending"), None)
                if pending:
                    error = ValueError("An earlier WhatsApp send is unconfirmed; use its original key for read-only lookup")
                    error.idempotency_key = pending[0]
                    error.whatsapp_media_id = pending[1].get("media_id")
                    raise error
        validate(origin)
        if context:
            from .imessage import checkpoint_whatsapp_tool_send
            checkpoint_whatsapp_tool_send(context, key, state="sending", text=text, media_id=args.get("media_id"), quote_target=reply_target, payload=kwargs)
        validate(origin)
        args["idempotency_key"] = key
        try:
            sent = method(**kwargs)
        except Exception as exc:
            if context and known_send_rejection(exc):
                checkpoint_whatsapp_tool_send(context, key, state="rejected", text=text, media_id=args.get("media_id"), quote_target=reply_target, payload=kwargs)
            raise
        if context:
            from .imessage import record_accepted_tool_send, record_tool_send
            target = next(iter(context.get("admitted_source_ids") or []), None)
            try:
                record_accepted_tool_send(context, sent, key, text=text, target=target)
                record_tool_send(context, sent, key, text=text, target=target)
                checkpoint_whatsapp_tool_send(context, key, state="accepted", text=text, media_id=args.get("media_id"), quote_target=reply_target, payload=kwargs)
            except Exception:
                logger.warning("WhatsApp accepted; local tracking needs recovery. Do not resend.")
        result = _json_safe(sent)
        return {**result, "idempotency_key": key} if isinstance(result, dict) else {"message": result, "idempotency_key": key}
    if operation == "upload_whatsapp_media":
        path = Path(args["media_path"]).expanduser().resolve()
        if path.stat().st_size > 20 * 1024 * 1024:
            raise ValueError("WhatsApp media must be at most 20 MiB")
        with path.open("rb") as stream:
            content = stream.read(20 * 1024 * 1024 + 1)
        if len(content) > 20 * 1024 * 1024:
            raise ValueError("WhatsApp media must be at most 20 MiB")
        if owner and (owner.get("route_meta") or {}).get("channel") == "whatsapp":
            from .imessage import validate_tool_target
            validate_tool_target(owner, owner["conversation_id"], None)
        validate(origin)
        return require_method(identity, operation)(content=content, filename=path.name,
            content_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream")
    return require_method(identity, operation)(**args)


async def send_reply(gateway, chat_id, content, meta):
    """Persist a source-bound reply; historical plain intents retain their exact payload."""
    from .prompts import strip_markdown

    text = strip_markdown(content)
    store = gateway._channel_store("whatsapp")
    final = meta.get("imessage_final_reply") or meta.get("final_reply")
    if meta.get("whatsapp_explicit_quote_attempted"):
        if final:
            store.mark(meta, "done", chat_id=chat_id)
        raise ValueError("An explicit quoted reply was already attempted; inspect its original receipt instead of sending an automatic replacement")
    if len(text) > 4000 or not text.strip():
        if final:
            store.mark(meta, "failed", chat_id=chat_id)
        raise WhatsAppReplyRejected("WhatsApp replies require nonempty text of at most 4000 characters")
    conversation = str(meta.get("conversation_id") or "")
    if not conversation or meta.get("companion"):
        raise ValueError("WhatsApp reply requires its original conversation")
    identity = gateway._identity
    method = require_method(identity, "send_whatsapp")
    if not await current_route_allowed(gateway, meta):
        if final:
            store.mark(meta, "cancelled", chat_id=chat_id)
        raise PermissionError("WhatsApp conversation participants are not allowed")
    sources = meta.get("imessage_event_ids") or [meta.get("source_event_id") or meta.get("message_id")]
    reply_options = auto_reply_kwargs(meta)
    key_material = [gateway.cfg.base_url, str(identity.id), conversation, sources, text]
    if reply_options:
        key_material.append(reply_options)
    if not final:
        key_material.append(uuid.uuid4().hex)
    key = (meta.get("whatsapp_idempotency_key") if final else None) or "codex-wa-" + hashlib.sha256(
        json.dumps(key_material, sort_keys=True).encode()).hexdigest()
    meta = {**meta, "whatsapp_idempotency_key": key}
    if final:
        store.mark(meta, "sending", chat_id=chat_id)
    task = asyncio.create_task(asyncio.to_thread(method, conversation_id=conversation, text=text, idempotency_key=key, **reply_options))
    try:
        sent = await asyncio.shield(task)
    except asyncio.CancelledError:
        # Complete the single in-flight SDK call; cancellation never starts a resend.
        try:
            await task
        finally:
            if final:
                store.mark(meta, "uncertain", chat_id=chat_id)
        raise
    try:
        store.record_outbound(sent, meta, chat_id)
        if final:
            store.mark(meta, "done", chat_id=chat_id)
    except Exception:
        logger.warning("WhatsApp accepted; local tracking needs recovery. Do not resend.")

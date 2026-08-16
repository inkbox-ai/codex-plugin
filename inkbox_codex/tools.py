"""Inkbox messaging tools exposed to Codex through a local MCP server.

The original bridge used provider-specific decorators to build an in-process
MCP server. Codex app-server loads MCP servers from config, so this module keeps
the same Inkbox tool surface but exposes it as plain handlers that
``mcp_stdio.py`` serves over stdio.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import mimetypes
import os
import secrets
import sys
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    from .media import file_to_email_attachment
except ImportError:  # pragma: no cover - direct local import/test fallback
    from media import file_to_email_attachment

try:
    from .a2a_delegations import (
        find_by_task,
        promote_after_send,
        record_before_send,
    )
    from .a2a_progress_gate import (
        acquire_a2a_progress_gate,
        fence_a2a_progress,
        release_a2a_progress_gate,
    )
    from .config import (
        INKBOX_WS_PATH,
        VoiceStack,
        a2a_turn_context_path,
        call_contexts_dir,
        channel_hints_path,
        hosted_sms_turn_context_path,
        read_config,
    )
    from .delivery_policy import sms_tool_failure_kind
    from .hosted_sms_guard import (
        reserve_hosted_sms_attempt,
        settle_hosted_sms_attempt,
    )
except ImportError:  # pragma: no cover - direct local import/test fallback
    from a2a_delegations import (
        find_by_task,
        promote_after_send,
        record_before_send,
    )
    from a2a_progress_gate import (
        acquire_a2a_progress_gate,
        fence_a2a_progress,
        release_a2a_progress_gate,
    )
    from config import (
        INKBOX_WS_PATH,
        VoiceStack,
        a2a_turn_context_path,
        call_contexts_dir,
        channel_hints_path,
        hosted_sms_turn_context_path,
        read_config,
    )
    from delivery_policy import sms_tool_failure_kind
    from hosted_sms_guard import (
        reserve_hosted_sms_attempt,
        settle_hosted_sms_attempt,
    )


JsonSchema = Dict[str, Any]

SMS_MAX_LENGTH = 1600
IMESSAGE_MAX_LENGTH = 18995
IMESSAGE_MAX_GROUP_RECIPIENTS = 8


def _normalize_imessage_recipients(value: Any) -> Optional[List[str]]:
    """`to` as a list of E.164 strings, or None when the caller omitted it."""
    if value is None:
        return None
    if isinstance(value, str):
        entry = value.strip()
        return [entry] if entry else []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return []


def _identity_can_start_imessage_conversations(identity: Any) -> bool:
    """Whether the identity holds a dedicated outbound iMessage line."""
    number = getattr(identity, "imessage_number", None) or getattr(identity, "imessageNumber", None)
    if number is None:
        return False
    can_start = getattr(number, "can_start_conversations", None)
    if can_start is None:
        can_start = getattr(number, "canStartConversations", None)
    if isinstance(can_start, bool):
        return can_start
    number_type = number.get("type") if isinstance(number, dict) else getattr(number, "type", None)
    number_type = getattr(number_type, "value", number_type)
    return str(number_type or "").strip().lower() == "dedicated_outbound"
A2A_TURN_CONTEXT: ContextVar[Dict[str, Any] | None] = ContextVar(
    "inkbox_codex_a2a_turn",
    default=None,
)


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: JsonSchema


def _schema(properties: Dict[str, JsonSchema], required: List[str] | None = None) -> JsonSchema:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


def _schema_with_exactly_one_alias(
    properties: Dict[str, JsonSchema],
    *,
    required: List[str],
    left: str,
    right: str,
) -> JsonSchema:
    """Require one spelling of an aliased field, never neither or both."""
    schema = _schema(properties, required)
    schema["oneOf"] = [
        {"required": [left], "not": {"required": [right]}},
        {"required": [right], "not": {"required": [left]}},
    ]
    return schema


def _str(desc: str = "", *, max_length: int | None = None) -> JsonSchema:
    schema: JsonSchema = {"type": "string"}
    if desc:
        schema["description"] = desc
    if max_length is not None:
        schema["maxLength"] = max_length
    return schema


def _int(desc: str = "") -> JsonSchema:
    schema: JsonSchema = {"type": "integer"}
    if desc:
        schema["description"] = desc
    return schema


def _enum(values: List[str], desc: str = "") -> JsonSchema:
    schema = _str(desc)
    schema["enum"] = values
    return schema


def _bounded_int(minimum: int, maximum: int, desc: str = "") -> JsonSchema:
    schema = _int(desc)
    schema["minimum"] = minimum
    schema["maximum"] = maximum
    schema["default"] = 50
    return schema


def _str_list(desc: str = "") -> JsonSchema:
    schema: JsonSchema = {"type": "array", "items": {"type": "string"}}
    if desc:
        schema["description"] = desc
    return schema


TOOL_SPECS: List[ToolSpec] = [
    ToolSpec(
        "inkbox_whoami",
        "Show this agent's Inkbox identity: handle, email address, iMessage status, "
        "and its two calling lines (dedicated phone number vs shared iMessage line).",
        _schema({}),
    ),
    ToolSpec(
        "inkbox_send_email",
        "Send an email from this agent's Inkbox mailbox. Pass attachment_paths for local files.",
        _schema(
            {
                "to": _str("Recipient email address."),
                "subject": _str("Email subject."),
                "body": _str("Plain-text email body."),
                "attachment_paths": _str_list("Local file paths to attach."),
            },
            ["to", "subject", "body"],
        ),
    ),
    ToolSpec(
        "inkbox_send_sms",
        "Send an SMS/MMS from this agent's Inkbox phone number.",
        _schema(
            {
                "to": _str("E.164 recipient number or an existing text conversation id."),
                "text": _str("Message body, max 1600 chars.", max_length=SMS_MAX_LENGTH),
                "media_paths": _str_list("Local file paths to upload and attach."),
                "media_urls": _str_list("Already-hosted media URLs to attach."),
            },
            ["to", "text"],
        ),
    ),
    ToolSpec(
        "inkbox_send_imessage",
        "Send an iMessage. Reply into an existing 1:1 or group conversation with "
        "conversation_id. A dedicated outbound iMessage line may instead start one "
        "with to: a single E.164 recipient, or 2-8 to open a group; shared and "
        "dedicated inbound lines stay recipient-first.",
        _schema(
            {
                "conversation_id": _str("Existing iMessage conversation id. Mutually exclusive with to."),
                "to": {
                    "description": (
                        "One E.164 recipient, or 1-8 distinct recipients. Two or more "
                        "starts a group and needs a dedicated outbound iMessage line. "
                        "Mutually exclusive with conversation_id."
                    ),
                    "oneOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 8},
                    ],
                },
                "text": _str("Message body, max 18995 chars.", max_length=IMESSAGE_MAX_LENGTH),
                "media_path": _str("Optional local file path to upload and attach."),
            },
            ["text"],
        ),
    ),
    ToolSpec(
        "inkbox_place_call",
        "Place an outbound voice call. Calls can go out over two lines: your own "
        "dedicated phone number, or the shared Inkbox iMessage line you are already "
        "messaging the recipient on. Match the channel you're talking on — call "
        "SMS/phone contacts from your dedicated number, and call an iMessage contact "
        "over the shared iMessage line (set `origination` accordingly). The selected "
        "phone voice stack handles the call. Always pass purpose so the call starts "
        "with a concrete task; voicemail behavior comes from gateway configuration.",
        _schema_with_exactly_one_alias(
            {
                "to_number": _str("E.164 recipient number, e.g. +15551234567."),
                "toNumber": _str("Alias for to_number."),
                "purpose": _str("Why Codex is placing this call."),
                "origination": {
                    "type": "string",
                    "enum": ["dedicated_number", "shared_imessage_number"],
                    "description": (
                        "Which line to call from. Use \"dedicated_number\" to call from "
                        "your own phone number (the same line SMS/voice conversations "
                        "use). Use \"shared_imessage_number\" to call someone over the "
                        "shared iMessage line you are already messaging them on — this "
                        "only works if they are connected to you over iMessage "
                        "(otherwise the call is rejected). If omitted, it is resolved "
                        "automatically: the only available line, or the line matching "
                        "the current conversation's channel."
                    ),
                },
                "opening_message": _str("Optional exact first line to say on pickup."),
                "openingMessage": _str("Alias for opening_message."),
                "context": _str("Optional extra background for the live call."),
                "client_websocket_url": _str("Optional override for the call-media WebSocket URL."),
                "clientWebsocketUrl": _str("Alias for client_websocket_url."),
            },
            required=["purpose"],
            left="to_number",
            right="toNumber",
        ),
    ),
    ToolSpec(
        "inkbox_list_calls",
        "List recent phone calls on this agent's Inkbox number, newest first.",
        _schema(
            {
                "limit": _int("Maximum calls to return."),
                "offset": _int("Pagination offset."),
            }
        ),
    ),
    ToolSpec(
        "inkbox_get_call_transcript",
        "Fetch transcript segments for one phone call by call_id.",
        _schema({"call_id": _str("Call id from inkbox_list_calls.")}, ["call_id"]),
    ),
    ToolSpec(
        "inkbox_list_text_conversations",
        "List this agent's SMS conversations, newest first.",
        _schema({"limit": _int("Maximum conversations to return.")}),
    ),
    ToolSpec(
        "inkbox_get_text_conversation",
        "Fetch message history for one SMS conversation by conversation_id.",
        _schema(
            {
                "conversation_id": _str("Text conversation id."),
                "limit": _int("Maximum messages to return."),
            },
            ["conversation_id"],
        ),
    ),
    ToolSpec(
        "inkbox_list_imessage_conversations",
        "List this agent's iMessage conversations, newest first.",
        _schema({"limit": _int("Maximum conversations to return.")}),
    ),
    ToolSpec(
        "inkbox_get_imessage_conversation",
        "Fetch message history for one iMessage conversation by conversation_id.",
        _schema(
            {
                "conversation_id": _str("iMessage conversation id."),
                "limit": _int("Maximum messages to return."),
            },
            ["conversation_id"],
        ),
    ),
    ToolSpec(
        "inkbox_lookup_contact",
        "Reverse-lookup contacts by exactly one field.",
        _schema({
            "email": _str(),
            "phone": _str(),
            "email_domain": _str(),
            "email_contains": _str(),
            "phone_contains": _str(),
        }),
    ),
    ToolSpec(
        "inkbox_list_contacts",
        "Search the address book by free text.",
        _schema({
            "q": _str("Search query."),
            "order": _str("Sort order: recent or name."),
            "limit": _int("Maximum contacts to return."),
        }),
    ),
    ToolSpec(
        "inkbox_get_contact",
        "Fetch one contact's full record by contact id.",
        _schema({"contact_id": _str("Contact id.")}, ["contact_id"]),
    ),
    ToolSpec(
        "inkbox_create_contact",
        "Save a new contact in the address book.",
        _schema({
            "given_name": _str(),
            "family_name": _str(),
            "preferred_name": _str(),
            "company_name": _str(),
            "job_title": _str(),
            "notes": _str(),
            "emails": _str_list(),
            "phones": _str_list(),
        }),
    ),
    ToolSpec(
        "inkbox_update_contact",
        "Update an existing contact by id. Omitted fields are left unchanged.",
        _schema({
            "contact_id": _str("Contact id."),
            "given_name": _str(),
            "family_name": _str(),
            "preferred_name": _str(),
            "company_name": _str(),
            "job_title": _str(),
            "notes": _str(),
            "emails": _str_list(),
            "phones": _str_list(),
        }, ["contact_id"]),
    ),
    ToolSpec(
        "inkbox_delete_contact",
        "Remove a contact from the address book by contact id. Look it up first to confirm the target.",
        _schema({"contact_id": _str("Contact id.")}, ["contact_id"]),
    ),
    ToolSpec(
        "inkbox_a2a_call",
        "Send a task to an A2A 1.0 Agent Card.",
        _schema({
            "card_url": _str("Agent Card URL."),
            "text": _str("Task text."),
            "context_id": _str("Optional context to continue."),
            "task_id": _str("Optional task requesting more input."),
            "message_id": _str("Stable idempotency id."),
        }, ["card_url", "text"]),
    ),
    ToolSpec(
        "inkbox_a2a_check",
        "Fetch an A2A task, or wait until it stops.",
        _schema({
            "card_url": _str("Agent Card URL."),
            "task_id": _str("Remote task id."),
            "wait": {"type": "boolean"},
        }, ["card_url", "task_id"]),
    ),
    ToolSpec(
        "inkbox_a2a_reply",
        "Reply to a remote A2A task that requested more input.",
        _schema({
            "card_url": _str("Agent Card URL."),
            "task_id": _str("Remote task id."),
            "text": _str("Reply text."),
            "message_id": _str("Stable idempotency id."),
        }, ["card_url", "task_id", "text"]),
    ),
    ToolSpec(
        "inkbox_list_a2a_tasks",
        "List this identity's A2A task history. Direction defaults to inbound; "
        "use the optional participant, state, context, keyword, timestamp, and "
        "cursor filters to narrow or continue the result.",
        _schema({
            "direction": _enum(
                ["inbound", "outbound", "both"],
                "Optional history direction.",
            ),
            "requester_handle": _str("Optional requester identity handle."),
            "worker_handle": _str("Optional worker identity handle."),
            "state": _enum(
                [
                    "submitted",
                    "working",
                    "input_required",
                    "auth_required",
                    "completed",
                    "failed",
                    "canceled",
                    "rejected",
                ],
                "Optional task lifecycle state.",
            ),
            "context_id": _str("Optional A2A context id."),
            "query": _str("Optional keyword search across task messages."),
            "since": _str("Optional ISO 8601 lower timestamp bound."),
            "cursor": _str("Opaque next_cursor from the previous page."),
            "limit": _bounded_int(1, 100, "Page size from 1 to 100."),
        }),
    ),
    ToolSpec(
        "inkbox_list_a2a_messages",
        "List messages from this identity's inbound and outbound A2A history. "
        "Use the optional participant, task, context, role, keyword, timestamp, "
        "and cursor filters to narrow or continue the result.",
        _schema({
            "direction": _enum(
                ["inbound", "outbound", "both"],
                "Optional history direction.",
            ),
            "requester_handle": _str("Optional requester identity handle."),
            "worker_handle": _str("Optional worker identity handle."),
            "task_id": _str("Optional A2A task id."),
            "context_id": _str("Optional A2A context id."),
            "role": _enum(["caller", "agent"], "Optional A2A message role."),
            "query": _str("Optional keyword search across message text."),
            "since": _str("Optional ISO 8601 lower timestamp bound."),
            "cursor": _str("Opaque next_cursor from the previous page."),
            "limit": _bounded_int(1, 100, "Page size from 1 to 100."),
        }),
    ),
    ToolSpec(
        "inkbox_a2a_complete",
        "Complete the active inbound A2A task with a final answer.",
        _schema({"text": _str("Final answer.")}, ["text"]),
    ),
    ToolSpec(
        "inkbox_a2a_ask_caller",
        "Ask the caller for more input on the active inbound A2A task.",
        _schema({"text": _str("Question for the caller.")}, ["text"]),
    ),
    ToolSpec(
        "inkbox_a2a_fail",
        "Fail the active inbound A2A task with a reason.",
        _schema({"reason": _str("Failure reason.")}, ["reason"]),
    ),
]


def _json_safe(value: Any) -> Any:
    """Convert SDK dataclasses, UUIDs, datetimes, and enums into JSON-safe data."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_safe(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    return str(getattr(value, "value", value))


def _tool_result(data: Any) -> Dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(_json_safe(data), ensure_ascii=False),
            }
        ]
    }


def _tool_error(message: str, **fields: Any) -> Dict[str, Any]:
    payload = {"error": message, **fields}
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(_json_safe(payload), ensure_ascii=False),
            }
        ],
        "isError": True,
    }


def _tool_exception_fields(exc: Exception) -> Dict[str, Any]:
    """Project canonical SDK failure metadata without duplicating raw payloads."""
    fields: Dict[str, Any] = {}
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        fields["status_code"] = status
    detail = getattr(exc, "detail", None)
    if isinstance(detail, dict):
        code = detail.get("error") or detail.get("error_code") or detail.get("code")
        rule = detail.get("rule")
        if code:
            fields["error_code"] = str(code)
        if rule:
            fields["rule"] = str(rule)
    return fields


def _message_too_long_reason(channel: str, content: str, max_chars: int) -> str:
    char_count = len(content or "")
    return (
        f"{channel} text is {char_count} characters; maximum is {max_chars}. "
        f"Shorten it or split it into smaller {channel} messages."
    )


def _hosted_sms_context() -> Optional[Dict[str, Any]]:
    """Load the gateway-bound hosted SMS context for this MCP subprocess."""
    chat_id = (os.getenv("INKBOX_CODEX_CHAT_ID") or "").strip()
    if not chat_id:
        return None
    path = hosted_sms_turn_context_path(chat_id)
    try:
        value = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(
            "Hosted-call SMS safety state is unavailable; send blocked."
        ) from exc
    if not isinstance(value, dict):
        raise RuntimeError(
            "Hosted-call SMS safety state is unavailable; send blocked."
        )
    return value


def _settle_hosted_sms_context(context: Dict[str, Any], state: str) -> None:
    settle_hosted_sms_attempt(
        str(context.get("call_id") or ""),
        int(context.get("attempt") or 1),
        state,
    )


def _reserve_hosted_sms_target(
    target: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Reserve one exact-target hosted SMS or return a fail-closed tool result."""
    try:
        context = _hosted_sms_context()
    except RuntimeError as exc:
        return None, _tool_error(
            str(exc),
            error_code="hosted_sms_send_blocked",
        )
    if context is None:
        return None, None

    expected = str(context.get("remote_phone") or "").strip()
    if not expected or target != expected:
        try:
            _settle_hosted_sms_context(context, "terminal")
        except Exception:
            pass
        return None, _tool_error(
            "Hosted-call SMS target does not match the authoritative caller; "
            "send blocked.",
            error_code="hosted_sms_send_blocked",
        )
    try:
        reserved = reserve_hosted_sms_attempt(
            str(context.get("call_id") or ""),
            int(context.get("attempt") or 1),
            expected,
        )
    except Exception:
        try:
            _settle_hosted_sms_context(context, "terminal")
        except Exception:
            pass
        return None, _tool_error(
            "Hosted-call SMS safety state is unavailable; send blocked.",
            error_code="hosted_sms_send_blocked",
        )
    if not reserved:
        return None, _tool_error(
            "This hosted-call SMS attempt was already used; duplicate send blocked.",
            error_code="hosted_sms_duplicate_blocked",
        )
    return context, None


def _upload_media_url(identity: Any, path: str) -> str:
    resolved = Path(path).expanduser()
    upload = identity.upload_imessage_media(
        content=resolved.read_bytes(),
        filename=resolved.name,
        content_type=mimetypes.guess_type(resolved.name)[0],
    )
    return upload.media_url


def _append_query_param(raw_url: str, key: str, value: str) -> str:
    """Append or replace one query param while preserving the rest."""
    parts = urlparse(raw_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunparse(parts._replace(query=urlencode(query)))


def _current_channel_hint() -> str | None:
    """Which Inkbox channel is the current conversation happening on?

    The gateway records every session's last inbound modality in the channel
    hints file and stamps this tool process with the session's
    ``INKBOX_CODEX_CHAT_ID``, so an outbound call can follow the conversation's
    channel without the agent having to say so. Returns ``"imessage"`` |
    ``"dedicated"`` | ``None`` (unknown / not in a bridged session).
    """
    chat_id = (os.environ.get("INKBOX_CODEX_CHAT_ID") or "").strip()
    if not chat_id:
        return None
    try:
        hints = json.loads(channel_hints_path().read_text())
        mode = str((hints.get(chat_id) or {}).get("mode") or "").strip().lower()
    except Exception:
        return None
    if mode == "imessage":
        return "imessage"
    if mode in {"sms", "text", "voice", "phone"}:
        return "dedicated"
    return None


def _resolve_call_origination(identity: Any, explicit: str) -> str | None:
    """Pick which line an outbound call originates from.

    Calls can go out over two paths: the agent's own ``dedicated_number`` or
    the ``shared_imessage_number`` it's already messaging the recipient on.
    Resolution order:

    1. An explicit choice (from the agent) always wins.
    2. If only one path exists, use it (dedicated number but no iMessage →
       dedicated; iMessage enabled but no number → shared).
    3. If BOTH exist, follow the channel the current conversation is on — an
       iMessage turn calls over the shared iMessage line, an SMS/phone turn
       over the dedicated number.  This makes "call me" do the right thing
       without the agent having to specify the line.
    4. If both exist but we can't tell the channel, default to the dedicated
       number (the open line that can reach anyone).

    Returns ``None`` when neither path exists (nothing to call from).
    """
    explicit = (explicit or "").strip().lower()
    if explicit in {"dedicated_number", "shared_imessage_number"}:
        return explicit
    has_number = getattr(identity, "phone_number", None) is not None
    imessage_enabled = bool(getattr(identity, "imessage_enabled", False))
    if has_number and imessage_enabled:
        # Both lines available — follow the conversation's channel.
        return "shared_imessage_number" if _current_channel_hint() == "imessage" else "dedicated_number"
    if has_number:
        return "dedicated_number"
    if imessage_enabled:
        return "shared_imessage_number"
    return None


def _call_ws_url(identity: Any) -> str:
    """Find the gateway's call-media WebSocket URL for an outbound call."""
    # Identity-scoped inbound-call config is the canonical row (one row covers
    # both lines); older SDKs only stamp the number-scoped shim.
    get_config = getattr(identity, "get_incoming_call_action", None)
    if callable(get_config):
        try:
            config = get_config()
            ws_url = str(getattr(config, "client_websocket_url", "") or "").strip()
            if ws_url:
                return ws_url
        except Exception:
            pass
    phone = getattr(identity, "phone_number", None)
    ws_url = str(getattr(phone, "client_websocket_url", "") or "").strip()
    if ws_url:
        return ws_url
    tunnel = getattr(identity, "tunnel", None)
    host = str(getattr(tunnel, "public_host", "") or "").strip()
    if host:
        return f"wss://{host}{INKBOX_WS_PATH}"
    return ""


def _write_call_context(
    *, purpose: str, opening_message: str, context: str, to_number: str
) -> str:
    """Persist outbound-call context for the gateway to load on connect."""
    token = secrets.token_urlsafe(18)
    payload = {
        "created_at": time.time(),
        "purpose": purpose,
        "opening_message": opening_message,
        "context": context,
        "to_number": to_number,
    }
    (call_contexts_dir() / f"{token}.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )
    return token


def _hosted_call_reason(args: Dict[str, Any]) -> str:
    """Build a bounded Voice AI task brief from the call request."""
    parts = [str(args.get("purpose") or "").strip()]
    opening = str(args.get("opening_message") or args.get("openingMessage") or "").strip()
    context = str(args.get("context") or "").strip()
    if opening:
        parts.append(f"Opening guidance: {opening}")
    if context:
        parts.append(f"Context: {context}")
    return "\n\n".join(part for part in parts if part)[:4000]


async def call_inkbox_tool(client: Any, identity_handle: str, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Run one Inkbox MCP tool and return an MCP ``tools/call`` result."""

    args = dict(args or {})
    hosted_sms_context: Optional[Dict[str, Any]] = None

    if name == "inkbox_send_sms":
        text = str(args.get("text") or "")
        target = str(args.get("to") or "").strip()
        hosted_sms_context, blocked = _reserve_hosted_sms_target(target)
        if blocked is not None:
            return blocked
        if len(text) > SMS_MAX_LENGTH:
            if hosted_sms_context is not None:
                _settle_hosted_sms_context(hosted_sms_context, "recoverable")
            return _tool_error(
                _message_too_long_reason("SMS", text, SMS_MAX_LENGTH),
                error_code="sms_too_long",
                char_count=len(text),
                max_chars=SMS_MAX_LENGTH,
            )

    if name == "inkbox_send_imessage":
        text = str(args.get("text") or "")
        if len(text) > IMESSAGE_MAX_LENGTH:
            return _tool_error(
                _message_too_long_reason("iMessage", text, IMESSAGE_MAX_LENGTH),
                error_code="imessage_too_long",
                char_count=len(text),
                max_chars=IMESSAGE_MAX_LENGTH,
            )

    def _identity():
        return client.get_identity(identity_handle)

    def _run() -> Any:
        if name == "inkbox_whoami":
            identity = _identity()
            phone = identity.phone_number
            mailbox = identity.mailbox
            dedicated_number = getattr(phone, "number", None)
            imessage_enabled = bool(getattr(identity, "imessage_enabled", False))
            return {
                "handle": identity.agent_handle,
                "email": getattr(mailbox, "email_address", None),
                "phone": dedicated_number,
                "imessage_enabled": imessage_enabled,
                # Explicit labels so the agent describes its two lines
                # correctly: its OWN dedicated phone line vs the SHARED
                # iMessage line, whose number is never surfaced.
                "lines": {
                    "dedicated_phone_line": dedicated_number or "(none provisioned)",
                    "dedicated_phone_line_note": (
                        "Your own phone line for SMS and voice calls. Call from it with "
                        "origination=dedicated_number."
                    ),
                    "shared_imessage_line": "enabled" if imessage_enabled else "disabled",
                    "shared_imessage_line_note": (
                        "Voice + iMessage with people connected to you over iMessage. Its "
                        "number is managed by Inkbox and not shown. Call over it with "
                        "origination=shared_imessage_number."
                    ),
                },
            }

        if name == "inkbox_send_email":
            paths = args.get("attachment_paths") or []
            attachments = [file_to_email_attachment(str(p)) for p in paths] or None
            msg = _identity().send_email(
                to=[str(args["to"])],
                subject=str(args.get("subject") or ""),
                body_text=str(args.get("body") or ""),
                attachments=attachments,
            )
            return {"sent": True, "id": str(getattr(msg, "id", "")), "attachments": len(paths)}

        if name == "inkbox_send_sms":
            identity = _identity()
            kwargs: Dict[str, Any] = {"text": str(args.get("text") or "")}
            target = str(args.get("to") or "").strip()
            if target.startswith("+"):
                kwargs["to"] = target
            else:
                kwargs["conversation_id"] = target
            urls = [str(u) for u in (args.get("media_urls") or [])]
            for path in (args.get("media_paths") or []):
                urls.append(_upload_media_url(identity, str(path)))
            if urls:
                kwargs["media_urls"] = urls
            msg = identity.send_text(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", "")), "media": len(urls)}

        if name == "inkbox_send_imessage":
            text = str(args.get("text") or "")
            identity = _identity()
            conversation_id = str(args.get("conversation_id") or "").strip()
            to_list = _normalize_imessage_recipients(args.get("to"))
            if bool(to_list) == bool(conversation_id):
                raise ValueError("Specify exactly one of `to` or `conversation_id`.")
            if to_list is not None and not to_list:
                raise ValueError("`to` must include at least one recipient.")
            if to_list and len(to_list) > IMESSAGE_MAX_GROUP_RECIPIENTS:
                raise ValueError(
                    f"Inkbox iMessage groups support at most {IMESSAGE_MAX_GROUP_RECIPIENTS} recipients."
                )
            if to_list and len(set(to_list)) != len(to_list):
                raise ValueError("iMessage recipients must be distinct.")
            if len(to_list or []) > 1 and not _identity_can_start_imessage_conversations(identity):
                raise ValueError(
                    "Starting an iMessage group requires a dedicated outbound iMessage "
                    "line. Reply to an existing group with conversation_id."
                )
            kwargs: Dict[str, Any] = {"text": text}
            if conversation_id:
                kwargs["conversation_id"] = conversation_id
            else:
                kwargs["to"] = to_list[0] if len(to_list) == 1 else to_list
            media_path = str(args.get("media_path") or "").strip()
            if media_path:
                kwargs["media_urls"] = [_upload_media_url(identity, media_path)]
            msg = identity.send_imessage(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", ""))}

        if name == "inkbox_place_call":
            snake_to_number = str(args.get("to_number") or "").strip()
            camel_to_number = str(args.get("toNumber") or "").strip()
            if bool(snake_to_number) == bool(camel_to_number):
                raise ValueError(
                    "Specify exactly one of to_number or toNumber "
                    "(E.164, e.g. +15551234567)"
                )
            to_number = snake_to_number or camel_to_number
            purpose = str(args.get("purpose") or "").strip()
            if not purpose:
                raise ValueError(
                    "purpose is required so the live call opens with context"
                )
            cfg = read_config()
            if cfg.voice_stack_invalid_value:
                raise ValueError(
                    f"Invalid INKBOX_VOICE_STACK={cfg.voice_stack_invalid_value!r}; rerun setup"
                )
            if cfg.voicemail_detection not in {"enabled", "disabled"}:
                raise ValueError(
                    "INKBOX_VOICEMAIL_DETECTION must be enabled or disabled"
                )
            identity = _identity()
            # Resolve the outbound line (dedicated number vs shared iMessage line).
            origination = _resolve_call_origination(
                identity, str(args.get("origination") or "")
            )
            if origination is None:
                raise RuntimeError(
                    "this identity can't place calls: it has no dedicated phone "
                    "number and iMessage is not enabled. Provision a number or "
                    "enable iMessage first."
                )
            hosted = cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI
            if hosted:
                call = identity.place_call(
                    to_number=to_number,
                    origination=origination,
                    mode="hosted_agent",
                    reason=_hosted_call_reason(args),
                    voicemail_detection=cfg.voicemail_detection,
                )
                return {
                    "placed": True,
                    "id": str(getattr(call, "id", "")),
                    "to": to_number,
                    "origination": origination,
                    "mode": _json_safe(getattr(call, "mode", None) or "hosted_agent"),
                    "hosted_agent_authority_mode": _json_safe(
                        getattr(call, "hosted_agent_authority_mode", None)
                    ),
                    "voicemail_detection": _json_safe(
                        getattr(call, "voicemail_detection", None) or cfg.voicemail_detection
                    ),
                    "status": _json_safe(getattr(call, "status", None)),
                }

            # An explicit override wins; otherwise resolve from the identity.
            ws_url = str(
                args.get("client_websocket_url")
                or args.get("clientWebsocketUrl")
                or ""
            ).strip()
            if not ws_url:
                ws_url = _call_ws_url(identity)
            if not ws_url:
                raise RuntimeError(
                    "no call-media WebSocket URL available; start the Inkbox "
                    "Codex gateway first"
                )
            token = _write_call_context(
                purpose=purpose,
                opening_message=str(
                    args.get("opening_message") or args.get("openingMessage") or ""
                ).strip(),
                context=str(args.get("context") or "").strip(),
                to_number=to_number,
            )
            ws_url = _append_query_param(ws_url, "context_token", token)
            call_kwargs = {
                "to_number": to_number,
                "origination": origination,
                "client_websocket_url": ws_url,
                "mode": "client_websocket",
                "voicemail_detection": cfg.voicemail_detection,
            }
            try:
                call = identity.place_call(**call_kwargs)
            except TypeError:
                raise RuntimeError(
                    "configured call options require inkbox SDK 0.5.9 or newer"
                )
            except Exception as exc:
                if "no_shared_connection" in str(exc):
                    # Surface a legible reason the agent can act on.
                    raise RuntimeError(
                        "Can't place a shared iMessage-line call: this person "
                        "isn't connected to you over iMessage yet. They need to "
                        "message your iMessage number first. To call from your "
                        "own phone number instead, set origination to "
                        '"dedicated_number".'
                    ) from exc
                raise
            return {
                "placed": True,
                "id": str(getattr(call, "id", "")),
                "to": to_number,
                "origination": origination,
                "mode": _json_safe(getattr(call, "mode", None) or "client_websocket"),
                "voicemail_detection": _json_safe(
                    getattr(call, "voicemail_detection", None) or cfg.voicemail_detection
                ),
                "context_token": token,
                "status": _json_safe(getattr(call, "status", None)),
            }

        if name == "inkbox_list_calls":
            return _identity().list_calls(
                limit=int(args.get("limit") or 25),
                offset=int(args.get("offset") or 0),
            )

        if name == "inkbox_get_call_transcript":
            call_id = str(args.get("call_id") or "").strip()
            if not call_id:
                raise ValueError("call_id is required (get one from inkbox_list_calls)")
            return _identity().list_transcripts(call_id)

        if name == "inkbox_list_text_conversations":
            return _identity().list_text_conversations(limit=int(args.get("limit") or 25))

        if name == "inkbox_get_text_conversation":
            return _identity().get_text_conversation(
                str(args["conversation_id"]), limit=int(args.get("limit") or 50)
            )

        if name == "inkbox_list_imessage_conversations":
            return _identity().list_imessage_conversations(limit=int(args.get("limit") or 25))

        if name == "inkbox_get_imessage_conversation":
            return _identity().get_imessage_conversation(
                str(args["conversation_id"]), limit=int(args.get("limit") or 50)
            )

        if name == "inkbox_lookup_contact":
            keys = ("email", "phone", "email_domain", "email_contains", "phone_contains")
            supplied = {k: str(args[k]) for k in keys if args.get(k)}
            if len(supplied) != 1:
                raise ValueError("pass exactly one of: " + ", ".join(keys))
            return client.contacts.lookup(**supplied)

        if name == "inkbox_list_contacts":
            return client.contacts.list(
                q=str(args["q"]) if args.get("q") else None,
                order=str(args["order"]) if args.get("order") else None,
                limit=int(args.get("limit") or 25),
            )

        if name == "inkbox_get_contact":
            return client.contacts.get(str(args["contact_id"]))

        if name == "inkbox_create_contact":
            from inkbox import ContactEmail, ContactPhone

            emails = [
                ContactEmail(label=None, value=str(e), is_primary=(i == 0))
                for i, e in enumerate(args.get("emails") or [])
            ]
            phones = [
                ContactPhone(label=None, value=str(p), is_primary=(i == 0))
                for i, p in enumerate(args.get("phones") or [])
            ]
            return client.contacts.create(
                given_name=str(args["given_name"]) if args.get("given_name") else None,
                family_name=str(args["family_name"]) if args.get("family_name") else None,
                preferred_name=str(args["preferred_name"]) if args.get("preferred_name") else None,
                company_name=str(args["company_name"]) if args.get("company_name") else None,
                job_title=str(args["job_title"]) if args.get("job_title") else None,
                notes=str(args["notes"]) if args.get("notes") else None,
                emails=emails or None,
                phones=phones or None,
            )

        if name == "inkbox_update_contact":
            from inkbox import ContactEmail, ContactPhone

            kwargs: Dict[str, Any] = {}
            for field in ("given_name", "family_name", "preferred_name", "company_name", "job_title", "notes"):
                if args.get(field):
                    kwargs[field] = str(args[field])
            if args.get("emails") is not None and args.get("emails") != "":
                kwargs["emails"] = [
                    ContactEmail(label=None, value=str(e), is_primary=(i == 0))
                    for i, e in enumerate(args.get("emails") or [])
                ]
            if args.get("phones") is not None and args.get("phones") != "":
                kwargs["phones"] = [
                    ContactPhone(label=None, value=str(p), is_primary=(i == 0))
                    for i, p in enumerate(args.get("phones") or [])
                ]
            return client.contacts.update(str(args["contact_id"]), **kwargs)

        if name == "inkbox_delete_contact":
            client.contacts.delete(str(args["contact_id"]))
            return {"deleted": str(args["contact_id"])}

        if name in {"inkbox_list_a2a_tasks", "inkbox_list_a2a_messages"}:
            limit = int(args.get("limit") or 50)
            if not 1 <= limit <= 100:
                raise ValueError("limit must be between 1 and 100")
            options: Dict[str, Any] = {
                "direction": str(args.get("direction") or "").strip() or None,
                "requester_handle": (
                    str(args.get("requester_handle") or "").strip() or None
                ),
                "worker_handle": (
                    str(args.get("worker_handle") or "").strip() or None
                ),
                "context_id": str(args.get("context_id") or "").strip() or None,
                "q": str(args.get("query") or "").strip() or None,
                "since": str(args.get("since") or "").strip() or None,
                "cursor": str(args.get("cursor") or "").strip() or None,
                "limit": limit,
            }
            identity = _identity()
            if name == "inkbox_list_a2a_tasks":
                options["state"] = str(args.get("state") or "").strip() or None
                return identity.a2a_tasks(**options)
            options["task_id"] = str(args.get("task_id") or "").strip() or None
            options["role"] = str(args.get("role") or "").strip() or None
            return identity.a2a_messages(**options)

        if name in {"inkbox_a2a_call", "inkbox_a2a_check", "inkbox_a2a_reply"}:
            identity = _identity()
            a2a = identity.a2a_client()
            try:
                target = a2a.fetch_card(str(args["card_url"]))
                if name == "inkbox_a2a_check":
                    if args.get("wait"):
                        return a2a.wait(target, str(args["task_id"]))
                    return a2a.get_task(target, str(args["task_id"]))
                task_id = str(args.get("task_id") or "") or None
                existing = find_by_task(task_id) if task_id else None
                message_id = str(args.get("message_id") or uuid.uuid4())
                pending_key = record_before_send(
                    identity_id=str(identity.id),
                    rpc_url=str(
                        getattr(target, "rpc_url", None)
                        or target["rpc_url"]
                    ),
                    card_url=str(args["card_url"]),
                    message_id=message_id,
                    context_id=(
                        args.get("context_id")
                        or (existing or {}).get("context_id")
                        or None
                    ),
                    task_id=task_id,
                    session_key=(
                        os.environ.get("INKBOX_CODEX_CHAT_ID")
                        or (existing or {}).get("session_key")
                        or None
                    ),
                )
                result = a2a.send(
                    target,
                    text=str(args["text"]),
                    context_id=args.get("context_id") or None,
                    task_id=task_id,
                    message_id=message_id,
                )
                task = getattr(result, "task", None)
                if task is None and isinstance(result, dict):
                    task = result.get("task")
                result_task_id = getattr(task, "id", None)
                context_id = getattr(task, "context_id", None)
                if isinstance(task, dict):
                    result_task_id = task.get("id")
                    context_id = task.get("context_id") or task.get("contextId")
                if result_task_id and context_id:
                    promote_after_send(
                        pending_key,
                        context_id=str(context_id),
                        task_id=str(result_task_id),
                    )
                return result
            finally:
                a2a.close()

        if name in {
            "inkbox_a2a_complete",
            "inkbox_a2a_ask_caller",
            "inkbox_a2a_fail",
        }:
            context = A2A_TURN_CONTEXT.get()
            context_path = None
            if context is None:
                chat_id = (os.environ.get("INKBOX_CODEX_CHAT_ID") or "").strip()
                if chat_id:
                    context_path = a2a_turn_context_path(chat_id)
                    try:
                        loaded = json.loads(context_path.read_text())
                        context = loaded if isinstance(loaded, dict) else None
                    except (FileNotFoundError, json.JSONDecodeError):
                        context = None
            if context is None:
                raise RuntimeError(
                    "This tool is only available during an inbound A2A task"
                )
            intent = {
                "inkbox_a2a_complete": "complete",
                "inkbox_a2a_ask_caller": "ask_caller",
                "inkbox_a2a_fail": "fail",
            }[name]
            text = str(args["reason"] if name == "inkbox_a2a_fail" else args["text"])
            task_id = str(context["task_id"])
            gate = acquire_a2a_progress_gate(task_id)
            try:
                fence_a2a_progress(
                    task_id,
                    str(context.get("message_id") or ""),
                )
                result = _identity().a2a_reply(
                    task_id,
                    intent=intent,
                    text=text,
                )
            finally:
                release_a2a_progress_gate(gate)
            context["reply_intent_committed"] = True
            context["reply_intent"] = intent
            if context_path is not None:
                tmp = context_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(context, sort_keys=True) + "\n")
                tmp.chmod(0o600)
                os.replace(tmp, context_path)
                context_path.chmod(0o600)
            return result

        raise ValueError(f"unknown Inkbox tool: {name}")

    try:
        result = await asyncio.to_thread(_run)
        if hosted_sms_context is not None:
            _settle_hosted_sms_context(hosted_sms_context, "success")
        return _tool_result(result)
    except Exception as exc:
        fields = _tool_exception_fields(exc)
        if hosted_sms_context is not None:
            _settle_hosted_sms_context(
                hosted_sms_context,
                sms_tool_failure_kind(message=str(exc), **fields),
            )
        return _tool_error(str(exc), **fields)


def _place_call_tool_entry(voice_stack: VoiceStack) -> Dict[str, Any]:
    hosted = voice_stack is VoiceStack.INKBOX_VOICE_AI
    properties: Dict[str, Any] = {
        "to_number": _str("E.164 recipient number, e.g. +15551234567."),
        "toNumber": _str("Alias for to_number."),
        "purpose": _str(
            "Why Codex is placing this call; becomes Inkbox Voice AI's task brief."
            if hosted else
            "Why Codex is placing this call; loaded before the live greeting."
        ),
        "origination": {
            "type": "string",
            "enum": ["dedicated_number", "shared_imessage_number"],
            "description": (
                "Which line to call from. Use dedicated_number for the agent's own "
                "phone line or shared_imessage_number for an existing iMessage contact."
            ),
        },
        "opening_message": _str(
            "Optional opening guidance included in the Voice AI task brief."
            if hosted else "Optional exact first line to say on pickup."
        ),
        "openingMessage": _str("Alias for opening_message."),
        "context": _str(
            "Optional concise background included in the Voice AI task brief."
            if hosted else "Optional extra background for the local voice agent."
        ),
    }
    if not hosted:
        properties.update({
            "client_websocket_url": _str("Optional override for the call-media WebSocket URL."),
            "clientWebsocketUrl": _str("Alias for client_websocket_url."),
        })
    return {
        "name": "inkbox_place_call",
        "description": (
            "Ask Inkbox Voice AI to place an outbound call and complete the stated "
            "task over either of the identity's two lines. Codex is notified "
            "after the call ends."
            if hosted else
            "Place an outbound voice call over either of the identity's two lines, "
            "handled by this Codex agent through the configured local voice stack."
        ),
        "inputSchema": _schema_with_exactly_one_alias(
            properties,
            required=["purpose"],
            left="to_number",
            right="toNumber",
        ),
    }


def mcp_tool_list() -> List[Dict[str, Any]]:
    """Return MCP ``tools/list`` entries for every Inkbox tool."""
    cfg = read_config()
    return [
        _place_call_tool_entry(cfg.voice_stack)
        if spec.name == "inkbox_place_call"
        else {
            "name": spec.name,
            "description": spec.description,
            "inputSchema": spec.input_schema,
        }
        for spec in TOOL_SPECS
    ]


def build_inkbox_mcp_server_config(cfg: Any) -> Tuple[Dict[str, Any], List[str]]:
    """Build Codex app-server config for the Inkbox stdio MCP server."""
    env = {
        "INKBOX_API_KEY": cfg.api_key,
        "INKBOX_IDENTITY": cfg.identity,
        "INKBOX_BASE_URL": cfg.base_url,
        "INKBOX_VOICE_STACK": cfg.voice_stack.value,
        "INKBOX_VOICE_AI_AUTHORITY_MODE": cfg.voice_ai_authority_mode,
        "INKBOX_VOICEMAIL_DETECTION": cfg.voicemail_detection,
    }
    # Keep the tool process on the same state dir (call contexts, channel
    # hints) when the operator moved it.
    home = os.getenv("INKBOX_CODEX_HOME") or ""
    if home:
        env["INKBOX_CODEX_HOME"] = home
    server = {
        "enabled": True,
        "required": True,
        "command": sys.executable,
        "args": ["-m", "inkbox_codex.mcp_stdio"],
        "env": env,
        "startup_timeout_sec": 10.0,
        "tool_timeout_sec": 60.0,
    }
    if cfg.auto_approve_inkbox_tools:
        server["default_tools_approval_mode"] = "approve"
    tool_names = [f"mcp__inkbox__{spec.name}" for spec in TOOL_SPECS]
    return server, tool_names

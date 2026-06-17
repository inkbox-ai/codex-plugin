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
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from .media import file_to_email_attachment
except ImportError:  # pragma: no cover - direct local import/test fallback
    from media import file_to_email_attachment


JsonSchema = Dict[str, Any]


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


def _str(desc: str = "") -> JsonSchema:
    schema: JsonSchema = {"type": "string"}
    if desc:
        schema["description"] = desc
    return schema


def _int(desc: str = "") -> JsonSchema:
    schema: JsonSchema = {"type": "integer"}
    if desc:
        schema["description"] = desc
    return schema


def _str_list(desc: str = "") -> JsonSchema:
    schema: JsonSchema = {"type": "array", "items": {"type": "string"}}
    if desc:
        schema["description"] = desc
    return schema


TOOL_SPECS: List[ToolSpec] = [
    ToolSpec(
        "inkbox_whoami",
        "Show this agent's Inkbox identity: handle, email address, phone number, and iMessage status.",
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
                "text": _str("Message body."),
                "media_paths": _str_list("Local file paths to upload and attach."),
                "media_urls": _str_list("Already-hosted media URLs to attach."),
            },
            ["to", "text"],
        ),
    ),
    ToolSpec(
        "inkbox_send_imessage",
        "Send an iMessage to an existing iMessage conversation.",
        _schema(
            {
                "conversation_id": _str("Existing iMessage conversation id."),
                "text": _str("Message body."),
                "media_path": _str("Optional local file path to upload and attach."),
            },
            ["conversation_id", "text"],
        ),
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
        "inkbox_export_contact_vcard",
        "Export one contact as a vCard 4.0 string by contact id.",
        _schema({"contact_id": _str("Contact id.")}, ["contact_id"]),
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


def _tool_error(message: str) -> Dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps({"error": message}, ensure_ascii=False),
            }
        ],
        "isError": True,
    }


def _upload_media_url(identity: Any, path: str) -> str:
    resolved = Path(path).expanduser()
    upload = identity.upload_imessage_media(
        content=resolved.read_bytes(),
        filename=resolved.name,
        content_type=mimetypes.guess_type(resolved.name)[0],
    )
    return upload.media_url


async def call_inkbox_tool(client: Any, identity_handle: str, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Run one Inkbox MCP tool and return an MCP ``tools/call`` result."""

    args = dict(args or {})

    def _identity():
        return client.get_identity(identity_handle)

    def _run() -> Any:
        if name == "inkbox_whoami":
            identity = _identity()
            phone = identity.phone_number
            mailbox = identity.mailbox
            return {
                "handle": identity.agent_handle,
                "email": getattr(mailbox, "email_address", None),
                "phone": getattr(phone, "number", None),
                "imessage_enabled": getattr(identity, "imessage_enabled", False),
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
            identity = _identity()
            kwargs: Dict[str, Any] = {
                "conversation_id": str(args["conversation_id"]),
                "text": str(args.get("text") or ""),
            }
            media_path = str(args.get("media_path") or "").strip()
            if media_path:
                kwargs["media_urls"] = [_upload_media_url(identity, media_path)]
            msg = identity.send_imessage(**kwargs)
            return {"sent": True, "id": str(getattr(msg, "id", ""))}

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

        if name == "inkbox_export_contact_vcard":
            return {"vcard": client.contacts.vcards.export_vcard(str(args["contact_id"]))}

        raise ValueError(f"unknown Inkbox tool: {name}")

    try:
        return _tool_result(await asyncio.to_thread(_run))
    except Exception as exc:
        return _tool_error(str(exc))


def mcp_tool_list() -> List[Dict[str, Any]]:
    """Return MCP ``tools/list`` entries for every Inkbox tool."""
    return [
        {
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
    }
    server = {
        "enabled": True,
        "required": True,
        "command": sys.executable,
        "args": ["-m", "inkbox_codex.mcp_stdio"],
        "env": env,
        "startup_timeout_sec": 10.0,
        "tool_timeout_sec": 60.0,
    }
    tool_names = [f"mcp__inkbox__{spec.name}" for spec in TOOL_SPECS]
    return server, tool_names

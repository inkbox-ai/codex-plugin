"""Translate MCP approval and form requests into explicit text-channel choices."""

from __future__ import annotations

import json
import math
import re
from typing import Any


_REPLIES = {
    "allow": {
        "1", "y", "yes", "ok", "okay", "sure", "allow", "approve", "approved",
        "yes approved", "yes please", "yes proceed", "yes please proceed", "proceed",
        "go", "go ahead", "yes go ahead", "yes please go ahead", "allow once",
        "you may proceed", "you are allowed to proceed", "yes you are allowed to proceed",
        "i approve", "i approve this request",
    },
    "session": {"2", "session", "allow for this session", "yes for this session", "allow this session"},
    "always": {"4", "always", "always allow", "allow always", "yes always", "allow permanently"},
    "deny": {"3", "n", "no", "deny", "decline", "disallow", "block", "no thanks", "do not allow", "don't allow", "dont allow"},
    "cancel": {
        "/stop", "stop", "cancel", "cancel task", "cancel the task", "cancel my task",
        "cancel my request", "please cancel my request", "please cancel the request",
        "please cancel the task", "please stop this task", "please stop", "please cancel",
        "stop the task",
    },
}


def parse_approval_reply(text: str) -> str | None:
    """Recognize complete decisions, never an affirmative substring in a task."""
    normalized = " ".join((text or "").strip().lower().replace("’", "'").replace(",", " ").rstrip(".! ").split())
    return next((choice for choice, words in _REPLIES.items() if normalized in words), None)


def _meta(params: dict[str, Any]) -> dict[str, Any]:
    value = params.get("_meta")
    return value if isinstance(value, dict) else {}


def _url_request(params: dict[str, Any]) -> bool:
    return params.get("mode") == "url"


def is_approval(params: dict[str, Any]) -> bool:
    """Keep structured input forms separate from message-only approvals."""
    if params.get("mode") not in (None, "form"):
        return False
    schema = params.get("requestedSchema")
    if schema is not None:
        if not isinstance(schema, dict) or schema.get("type") != "object" or schema.get("properties") != {}:
            return False
        return not schema.get("required")
    meta = _meta(params)
    if meta.get("codex_approval_kind") in ("mcp_tool_call", "browser_auth", "tool_suggestion") or meta.get("codex_request_type") == "approval_request":
        return True
    if "requestedSchema" in params:
        return True
    message = " ".join(str(params.get("message") or params.get("prompt") or "").split())
    return re.fullmatch(r"Allow [^?]{1,200} to [^?]{1,500}\?", message, re.IGNORECASE) is not None


def approval_choices(params: dict[str, Any]) -> frozenset[str]:
    """Expose only persistence scopes advertised by this particular request."""
    persist = _meta(params).get("persist")
    advertised = [persist] if isinstance(persist, str) else persist if isinstance(persist, list) else []
    return frozenset({"allow", "deny"} | {value for value in advertised if isinstance(value, str) and value in {"session", "always"}})


def _line(value: Any, limit: int = 120) -> str:
    rendered = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    rendered = " ".join(rendered.split())
    return rendered if len(rendered) <= limit else rendered[:limit - 1] + "…"


def _action_details(params: dict[str, Any]) -> list[str]:
    """Use explicit display metadata, not raw tool arguments or credentials."""
    display = _meta(params).get("tool_params_display")
    if not isinstance(display, list):
        return []
    result = []
    for item in display:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or "value" not in item:
            continue
        name = item.get("display_name") or item["name"]
        if not isinstance(name, str) or not name.strip():
            continue
        sensitive = re.search(r"password|secret|token|credential|api.?key|authorization", item["name"] + " " + name, re.IGNORECASE)
        value = "[hidden]" if sensitive else _line(item["value"])
        result.append(f"{_line(name, 60)}: {value}")
        if len(result) == 3:
            break
    return result


_ANNOTATIONS = {"title", "description", "default"}
_FIELD_KEYS = _ANNOTATIONS | {"type", "enum", "enumNames", "oneOf", "minimum", "maximum", "minLength", "maxLength"}
_ROOT_KEYS = _ANNOTATIONS | {"type", "properties", "required", "additionalProperties", "$schema"}


def _options(field: dict[str, Any]) -> list[Any] | None:
    if "enum" in field:
        return field["enum"]
    if "oneOf" in field:
        return [entry["const"] for entry in field["oneOf"]]
    return None


def _type_matches(value: Any, kind: str) -> bool:
    if kind == "string":
        return isinstance(value, str)
    if kind == "boolean":
        return isinstance(value, bool)
    if kind in {"number", "integer"}:
        if type(value) is int:
            return True
        return type(value) is float and math.isfinite(value) and (kind == "number" or value.is_integer())
    return False


def _fields(params: dict[str, Any]) -> tuple[dict[str, Any], list[str]] | None:
    """Validate the supported primitive schema subset before asking for input."""
    schema = params.get("requestedSchema")
    if not isinstance(schema, dict) or schema.get("type") != "object" or set(schema) - _ROOT_KEYS:
        return None
    fields, required = schema.get("properties"), schema.get("required", [])
    if not isinstance(fields, dict) or not fields or not isinstance(required, list) or any(not isinstance(key, str) or key not in fields for key in required):
        return None
    if not isinstance(schema.get("additionalProperties", False), bool):
        return None
    for field in fields.values():
        if not isinstance(field, dict) or set(field) - _FIELD_KEYS or field.get("type") not in ("string", "boolean", "number", "integer"):
            return None
        if "enum" in field and (not isinstance(field["enum"], list) or not field["enum"] or "oneOf" in field):
            return None
        if "oneOf" in field and (not isinstance(field["oneOf"], list) or not field["oneOf"] or any(not isinstance(option, dict) or "const" not in option or set(option) - {"const", "title", "description"} for option in field["oneOf"])):
            return None
        options = _options(field)
        if options is not None and any(not _type_matches(value, field["type"]) for value in options):
            return None
        for key in ("minimum", "maximum", "minLength", "maxLength"):
            if key in field:
                length = key.endswith("Length")
                if length and (field["type"] != "string" or type(field[key]) is not int or field[key] < 0):
                    return None
                if not length and (field["type"] not in {"number", "integer"} or not _type_matches(field[key], "number")):
                    return None
    return fields, required


def format_elicitation(params: dict[str, Any]) -> str:
    """Show concrete approval scopes or the form values the server expects."""
    message = str(params.get("message") or params.get("prompt") or "Codex needs your input.")
    if _url_request(params):
        return f"{message}\n\nOpen this URL and complete the requested step yourself:\n{params.get('url') or '(URL unavailable)'}\nReply DONE after completing it, or /stop to cancel the task."
    if is_approval(params):
        lines = [message, *_action_details(params), "", "1 — Allow once (YES)"]
        choices = approval_choices(params)
        if "session" in choices:
            lines.append("2 — Allow for this session (SESSION)")
        lines.append("3 — Deny this request (NO)")
        if "always" in choices:
            lines.append("4 — Always allow across sessions (ALWAYS)")
        lines.append("/stop — Cancel the entire task")
        return "\n".join(lines)
    form = _fields(params)
    if form is None:
        return f"{message}\n\nThis input form cannot be answered over text. Use Codex directly, or reply /stop to cancel the task."
    fields, required = form
    lines = [message, ""]
    for name, field in fields.items():
        label = str(field.get("title") or name)
        description = f" — {_line(field['description'], 180)}" if field.get("description") else ""
        lines.append(f"{name}: {label} ({field['type']}, {'required' if name in required else 'optional'}){description}")
        options = _options(field)
        if options is not None:
            for index, option in enumerate(options):
                titles = field.get("enumNames")
                title = field["oneOf"][index].get("title") if "oneOf" in field else titles[index] if isinstance(titles, list) and index < len(titles) else None
                lines.append(f"  {json.dumps(option, ensure_ascii=False)}" + (f" — {_line(title)}" if title else ""))
    if len(fields) == 1 and next(iter(fields.values()))["type"] == "boolean":
        lines.append("Reply YES or NO, or a JSON object using the field name and true or false.")
    else:
        lines.append("Reply with a JSON object using these field names." if len(fields) > 1 else "Reply with the field value, or a JSON object using its field name.")
    lines.append("/stop — Cancel the entire task")
    return "\n".join(lines)


def _valid_value(value: Any, field: dict[str, Any]) -> bool:
    if not _type_matches(value, field["type"]):
        return False
    options = _options(field)
    if options is not None and value not in options:
        return False
    for key, lower in (("minimum", True), ("maximum", False), ("minLength", True), ("maxLength", False)):
        if key in field:
            actual = len(value) if key.endswith("Length") else value
            if actual < field[key] if lower else actual > field[key]:
                return False
    return True


def elicitation_response(params: dict[str, Any], reply: str | None) -> dict[str, Any] | None:
    """Encode valid replies; return None to clarify rather than grant by default."""
    if reply is None:
        return {"action": "cancel", "content": None}
    decision = parse_approval_reply(reply)
    if decision == "cancel":
        return {"action": "cancel", "content": None}
    if _url_request(params):
        if decision == "deny":
            return {"action": "decline", "content": None}
        if params.get("url") and reply.strip().lower().rstrip(".!") in {"done", "confirm", "confirmed", "completed"}:
            return {"action": "accept", "content": None}
        return None
    if params.get("mode") not in (None, "form"):
        return None
    if is_approval(params):
        if decision not in approval_choices(params):
            return None
        response = {"action": "decline" if decision == "deny" else "accept", "content": None}
        if decision in {"session", "always"}:
            response["_meta"] = {"persist": decision}
        return response
    form = _fields(params)
    if form is None:
        return None
    fields, required = form
    try:
        value = json.loads(reply)
    except (ValueError, TypeError):
        value = reply.strip()
    if not isinstance(value, dict):
        if len(fields) != 1:
            return None
        name = next(iter(fields))
        if fields[name]["type"] == "string":
            value = value if isinstance(value, str) else reply.strip()
        elif fields[name]["type"] == "boolean" and isinstance(value, str):
            value = {"yes": True, "y": True, "no": False, "n": False}.get(value.lower(), value)
        value = {name: value}
    if set(value) - set(fields) or any(key not in value for key in required):
        return None
    if any(not _valid_value(answer, fields[name]) for name, answer in value.items()):
        return None
    return {"action": "accept", "content": value}

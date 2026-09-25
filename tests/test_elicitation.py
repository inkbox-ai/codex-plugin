"""Text approvals preserve native scopes and do not invent form answers."""

import pytest

from inkbox_codex.elicitation import (
    approval_choices,
    elicitation_response,
    format_elicitation,
    is_approval,
    parse_approval_reply,
)


def approval(persist=None):
    meta = {"codex_approval_kind": "mcp_tool_call"}
    if persist is not None:
        meta["persist"] = persist
    return {
        "message": 'Allow Example App to use "Calendar"?',
        "requestedSchema": {"type": "object", "properties": {}},
        "_meta": meta,
    }


def form(properties, required=None):
    return {
        "message": "Choose the requested values.",
        "requestedSchema": {"type": "object", "properties": properties, "required": list(properties) if required is None else required},
    }


@pytest.mark.parametrize(("text", "decision"), [
    ("1", "allow"), ("Yes, you are allowed to proceed!", "allow"),
    ("yes, approved", "allow"), ("yes please go ahead", "allow"),
    ("2", "session"), ("SESSION", "session"), ("allow for this session", "session"),
    ("3", "deny"), ("No thanks", "deny"), ("don't allow", "deny"),
    ("4", "always"), ("ALWAYS!", "always"), ("always allow", "always"),
    ("/stop", "cancel"), ("Please cancel my request", "cancel"),
    ("Please cancel the request", "cancel"), ("Please cancel the task", "cancel"),
    ("Please stop this task", "cancel"),
    ("yes, but don't send it", None), ("yes and delete everything", None),
    ("How is the weather?", None), ("always deny", None), ("", None),
    ("Yes, you are allowed to use my computer and the example application", None),
])
def test_decisions_are_whole_bounded_phrases(text, decision):
    assert parse_approval_reply(text) == decision


@pytest.mark.parametrize("message", [
    'Allow the example MCP server to run tool "list calendars"?',
    'Allow Example App to use "Calendar"?',
    'Allow Example Service to read your calendar?',
])
def test_legacy_and_app_approval_prompts(message):
    assert is_approval({"message": message})
    assert elicitation_response({"message": message}, "2") == {"action": "decline", "content": None}


@pytest.mark.parametrize("params", [
    {"message": "Which calendar should I use?"},
    {"message": "Allow this? And what is your time zone?"},
    {"message": "Please allow Example App to use Calendar?"},
    {"message": "Allow Example App to use Calendar?", "requestedSchema": {"type": "array"}},
    {"mode": "url", "message": "Allow Example App to use Calendar?"},
])
def test_non_approval_questions_and_unsupported_shapes_are_not_guessed(params):
    assert not is_approval(params)


def test_explicit_message_only_form_uses_action_not_invented_text_field():
    params = {"message": "Continue?", "requestedSchema": {"type": "object", "properties": {}}}
    assert is_approval(params)
    assert elicitation_response(params, "yes") == {"action": "accept", "content": None}


@pytest.mark.parametrize(("persist", "choices"), [
    (None, {"allow", "deny"}), ("session", {"allow", "deny", "session"}),
    ("always", {"allow", "deny", "always"}),
    (["session", "always"], {"allow", "deny", "session", "always"}),
    (True, {"allow", "deny"}), ({"always": True}, {"allow", "deny"}),
    ([{}, None, "forever", "always"], {"allow", "deny", "always"}),
])
def test_scopes_are_only_advertised_native_capabilities(persist, choices):
    params = approval(persist)
    assert approval_choices(params) == frozenset(choices)
    prompt = format_elicitation(params)
    assert "1 — Allow once" in prompt
    assert f"{3 if 'session' in choices else 2} — Deny this request" in prompt
    assert ("2 — Allow for this session" in prompt) == ("session" in choices)
    assert ("Always allow across sessions" in prompt) == ("always" in choices)
    assert "/stop — Cancel the entire task" in prompt


@pytest.mark.parametrize(("reply", "persist"), [("2", "session"), ("SESSION", "session"), ("4", "always"), ("ALWAYS", "always")])
def test_native_persistence_metadata_is_returned(reply, persist):
    assert elicitation_response(approval(["session", "always"]), reply) == {
        "action": "accept", "content": None, "_meta": {"persist": persist},
    }


@pytest.mark.parametrize(("supported", "reply"), [(None, "3"), (None, "4"), (None, "session"), ("session", "always"), ("always", "session")])
def test_unavailable_choice_never_downgrades_to_accept_once(supported, reply):
    assert elicitation_response(approval(supported), reply) is None


@pytest.mark.parametrize(("persist", "labels", "decisions"), [
    (None, ["Allow once (YES)", "Deny this request (NO)"], ["allow", "deny"]),
    ("session", ["Allow once (YES)", "Allow for this session (SESSION)", "Deny this request (NO)"], ["allow", "session", "deny"]),
    ("always", ["Allow once (YES)", "Deny this request (NO)", "Always allow across sessions (ALWAYS)"], ["allow", "deny", "always"]),
    (["always", "session"], ["Allow once (YES)", "Allow for this session (SESSION)", "Deny this request (NO)", "Always allow across sessions (ALWAYS)"], ["allow", "session", "deny", "always"]),
])
def test_visible_options_are_consecutive_and_numbers_execute_the_displayed_decision(persist, labels, decisions):
    params = approval(persist)
    numbered = [line for line in format_elicitation(params).splitlines() if line[:1].isdigit()]
    assert numbered == [f"{index} — {label}" for index, label in enumerate(labels, 1)]
    for index, decision in enumerate(decisions, 1):
        expected = {"action": "decline" if decision == "deny" else "accept", "content": None}
        if decision in {"session", "always"}:
            expected["_meta"] = {"persist": decision}
        for reply in (str(index), f" {index}! "):
            assert elicitation_response(params, reply) == expected
    for reply in ("0", str(len(labels) + 1), "999"):
        assert elicitation_response(params, reply) is None


@pytest.mark.parametrize(("reply", "expected"), [(None, "cancel"), ("Please cancel my request", "cancel"), ("no", "decline"), ("yes", "accept")])
def test_approval_timeout_cancel_deny_and_accept_remain_distinct(reply, expected):
    assert elicitation_response(approval(), reply) == {"action": expected, "content": None}


def test_action_details_are_bounded_and_use_explicit_display_metadata_only():
    params = approval()
    params["_meta"].update({
        "tool_params": {"private_input": "not for display"},
        "tool_params_display": [
            {"name": "destination", "display_name": "Calendar", "value": "Team\nPlanning"},
            {"name": "note", "value": "x" * 500},
            {"name": "api_key", "value": "synthetic-secret"},
            {"name": "ignored", "value": "fourth field"},
        ],
    })
    text = format_elicitation(params)
    assert "Calendar: Team Planning" in text
    assert "x" * 120 not in text
    assert "api_key: [hidden]" in text
    assert "synthetic-secret" not in text
    assert "not for display" not in text
    assert "fourth field" not in text


def test_nonempty_schema_is_a_form_even_with_approval_metadata():
    params = form({"locale": {"type": "string"}})
    params["_meta"] = {"codex_approval_kind": "mcp_tool_call", "persist": "always"}
    assert not is_approval(params)
    assert "4 — Always" not in format_elicitation(params)
    assert elicitation_response(params, "no") == {"action": "accept", "content": {"locale": "no"}}


@pytest.mark.parametrize(("schema", "reply", "expected"), [
    ({"type": "string"}, "123", "123"),
    ({"type": "string"}, "no", "no"),
    ({"type": "string"}, '"quoted text"', "quoted text"),
    ({"type": "integer", "minimum": 1, "maximum": 5}, "3", 3),
    ({"type": "number"}, "1.25", 1.25),
    ({"type": "boolean"}, "false", False),
    ({"type": "boolean"}, "YES", True),
    ({"type": "boolean"}, "No", False),
    ({"type": "string", "enum": ["personal", "work"]}, "work", "work"),
    ({"type": "string", "oneOf": [{"const": "work", "title": "Work calendar"}]}, "work", "work"),
])
def test_single_primitive_form_answer_uses_actual_field_name(schema, reply, expected):
    assert elicitation_response(form({"answer": schema}), reply) == {"action": "accept", "content": {"answer": expected}}


def test_multifield_schema_shows_fields_options_and_accepts_json_object():
    params = form({
        "calendar": {"type": "string", "enum": ["personal", "work"], "enumNames": ["Personal calendar", "Work calendar"]},
        "attendees": {"type": "integer", "minimum": 1},
        "notify": {"type": "boolean"},
    }, required=["calendar", "attendees"])
    text = format_elicitation(params)
    assert '"work" — Work calendar' in text
    assert "attendees (integer, required)" in text
    assert "notify (boolean, optional)" in text
    assert "JSON object" in text
    assert elicitation_response(params, '{"calendar":"work","attendees":2}') == {
        "action": "accept", "content": {"calendar": "work", "attendees": 2},
    }
    for reply in ('yes', '{"calendar":"work"}', '{"calendar":"work","attendees":2,"extra":true}'):
        assert elicitation_response(params, reply) is None


@pytest.mark.parametrize(("field", "reply"), [
    ({"type": "integer"}, "true"), ({"type": "integer"}, "1.25"),
    ({"type": "number"}, "NaN"), ({"type": "number"}, "Infinity"),
    ({"type": "integer", "minimum": 2}, "1"),
    ({"type": "number", "maximum": 5}, "6"),
    ({"type": "boolean"}, "maybe"),
    ({"type": "string", "minLength": 2}, '"a"'),
    ({"type": "string", "maxLength": 2}, '"long"'),
    ({"type": "string", "enum": ["work"]}, "personal"),
])
def test_invalid_form_values_require_clarification(field, reply):
    assert elicitation_response(form({"answer": field}), reply) is None


@pytest.mark.parametrize("schema", [
    {"type": "array"}, {"type": "object", "properties": {"nested": {"type": "object"}}},
    {"type": "object", "properties": {"answer": {"type": "string", "pattern": "^a"}}},
    {"type": "object", "properties": {"answer": {"type": "string", "enum": "invalid"}}},
    {"type": "object", "properties": {"answer": {"type": "string", "oneOf": [{"title": "missing value"}]}}},
    {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["missing"]},
    {"type": "object", "properties": {"answer": {"type": "string"}}, "oneOf": []},
])
def test_unsupported_or_malformed_forms_fail_closed(schema):
    params = {"message": "Input needed", "requestedSchema": schema}
    assert elicitation_response(params, "yes") is None
    assert "cannot be answered over text" in format_elicitation(params)


def test_missing_schema_does_not_fabricate_text_response():
    params = {"message": "Which calendar?"}
    assert elicitation_response(params, "work") is None


def test_boolean_form_instructs_yes_no_and_does_not_coerce_json_string():
    params = form({"confirmed": {"type": "boolean"}})
    assert "Reply YES or NO" in format_elicitation(params)
    assert elicitation_response(params, "No") == {"action": "accept", "content": {"confirmed": False}}
    assert elicitation_response(params, '{"confirmed":"yes"}') is None


@pytest.mark.parametrize("params", [
    {"message": "Input needed", "_meta": {"codex_approval_kind": []}},
    {"message": "Input needed", "requestedSchema": {"type": "object", "properties": {"answer": {"type": []}}}},
    {"message": "Input needed", "requestedSchema": {"type": "object", "properties": {"answer": {"type": {}}}}},
    {"mode": "future", "message": "Allow Example App to use Calendar?"},
])
def test_malformed_or_unknown_protocol_shapes_do_not_grant_or_raise(params):
    assert not is_approval(params)
    assert elicitation_response(params, "yes") is None


def test_integer_field_handles_large_json_integer_without_overflow():
    digits = "9" * 400
    assert elicitation_response(form({"answer": {"type": "integer"}}), digits) == {
        "action": "accept", "content": {"answer": int(digits)},
    }


def test_url_flow_requires_explicit_completion_not_generic_approval():
    params = {"mode": "url", "message": "Connect your calendar", "url": "https://example.com/connect", "_meta": {"persist": "always"}}
    text = format_elicitation(params)
    assert "https://example.com/connect" in text
    assert "complete the requested step yourself" in text
    assert "Always" not in text
    assert elicitation_response(params, "yes") is None
    assert elicitation_response(params, "always") is None
    assert elicitation_response(params, "DONE") == {"action": "accept", "content": None}
    assert elicitation_response(params, "no") == {"action": "decline", "content": None}
    assert elicitation_response({"mode": "url", "message": "Connect your calendar"}, "done") is None

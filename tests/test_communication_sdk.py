"""Agent tools exercise the released SDK with an isolated HTTP boundary."""

import asyncio
import json
from types import SimpleNamespace as NS
from uuid import UUID

import httpx
import pytest
from inkbox import Inkbox, _config as sdk_config

from inkbox_codex import tools
from inkbox_codex.config import BridgeConfig
from inkbox_codex.mcp_stdio import InkboxMcpServer


IDENTITY, MAILBOX, PHONE, MESSAGE, THREAD = [str(UUID(int=i)) for i in range(1, 6)]
NOW = "2026-01-01T00:00:00+00:00"
ADDRESS = "agent@example.com"
MAIL = f"/api/v1/mail/mailboxes/{ADDRESS}"


def email(**changes):
    return dict(id=MESSAGE, mailbox_id=MAILBOX, thread_id=THREAD, message_id="<source@example.com>",
        from_address="sender@example.com", to_addresses=[ADDRESS], subject="Question", snippet="Please help",
        direction="inbound", status="received", is_read=False, is_starred=False, has_attachments=False,
        created_at=NOW, **changes)


def call_record():
    return dict(id=MESSAGE, remote_phone_number="+15550000002", direction="outbound", status="in_progress",
        created_at=NOW, updated_at=NOW, connections=[], post_call_action_items=[])


@pytest.fixture
def sdk(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("INKBOX_BASE_URL", "https://example.com")
    monkeypatch.setenv("INKBOX_VAULT_KEY", "")
    for key in ("INKBOX_CODEX_CHAT_ID", "INKBOX_CODEX_HOST_EPOCH", "INKBOX_IMESSAGE_THREADED_REPLIES", "INKBOX_SLACK_ENABLED"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(sdk_config, "_CONFIG_PATH", tmp_path / "unused-config")
    state = NS(requests=[], responses={}, whatsapp_configured=False, whatsapp_enabled=False)

    def handle(request):
        assert request.url.host == "example.com"
        state.requests.append(request)
        if request.url.path == "/api/v1/identities/agent":
            return httpx.Response(200, json=dict(id=IDENTITY, organization_id="org_example", agent_handle="agent",
                created_at=NOW, updated_at=NOW, imessage_enabled=True, whatsapp_configured=state.whatsapp_configured,
                whatsapp_enabled=state.whatsapp_enabled,
                mailbox=dict(id=MAILBOX, email_address=ADDRESS, created_at=NOW, updated_at=NOW),
                phone_number=dict(id=PHONE, number="+15550000001", type="local", status="active",
                    incoming_call_action="auto_reject", created_at=NOW, updated_at=NOW)))
        response = state.responses[(request.method, request.url.path)]
        return response(request) if callable(response) else httpx.Response(200, json=response)

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(handle))
    state.client = Inkbox(api_key="ApiKey_synthetic_test_only", base_url="https://example.com")
    yield state
    state.client.close()


def invoke(sdk, name, **args):
    response = asyncio.run(tools.call_inkbox_tool(sdk.client, "agent", name, args))
    return response, json.loads(response["content"][0]["text"])


def success(sdk, name, **args):
    response, value = invoke(sdk, name, **args)
    assert not response.get("isError"), value
    return value


def posts(sdk):
    return [r for r in sdk.requests if r.method == "POST"]


def test_email_recipients_html_attachments_key_and_pending_state(sdk, tmp_path):
    sdk.responses["POST", MAIL + "/messages"] = {**email(), "direction": "outbound", "status": "queued"}
    attachment = tmp_path / "hello.txt"
    attachment.write_text("hello")
    result = success(sdk, "inkbox_send_email", to=["one@example.com", "two@example.com"], subject="Hello",
        body_html="<p>Hello</p>", cc=["copy@example.com"], bcc=["private@example.com"],
        attachment_paths=[str(attachment)], idempotency_key="email-1")
    sent = posts(sdk)[0]
    body = json.loads(sent.content)
    assert body["recipients"]["to"] == ["one@example.com", "two@example.com"]
    assert body["recipients"]["cc"] == ["copy@example.com"] and body["recipients"]["bcc"] == ["private@example.com"]
    assert body["body_html"] == "<p>Hello</p>" and body["attachments"][0]["filename"] == "hello.txt"
    assert sent.headers["Idempotency-Key"] == result["idempotency_key"] == "email-1"
    assert result["accepted"] and result["status"] == "queued" and "sent" not in result


@pytest.mark.parametrize("reply_all,operation,suffix", [(False, "mail.send", ""), (True, "mail.reply_all", f"/{MESSAGE}/reply-all")])
def test_reply_preserves_source_identifier_and_uses_right_operation(sdk, reply_all, operation, suffix):
    sdk.responses["GET", MAIL + f"/messages/{MESSAGE}"] = email()
    sdk.responses["POST", MAIL + "/messages" + suffix] = email()
    result = success(sdk, "inkbox_reply_email", message_id=MESSAGE, body="Answer", reply_all=reply_all, idempotency_key="reply-1")
    assert result["operation"] == operation
    body = json.loads(posts(sdk)[0].content)
    if not reply_all:
        assert body["recipients"]["to"] == ["sender@example.com"]
        assert body["in_reply_to_message_id"] == "<source@example.com>"
        assert body["subject"] == "Re: Question"
    else:
        assert "to" not in body and "cc" not in body


def test_sender_reply_rejects_outbound_source_without_sending(sdk):
    sdk.responses["GET", MAIL + f"/messages/{MESSAGE}"] = {**email(), "direction": "outbound"}
    response, value = invoke(sdk, "inkbox_reply_email", message_id=MESSAGE, body="Answer")
    assert response["isError"] and "inbound" in value["error"]
    assert not posts(sdk)


def test_forward_preserves_attachment_and_recipient_options(sdk, tmp_path):
    sdk.responses["POST", MAIL + f"/messages/{MESSAGE}/forward"] = email()
    path = tmp_path / "note.txt"
    path.write_text("note")
    result = success(sdk, "inkbox_forward_email", message_id=MESSAGE, to="next@example.com", mode="wrapped",
        body="FYI", include_original_attachments=False, attachment_paths=[str(path)], idempotency_key="forward-1")
    body = json.loads(posts(sdk)[0].content)
    assert body["recipients"]["to"] == ["next@example.com"] and body["mode"] == "wrapped"
    assert body["include_original_attachments"] is False
    assert body["additional_attachments"][0]["filename"] == "note.txt"
    assert result["operation"] == "mail.forward"


def test_email_filtered_empty_page_can_continue_and_never_fetches_bodies(sdk):
    def page(request):
        assert request.url.params["direction"] == "inbound"
        assert request.url.params["start_datetime"] == "2026-01-01"
        assert request.url.params["tz"] == "UTC"
        if not request.url.params.get("cursor"):
            return httpx.Response(200, json={"items": [{**email(), "is_read": True}], "has_more": True, "next_cursor": "page2"})
        return httpx.Response(200, json={"items": [email()], "has_more": False, "next_cursor": None})
    sdk.responses["GET", MAIL + "/messages"] = page
    args = dict(limit=1, unread_only=True, query="question", direction="inbound", start_datetime="2026-01-01", tz="UTC")
    first = success(sdk, "inkbox_list_emails", **args)
    second = success(sdk, "inkbox_list_emails", offset=first["next_offset"], **args)
    assert first == {"items": [], "scanned": 1, "next_offset": 1}
    assert second["items"][0]["id"] == MESSAGE and second["next_offset"] is None
    assert all(r.url.path in {"/api/v1/identities/agent", MAIL + "/messages"} for r in sdk.requests)


@pytest.mark.parametrize("name,args", [
    ("inkbox_list_emails", {"limit": 0}),
    ("inkbox_list_calls", {"offset": -1}),
    ("inkbox_get_imessage_conversation", {"conversation_id": THREAD, "limit": True}),
])
def test_invalid_pagination_fails_without_history_reads(sdk, name, args):
    response, value = invoke(sdk, name, **args)
    assert response["isError"] and "integer" in value["error"]
    assert all(r.url.path == "/api/v1/identities/agent" for r in sdk.requests)


def test_email_read_and_partial_mark_results(sdk):
    sdk.responses["GET", MAIL + f"/messages/{MESSAGE}"] = {**email(), "body_text": "Full body", "attachment_metadata": [{"filename": "a.txt"}]}
    result = success(sdk, "inkbox_get_email", message_id=MESSAGE)
    assert result["body_text"] == "Full body" and result["attachment_metadata"][0]["filename"] == "a.txt"
    sdk.responses["PATCH", MAIL + f"/messages/{MESSAGE}"] = email()
    sdk.responses["PATCH", MAIL + f"/messages/{THREAD}"] = lambda r: httpx.Response(403, json={"detail": "unavailable"})
    marked = success(sdk, "inkbox_mark_emails", message_ids=[MESSAGE, THREAD], is_read=False)
    assert not marked["complete"]
    assert marked["results"][0] == {"message_id": MESSAGE, "is_read": False}
    assert marked["results"][1]["status_code"] == 403
    assert [json.loads(r.content) for r in sdk.requests if r.method == "PATCH"] == [{"is_read": False}] * 2


@pytest.mark.parametrize("operation,kind,sender", [
    ("mail.send", "mailbox", MAILBOX), ("mail.reply_all", "mailbox", MAILBOX), ("mail.forward", "mailbox", MAILBOX),
    ("text.send", "phone_number", PHONE), ("imessage.send", "imessage_identity", IDENTITY),
])
def test_lookup_binds_sender_to_identity_and_never_writes(sdk, operation, kind, sender):
    sdk.responses["GET", MAIL] = dict(id=MAILBOX, email_address=ADDRESS, created_at=NOW, updated_at=NOW,
        status="active", filter_mode="blacklist")
    sdk.responses["GET", "/api/v1/message-sends/lookup"] = {"message_id": MESSAGE}
    result = success(sdk, "inkbox_lookup_send", operation=operation, idempotency_key="original")
    request = sdk.requests[-1]
    assert dict(request.url.params) == {"sender_kind": kind, "sender_id": sender, "operation": operation}
    assert request.headers["Idempotency-Key"] == "original" and result["message_id"] == MESSAGE
    assert all(r.method == "GET" for r in sdk.requests)


def test_uncertain_send_preserves_original_key_without_replacement(sdk):
    sdk.responses["POST", MAIL + "/messages"] = lambda r: httpx.Response(409, json={"detail": {"error": "result_unavailable"}})
    response, result = invoke(sdk, "inkbox_send_email", to="next@example.com", subject="Hello", body="Hello", idempotency_key="original")
    assert response["isError"] and result["idempotency_key"] == "original"
    assert result["error_code"] == "result_unavailable" and result["operation"] == "mail.send"
    assert len(posts(sdk)) == 1 and posts(sdk)[0].headers["Idempotency-Key"] == "original"


def test_successful_but_undecodable_send_keeps_key(sdk):
    sdk.responses["POST", MAIL + "/messages"] = {"id": MESSAGE}
    response, result = invoke(sdk, "inkbox_send_email", to="next@example.com", subject="Hello", body="Hello")
    assert response["isError"] and result["idempotency_key"] == posts(sdk)[0].headers["Idempotency-Key"]
    assert len(posts(sdk)) == 1


def test_response_notices_reach_agent_without_changing_plain_results(sdk):
    notice = {"code": "example_notice", "level": "info", "message": "Synthetic advisory"}
    sdk.responses["GET", MAIL + f"/messages/{MESSAGE}"] = lambda r: httpx.Response(200, json=email(),
        headers={"Inkbox-Notices": json.dumps([notice])})
    result = success(sdk, "inkbox_get_email", message_id=MESSAGE)
    assert result["notices"] == [notice] and result["data"]["id"] == MESSAGE


def test_failure_keeps_notices_and_structured_error(sdk):
    notice = {"code": "example_notice", "level": "info", "message": "Synthetic advisory"}
    sdk.responses["POST", MAIL + "/messages"] = lambda r: httpx.Response(422,
        json={"detail": {"error": "invalid_input"}}, headers={"Inkbox-Notices": json.dumps([notice])})
    response, result = invoke(sdk, "inkbox_send_email", to="next@example.com", subject="Hello", body="Hello", idempotency_key="original")
    assert response["isError"] and result["notices"] == [notice]
    assert result["error_code"] == "invalid_input" and result["idempotency_key"] == "original"


def test_imessage_history_uses_message_list_with_identity_group_and_date_filters(sdk):
    sdk.responses["GET", "/api/v1/imessage/messages"] = []
    result = success(sdk, "inkbox_get_imessage_conversation", conversation_id=THREAD, limit=12, offset=24,
        start_datetime="2026-01-01", end_datetime="2026-02-01", tz="UTC")
    assert result == []
    assert dict(sdk.requests[-1].url.params) == dict(agent_identity_id=IDENTITY, conversation_id=THREAD,
        limit="12", offset="24", include_groups="true", start_datetime="2026-01-01", end_datetime="2026-02-01", tz="UTC")


@pytest.mark.parametrize("name,path,params", [
    ("inkbox_list_text_conversations", f"/api/v1/phone/numbers/{PHONE}/texts/conversations", {"include_groups": "true"}),
    ("inkbox_list_imessage_conversations", "/api/v1/imessage/conversations", {"include_groups": "true", "agent_identity_id": IDENTITY}),
    ("inkbox_list_calls", "/api/v1/phone/calls", {"agent_identity_id": IDENTITY}),
])
def test_list_filters_reach_sdk(sdk, name, path, params):
    sdk.responses["GET", path] = []
    assert success(sdk, name, limit=10, offset=20, start_datetime="2026-01-01") == []
    assert dict(sdk.requests[-1].url.params) == {"limit": "10", "offset": "20", "start_datetime": "2026-01-01", **params}


def test_call_reads_and_hangup_preserve_pending_teardown(sdk):
    sdk.responses["GET", f"/api/v1/phone/calls/{MESSAGE}"] = call_record()
    sdk.responses["POST", f"/api/v1/phone/calls/{MESSAGE}/hangup"] = call_record()
    read = success(sdk, "inkbox_get_call", call_id=MESSAGE)
    ended = success(sdk, "inkbox_hangup_call", call_id=MESSAGE)
    assert read["connections"] == []
    assert ended["status"] == "in_progress" and "ended" not in ended


def test_call_activity_and_transcript_preserve_unknown_outcomes_and_attribution(sdk):
    path = f"/api/v1/phone/calls/{MESSAGE}"
    sdk.responses["GET", path + "/tool-invocations"] = dict(items=[dict(id=THREAD, call_id=MESSAGE,
        tool_name="lookup", status="unknown", started_at=None, observed_at=NOW, result=None,
        source="external_mcp", observation_coverage="partial", result_available=False)],
        limit=10, offset=20, has_more=True, incomplete=True)
    sdk.responses["GET", path + "/transcripts"] = [dict(id=THREAD, call_id=MESSAGE, seq=1, ts_ms=0,
        party="remote", text="Hello", phone_number="+15550000002", created_at=NOW)]
    activity = success(sdk, "inkbox_list_call_tool_activity", call_id=MESSAGE, limit=10, offset=20)
    assert activity["incomplete"] and activity["has_more"]
    assert activity["items"][0]["status"] == "unknown" and activity["items"][0]["result_available"] is False
    assert dict(sdk.requests[-1].url.params) == {"limit": "10", "offset": "20"}
    transcript = success(sdk, "inkbox_get_call_transcript", call_id=MESSAGE)
    assert transcript[0]["phone_number"] == "+15550000002"


def test_thread_read_preserves_order_without_marking_messages(sdk):
    sdk.responses["GET", MAIL + f"/threads/{THREAD}"] = dict(id=THREAD, mailbox_id=MAILBOX,
        subject="Question", message_count=2, last_message_at=NOW, created_at=NOW,
        messages=[email(), {**email(), "id": PHONE}])
    thread = success(sdk, "inkbox_get_email_thread", thread_id=THREAD)
    assert [m["id"] for m in thread["messages"]] == [MESSAGE, PHONE]
    assert all(r.method == "GET" for r in sdk.requests)


def sms():
    return dict(id=MESSAGE, direction="outbound", local_phone_number="+15550000001", remote_phone_number=None,
        text="Hello", type="mms", is_read=True, created_at=NOW, updated_at=NOW, delivery_status="queued",
        conversation_id=THREAD)


@pytest.mark.parametrize("target", [{"to": ["+15550000002", "+15550000003"]}, {"conversation_id": THREAD}, {"to": THREAD}])
def test_sms_group_and_conversation_sends_preserve_key_and_queued_status(sdk, target):
    sdk.responses["POST", f"/api/v1/phone/numbers/{PHONE}/texts"] = sms()
    result = success(sdk, "inkbox_send_sms", **target, text="Hello", idempotency_key="sms-1")
    body = json.loads(posts(sdk)[0].content)
    expected = target if isinstance(target.get("to"), list) else {"conversation_id": THREAD}
    assert body == {**expected, "text": "Hello"}
    assert result["accepted"] and result["delivery_status"] == "queued"
    assert result["idempotency_key"] == posts(sdk)[0].headers["Idempotency-Key"] == "sms-1"


def test_sms_media_failure_retains_uploaded_url_for_retry(sdk, monkeypatch):
    uploads = []
    monkeypatch.setattr(tools, "_upload_media_url", lambda identity, path: uploads.append(path) or "https://example.com/media")
    sdk.responses["POST", f"/api/v1/phone/numbers/{PHONE}/texts"] = lambda r: httpx.Response(409, json={"detail": {"error": "result_unavailable"}})
    response, failed = invoke(sdk, "inkbox_send_sms", to="+15550000002", media_paths=["file.png"], idempotency_key="media-1")
    assert response["isError"] and failed["media_urls"] == ["https://example.com/media"]
    sdk.responses["POST", f"/api/v1/phone/numbers/{PHONE}/texts"] = sms()
    result = success(sdk, "inkbox_send_sms", to="+15550000002", media_urls=failed["media_urls"], idempotency_key=failed["idempotency_key"])
    assert len(uploads) == 1 and result["idempotency_key"] == "media-1"
    assert posts(sdk)[0].content == posts(sdk)[1].content


def test_sms_reads_and_conversation_pagination(sdk):
    path = f"/api/v1/phone/numbers/{PHONE}/texts"
    sdk.responses["GET", path + f"/{MESSAGE}"] = sms()
    sdk.responses["GET", path + f"/conversations/{THREAD}"] = [sms()]
    assert success(sdk, "inkbox_get_sms", message_id=MESSAGE)["delivery_status"] == "queued"
    assert success(sdk, "inkbox_get_text_conversation", conversation_id=THREAD, limit=5, offset=10)[0]["id"] == MESSAGE
    assert dict(sdk.requests[-1].url.params) == {"limit": "5", "offset": "10"}


def imessage():
    return dict(id=MESSAGE, conversation_id=THREAD, direction="outbound", content="Hello", message_type="message",
        service="imessage", is_read=True, status="pending", created_at=NOW, updated_at=NOW)


def test_plain_imessage_send_and_read_without_threading(sdk):
    sdk.responses["POST", "/api/v1/imessage/messages"] = {"message": imessage()}
    sdk.responses["GET", f"/api/v1/imessage/messages/{MESSAGE}"] = imessage()
    result = success(sdk, "inkbox_send_imessage", conversation_id=THREAD, text="Hello", idempotency_key="plain-1")
    assert result["status"] == "pending" and result["idempotency_key"] == "plain-1"
    assert json.loads(posts(sdk)[0].content) == {"conversation_id": THREAD, "text": "Hello"}
    message = success(sdk, "inkbox_get_imessage", message_id=MESSAGE)
    assert message["id"] == MESSAGE and sdk.requests[-1].url.params["agent_identity_id"] == IDENTITY


def test_imessage_reactions_keep_resolved_target_and_remove_own_receipt(sdk):
    sdk.responses["POST", "/api/v1/imessage/reactions"] = dict(id=PHONE, conversation_id=THREAD,
        target_message_id=MAILBOX, direction="outbound", reaction="eyes", remote_number="+15550000002",
        part_index=2, created_at=NOW, updated_at=NOW)
    sdk.responses["DELETE", f"/api/v1/imessage/reactions/{PHONE}"] = lambda r: httpx.Response(204)
    result = success(sdk, "inkbox_react_imessage", message_id=MESSAGE, reaction="eyes", part_index=0)
    assert result["target_message_id"] == MAILBOX and result["part_index"] == 2
    assert json.loads(posts(sdk)[0].content) == {"message_id": MESSAGE, "reaction": "eyes", "part_index": 0}
    removed = success(sdk, "inkbox_remove_imessage_reaction", reaction_id=result["id"])
    assert removed == {"removed": PHONE}


def test_stopped_source_cannot_submit_new_mutation(sdk, monkeypatch):
    from inkbox_codex import turn_authority
    cfg = BridgeConfig(identity="agent", base_url="https://example.com")
    epoch = turn_authority.new_host("chat", cfg)
    token = turn_authority.begin("chat", cfg, epoch, "email")
    turn_authority.retire(token)
    monkeypatch.setenv("INKBOX_CODEX_CHAT_ID", "chat")
    monkeypatch.setenv(turn_authority.HOST_ENV, epoch)
    response, failed = invoke(sdk, "inkbox_hangup_call", call_id=MESSAGE)
    assert response["isError"] and "active gateway source" in failed["error"]
    assert not sdk.requests


@pytest.mark.parametrize("configured,enabled,count", [(False, False, 0), (False, True, 0), (True, False, 15), (True, True, 15)])
def test_mcp_registration_uses_configuration_not_sending_access(sdk, configured, enabled, count):
    sdk.whatsapp_configured, sdk.whatsapp_enabled = configured, enabled
    server = InkboxMcpServer()
    server.api_key, server.identity, server._client = "ApiKey_synthetic_test_only", "agent", sdk.client
    result = asyncio.run(server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
    names = [t["name"] for t in result["result"]["tools"]]
    assert sum("whatsapp" in name for name in names) == count
    _, config_names = tools.build_inkbox_mcp_server_config(BridgeConfig(), whatsapp_configured=configured)
    assert set(config_names) == {"mcp__inkbox__" + name for name in names}


def test_mcp_refreshes_registration_and_failure_keeps_other_tools(sdk):
    server = InkboxMcpServer()
    server.api_key, server.identity, server._client = "ApiKey_synthetic_test_only", "agent", sdk.client
    def names():
        value = asyncio.run(server.handle({"id": 1, "method": "tools/list"}))
        return {t["name"] for t in value["result"]["tools"]}
    assert "inkbox_send_whatsapp" not in names()
    sdk.whatsapp_configured = True
    assert "inkbox_send_whatsapp" in names()
    server._client = NS(get_identity=lambda handle: (_ for _ in ()).throw(RuntimeError("offline")))
    available = names()
    assert "inkbox_send_whatsapp" not in available and "inkbox_list_emails" in available

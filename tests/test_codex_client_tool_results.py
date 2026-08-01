import asyncio
import json

import pytest

from inkbox_codex.codex_client import CodexAppServerClient, _TurnCapture
from inkbox_codex.config import BridgeConfig


def _client():
    return CodexAppServerClient(
        BridgeConfig(identity="agent"),
        developer_instructions="test",
    )


def test_mcp_completed_item_captures_only_settlement_fields():
    async def scenario():
        client = _client()
        loop = asyncio.get_running_loop()
        capture = _TurnCapture("thread-1", "turn-1", loop.create_future())
        client._turns["turn-1"] = capture

        client._handle_notification({
            "method": "item/completed",
            "params": {
                "turnId": "turn-1",
                "item": {
                    "type": "mcpToolCall",
                    "server": "inkbox",
                    "tool": "inkbox_send_sms",
                    "status": "completed",
                    "arguments": {"to": "+15167251294", "text": "private body"},
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": '{"sent":true,"id":"private-provider-id"}',
                        }],
                    },
                },
            },
        })
        client._handle_notification({
            "method": "turn/completed",
            "params": {"turnId": "turn-1", "turn": {"status": "completed"}},
        })

        result = await capture.future
        assert result.text == ""
        assert len(result.mcp_tool_calls) == 1
        call = result.mcp_tool_calls[0]
        assert call.server == "inkbox"
        assert call.tool == "inkbox_send_sms"
        assert call.status == "completed"
        assert call.arguments["to"] == "+15167251294"
        assert call.sent is True
        assert call.error_kind == "unknown"
        assert not hasattr(call, "result")
        assert not hasattr(call, "error")
        assert "private body" not in repr(call)
        assert "private-provider-id" not in repr(call)

    asyncio.run(scenario())


def test_mcp_failed_item_reduces_raw_error_to_recoverable_kind():
    async def scenario():
        client = _client()
        loop = asyncio.get_running_loop()
        capture = _TurnCapture("thread-1", "turn-1", loop.create_future())
        client._turns["turn-1"] = capture

        client._handle_notification({
            "method": "item/completed",
            "params": {
                "turnId": "turn-1",
                "item": {
                    "type": "mcpToolCall",
                    "server": "inkbox",
                    "tool": "inkbox_send_sms",
                    "status": "failed",
                    "arguments": '{"to_number":"+15167251294"}',
                    "error": {
                        "message": "Invalid arguments: required property `to` is missing; private payload",
                    },
                },
            },
        })
        client._handle_notification({
            "method": "turn/completed",
            "params": {"turnId": "turn-1", "turn": {"status": "completed"}},
        })

        call = (await capture.future).mcp_tool_calls[0]
        assert call.arguments == {"to_number": "+15167251294"}
        assert call.sent is False
        assert call.error_kind == "recoverable"
        assert "private" not in repr(call)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("error_code", "rule", "status_code", "expected"),
    [
        ("message_blocked_spam_filter", "emoji_overload", 422, "recoverable"),
        ("message_blocked_spam_filter", "profanity", 422, "recoverable"),
        ("carrier_unavailable", "", 502, "terminal"),
        ("carrier_rate_limit", "", 429, "terminal"),
        ("inkbox_duplicate_body", "", 422, "terminal"),
        ("request_timeout", "", 408, "terminal"),
        ("upstream_failure", "", 503, "terminal"),
        ("recipient_opted_out", "", 403, "terminal"),
        ("invalid_phone_number", "", 422, "terminal"),
    ],
)
def test_mcp_failed_item_uses_structured_sms_error_metadata(
    error_code,
    rule,
    status_code,
    expected,
):
    async def scenario():
        client = _client()
        loop = asyncio.get_running_loop()
        capture = _TurnCapture("thread-1", "turn-1", loop.create_future())
        client._turns["turn-1"] = capture

        payload = {
            "error": "private provider response",
            "error_code": error_code,
            "status_code": status_code,
        }
        if rule:
            payload["rule"] = rule
        client._handle_notification({
            "method": "item/completed",
            "params": {
                "turnId": "turn-1",
                "item": {
                    "type": "mcpToolCall",
                    "server": "inkbox",
                    "tool": "inkbox_send_sms",
                    "status": "failed",
                    "arguments": {"to": "+15167251294", "text": "private body"},
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(payload)}],
                    },
                },
            },
        })
        client._handle_notification({
            "method": "turn/completed",
            "params": {"turnId": "turn-1", "turn": {"status": "completed"}},
        })

        call = (await capture.future).mcp_tool_calls[0]
        assert call.error_kind == expected
        assert "private" not in repr(call)
        assert error_code not in repr(call)
        if rule:
            assert rule not in repr(call)

    asyncio.run(scenario())

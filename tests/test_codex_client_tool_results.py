import asyncio

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

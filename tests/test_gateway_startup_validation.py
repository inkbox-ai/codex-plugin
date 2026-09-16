import asyncio
import socket

import pytest

from inkbox_codex.config import BridgeConfig, RealtimeConfig, VoiceStack
from inkbox_codex import gateway as gateway_module
from inkbox_codex.gateway import InkboxGateway


def test_gateway_rejects_realtime_stack_without_api_key(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)
    gateway = InkboxGateway(BridgeConfig(
        api_key="ApiKey_test",
        identity="agent",
        voice_stack=VoiceStack.OPENAI_REALTIME,
        realtime=RealtimeConfig(enabled=False, api_key=""),
    ))

    with pytest.raises(
        RuntimeError,
        match="openai_realtime requires INKBOX_REALTIME_API_KEY",
    ):
        asyncio.run(gateway.run())


def test_gateway_rejects_invalid_voice_ai_authority(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)
    gateway = InkboxGateway(BridgeConfig(
        api_key="ApiKey_test",
        identity="agent",
        voice_stack=VoiceStack.INKBOX_VOICE_AI,
        voice_ai_authority_mode="unbounded",
    ))

    with pytest.raises(
        RuntimeError,
        match="INKBOX_VOICE_AI_AUTHORITY_MODE must be contact_scoped or yolo",
    ):
        asyncio.run(gateway.run())


def test_start_http_server_reports_port_already_in_use(monkeypatch):
    monkeypatch.setattr(gateway_module, "AIOHTTP_AVAILABLE", True)
    monkeypatch.setattr(gateway_module, "INKBOX_AVAILABLE", True)

    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    try:
        gateway = InkboxGateway(BridgeConfig(
            api_key="ApiKey_test", identity="agent", host="127.0.0.1", port=port,
        ))
        with pytest.raises(RuntimeError, match="Another Inkbox bridge"):
            asyncio.run(gateway._start_http_server())
    finally:
        blocker.close()

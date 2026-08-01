import asyncio

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

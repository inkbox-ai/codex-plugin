import asyncio

import pytest

from inkbox_codex.config import BridgeConfig, RealtimeConfig, VoiceStack
from inkbox_codex.gateway import InkboxGateway


def test_gateway_rejects_realtime_stack_without_api_key():
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

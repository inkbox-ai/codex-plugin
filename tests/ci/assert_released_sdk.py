"""Prove the registered MCP interpreter resolves the selected SDK."""

from importlib.metadata import version
import os
import subprocess

from inkbox_codex.config import BridgeConfig
from inkbox_codex.tools import build_inkbox_mcp_server_config

assert version("inkbox") == "0.7.38"
server, _ = build_inkbox_mcp_server_config(BridgeConfig())
code = """
from importlib.metadata import version
from inkbox.agent_identity import AgentIdentity
from inkbox.whatsapp import WhatsAppConversation, WhatsAppMessage
assert version("inkbox") == "0.7.38"
for name in ("send_whatsapp", "get_whatsapp_message", "get_whatsapp_status",
             "get_whatsapp_limits", "upload_whatsapp_media"):
    assert callable(getattr(AgentIdentity, name))
for model in (WhatsAppConversation, WhatsAppMessage):
    assert "is_group" in model.__dataclass_fields__
"""
subprocess.run([server["command"], "-c", code], env={**os.environ, **server["env"]}, check=True)

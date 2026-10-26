"""Prove the registered MCP interpreter resolves the real candidate SDK."""

from importlib.metadata import version
import os
import subprocess

from inkbox_codex.config import BridgeConfig
from inkbox_codex.tools import build_inkbox_mcp_server_config

assert version("inkbox") == "0.7.21"
server, _ = build_inkbox_mcp_server_config(BridgeConfig())
code = "from importlib.metadata import version; from inkbox.agent_identity import AgentIdentity; assert version('inkbox') == '0.7.21'; assert callable(AgentIdentity.send_whatsapp); assert callable(AgentIdentity.get_whatsapp_status); assert callable(AgentIdentity.get_whatsapp_limits); assert callable(AgentIdentity.upload_whatsapp_media)"
subprocess.run([server["command"], "-c", code], env={**os.environ, **server["env"]}, check=True)

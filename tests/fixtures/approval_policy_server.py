"""Local read-only MCP tool; approval is the host's job, not the peer's."""

import json
from pathlib import Path
import sys


for line in sys.stdin:
    message = json.loads(line)
    method, request_id = message.get("method"), message.get("id")
    if method == "initialize":
        result = {
            "protocolVersion": message["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "approval-policy-contract", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {"tools": [{
            "name": "lookup_example", "description": "Look up a synthetic local example.",
            "inputSchema": {"type": "object", "properties": {}},
            "annotations": {"readOnlyHint": True},
        }]}
    elif method == "tools/call":
        Path(sys.argv[1]).write_text("executed")
        result = {"content": [{"type": "text", "text": "EXAMPLE_OK"}]}
    elif method == "ping":
        result = {}
    else:
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)

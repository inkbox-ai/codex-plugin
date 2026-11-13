"""Synthetic catalog controlled by a private test file."""

import json
from pathlib import Path
import sys


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {"protocolVersion": message["params"]["protocolVersion"], "capabilities": {"tools": {}},
                  "serverInfo": {"name": "catalog-contract", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": json.loads(Path(sys.argv[1]).read_text())}
    elif method == "ping":
        result = {}
    else:
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": message.get("id"), "result": result}), flush=True)

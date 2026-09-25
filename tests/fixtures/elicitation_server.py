"""Local MCP peer that validates replies forwarded by the real app-server."""

import json
import sys


def send(message):
    print(json.dumps({"jsonrpc": "2.0", **message}), flush=True)


def main():
    pending = {}
    next_id = 1000
    for line in sys.stdin:
        message = json.loads(line)
        method, request_id = message.get("method"), message.get("id")
        if method == "initialize":
            send({"id": request_id, "result": {
                "protocolVersion": message["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "elicitation-contract", "version": "1.0.0"},
            }})
        elif method == "tools/list":
            send({"id": request_id, "result": {"tools": [{
                "name": "lookup_example", "description": "Confirm a synthetic local lookup.",
                "inputSchema": {"type": "object", "properties": {"case": {"type": "string"}}, "required": ["case"]},
                "annotations": {"readOnlyHint": True},
            }]}})
        elif method == "tools/call":
            case = message["params"]["arguments"]["case"]
            next_id += 1
            pending[next_id] = (request_id, case)
            params = {"mode": "form", "message": "Allow Example to run tool \"lookup_example\"?",
                      "requestedSchema": {"type": "object", "properties": {}}}
            if case == "form":
                params.update(message="Confirm the example selection.", requestedSchema={
                    "type": "object", "properties": {"confirmed": {"type": "boolean"}}, "required": ["confirmed"],
                })
            elif case in {"persist", "persist-session", "persist-always"}:
                scopes = ["session", "always"] if case == "persist" else [case.removeprefix("persist-")]
                params["_meta"] = {"codex_request_type": "approval_request", "codex_approval_kind": "mcp_tool_call",
                                   "tool_name": "lookup_example", "persist": scopes}
            send({"id": next_id, "method": "elicitation/create", "params": params})
        elif method == "ping":
            send({"id": request_id, "result": {}})
        elif method is None and request_id in pending:
            original_id, case = pending.pop(request_id)
            response = message.get("result") or {}
            accepted = response.get("action") == "accept"
            valid = (response.get("content") == {"confirmed": True} if case == "form"
                     else response.get("content") in (None, {}))
            send({"id": original_id, "result": {"content": [{
                "type": "text", "text": json.dumps({"response": response, "executed": accepted and valid}),
            }], "isError": accepted and not valid}})


if __name__ == "__main__":
    main()

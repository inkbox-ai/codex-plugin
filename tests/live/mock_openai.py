"""Deterministic OpenAI-compatible mock for live agent tests.

Codex's model providers honour a configured ``base_url``, so pointing a custom
provider at this server makes the agent "think" here instead of against real
OpenAI: no real key, no tokens, no flakiness, fully deterministic. We still
exercise the entire real pipeline (bridge, tunnel, inbound routing, Codex
app-server, Inkbox send + delivery) — only the LLM brain is faked.

Codex speaks the **Responses API** (``wire_api = "chat"`` was removed from the
host), so this serves ``POST /v1/responses`` — streaming (SSE) and not — plus
the chat-completions shape and a ``GET /v1/models`` probe for good measure.

Every reply contains ``REPLY_OK`` plus, when present, the inbound's smoke nonce,
so a live test can assert the canned content travelled inbound → model → reply →
delivery end to end (and that the agent did NOT fall back to an error message).

Run: ``python mock_openai.py [port]`` (default 8088). Stdlib only.
"""

from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_NONCE = re.compile(r"smoke-[0-9a-f]{6,}")


def _reply_text(req: dict) -> str:
    m = _NONCE.search(json.dumps(req))
    tag = m.group(0) if m else "no-nonce"
    return f"REPLY_OK {tag} — automated reachability reply from the agent."


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):  # quiet
        pass

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802  (model-probe + health)
        if self.path.rstrip("/").endswith("/models"):
            self._send_json(200, {"object": "list", "data": [
                {"id": "mock-model", "object": "model", "owned_by": "mock"},
            ]})
        else:
            self._send_json(200, {"ok": True})

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length") or 0)
        try:
            req = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            req = {}
        if self.path.rstrip("/").endswith("/responses"):
            self._respond_responses(req)
        else:
            self._respond_chat(req)

    # ---- Responses API (what Codex speaks) ----

    def _respond_responses(self, req: dict) -> None:
        text = _reply_text(req)
        item = {
            "type": "message",
            "id": "msg_mock",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        }
        response = {
            "id": "resp_mock",
            "object": "response",
            "created_at": 0,
            "model": req.get("model", "mock-model"),
            "status": "completed",
            "output": [item],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 2,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0},
            },
        }
        if not req.get("stream"):
            self._send_json(200, response)
            return
        events = [
            ("response.created", {"response": {**response, "status": "in_progress", "output": []}}),
            ("response.output_item.added", {"output_index": 0, "item": {**item, "status": "in_progress", "content": []}}),
            ("response.content_part.added", {"item_id": "msg_mock", "output_index": 0, "content_index": 0,
                                             "part": {"type": "output_text", "text": "", "annotations": []}}),
            ("response.output_text.delta", {"item_id": "msg_mock", "output_index": 0, "content_index": 0,
                                            "delta": text, "logprobs": []}),
            ("response.output_text.done", {"item_id": "msg_mock", "output_index": 0, "content_index": 0,
                                           "text": text, "logprobs": []}),
            ("response.content_part.done", {"item_id": "msg_mock", "output_index": 0, "content_index": 0,
                                            "part": {"type": "output_text", "text": text, "annotations": []}}),
            ("response.output_item.done", {"output_index": 0, "item": item}),
            ("response.completed", {"response": response}),
        ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        for seq, (name, payload) in enumerate(events):
            data = {"type": name, "sequence_number": seq, **payload}
            self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
        self.wfile.flush()

    # ---- Chat Completions (kept for any chat-speaking client) ----

    def _respond_chat(self, req: dict) -> None:
        text = _reply_text(req)
        model = req.get("model", "mock-model")
        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            chunks = [
                {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model,
                 "choices": [{"index": 0, "delta": {"role": "assistant", "content": text}, "finish_reason": None}]},
                {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model,
                 "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            ]
            for chunk in chunks:
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        else:
            self._send_json(200, {
                "id": "chatcmpl-mock", "object": "chat.completion", "created": 0, "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            })


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8088
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()

"""Async Codex app-server client used by the Inkbox bridge."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from .config import BridgeConfig
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import BridgeConfig

logger = logging.getLogger(__name__)


ApprovalHandler = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]


class CodexAppServerError(RuntimeError):
    """Raised when codex app-server returns an error or exits unexpectedly."""


@dataclass
class _TurnCapture:
    thread_id: str
    turn_id: str
    future: "asyncio.Future[str]"
    messages: list[Dict[str, Any]] = field(default_factory=list)
    deltas: list[str] = field(default_factory=list)


class CodexAppServerClient:
    """Small JSON-RPC client for ``codex app-server`` over stdio."""

    def __init__(
        self,
        cfg: BridgeConfig,
        *,
        developer_instructions: str,
        mcp_server_config: Optional[Dict[str, Any]] = None,
        approval_handler: Optional[ApprovalHandler] = None,
    ) -> None:
        self.cfg = cfg
        self.developer_instructions = developer_instructions
        self.mcp_server_config = dict(mcp_server_config or {})
        self.approval_handler = approval_handler

        self.thread_id: Optional[str] = None
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._next_id = 1
        self._pending: Dict[int, "asyncio.Future[Any]"] = {}
        self._turns: Dict[str, _TurnCapture] = {}
        self._current_turn_id: Optional[str] = None
        self._initialized = False

    async def connect(self, resume_thread_id: Optional[str] = None) -> str:
        """Start app-server and create or resume a Codex thread."""
        await self._ensure_process()
        if not self._initialized:
            await self._initialize()

        params = self._thread_params()
        if resume_thread_id:
            params["threadId"] = resume_thread_id
            result = await self._request("thread/resume", params)
        else:
            result = await self._request("thread/start", params)
        thread = result.get("thread") or {}
        thread_id = str(thread.get("id") or "")
        if not thread_id:
            raise CodexAppServerError(f"app-server did not return a thread id: {result!r}")
        self.thread_id = thread_id
        return thread_id

    async def run(self, text: str) -> str:
        """Run one turn in the current thread and return the final reply text."""
        if not self.thread_id:
            await self.connect()
        assert self.thread_id is not None

        result = await self._request(
            "turn/start",
            {
                "threadId": self.thread_id,
                "input": [{"type": "text", "text": text}],
                "cwd": self.cfg.project_dir or None,
                "model": self.cfg.codex_model or None,
                "approvalPolicy": self.cfg.codex_approval_policy or "on-request",
            },
        )
        turn = result.get("turn") or {}
        turn_id = str(turn.get("id") or "")
        if not turn_id:
            raise CodexAppServerError(f"app-server did not return a turn id: {result!r}")

        loop = asyncio.get_running_loop()
        capture = _TurnCapture(
            thread_id=self.thread_id,
            turn_id=turn_id,
            future=loop.create_future(),
        )
        self._turns[turn_id] = capture
        self._current_turn_id = turn_id
        try:
            return await capture.future
        finally:
            self._turns.pop(turn_id, None)
            if self._current_turn_id == turn_id:
                self._current_turn_id = None

    async def interrupt(self) -> None:
        """Interrupt the active turn, if app-server has accepted one."""
        if not self.thread_id or not self._current_turn_id:
            return
        await self._request(
            "turn/interrupt",
            {"threadId": self.thread_id, "turnId": self._current_turn_id},
        )

    async def disconnect(self) -> None:
        """Terminate the app-server process."""
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(CodexAppServerError("Codex app-server disconnected"))
        self._pending.clear()
        for capture in list(self._turns.values()):
            if not capture.future.done():
                capture.future.set_exception(CodexAppServerError("Codex app-server disconnected"))
        self._turns.clear()

        if self._proc is not None and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
        self._proc = None

    def _thread_params(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {}
        if self.mcp_server_config:
            config["mcp_servers"] = {"inkbox": self.mcp_server_config}
        return {
            "cwd": self.cfg.project_dir or None,
            "model": self.cfg.codex_model or None,
            "approvalPolicy": self.cfg.codex_approval_policy or "on-request",
            "approvalsReviewer": "user",
            "developerInstructions": self.developer_instructions,
            "sandbox": self.cfg.codex_sandbox or "workspace-write",
            "config": config or None,
            "serviceName": "inkbox-codex",
        }

    async def _ensure_process(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            return
        env = os.environ.copy()
        self._proc = await asyncio.create_subprocess_exec(
            self.cfg.codex_bin or "codex",
            "app-server",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._stderr_task = asyncio.create_task(self._stderr_loop())

    async def _initialize(self) -> None:
        await self._request(
            "initialize",
            {
                "clientInfo": {
                    "name": "inkbox_codex",
                    "title": "Inkbox Codex Bridge",
                    "version": "0.1.0",
                },
                "capabilities": {"experimentalApi": True},
            },
        )
        await self._notify("initialized", {})
        self._initialized = True

    async def _request(self, method: str, params: Dict[str, Any]) -> Any:
        if self._proc is None or self._proc.stdin is None:
            raise CodexAppServerError("Codex app-server is not running")
        message_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[message_id] = future
        self._write({"id": message_id, "method": method, "params": params})
        return await future

    async def _notify(self, method: str, params: Dict[str, Any]) -> None:
        self._write({"method": method, "params": params})

    def _write(self, message: Dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise CodexAppServerError("Codex app-server is not running")
        self._proc.stdin.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")

    async def _reader_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        while True:
            line = await self._proc.stdout.readline()
            if not line:
                self._fail_all(CodexAppServerError("Codex app-server exited"))
                return
            try:
                message = json.loads(line.decode())
            except json.JSONDecodeError:
                logger.warning("invalid app-server JSON: %r", line[:500])
                continue

            if "id" in message and ("result" in message or "error" in message) and "method" not in message:
                self._handle_response(message)
                continue
            if "id" in message and "method" in message:
                asyncio.create_task(self._handle_server_request(message))
                continue
            if "method" in message:
                self._handle_notification(message)

    async def _stderr_loop(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while True:
            line = await self._proc.stderr.readline()
            if not line:
                return
            logger.debug("[codex app-server] %s", line.decode(errors="replace").rstrip())

    def _handle_response(self, message: Dict[str, Any]) -> None:
        future = self._pending.pop(int(message["id"]), None)
        if future is None or future.done():
            return
        if "error" in message:
            error = message.get("error") or {}
            future.set_exception(CodexAppServerError(str(error.get("message") or error)))
        else:
            future.set_result(message.get("result"))

    async def _handle_server_request(self, message: Dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        request_id = message.get("id")
        params = message.get("params") or {}
        try:
            if self.approval_handler is None:
                raise CodexAppServerError(f"no handler for app-server request {method}")
            result = await self.approval_handler(method, params)
            self._write({"id": request_id, "result": result})
        except Exception as exc:
            self._write({
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": str(exc),
                },
            })

    def _handle_notification(self, message: Dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params") or {}
        turn_id = str(params.get("turnId") or (params.get("turn") or {}).get("id") or "")

        if method == "item/agentMessage/delta":
            capture = self._turns.get(turn_id)
            if capture is not None:
                capture.deltas.append(str(params.get("delta") or ""))
            return

        if method == "item/completed":
            capture = self._turns.get(turn_id)
            item = params.get("item") or {}
            if capture is not None and item.get("type") == "agentMessage":
                capture.messages.append(item)
            return

        if method == "turn/completed":
            capture = self._turns.get(turn_id)
            if capture is None or capture.future.done():
                return
            turn = params.get("turn") or {}
            status = turn.get("status")
            if status == "failed":
                error = turn.get("error") or turn.get("codexErrorInfo") or "turn failed"
                capture.future.set_exception(CodexAppServerError(str(error)))
                return
            capture.future.set_result(_final_message(capture))

    def _fail_all(self, exc: Exception) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()
        for capture in list(self._turns.values()):
            if not capture.future.done():
                capture.future.set_exception(exc)


def _final_message(capture: _TurnCapture) -> str:
    final = [
        str(item.get("text") or "")
        for item in capture.messages
        if item.get("phase") in ("final", None)
    ]
    text = "\n\n".join(t for t in final if t).strip()
    if text:
        return text
    return "".join(capture.deltas).strip()

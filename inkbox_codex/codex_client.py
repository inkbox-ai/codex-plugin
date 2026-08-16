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
    from .delivery_policy import sms_tool_failure_kind
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import BridgeConfig
    from delivery_policy import sms_tool_failure_kind

logger = logging.getLogger(__name__)


ApprovalHandler = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]
ActivityHandler = Callable[[str, str], None]


class CodexAppServerError(RuntimeError):
    """Raised when codex app-server returns an error or exits unexpectedly."""


@dataclass
class _TurnCapture:
    thread_id: str
    turn_id: str
    future: "asyncio.Future[CodexTurnResult]"
    messages: list[Dict[str, Any]] = field(default_factory=list)
    deltas: list[str] = field(default_factory=list)
    mcp_tool_calls: list["McpToolCallResult"] = field(default_factory=list)
    activity_handler: Optional[ActivityHandler] = None


@dataclass(frozen=True)
class McpToolCallResult:
    """Sanitized final state for one MCP tool item.

    Tool result bodies and raw errors can contain message content or provider
    payloads. Keep only the fields needed to settle a required side effect.
    """

    server: str
    tool: str
    status: str
    arguments: Dict[str, Any]
    sent: bool
    error_kind: str


@dataclass(frozen=True)
class CodexTurnResult:
    """Final reply plus sanitized MCP outcomes for one app-server turn."""

    text: str
    mcp_tool_calls: tuple[McpToolCallResult, ...]
    aborted: bool = False


class CodexAppServerClient:
    """Small JSON-RPC client for ``codex app-server`` over stdio."""

    def __init__(
        self,
        cfg: BridgeConfig,
        *,
        developer_instructions: str,
        mcp_server_config: Optional[Dict[str, Any]] = None,
        approval_handler: Optional[ApprovalHandler] = None,
        tools_enabled: bool = True,
    ) -> None:
        self.cfg = cfg
        self.developer_instructions = developer_instructions
        self.mcp_server_config = dict(mcp_server_config or {})
        self.approval_handler = approval_handler
        self.tools_enabled = tools_enabled

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

    async def run(
        self,
        text: str,
        *,
        activity_handler: Optional[ActivityHandler] = None,
    ) -> str:
        """Run one turn in the current thread and return the final reply text."""
        return (await self.run_detailed(text, activity_handler=activity_handler)).text

    async def run_detailed(
        self,
        text: str,
        *,
        activity_handler: Optional[ActivityHandler] = None,
    ) -> CodexTurnResult:
        """Run one turn and return its final reply and sanitized MCP outcomes."""
        if not self.thread_id:
            await self.connect()
        assert self.thread_id is not None

        params = {
            "threadId": self.thread_id,
            "input": [{"type": "text", "text": text}],
            "cwd": self.cfg.project_dir or None,
            "model": self.cfg.codex_model or None,
            "approvalPolicy": self.cfg.codex_approval_policy or "on-request",
        }
        if not self.tools_enabled:
            # A turn-level cwd override otherwise restores the default local
            # environment after thread/start disabled it.
            params["environments"] = []
        result = await self._request("turn/start", params)
        turn = result.get("turn") or {}
        turn_id = str(turn.get("id") or "")
        if not turn_id:
            raise CodexAppServerError(f"app-server did not return a turn id: {result!r}")

        loop = asyncio.get_running_loop()
        capture = _TurnCapture(
            thread_id=self.thread_id,
            turn_id=turn_id,
            future=loop.create_future(),
            activity_handler=activity_handler,
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
        if self.tools_enabled and self.mcp_server_config:
            config["mcp_servers"] = {"inkbox": self.mcp_server_config}
        params = {
            "cwd": self.cfg.project_dir or None,
            "model": self.cfg.codex_model or None,
            "approvalPolicy": self.cfg.codex_approval_policy or "on-request",
            "approvalsReviewer": "user",
            "developerInstructions": self.developer_instructions,
            "sandbox": self.cfg.codex_sandbox or "workspace-write",
            "config": config or None,
            "serviceName": "inkbox-codex",
        }
        if not self.tools_enabled:
            # app-server has no single `tools: []` thread option. These are
            # its host-native gates for every built-in/external tool source.
            params.update(
                {
                    "environments": [],
                    "dynamicTools": [],
                    "selectedCapabilityRoots": [],
                    "config": {
                        "web_search": "disabled",
                        "apps": {"_default": {"enabled": False}},
                        "orchestrator": {
                            "skills": {"enabled": False},
                            "mcp": {"enabled": False},
                        },
                        "tools": {
                            "update_plan": {"enabled": False},
                            "experimental_request_user_input": {"enabled": False},
                        },
                        "features": {
                            "apps": False,
                            "goals": False,
                            "image_generation": False,
                            "multi_agent": False,
                            "multi_agent_v2": False,
                            "plugins": False,
                            "shell_tool": False,
                            "tool_suggest": False,
                            "unified_exec": False,
                            "view_image": False,
                        },
                    },
                }
            )
        return params

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
                    "version": "0.2.9",
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

        if method == "item/started":
            capture = self._turns.get(turn_id)
            item = params.get("item") or {}
            if capture is not None and capture.activity_handler is not None:
                item_type = str(item.get("type") or "")
                tool_name = str(item.get("tool") or item.get("name") or "")
                try:
                    capture.activity_handler(item_type, tool_name)
                except Exception:
                    logger.debug("turn activity handler failed", exc_info=True)

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
            if capture is not None and item.get("type") == "mcpToolCall":
                capture.mcp_tool_calls.append(_mcp_tool_call_result(item))
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
            capture.future.set_result(CodexTurnResult(
                text=_final_message(capture),
                mcp_tool_calls=tuple(capture.mcp_tool_calls),
            ))

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


def _mcp_tool_call_result(item: Dict[str, Any]) -> McpToolCallResult:
    """Reduce a final MCP item without retaining result or error payloads."""
    arguments = item.get("arguments")
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            arguments = {}
    if not isinstance(arguments, dict):
        arguments = {}

    result = item.get("result")
    sent = False
    error_fragments: list[str] = []
    error_code: Any = None
    error_rule: Any = None
    status_code: Any = None

    def capture_error_metadata(payload: Dict[str, Any]) -> None:
        nonlocal error_code, error_rule, status_code
        error_code = payload.get("error_code") or payload.get("code") or error_code
        error_rule = payload.get("rule") or error_rule
        status_code = payload.get("status_code") or status_code
        if payload.get("error"):
            error_fragments.append(str(payload["error"]))

    if isinstance(result, dict):
        structured = result.get("structuredContent") or result.get("structured_content")
        if isinstance(structured, dict):
            sent = structured.get("sent") is True
            capture_error_metadata(structured)
        content = result.get("content")
        for block in content if isinstance(content, list) else []:
            if not isinstance(block, dict):
                continue
            text = str(block.get("text") or "")
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                sent = sent or payload.get("sent") is True
                capture_error_metadata(payload)

    error = item.get("error")
    if isinstance(error, dict) and error.get("message"):
        error_fragments.append(str(error["message"]))
    elif error:
        error_fragments.append(str(error))

    safe_arguments = {
        key: value
        for key in ("to", "to_number", "toNumber", "conversation_id")
        if (value := arguments.get(key)) is not None
        and isinstance(value, (str, int, float, bool))
    }
    return McpToolCallResult(
        server=str(item.get("server") or ""),
        tool=str(item.get("tool") or ""),
        status=str(item.get("status") or ""),
        arguments=safe_arguments,
        sent=sent,
        error_kind=sms_tool_failure_kind(
            error_code=error_code,
            rule=error_rule,
            status_code=status_code,
            message=" ".join(error_fragments),
        ),
    )

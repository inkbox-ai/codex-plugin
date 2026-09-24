"""Async Codex app-server client used by the Inkbox bridge."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from .config import BridgeConfig
    from .delivery_policy import sms_tool_failure_kind
    from .launcher import resolve_codex_launcher
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import BridgeConfig
    from delivery_policy import sms_tool_failure_kind
    from launcher import resolve_codex_launcher

logger = logging.getLogger(__name__)
STARTUP_TIMEOUT_SECONDS = 15.0
SHUTDOWN_TIMEOUT_SECONDS = 5.0


ApprovalHandler = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]
ActivityHandler = Callable[[str, str], None]


class CodexAppServerError(RuntimeError):
    """Raised when codex app-server returns an error or exits unexpectedly."""


class CodexStartupError(CodexAppServerError):
    """A failure before any thread or turn submission, with safe diagnostics."""


def _startup_diagnostic(line: str) -> str | None:
    # Raw stderr can contain credentials, config, and message content. Retain
    # only recognized diagnostic categories, never the original line.
    text = line.lower()
    if "node" in text and any(part in text for part in ("no such file", "not found", "not recognized")):
        return "Node interpreter not found on PATH; use a working CODEX_BIN or install Node"
    if "permission denied" in text:
        return "permission denied while launching Codex"
    if "exec format error" in text or "bad cpu type" in text:
        return "Codex executable is incompatible with this platform"
    if "error loading shared" in text or "library not loaded" in text:
        return "Codex runtime library is missing"
    if "no such file or directory" in text:
        return "Codex launcher dependency or executable is missing"
    return None


async def probe_codex(cfg: BridgeConfig) -> tuple[bool, str]:
    """Check the effective launcher's handshake without creating a thread/turn."""
    client = CodexAppServerClient(cfg, developer_instructions="", tools_enabled=False,
                                 isolate_process_group=True)
    try:
        await client._ensure_process()
        await client._initialize()
        return True, "app-server initialized (model execution not tested)"
    except CodexStartupError as exc:
        return False, str(exc)
    except Exception:
        return False, "Codex app-server readiness check failed"
    finally:
        await client.disconnect()


async def recover_saved_answer(cfg: BridgeConfig, thread_id: str, receipt_token: str) -> Optional[str]:
    """Recover only a positively matched completed turn, without rerunning it."""
    client = CodexAppServerClient(cfg, developer_instructions="", tools_enabled=False,
                                 isolate_process_group=True)
    try:
        await client._ensure_process()
        await client._initialize()
        try:
            result = await asyncio.wait_for(client._request("thread/read", {
                "threadId": thread_id, "includeTurns": True,
            }), timeout=STARTUP_TIMEOUT_SECONDS)
        except (CodexAppServerError, TimeoutError):
            return None
        if not isinstance(result, dict) or not isinstance(result.get("thread"), dict):
            return None
        thread = result["thread"]
        if thread.get("id") != thread_id or not isinstance(thread.get("turns"), list):
            return None
        marker = f"Companion receipt: {receipt_token}\n"
        def objects(value):
            return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        matches = [turn for turn in objects(thread["turns"]) if any(
            item.get("type") == "userMessage" and any(
                entry.get("type") == "text" and marker in str(entry.get("text") or "")
                for entry in objects(item.get("content"))
            ) for item in objects(turn.get("items"))
        )]
        if len(matches) != 1 or matches[0].get("status") != "completed":
            return None
        return "\n\n".join(
            item["text"] for item in objects(matches[0].get("items"))
            if item.get("type") == "agentMessage" and item.get("phase") in {None, "final", "final_answer"}
            and isinstance(item.get("text"), str)
        ).strip() or None
    finally:
        await client.disconnect()


async def _read_protocol_line(reader: asyncio.StreamReader) -> bytes:
    """Read a host line without truncating large resumed-thread histories."""
    chunks = []
    while True:
        try:
            chunks.append(await reader.readuntil(b"\n"))
            return b"".join(chunks)
        except asyncio.LimitOverrunError as exc:
            chunks.append(await reader.readexactly(exc.consumed))
        except asyncio.IncompleteReadError as exc:
            chunks.append(exc.partial)
            return b"".join(chunks)


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
        isolate_process_group: bool = False,
    ) -> None:
        self.cfg = cfg
        self.developer_instructions = developer_instructions
        self.mcp_server_config = dict(mcp_server_config or {})
        self.approval_handler = approval_handler
        self.tools_enabled = tools_enabled
        self._isolated_process_group = isolate_process_group and os.name == "posix"
        self._owns_process_group = os.name == "posix"
        self._process_group_id: Optional[int] = None

        self.thread_id: Optional[str] = None
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._server_requests: dict[asyncio.Task, tuple[Any, str]] = {}
        self._finished_turns: set[str] = set()
        self._next_id = 1
        self._pending: Dict[int, "asyncio.Future[Any]"] = {}
        self._turns: Dict[str, _TurnCapture] = {}
        self._starting_turns: Dict[int, _TurnCapture] = {}
        self._early_notifications: Dict[str, list[Dict[str, Any]]] = {}
        self._current_turn_id: Optional[str] = None
        self._initialized = False
        self._stderr_tail: deque[str] = deque(maxlen=8)

    @property
    def process_group_id(self) -> Optional[int]:
        """The group created for this host, retained for recovery fencing."""
        return self._process_group_id

    @property
    def is_alive(self) -> bool:
        return (self._proc is not None and self._proc.returncode is None
                and self._reader_task is not None and not self._reader_task.done())

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
        loop = asyncio.get_running_loop()
        capture = _TurnCapture(
            thread_id=self.thread_id,
            turn_id="",
            future=loop.create_future(),
            activity_handler=activity_handler,
        )
        try:
            await self._request("turn/start", params, turn_capture=capture)
            return await capture.future
        finally:
            self._turns.pop(capture.turn_id, None)
            if not capture.future.done():
                capture.future.cancel()
            elif not capture.future.cancelled():
                capture.future.exception()
            if self._current_turn_id == capture.turn_id:
                self._current_turn_id = None

    async def append_context(self, messages: list[str]) -> None:
        """Persist background messages without starting model generation."""
        if not self.thread_id:
            await self.connect()
        await self._request("thread/inject_items", {
            "threadId": self.thread_id,
            "items": [
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": text},
                ]}
                for text in messages
            ],
        })

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
        if self._reader_task is not None:
            self._reader_task.cancel()
        await self._cancel_server_requests()
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(CodexAppServerError("Codex app-server disconnected"))
        self._pending.clear()
        for capture in list(self._turns.values()):
            if not capture.future.done():
                capture.future.set_exception(CodexAppServerError("Codex app-server disconnected"))
        self._turns.clear()

        proc = self._proc
        if proc is not None and (proc.returncode is None or self._isolated_process_group):
            def stop(sig):
                try:
                    if self._isolated_process_group:
                        os.killpg(proc.pid, sig)
                    elif sig == signal.SIGTERM:
                        proc.terminate()
                    else:
                        proc.kill()
                except ProcessLookupError:
                    pass

            stop(signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except TimeoutError:
                pass
            # A dedicated probe owns its launcher descendants too. Ordinary
            # sessions keep their existing user-tool lifecycle semantics.
            if proc.returncode is None or self._isolated_process_group:
                stop(signal.SIGKILL)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=1)
                except TimeoutError:
                    logger.warning("Codex process cleanup exceeded its deadline")
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
        await asyncio.gather(*(task for task in (self._reader_task, self._stderr_task)
                               if task is not None), return_exceptions=True)
        self._proc = None
        self._initialized = False

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
        self._stderr_tail.clear()
        try:
            self._proc = await asyncio.create_subprocess_exec(
                resolve_codex_launcher(self.cfg.codex_bin or "codex"),
                "app-server",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                start_new_session=self._owns_process_group,
            )
        except OSError as exc:
            detail = ("configured executable not found" if isinstance(exc, FileNotFoundError)
                      else "configured executable is not runnable")
            raise CodexStartupError(f"Codex startup failed: {detail}; check CODEX_BIN") from None
        self._process_group_id = self._proc.pid if self._owns_process_group else None
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._stderr_task = asyncio.create_task(self._stderr_loop())

    async def _initialize(self) -> None:
        try:
            await asyncio.wait_for(self._initialize_request(), timeout=STARTUP_TIMEOUT_SECONDS)
        except (TimeoutError, CodexAppServerError, OSError) as exc:
            reason = "initialize timed out" if isinstance(exc, TimeoutError) else "initialize failed"
            code = getattr(self._proc, "returncode", None)
            detail = "; ".join(self._stderr_tail)
            message = f"Codex startup failed: {reason}"
            if code is not None:
                message += f" (exit code {code})"
            if detail:
                message += f"; {detail}"
            logger.error("%s", message)
            raise CodexStartupError(message) from None

    async def _initialize_request(self) -> None:
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

    async def _request(
        self, method: str, params: Dict[str, Any], *,
        turn_capture: Optional[_TurnCapture] = None,
    ) -> Any:
        if self._proc is None or self._proc.stdin is None:
            raise CodexAppServerError("Codex app-server is not running")
        if self._reader_task is not None and self._reader_task.done():
            raise CodexAppServerError("Codex app-server output reader is not running")
        message_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[message_id] = future
        if turn_capture is not None:
            self._starting_turns[message_id] = turn_capture
        try:
            self._write({"id": message_id, "method": method, "params": params})
            return await future
        finally:
            self._pending.pop(message_id, None)
            self._starting_turns.pop(message_id, None)
            if not self._starting_turns:
                self._early_notifications.clear()

    async def _notify(self, method: str, params: Dict[str, Any]) -> None:
        self._write({"method": method, "params": params})

    def _write(self, message: Dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise CodexAppServerError("Codex app-server is not running")
        self._proc.stdin.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")

    async def _reader_loop(self) -> None:
        try:
            await self._read_messages()
        except Exception:
            # A dead reader must reject waiters, not leave thread/resume or a
            # model turn waiting forever. Do not include protocol payloads.
            logger.error("Codex app-server output reader failed")
            self._fail_all(CodexAppServerError("Codex app-server output reader failed"))
        finally:
            await self._cancel_server_requests()

    async def _cancel_server_requests(self) -> None:
        # Human interactions belong to this transport, not its replacement.
        tasks = [task for task in self._server_requests if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _read_messages(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        while True:
            line = await _read_protocol_line(self._proc.stdout)
            if not line:
                # stderr and stdout use independent pipes; collect the bounded
                # diagnostic tail before reporting an early process exit.
                if self._stderr_task is not None:
                    try:
                        await asyncio.wait_for(asyncio.shield(self._stderr_task), timeout=0.2)
                    except (TimeoutError, OSError):
                        pass
                if callable(getattr(self._proc, "wait", None)):
                    try:
                        await asyncio.wait_for(self._proc.wait(), timeout=0.2)
                    except TimeoutError:
                        pass
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
                params = message.get("params") or {}
                turn_id = str(params.get("turnId") or "")
                if turn_id and turn_id in self._finished_turns:
                    continue
                task = asyncio.create_task(self._handle_server_request(message))
                self._server_requests[task] = (message["id"], turn_id)
                task.add_done_callback(lambda done: self._server_requests.pop(done, None))
                continue
            if "method" in message:
                self._handle_notification(message)

    async def _stderr_loop(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        pending = b""
        while True:
            chunk = await self._proc.stderr.read(4096)
            pending += chunk
            while b"\n" in pending or len(pending) >= 4096 or (pending and not chunk):
                if b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                else:
                    line, pending = pending[:4096], pending[4096:]
                detail = _startup_diagnostic(line.decode(errors="replace"))
                if detail:
                    self._stderr_tail.append(detail)
                    logger.debug("[codex app-server] %s", detail)
            if not chunk:
                return

    def _handle_response(self, message: Dict[str, Any]) -> None:
        message_id = int(message["id"])
        future = self._pending.pop(message_id, None)
        if future is None or future.done():
            return
        if "error" in message:
            error = message.get("error") or {}
            detail = error.get("message") or error if isinstance(error, dict) else error
            future.set_exception(CodexAppServerError(str(detail)))
        else:
            result = message.get("result")
            capture = self._starting_turns.pop(message_id, None)
            notifications = []
            if capture is not None:
                turn = result.get("turn") if isinstance(result, dict) else None
                turn_id = str(turn.get("id") or "") if isinstance(turn, dict) else ""
                if not turn_id:
                    future.set_exception(CodexAppServerError("app-server did not return a turn id"))
                    return
                # Bind before reading another line: the completion can already
                # be buffered behind this response, before run_detailed resumes.
                capture.turn_id = turn_id
                self._turns[turn_id] = capture
                self._current_turn_id = turn_id
                notifications = self._early_notifications.pop(turn_id, [])
            future.set_result(result)
            for notification in notifications:
                self._handle_notification(notification)

    async def _handle_server_request(self, message: Dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        request_id = message.get("id")
        params = message.get("params") or {}
        try:
            if self.approval_handler is None:
                raise CodexAppServerError(f"no handler for app-server request {method}")
            result = await self.approval_handler(method, params)
            response = {"id": request_id, "result": result}
        except Exception as exc:
            response = {
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": str(exc),
                },
            }
        try:
            self._write(response)
        except (OSError, CodexAppServerError):
            logger.debug("Discarded response for a closed app-server request")

    def _handle_notification(self, message: Dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params") or {}
        turn_id = str(params.get("turnId") or (params.get("turn") or {}).get("id") or "")

        if method == "serverRequest/resolved":
            for task, (request_id, _) in list(self._server_requests.items()):
                if request_id == params.get("requestId"):
                    task.cancel()
            return

        if (turn_id and turn_id not in self._turns and self._starting_turns
                and method in {"item/started", "item/agentMessage/delta", "item/completed", "turn/completed"}):
            # Some hosts emit turn notifications before acknowledging turn/start.
            # Retain them only while starts are outstanding, keyed by turn ID.
            self._early_notifications.setdefault(turn_id, []).append(message)
            return

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
            if turn_id:
                self._finished_turns.add(turn_id)
            for task, (_, request_turn) in list(self._server_requests.items()):
                if turn_id and request_turn == turn_id:
                    task.cancel()
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

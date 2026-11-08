"""Map native chat activity and suspended requests to typed controls."""

from __future__ import annotations

import json
import uuid


class ChatAdapter:
    def __init__(self, sessions):
        self.sessions = sessions
        self.cancelled = set()
        self.native_turns = {}

    async def cancel(self, session_id):
        self.cancelled.add(session_id)
        session = self.sessions.sessions.get(f"chat:{session_id}")
        if session is not None:
            session._interrupting = True
            await session._interrupt_client()

    async def settled(self, session_id):
        session = self.sessions.sessions.get(f"chat:{session_id}")
        client = getattr(session, "_client", None)
        turn = self.native_turns.get(session_id)
        return bool(client and turn and turn in client._finished_turns)

    async def run(self, spec, emit, ask):
        from .managed_chat import failure_code, file_instruction, safe_text

        if spec["session_id"] in self.cancelled:
            self.cancelled.discard(spec["session_id"])
            return {"text": "", "cancelled": True}
        self.native_turns.pop(spec["session_id"], None)
        session = self.sessions.get(f"chat:{spec['session_id']}")
        tool_ids = {}

        def event(method, params):
            turn = params.get("turnId") or (params.get("turn") or {}).get("id")
            if turn:
                self.native_turns[spec["session_id"]] = turn
            if method == "item/agentMessage/delta":
                emit("message.delta", {"text": str(params.get("delta") or "")})
            if method not in {"item/started", "item/completed"}:
                return
            item = params.get("item") or {}
            kind = item.get("type")
            titles = {
                "commandExecution": "Run command",
                "fileChange": "Change files",
                "mcpToolCall": "Call tool",
                "webSearch": "Search",
                "collabAgentToolCall": "Agent task",
            }
            if kind not in titles or not item.get("id"):
                return
            identifier = tool_ids.setdefault(item["id"], str(uuid.uuid4()))
            emit(
                "tool.started" if method == "item/started" else "tool.completed",
                {
                    "id": identifier,
                    "title": titles[kind],
                    "detail": safe_text(item.get("command") or "")[:4096]
                    if kind == "commandExecution"
                    else None,
                    "status": "running"
                    if method == "item/started"
                    else "failed"
                    if item.get("error")
                    or item.get("status") == "failed"
                    or item.get("exitCode") not in (None, 0)
                    else "completed",
                },
            )

        async def request(method, params):
            if method == "item/tool/requestUserInput":
                questions = params.get("questions") or []
                if not 1 <= len(questions) <= 8:
                    raise ValueError("Unsupported question")
                fields = []
                for index, question in enumerate(questions):
                    if question.get("isSecret"):
                        raise ValueError("Secret input is not supported")
                    choices = [
                        {
                            "id": str(i),
                            "label": safe_text(option.get("label", ""))[:256],
                        }
                        for i, option in enumerate(question.get("options") or [])
                    ]
                    fields.append(
                        {
                            "id": str(question.get("id") or index),
                            "label": safe_text(question.get("question") or "")[:2048],
                            "required": True,
                            "type": "single_select" if choices else "text",
                            "choices": choices,
                            "allow_free_text": bool(question.get("isOther", True)),
                        }
                    )
                result = await ask(
                    {"kind": "question", "title": "Input needed", "fields": fields}
                )
                if result.get("cancelled") or spec["session_id"] in self.cancelled:
                    return {"answers": {}}
                answers = {}
                for field in fields:
                    answer = result["answers"][field["id"]]
                    answer = next(
                        (
                            choice["label"]
                            for choice in field["choices"]
                            if choice["id"] == answer
                        ),
                        answer,
                    )
                    answers[field["id"]] = {"answers": [answer]}
                return {"answers": answers}
            approvals = {
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
                "item/permissions/requestApproval",
                "execCommandApproval",
                "applyPatchApproval",
            }
            if method not in approvals:
                raise ValueError("Unsupported interaction")
            permission = method == "item/permissions/requestApproval"
            result = await ask(
                {
                    "kind": "approval",
                    "title": "Permission requested",
                    "effective_scope": "turn" if permission else "operation",
                    "detail": safe_text(
                        (
                            json.dumps(params.get("permissions") or {}, sort_keys=True)
                            if permission
                            else params.get("command")
                        )
                        or params.get("reason")
                        or "Review the requested permission"
                    )[:2048],
                    "tool_call_id": tool_ids.get(params.get("itemId")),
                    "decisions": ["approve_once", "deny"],
                }
            )
            approved = (
                not result.get("cancelled")
                and spec["session_id"] not in self.cancelled
                and result.get("decision") == "approve_once"
            )
            if permission:
                return {
                    "permissions": params.get("permissions") or {} if approved else {},
                    "scope": "turn",
                }
            return {"decision": "accept" if approved else "decline"}

        from .codex_client import CodexTurnFailed

        try:
            result = await session.run_chat(
                spec["prompt"] + file_instruction(spec["run_id"]),
                event,
                request,
                before_submit=lambda: self.check_cancelled(spec["session_id"]),
                **({"model": spec["model"]} if spec.get("model") else {}),
            )
        except CodexTurnFailed as exc:
            return {
                "text": "",
                "failed": True,
                "error_code": failure_code(exc.error),
                "session_id": session.resume_session_id,
            }
        finally:
            self.cancelled.discard(spec["session_id"])
        return {
            "text": result.text,
            "cancelled": result.aborted,
            "session_id": session.resume_session_id,
        }

    async def check_cancelled(self, session_id):
        if session_id in self.cancelled:
            session = self.sessions.sessions.get(f"chat:{session_id}")
            if session is not None:
                session._interrupting = True

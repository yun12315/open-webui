#!/usr/bin/env python3
"""Thin OpenWebUI -> Hermes bridge.

This process intentionally stays small:
- normal OpenAI-compatible traffic is proxied byte-for-byte to Hermes 8642;
- a small allowlist of Hermes slash commands is handled before the request
  reaches the LLM, so OpenWebUI can type commands such as ``/goal``;
- goal continuation reuses Hermes' ``GoalManager`` judge/state machinery and
  Hermes' own API server for agent turns.

Run this with the Hermes venv from WSL:
    /root/.hermes/hermes-agent/venv/bin/python3 /path/to/hermes-openwebui/hermes-openwebui-bridge.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from importlib import import_module
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional

from aiohttp import ClientSession, ClientTimeout, web


HERMES_REPO = os.getenv("HERMES_REPO", "/root/.hermes/hermes-agent")
if HERMES_REPO not in sys.path:
    sys.path.insert(0, HERMES_REPO)

try:
    _commands_module = import_module("hermes_cli.commands")
    _goals_module = import_module("hermes_cli.goals")
    COMMAND_REGISTRY = getattr(_commands_module, "COMMAND_REGISTRY")
    GoalManager = getattr(_goals_module, "GoalManager")
except Exception as exc:  # pragma: no cover - startup guard
    raise RuntimeError(f"Unable to import Hermes modules from {HERMES_REPO}: {exc}") from exc


DEFAULT_BRIDGE_KEY = os.getenv("HERMES_OPENWEBUI_DEFAULT_KEY", "hermes-openwebui-local-only")
BRIDGE_HOST = os.getenv("HERMES_OPENWEBUI_BRIDGE_HOST", "127.0.0.1")
BRIDGE_PORT = int(os.getenv("HERMES_OPENWEBUI_BRIDGE_PORT", "8650"))
HERMES_BASE = os.getenv("HERMES_API_BASE", "http://127.0.0.1:8642").rstrip("/")
HERMES_API_KEY = os.getenv("HERMES_API_KEY", os.getenv("API_SERVER_KEY", DEFAULT_BRIDGE_KEY))
BRIDGE_API_KEY = os.getenv("HERMES_OPENWEBUI_BRIDGE_KEY", HERMES_API_KEY)
REQUEST_TIMEOUT_S = float(os.getenv("HERMES_OPENWEBUI_BRIDGE_TIMEOUT_S", "86400"))
POLL_INTERVAL_S = float(os.getenv("HERMES_OPENWEBUI_BRIDGE_POLL_S", "2"))
GOAL_STREAM_INTERVAL_S = float(os.getenv("HERMES_OPENWEBUI_GOAL_STREAM_INTERVAL_S", "5"))
GOAL_VISIBLE_PROGRESS_INTERVAL_S = float(os.getenv("HERMES_OPENWEBUI_GOAL_VISIBLE_PROGRESS_INTERVAL_S", "12"))
GOAL_TOOL_EVENT_WINDOW_S = float(os.getenv("HERMES_OPENWEBUI_GOAL_TOOL_EVENT_WINDOW_S", "20"))
GOAL_TOOL_EVENT_MAX_PER_WINDOW = int(os.getenv("HERMES_OPENWEBUI_GOAL_TOOL_EVENT_MAX_PER_WINDOW", "8"))

ACTIVE_RUN_STATUSES = {"started", "running"}
SUCCESS_RUN_STATUSES = {"completed", "done"}
NON_SUCCESS_TERMINAL_RUN_STATUSES = {"failed", "cancelled", "inactive", "paused"}
TERMINAL_RUN_STATUSES = SUCCESS_RUN_STATUSES | NON_SUCCESS_TERMINAL_RUN_STATUSES
BRIDGE_SUPPORTED_COMMANDS = {
    "goal",
    "subgoal",
    "background",
    "agents",
    "tasks",
    "stop",
    "status",
    "yolo",
    "commands",
    "help",
}
SSE_HEADERS = {"Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache"}


@dataclass
class BridgeRun:
    run_id: str
    kind: str
    session_id: str
    prompt: str
    status: str = "started"
    output: str = ""
    error: str = ""
    hermes_run_ids: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    last_message: str = ""

    def line(self) -> str:
        detail = self.error or self.last_message or self.output[:120]
        if detail:
            return f"- {self.kind} {self.run_id} [{self.status}] {self.prompt[:80]} — {detail}"
        return f"- {self.kind} {self.run_id} [{self.status}] {self.prompt[:80]}"


RUNS: dict[str, BridgeRun] = {}
GOAL_TASKS: dict[str, asyncio.Task[None]] = {}
TUI_SESSIONS: dict[str, str] = {}
TUI_SESSION_KEYS: dict[str, str] = {}


class JsonRpcError(RuntimeError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class TuiGatewayClient:
    def __init__(self) -> None:
        self._process: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._start_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._next_id = 0
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._events: list[dict[str, Any]] = []
        self._event_queues: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}
        self._stderr_tail: list[str] = []

    async def _ensure_started(self) -> None:
        async with self._start_lock:
            if self._process is not None and self._process.returncode is None:
                return
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("Hermes TUI gateway restarted"))
            self._pending.clear()
            env = os.environ.copy()
            env["PYTHONPATH"] = f"{HERMES_REPO}:{env.get('PYTHONPATH', '')}" if env.get("PYTHONPATH") else HERMES_REPO
            self._process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "tui_gateway.entry",
                cwd=HERMES_REPO,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._reader_task = asyncio.create_task(self._read_loop())
            asyncio.create_task(self._stderr_loop())

    async def _read_loop(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            while True:
                raw = await process.stdout.readline()
                if not raw:
                    break
                try:
                    message = json.loads(raw.decode("utf-8", "replace"))
                except json.JSONDecodeError:
                    continue
                response_id = message.get("id")
                if isinstance(response_id, int) and response_id in self._pending:
                    future = self._pending.pop(response_id)
                    if not future.done():
                        future.set_result(message)
                else:
                    self._events.append(message)
                    if len(self._events) > 200:
                        del self._events[:100]
                    if message.get("method") == "event":
                        params = message.get("params") or {}
                        if isinstance(params, dict):
                            sid = str(params.get("session_id") or "")
                            for queue in list(self._event_queues.get(sid, [])):
                                try:
                                    queue.put_nowait(params)
                                except asyncio.QueueFull:
                                    try:
                                        _ = queue.get_nowait()
                                    except Exception:
                                        pass
                                    try:
                                        queue.put_nowait(params)
                                    except Exception:
                                        pass
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("Hermes TUI gateway exited"))
            self._pending.clear()

    async def _stderr_loop(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        while True:
            raw = await process.stderr.readline()
            if not raw:
                break
            text = raw.decode("utf-8", "replace").rstrip()
            if text:
                self._stderr_tail.append(text)
                if len(self._stderr_tail) > 200:
                    del self._stderr_tail[:100]

    async def call(self, method: str, params: dict[str, Any], *, timeout: float = 60.0) -> Any:
        await self._ensure_started()
        process = self._process
        if process is None or process.stdin is None or process.returncode is not None:
            raise RuntimeError("Hermes TUI gateway is not running")
        async with self._write_lock:
            self._next_id += 1
            request_id = self._next_id
            loop = asyncio.get_running_loop()
            future: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._pending[request_id] = future
            payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
            process.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
            await process.stdin.drain()
        try:
            response = await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise
        if "error" in response:
            error = response.get("error") or {}
            raise JsonRpcError(int(error.get("code") or 0), str(error.get("message") or "Hermes TUI RPC error"))
        return response.get("result")

    def diagnostics(self) -> dict[str, Any]:
        process = self._process
        return {
            "running": process is not None and process.returncode is None,
            "pending": len(self._pending),
            "events_buffered": len(self._events),
            "event_subscribers": sum(len(queues) for queues in self._event_queues.values()),
            "stderr_tail": self._stderr_tail[-20:],
        }

    def subscribe(self, session_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._event_queues.setdefault(session_id, []).append(queue)
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        queues = self._event_queues.get(session_id)
        if not queues:
            return
        try:
            queues.remove(queue)
        except ValueError:
            return
        if not queues:
            self._event_queues.pop(session_id, None)


TUI_GATEWAY = TuiGatewayClient()


def _now() -> int:
    return int(time.time())


def _normalize_run_status(status: str) -> str:
    normalized = (status or "").strip().lower()
    if normalized in {"complete", "succeeded", "success"}:
        return "completed"
    if normalized in {"cancel", "canceled"}:
        return "cancelled"
    return normalized


def _auth_error() -> web.Response:
    return web.json_response(
        {"error": {"message": "Invalid API key", "type": "invalid_request_error", "code": "invalid_api_key"}},
        status=401,
    )


def _check_auth(request: web.Request) -> Optional[web.Response]:
    if not BRIDGE_API_KEY:
        return None
    auth = request.headers.get("Authorization", "")
    expected = f"Bearer {BRIDGE_API_KEY}"
    if auth != expected:
        return _auth_error()
    return None


def _headers_for_hermes(extra: Optional[dict[str, str]] = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {HERMES_API_KEY}", "Content-Type": "application/json"}
    if extra:
        headers.update(extra)
    return headers


def _normalize_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif item.get("type") == "text" and isinstance(item.get("content"), str):
                    parts.append(item["content"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts)
    return str(content or "")


def _compact_text(text: str) -> str:
    return " ".join(text.split()).strip()


def _join_unique_text(parts: list[str]) -> str:
    unique_parts: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = _compact_text(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_parts.append(part.strip())
    return "\n\n".join(unique_parts)


def _looks_like_answer_echo(reasoning_text: str, final_text: str) -> bool:
    reasoning_norm = _compact_text(reasoning_text)
    final_norm = _compact_text(final_text)
    if not reasoning_norm or not final_norm:
        return False
    if reasoning_norm == final_norm:
        return True
    if final_norm in reasoning_norm and len(reasoning_norm) <= max(len(final_norm) * 2 + 40, len(final_norm) + 120):
        remainder = reasoning_norm.replace(final_norm, "").strip(" -:\n\t")
        return not remainder or remainder == final_norm
    return False


def _latest_user_message_from_chat(body: dict[str, Any]) -> str:
    messages = body.get("messages") or []
    if not isinstance(messages, list):
        return ""
    for msg in reversed(messages):
        if isinstance(msg, dict) and msg.get("role") == "user":
            return _normalize_content(msg.get("content", ""))
    return ""


def _latest_user_message_from_responses(body: dict[str, Any]) -> str:
    raw_input = body.get("input")
    if isinstance(raw_input, str):
        return raw_input
    if isinstance(raw_input, list):
        for item in reversed(raw_input):
            if isinstance(item, str):
                return item
            if isinstance(item, dict) and item.get("role", "user") == "user":
                return _normalize_content(item.get("content", ""))
    return ""


def _session_id(request: web.Request, body: dict[str, Any], message: str) -> str:
    for header in ("X-Hermes-Session-Key", "X-Hermes-Session-Id", "X-OpenWebUI-Chat-Id"):
        value = request.headers.get(header)
        if value:
            return value.strip()
    for key in ("session_id", "conversation", "chat_id"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    metadata = body.get("metadata")
    if isinstance(metadata, dict):
        for key in ("chat_id", "session_id", "conversation_id"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    # Stable fallback for a single-user local OpenWebUI.  Users can later add
    # X-Hermes-Session-Key support if they need strict per-chat isolation.
    return "openwebui-default"


def _parse_slash(text: str) -> Optional[tuple[str, str]]:
    stripped = text.strip()
    if not stripped.startswith("/") or len(stripped) == 1:
        return None
    raw = stripped[1:]
    parts = raw.split(maxsplit=1)
    if not parts:
        return None
    name = parts[0].lower().strip()
    arg = parts[1].strip() if len(parts) > 1 else ""
    return name, arg


def _command_catalog() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cmd in COMMAND_REGISTRY:
        rows.append(
            {
                "name": cmd.name,
                "command": f"/{cmd.name}",
                "description": cmd.description,
                "category": cmd.category,
                "aliases": list(cmd.aliases),
                "args_hint": cmd.args_hint,
                "cli_only": bool(cmd.cli_only),
                "gateway_only": bool(cmd.gateway_only),
            }
        )
    return rows


def _bridge_local_command_catalog() -> list[dict[str, Any]]:
    return [
        {
            "name": "tasks",
            "command": "/tasks",
            "description": "Show active Hermes bridge tasks and current goal status",
            "category": "Hermes Bridge",
            "aliases": [],
            "args_hint": "",
            "cli_only": False,
            "gateway_only": False,
        }
    ]


def _commands_text(arg: str) -> str:
    mode = arg.strip().lower()
    rows = _command_catalog()
    by_name = {str(row["name"]): row for row in rows}
    for row in _bridge_local_command_catalog():
        by_name.setdefault(str(row["name"]), row)
    native = [by_name[name] for name in sorted(BRIDGE_SUPPORTED_COMMANDS) if name in by_name]
    generic = [row for row in rows if str(row["name"]) not in BRIDGE_SUPPORTED_COMMANDS]
    if mode in {"native", "bridge"}:
        lines = [f"Bridge-native OpenWebUI commands ({len(native)}). Other Hermes commands use generic TUI dispatch."]
        lines.extend(f"/[NATIVE] {row['name']} {row['args_hint']} — {row['description']}".strip() for row in native)
        return "\n".join(lines)
    lines = [
        f"Hermes commands in OpenWebUI: {len(rows)} registry commands; {len(native)} bridge-native; {len(generic)} generic via Hermes TUI dispatch."
    ]
    for row in sorted(rows, key=lambda item: str(item["name"])):
        marker = "NATIVE" if str(row["name"]) in BRIDGE_SUPPORTED_COMMANDS else "GENERIC"
        lines.append(f"[{marker}] /{row['name']} {row['args_hint']} — {row['description']}".strip())
    if "tasks" in BRIDGE_SUPPORTED_COMMANDS:
        lines.append("[NATIVE] /tasks — Show active Hermes bridge tasks and current goal status")
    return "\n".join(lines)


async def _post_json(path: str, payload: dict[str, Any], *, session_id: Optional[str] = None) -> dict[str, Any]:
    headers = _headers_for_hermes({"X-Hermes-Session-Key": session_id or ""} if session_id else None)
    timeout = ClientTimeout(total=REQUEST_TIMEOUT_S)
    async with ClientSession(timeout=timeout) as session:
        async with session.post(f"{HERMES_BASE}{path}", headers=headers, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Hermes {path} returned {resp.status}: {text[:500]}")
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Hermes {path} returned invalid JSON: {text[:500]}") from exc


async def _get_json(path: str) -> dict[str, Any]:
    timeout = ClientTimeout(total=REQUEST_TIMEOUT_S)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f"{HERMES_BASE}{path}", headers=_headers_for_hermes()) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Hermes {path} returned {resp.status}: {text[:500]}")
            return json.loads(text)


async def _get_tui_session(openwebui_session_id: str) -> str:
    existing = TUI_SESSIONS.get(openwebui_session_id)
    if existing:
        return existing
    result = await TUI_GATEWAY.call("session.create", {"cols": 120}, timeout=60.0)
    if not isinstance(result, dict) or not isinstance(result.get("session_id"), str):
        raise RuntimeError(f"Hermes TUI session.create returned unexpected result: {result}")
    session_id = result["session_id"]
    TUI_SESSIONS[openwebui_session_id] = session_id
    return session_id


async def _get_tui_session_key(openwebui_session_id: str) -> tuple[str, str]:
    tui_session_id = await _get_tui_session(openwebui_session_id)
    existing_key = TUI_SESSION_KEYS.get(openwebui_session_id)
    if existing_key:
        return tui_session_id, existing_key
    result = await TUI_GATEWAY.call("session.status", {"session_id": tui_session_id}, timeout=60.0)
    output = ""
    if isinstance(result, dict):
        output = str(result.get("output") or "")
    for line in output.splitlines():
        if line.startswith("Session ID:"):
            key = line.split(":", 1)[1].strip()
            if key:
                TUI_SESSION_KEYS[openwebui_session_id] = key
                return tui_session_id, key
    TUI_SESSION_KEYS[openwebui_session_id] = tui_session_id
    return tui_session_id, tui_session_id


async def _tui_session_running(tui_session_id: str) -> bool:
    try:
        result = await TUI_GATEWAY.call("session.status", {"session_id": tui_session_id}, timeout=10.0)
    except Exception:
        return False
    output = str(result.get("output") or "") if isinstance(result, dict) else str(result or "")
    return "Agent Running: Yes" in output


def _format_tui_result(result: Any) -> str:
    if isinstance(result, str):
        return result
    if not isinstance(result, dict):
        return json.dumps(result, ensure_ascii=False, indent=2)
    parts: list[str] = []
    for key in ("output", "notice", "message", "warning"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    if parts:
        return "\n\n".join(parts)
    if isinstance(result.get("output"), str):
        return "✓ Hermes command completed with no text output."
    return json.dumps(result, ensure_ascii=False, indent=2)


async def _dispatch_tui_slash(session_id: str, name: str, arg: str, *, depth: int = 0) -> str:
    if depth > 3:
        return "Hermes command alias recursion limit reached."
    tui_session_id = await _get_tui_session(session_id)
    command = f"/{name}" + (f" {arg}" if arg else "")
    try:
        result = await TUI_GATEWAY.call(
            "slash.exec",
            {"session_id": tui_session_id, "command": command},
            timeout=90.0,
        )
        return _format_tui_result(result)
    except JsonRpcError as exc:
        if exc.code != 4018:
            if exc.code in {4004, 4005, 4010, 4011}:
                TUI_SESSIONS.pop(session_id, None)
            return f"Hermes /{name} failed ({exc.code}): {exc.message}"

    try:
        result = await TUI_GATEWAY.call(
            "command.dispatch",
            {"session_id": tui_session_id, "name": name, "arg": arg},
            timeout=90.0,
        )
    except JsonRpcError as exc:
        if exc.code in {4004, 4005, 4010, 4011}:
            TUI_SESSIONS.pop(session_id, None)
        return f"Hermes /{name} failed ({exc.code}): {exc.message}"
    if isinstance(result, dict) and result.get("type") == "alias" and isinstance(result.get("target"), str):
        parsed = _parse_slash(result["target"])
        if parsed is None:
            return f"Hermes command alias target is not a slash command: {result['target']}"
        alias_name, alias_arg = parsed
        return await _dispatch_tui_slash(session_id, alias_name, alias_arg, depth=depth + 1)
    return _format_tui_result(result)


async def _start_hermes_run(prompt: str, session_id: str, *, instructions: Optional[str] = None) -> str:
    payload: dict[str, Any] = {"model": "hermes-agent", "input": prompt, "session_id": session_id}
    if instructions:
        payload["instructions"] = instructions
    data = await _post_json("/v1/runs", payload, session_id=session_id)
    run_id = str(data.get("run_id") or "")
    if not run_id:
        raise RuntimeError(f"Hermes /v1/runs response did not include run_id: {data}")
    return run_id


def _responses_history_and_message(body: dict[str, Any]) -> tuple[str, list[dict[str, str]]]:
    raw_input = body.get("input")
    if isinstance(raw_input, str):
        return raw_input, []
    history: list[dict[str, str]] = []
    if isinstance(raw_input, list):
        normalized: list[dict[str, str]] = []
        for item in raw_input:
            if isinstance(item, str):
                normalized.append({"role": "user", "content": item})
            elif isinstance(item, dict):
                role = str(item.get("role") or "user")
                normalized.append({"role": role, "content": _normalize_content(item.get("content", ""))})
        if normalized:
            return normalized[-1]["content"], normalized[:-1]
    return _latest_user_message_from_responses(body), []


async def _start_hermes_run_for_responses(body: dict[str, Any], session_id: str) -> str:
    user_message, history = _responses_history_and_message(body)
    payload: dict[str, Any] = {
        "model": body.get("model", "hermes-agent"),
        "input": user_message,
        "session_id": session_id,
    }
    if history:
        payload["conversation_history"] = history
    if isinstance(body.get("instructions"), str):
        payload["instructions"] = body["instructions"]
    data = await _post_json("/v1/runs", payload, session_id=session_id)
    run_id = str(data.get("run_id") or "")
    if not run_id:
        raise RuntimeError(f"Hermes /v1/runs response did not include run_id: {data}")
    return run_id


async def _wait_for_hermes_run(run_id: str) -> dict[str, Any]:
    while True:
        status = await _get_json(f"/v1/runs/{run_id}")
        if status.get("status") in TERMINAL_RUN_STATUSES:
            return status
        await asyncio.sleep(POLL_INTERVAL_S)


async def _run_goal_loop(session_id: str, prompt: str, bridge_run_id: str) -> None:
    record = RUNS[bridge_run_id]
    mgr = GoalManager(session_id=session_id)
    next_prompt = prompt
    record.status = "running"
    record.last_message = "goal loop started"
    record.updated_at = time.time()
    try:
        while mgr.is_active():
            hermes_run_id = await _start_hermes_run(next_prompt, session_id)
            record.hermes_run_ids.append(hermes_run_id)
            record.last_message = f"Hermes run {hermes_run_id} running"
            record.updated_at = time.time()
            status = await _wait_for_hermes_run(hermes_run_id)
            if status.get("status") != "completed":
                record.status = str(status.get("status") or "failed")
                record.error = str(status.get("error") or status)
                try:
                    mgr.pause(reason=f"Hermes run {hermes_run_id} ended with {record.status}")
                except Exception:
                    pass
                return
            final_output = str(status.get("output") or "")
            record.output = final_output
            decision = mgr.evaluate_after_turn(final_output, user_initiated=True)
            record.last_message = str(decision.get("message") or decision.get("reason") or "")
            record.updated_at = time.time()
            if not decision.get("should_continue"):
                record.status = str(decision.get("status") or "completed")
                return
            next_prompt = str(decision.get("continuation_prompt") or "").strip()
            if not next_prompt:
                record.status = "paused"
                record.error = "Goal judge requested continuation but did not provide a prompt"
                try:
                    mgr.pause(reason=record.error)
                except Exception:
                    pass
                return
        record.status = "inactive"
    except asyncio.CancelledError:
        record.status = "cancelled"
        record.last_message = "bridge goal task cancelled"
        raise
    except Exception as exc:
        record.status = "failed"
        record.error = str(exc)
        try:
            mgr.pause(reason=f"bridge error: {exc}")
        except Exception:
            pass
    finally:
        record.updated_at = time.time()
        GOAL_TASKS.pop(session_id, None)


def _ensure_goal_task(session_id: str, prompt: str) -> BridgeRun:
    existing_task = GOAL_TASKS.get(session_id)
    if existing_task and not existing_task.done():
        for record in RUNS.values():
            if record.kind == "goal" and record.session_id == session_id and record.status in ACTIVE_RUN_STATUSES:
                return record
    run_id = f"goal_{uuid.uuid4().hex[:12]}"
    record = BridgeRun(run_id=run_id, kind="goal", session_id=session_id, prompt=prompt)
    RUNS[run_id] = record
    GOAL_TASKS[session_id] = asyncio.create_task(_run_goal_loop(session_id, prompt, run_id))
    return record


def _create_goal_stream_record(session_id: str, prompt: str) -> BridgeRun:
    run_id = f"goal_{uuid.uuid4().hex[:12]}"
    record = BridgeRun(run_id=run_id, kind="goal", session_id=session_id, prompt=prompt)
    RUNS[run_id] = record
    return record


def _active_goal_record(session_id: str) -> Optional[BridgeRun]:
    matches = [
        record
        for record in RUNS.values()
        if record.kind == "goal" and record.session_id == session_id and record.status in ACTIVE_RUN_STATUSES
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda record: record.created_at)[-1]


async def _cancel_goal_records(session_id: str, reason: str, *, clear: bool = False) -> None:
    task = GOAL_TASKS.get(session_id)
    if task and not task.done():
        task.cancel()
    for record in RUNS.values():
        if record.kind != "goal" or record.session_id != session_id or record.status not in ACTIVE_RUN_STATUSES:
            continue
        for hermes_run_id in list(record.hermes_run_ids):
            try:
                await _post_json(f"/v1/runs/{hermes_run_id}/stop", {}, session_id=session_id)
            except Exception:
                pass
        record.status = "cancelled"
        record.last_message = reason
        record.updated_at = time.time()
    mgr = GoalManager(session_id=session_id)
    if clear:
        mgr.clear()
    else:
        try:
            mgr.pause(reason=reason)
        except Exception:
            pass


async def _goal_control(session_id: str, arg: str) -> tuple[str, Optional[BridgeRun]]:
    mgr = GoalManager(session_id=session_id)
    lower = arg.strip().lower()
    if not arg or lower == "status":
        lines = [mgr.status_line()]
        active = [r.line() for r in RUNS.values() if r.kind == "goal" and r.session_id == session_id]
        if active:
            lines.append("Bridge goal jobs:")
            lines.extend(active[-5:])
        return "\n".join(lines), None
    if lower == "pause":
        state = mgr.pause(reason="user-paused")
        await _cancel_goal_records(session_id, "paused by user", clear=False)
        return ("No goal set." if state is None else f"⏸ Goal paused: {state.goal}"), None
    if lower == "resume":
        state = mgr.resume()
        if state is None:
            return "No goal to resume.", None
        prompt = mgr.next_continuation_prompt() or state.goal
        record = _ensure_goal_task(session_id, prompt)
        return f"▶ Goal resumed: {state.goal}\nBridge job: {record.run_id}", record
    if lower in {"clear", "stop", "done"}:
        had = mgr.has_goal()
        await _cancel_goal_records(session_id, "cleared by user", clear=True)
        return ("✓ Goal cleared." if had else "No active goal."), None
    state = mgr.set(arg)
    record = _ensure_goal_task(session_id, state.goal)
    return (
        f"⊙ Goal set ({state.max_turns}-turn budget): {state.goal}\n"
        "OpenWebUI will stay in running mode while this stream is open. Click the stop button to terminate the goal.\n"
        f"Bridge job: {record.run_id}\n"
        "Controls: /goal status · /goal pause · /goal resume · /goal clear"
    ), record


async def _handle_goal(session_id: str, arg: str) -> str:
    content, _record = await _goal_control(session_id, arg)
    return content


async def _handle_goal_tui(session_id: str, arg: str) -> str:
    tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
    mgr = GoalManager(session_id=tui_session_key)
    lower = arg.strip().lower()
    if not arg or lower == "status":
        lines = [mgr.status_line()]
        active = [r.line() for r in RUNS.values() if r.kind == "goal" and r.session_id == session_id]
        if active:
            lines.append("Bridge goal streams:")
            lines.extend(active[-5:])
        return "\n".join(lines)
    if lower in {"pause", "stop", "done"}:
        state = mgr.pause(reason="user-paused")
        if await _tui_session_running(tui_session_id):
            try:
                await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
            except Exception:
                pass
        return "No goal set." if state is None else f"⏸ Goal paused: {state.goal}"
    if lower == "clear":
        had = mgr.has_goal()
        mgr.clear()
        if await _tui_session_running(tui_session_id):
            try:
                await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
            except Exception:
                pass
        for record in RUNS.values():
            if record.kind == "goal" and record.session_id == session_id and record.status in ACTIVE_RUN_STATUSES:
                record.status = "cancelled"
                record.last_message = "cleared by user"
                record.updated_at = time.time()
        return "✓ Goal cleared." if had else "No active goal."
    if lower == "resume":
        state = mgr.resume()
        if state is None:
            return "No goal to resume."
        return "▶ Goal resumed. Send this as a streaming /goal resume message to watch the Hermes TUI transcript."
    state = mgr.set(arg)
    return (
        f"⊙ Goal set ({state.max_turns}-turn budget): {state.goal}\n"
        "Send this as a streaming /goal message to watch the Hermes TUI transcript in OpenWebUI."
    )


async def _goal_stream_control(session_id: str, arg: str) -> tuple[str, Optional[BridgeRun], str]:
    mgr = GoalManager(session_id=session_id)
    lower = arg.strip().lower()
    if not arg or lower == "status":
        content, record = await _goal_control(session_id, arg)
        return content, record, ""
    if lower in {"pause", "clear", "stop", "done"}:
        content, record = await _goal_control(session_id, arg)
        return content, record, ""
    if lower == "resume":
        active_record = _active_goal_record(session_id)
        if active_record is not None:
            return (
                f"A goal stream is already running for this session.\nBridge job: {active_record.run_id}\n"
                "Use /goal status, /goal pause, /goal clear, or the OpenWebUI stop button before starting another stream.",
                None,
                "",
            )
        state = mgr.resume()
        if state is None:
            return "No goal to resume.", None, ""
        prompt = mgr.next_continuation_prompt() or state.goal
        record = _create_goal_stream_record(session_id, prompt)
        return "▶ Goal resumed. Streaming Hermes output below.", record, prompt
    active_record = _active_goal_record(session_id)
    if active_record is not None:
        return (
            f"A goal stream is already running for this session.\nBridge job: {active_record.run_id}\n"
            "Use /goal status, /goal pause, /goal clear, or the OpenWebUI stop button before starting another stream.",
            None,
            "",
        )
    state = mgr.set(arg)
    prompt = state.goal
    record = _create_goal_stream_record(session_id, prompt)
    return (
        "⊙ Goal started. Streaming Hermes reasoning and answers below. Click stop to terminate.",
        record,
        prompt,
    )


def _goal_reasoning_status_text(record: BridgeRun) -> str:
    elapsed = int(time.time() - record.created_at)
    detail = record.last_message or "waiting for Hermes run to report progress"
    return (
        f"Goal job: {record.run_id}\n"
        f"Status: {record.status}\n"
        f"Elapsed: {elapsed}s\n"
        f"Latest: {detail}\n\n"
        "OpenWebUI is keeping this stream open so the stop button can cancel the Hermes goal."
    )


def _compact_progress_value(value: Any, *, limit: int = 220) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False, default=str)
    text = " ".join(text.replace("\r", " ").replace("\n", " ").split())
    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def _format_goal_tool_event(event_data: dict[str, Any], active_tool_previews: dict[str, str]) -> tuple[str, str]:
    event_name = str(event_data.get("event") or "")
    tool = str(event_data.get("tool") or event_data.get("tool_name") or "tool")
    preview = _compact_progress_value(event_data.get("preview"), limit=260)
    duration = event_data.get("duration")
    is_error = bool(event_data.get("error"))
    if event_name == "tool.started":
        if preview:
            active_tool_previews[tool] = preview
            return f"{event_name}:{tool}:{preview}", f"🔧 Hermes tool `{tool}` started: {preview}"
        active_tool_previews.setdefault(tool, "")
        return f"{event_name}:{tool}", f"🔧 Hermes tool `{tool}` started"
    if event_name == "tool.completed":
        started_preview = active_tool_previews.pop(tool, "")
        detail_parts = []
        if isinstance(duration, (int, float)):
            detail_parts.append(f"{duration:.3g}s")
        if started_preview:
            detail_parts.append(started_preview)
        detail = ": " + " · ".join(detail_parts) if detail_parts else ""
        icon = "⚠️" if is_error else "✅"
        status = "failed" if is_error else "completed"
        return f"{event_name}:{tool}:{started_preview}:{is_error}", f"{icon} Hermes tool `{tool}` {status}{detail}"
    if event_name == "tool.failed":
        error = _compact_progress_value(event_data.get("error"), limit=260)
        detail = f": {error}" if error else ""
        return f"{event_name}:{tool}:{error}", f"⚠️ Hermes tool `{tool}` failed{detail}"
    return f"{event_name}:{tool}:{preview}", f"↪ Hermes event `{event_name}` for tool `{tool}`{(': ' + preview) if preview else ''}"


def _hermes_tool_line(name: str, context: str = "", *, default: str = "⚙️") -> str:
    try:
        display_module = import_module("agent.display")
        get_tool_emoji = getattr(display_module, "get_tool_emoji")
        emoji = str(get_tool_emoji(name, default=default))
    except Exception:
        emoji = default
    if context:
        return f'{emoji} {name}: "{context}"'
    return f"{emoji} {name}..."


def _tui_status_text(payload: dict[str, Any]) -> str:
    text = str(payload.get("text") or payload.get("message") or "").strip()
    kind = str(payload.get("kind") or "").strip()
    if text:
        return text if not kind else f"[{kind}] {text}"
    return json.dumps(payload, ensure_ascii=False, default=str)


def _flatten_tui_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return event
    flattened = {"type": event.get("type"), "session_id": event.get("session_id")}
    flattened.update(payload)
    return flattened


def _format_tui_event_for_openwebui(event: dict[str, Any], *, message_delta_seen: bool) -> tuple[str, str]:
    event = _flatten_tui_event(event)
    event_type = str(event.get("type") or "")
    if event_type == "message.delta":
        return str(event.get("text") or ""), ""
    if event_type == "message.complete":
        text = str(event.get("text") or "")
        warning = str(event.get("warning") or "").strip()
        status = str(event.get("status") or "").strip()
        parts: list[str] = []
        if text and not message_delta_seen:
            parts.append(text)
        if warning:
            parts.append(f"⚠ {warning}")
        if status and status not in {"complete", "completed"}:
            parts.append(f"[{status}]")
        return ("\n\n".join(parts), str(event.get("reasoning") or ""))
    if event_type in {"reasoning.available", "reasoning.delta", "thinking.delta"}:
        return "", str(event.get("text") or "")
    if event_type in {"tool.progress", "tool.start"}:
        name = str(event.get("name") or event.get("tool_name") or "tool")
        context = str(event.get("context") or event.get("preview") or event.get("text") or "").strip()
        args_text = str(event.get("args_text") or "").strip()
        line = _hermes_tool_line(name, context)
        if args_text:
            line = f"{line}\n{args_text}"
        return line, ""
    if event_type == "tool.generating":
        name = str(event.get("name") or "tool")
        return _hermes_tool_line(name), ""
    if event_type == "tool.complete":
        name = str(event.get("name") or "tool")
        summary = str(event.get("summary") or "").strip()
        inline_diff = str(event.get("inline_diff") or "").strip()
        result_text = str(event.get("result_text") or "").strip()
        todos = event.get("todos")
        parts: list[str] = []
        if summary:
            parts.append(summary)
        if inline_diff:
            parts.append(inline_diff)
        if result_text:
            parts.append(result_text)
        if todos:
            parts.append(json.dumps(todos, ensure_ascii=False, indent=2, default=str))
        if not parts:
            return "", ""
        return f"{_hermes_tool_line(name)}\n" + "\n".join(parts), ""
    if event_type == "status.update":
        return _tui_status_text(event), ""
    if event_type == "error":
        return f"Error: {str(event.get('message') or event)}", ""
    if event_type == "clarify.request":
        question = str(event.get("question") or "").strip()
        choices = event.get("choices")
        suffix = ""
        if isinstance(choices, list) and choices:
            suffix = "\n" + "\n".join(f"- {choice}" for choice in choices)
        return f"Clarification requested: {question}{suffix}", ""
    if event_type in {"approval.request", "sudo.request", "secret.request"}:
        return json.dumps({k: v for k, v in event.items() if k not in {"session_id", "type"}}, ensure_ascii=False, indent=2, default=str), ""
    text = str(event.get("text") or event.get("summary") or "").strip()
    if text:
        return text, ""
    return "", ""


async def _stop_goal_from_stream_disconnect(record: BridgeRun) -> None:
    await _cancel_goal_records(record.session_id, "stopped from OpenWebUI running button", clear=False)


async def _handle_subgoal(session_id: str, arg: str) -> str:
    _tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
    mgr = GoalManager(session_id=tui_session_key)
    if not mgr.has_goal():
        return "No active goal. Set one with /goal <text>."
    lower = arg.strip().lower()
    if not arg:
        return f"{mgr.status_line()}\n{mgr.render_subgoals()}"
    if lower == "clear":
        count = mgr.clear_subgoals()
        return f"✓ Cleared {count} subgoal{'s' if count != 1 else ''}." if count else "No subgoals to clear."
    if lower.startswith("remove "):
        try:
            removed = mgr.remove_subgoal(int(lower.split(None, 1)[1]))
            return f"✓ Removed subgoal: {removed}"
        except Exception as exc:
            return f"/subgoal remove failed: {exc}"
    try:
        text = mgr.add_subgoal(arg)
        return f"✓ Added subgoal: {text}"
    except Exception as exc:
        return f"/subgoal failed: {exc}"


async def _handle_background(session_id: str, arg: str) -> str:
    if not arg:
        return "Usage: /background <prompt>"
    run_id = await _start_hermes_run(arg, session_id)
    bridge_id = f"run_{uuid.uuid4().hex[:12]}"
    record = BridgeRun(run_id=bridge_id, kind="background", session_id=session_id, prompt=arg, hermes_run_ids=[run_id])
    record.last_message = f"Hermes run {run_id} started"
    RUNS[bridge_id] = record
    return f"▶ Background run started.\nBridge job: {bridge_id}\nHermes run: {run_id}\nUse /tasks to check status."


async def _refresh_background_records() -> None:
    for record in list(RUNS.values()):
        if record.kind != "background" or record.status in TERMINAL_RUN_STATUSES:
            continue
        if not record.hermes_run_ids:
            continue
        try:
            status = await _get_json(f"/v1/runs/{record.hermes_run_ids[-1]}")
            record.status = str(status.get("status") or record.status)
            record.output = str(status.get("output") or record.output or "")
            record.error = str(status.get("error") or record.error or "")
            record.updated_at = time.time()
        except Exception as exc:
            record.error = str(exc)
            record.updated_at = time.time()


async def _handle_tasks(session_id: str) -> str:
    await _refresh_background_records()
    lines = [f"Hermes bridge tasks for session {session_id}:"]
    matches = [r for r in RUNS.values() if r.session_id == session_id]
    if not matches:
        lines.append("- No bridge-started tasks.")
    else:
        lines.extend(record.line() for record in sorted(matches, key=lambda r: r.created_at)[-20:])
    try:
        _tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
        lines.append(GoalManager(session_id=tui_session_key).status_line())
    except Exception:
        lines.append(GoalManager(session_id=session_id).status_line())
    return "\n".join(lines)


async def _handle_stop(session_id: str, arg: str) -> str:
    targets = [arg.strip()] if arg.strip() else [r.run_id for r in RUNS.values() if r.session_id == session_id]
    stopped: list[str] = []
    tui_session_id = ""
    tui_session_key = ""
    try:
        tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
        if await _tui_session_running(tui_session_id):
            await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
            stopped.append(f"tui:{tui_session_id}")
        if tui_session_key:
            GoalManager(session_id=tui_session_key).pause(reason="stopped-by-user")
    except Exception:
        pass
    if not targets:
        return "Stopped: " + ", ".join(stopped) if stopped else "No bridge-started tasks to stop."
    for target in targets:
        record = RUNS.get(target)
        if not record:
            continue
        if record.kind == "goal":
            task = GOAL_TASKS.get(record.session_id)
            if task and not task.done():
                task.cancel()
            try:
                GoalManager(session_id=tui_session_key or record.session_id).pause(reason="stopped-by-user")
            except Exception:
                GoalManager(session_id=record.session_id).pause(reason="stopped-by-user")
        for hermes_run_id in record.hermes_run_ids:
            try:
                await _post_json(f"/v1/runs/{hermes_run_id}/stop", {}, session_id=record.session_id)
            except Exception:
                pass
        record.status = "cancelled"
        record.updated_at = time.time()
        stopped.append(target)
    return "Stopped: " + ", ".join(stopped) if stopped else "No matching bridge task found."


async def _handle_status(session_id: str) -> str:
    health = await _get_json("/health")
    try:
        tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
        goal_status = GoalManager(session_id=tui_session_key).status_line()
    except Exception:
        tui_session_id = ""
        goal_status = GoalManager(session_id=session_id).status_line()
    return json.dumps(
        {
            "bridge": "ok",
            "hermes": health,
            "session_id": session_id,
            "tui_session_id": tui_session_id,
            "active_bridge_tasks": sum(1 for r in RUNS.values() if r.session_id == session_id and r.status not in TERMINAL_RUN_STATUSES),
            "goal": goal_status,
            "tui_gateway": TUI_GATEWAY.diagnostics(),
        },
        ensure_ascii=False,
        indent=2,
    )


async def _handle_yolo(session_id: str, arg: str) -> str:
    approval_module = import_module("tools.approval")
    disable_session_yolo = getattr(approval_module, "disable_session_yolo")
    enable_session_yolo = getattr(approval_module, "enable_session_yolo")
    is_session_yolo_enabled = getattr(approval_module, "is_session_yolo_enabled")

    action = arg.strip().lower()
    current = is_session_yolo_enabled(session_id)
    if action in {"", "toggle"}:
        enabled = not current
    elif action in {"on", "enable", "enabled", "true", "1", "yes"}:
        enabled = True
    elif action in {"off", "disable", "disabled", "false", "0", "no"}:
        enabled = False
    elif action in {"status", "state"}:
        return f"YOLO mode is {'enabled' if current else 'disabled'} for this OpenWebUI session."
    else:
        return "Usage: /yolo [on|off|status]. Without an argument, /yolo toggles this OpenWebUI session."

    if enabled:
        enable_session_yolo(session_id)
    else:
        disable_session_yolo(session_id)
    return (
        f"YOLO mode {'enabled' if enabled else 'disabled'} for this OpenWebUI session. "
        "This only affects dangerous-command approval prompts for Hermes runs using the same session key."
    )


async def _dispatch_slash(session_id: str, name: str, arg: str) -> str:
    aliases = {"tasks": "agents", "bg": "background", "btw": "background", "q": "queue"}
    name = aliases.get(name, name)
    if name == "goal":
        return await _handle_goal_tui(session_id, arg)
    if name == "subgoal":
        return await _handle_subgoal(session_id, arg)
    if name == "background":
        return await _handle_background(session_id, arg)
    if name == "agents":
        return await _handle_tasks(session_id)
    if name == "stop":
        return await _handle_stop(session_id, arg)
    if name == "status":
        return await _handle_status(session_id)
    if name == "yolo":
        return await _handle_yolo(session_id, arg)
    if name == "commands":
        return _commands_text(arg)
    if name == "help":
        return "Hermes commands are available through OpenWebUI. Bridge-native commands: /goal, /subgoal, /background, /tasks, /agents, /stop, /status, /yolo, /commands. Other Hermes slash commands are forwarded through Hermes TUI generic dispatch."
    if name == "queue":
        return await _dispatch_tui_slash(session_id, name, arg)
    return await _dispatch_tui_slash(session_id, name, arg)


def _chat_response(content: str) -> web.Response:
    return web.json_response(
        {
            "id": f"chatcmpl-bridge-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": _now(),
            "model": "hermes-agent",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        }
    )


async def _write_chat_stream(request: web.Request, content: str) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)
    cid = f"chatcmpl-bridge-{uuid.uuid4().hex}"
    first = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}
    await resp.write(f"data: {json.dumps(first, ensure_ascii=False)}\n\n".encode())
    delta = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}]}
    await resp.write(f"data: {json.dumps(delta, ensure_ascii=False)}\n\n".encode())
    done = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
    await resp.write(f"data: {json.dumps(done, ensure_ascii=False)}\n\n".encode())
    await resp.write(b"data: [DONE]\n\n")
    await resp.write_eof()
    return resp


async def _safe_sse_write(resp: web.StreamResponse, event: Optional[str], payload: dict[str, Any]) -> bool:
    try:
        prefix = f"event: {event}\n" if event else ""
        await resp.write(f"{prefix}data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))
        return True
    except Exception:
        return False


async def _safe_raw_write(resp: web.StreamResponse, raw: bytes) -> bool:
    try:
        await resp.write(raw)
        return True
    except Exception:
        return False


async def _write_goal_chat_stream(request: web.Request, session_id: str, arg: str) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)
    cid = f"chatcmpl-bridge-{uuid.uuid4().hex}"
    content, record = await _goal_control(session_id, arg)
    first = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}
    if not await _safe_sse_write(resp, None, first):
        if record:
            await _stop_goal_from_stream_disconnect(record)
        return resp
    delta = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}]}
    if not await _safe_sse_write(resp, None, delta):
        if record:
            await _stop_goal_from_stream_disconnect(record)
        return resp
    if record is not None:
        while record.status in {"started", "running"}:
            await asyncio.sleep(GOAL_STREAM_INTERVAL_S)
            if not await _safe_raw_write(resp, b": goal-progress\n\n"):
                await _stop_goal_from_stream_disconnect(record)
                return resp
        final_line = f"\n\n✅ Goal stream ended: {record.status}. {record.last_message or record.output[:500]}"
        final_delta = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {"content": final_line}, "finish_reason": None}]}
        await _safe_sse_write(resp, None, final_delta)
    done = {"id": cid, "object": "chat.completion.chunk", "created": _now(), "model": "hermes-agent", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
    await _safe_sse_write(resp, None, done)
    await _safe_raw_write(resp, b"data: [DONE]\n\n")
    try:
        await resp.write_eof()
    except Exception:
        pass
    return resp


def _responses_response(content: str) -> web.Response:
    return web.json_response(
        {
            "id": f"resp_bridge_{uuid.uuid4().hex}",
            "object": "response",
            "status": "completed",
            "created_at": _now(),
            "model": "hermes-agent",
            "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": content}]}],
        }
    )


async def _write_responses_stream(request: web.Request, content: str) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)
    rid = f"resp_bridge_{uuid.uuid4().hex}"
    msg_id = f"msg_{uuid.uuid4().hex}"
    events = [
        ("response.created", {"type": "response.created", "response": {"id": rid, "object": "response", "status": "in_progress", "created_at": _now(), "model": "hermes-agent", "output": []}, "sequence_number": 0}),
        ("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": {"id": msg_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}, "sequence_number": 1}),
        ("response.output_text.delta", {"type": "response.output_text.delta", "item_id": msg_id, "output_index": 0, "content_index": 0, "delta": content, "sequence_number": 2}),
        ("response.output_text.done", {"type": "response.output_text.done", "item_id": msg_id, "output_index": 0, "content_index": 0, "text": content, "sequence_number": 3}),
        ("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": {"id": msg_id, "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": content}]}, "sequence_number": 4}),
        ("response.completed", {"type": "response.completed", "response": {"id": rid, "object": "response", "status": "completed", "created_at": _now(), "model": "hermes-agent", "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": content}]}]}, "sequence_number": 5}),
    ]
    for event, payload in events:
        await resp.write(f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode())
    await resp.write_eof()
    return resp


async def _next_hermes_run_event(iterator: AsyncIterator[dict[str, Any]]) -> dict[str, Any]:
    return await iterator.__anext__()

async def _iter_hermes_run_events(run_id: str) -> AsyncIterator[dict[str, Any]]:
    timeout = ClientTimeout(total=REQUEST_TIMEOUT_S)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f"{HERMES_BASE}/v1/runs/{run_id}/events", headers=_headers_for_hermes()) as upstream:
            if upstream.status >= 400:
                text = await upstream.text()
                raise RuntimeError(f"Hermes run events returned {upstream.status}: {text[:500]}")
            buffer = ""
            async for raw in upstream.content:
                buffer += raw.decode("utf-8", "replace")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    data_text = line[5:].strip()
                    if not data_text or data_text == "[DONE]":
                        continue
                    try:
                        yield json.loads(data_text)
                    except json.JSONDecodeError:
                        continue


async def _write_goal_responses_stream(request: web.Request, session_id: str, arg: str) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)
    response_id = f"resp_bridge_{uuid.uuid4().hex}"
    reasoning_id = f"rs_{uuid.uuid4().hex}"
    message_id = f"msg_{uuid.uuid4().hex}"
    sequence = 0
    final_text_parts: list[str] = []
    reasoning_text = "Waiting for Hermes reasoning..."

    async def send_event(event: str, payload: dict[str, Any]) -> bool:
        nonlocal sequence
        payload.setdefault("type", event)
        payload.setdefault("sequence_number", sequence)
        sequence += 1
        return await _safe_sse_write(resp, event, payload)

    async def send_message_delta(delta: str) -> bool:
        if not delta:
            return True
        final_text_parts.append(delta)
        return await send_event(
            "response.output_text.delta",
            {"item_id": message_id, "output_index": 1, "content_index": 0, "delta": delta, "logprobs": []},
        )

    async def update_reasoning(text: str) -> bool:
        nonlocal reasoning_text
        reasoning_text = text or reasoning_text
        return await send_event(
            "response.reasoning_summary_part.done",
            {
                "item_id": reasoning_id,
                "output_index": 0,
                "summary_index": 0,
                "part": {"type": "summary_text", "text": reasoning_text},
            },
        )

    content, record, next_prompt = await _goal_stream_control(session_id, arg)
    try:
        if not await send_event(
            "response.created",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "in_progress",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "output": [],
                }
            },
        ):
            if record:
                await _stop_goal_from_stream_disconnect(record)
            return resp

        if record is None:
            if not await send_event(
                "response.output_item.added",
                {"output_index": 0, "item": {"id": message_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}},
            ):
                return resp
            final_text_parts.append(content)
            await send_event(
                "response.output_text.delta",
                {"item_id": message_id, "output_index": 0, "content_index": 0, "delta": content, "logprobs": []},
            )
            message_item = {"id": message_id, "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": content}]}
            await send_event("response.output_text.done", {"item_id": message_id, "output_index": 0, "content_index": 0, "text": content, "logprobs": []})
            await send_event("response.output_item.done", {"output_index": 0, "item": message_item})
            await send_event(
                "response.completed",
                {"response": {"id": response_id, "object": "response", "status": "completed", "created_at": _now(), "model": "hermes-agent", "output": [message_item]}},
            )
            await resp.write_eof()
            return resp

        if not await send_event(
            "response.output_item.added",
            {"output_index": 0, "item": {"id": reasoning_id, "type": "reasoning", "status": "in_progress", "summary": []}},
        ):
            await _stop_goal_from_stream_disconnect(record)
            return resp
        if not await send_event(
            "response.reasoning_summary_part.added",
            {"item_id": reasoning_id, "output_index": 0, "summary_index": 0, "part": {"type": "summary_text", "text": reasoning_text}},
        ):
            await _stop_goal_from_stream_disconnect(record)
            return resp
        if not await send_event(
            "response.output_item.added",
            {"output_index": 1, "item": {"id": message_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}},
        ):
            await _stop_goal_from_stream_disconnect(record)
            return resp
        if not await send_message_delta(content):
            await _stop_goal_from_stream_disconnect(record)
            return resp

        mgr = GoalManager(session_id=session_id)
        record.status = "running"
        record.last_message = "goal stream started"
        record.updated_at = time.time()
        reasoning_parts: list[str] = []
        turn = 0

        while mgr.is_active() and record.status not in TERMINAL_RUN_STATUSES:
            turn += 1
            hermes_run_id = await _start_hermes_run(next_prompt, session_id)
            turn_started_at = time.time()
            last_event_name = "run.started"
            record.hermes_run_ids.append(hermes_run_id)
            record.last_message = f"Hermes run {hermes_run_id} streaming"
            record.updated_at = time.time()
            if not await send_message_delta(
                f"\n\n---\n\n### Hermes goal turn {turn}\n\n"
                f"⏳ Started Hermes run `{hermes_run_id}`. Waiting for Hermes reasoning/output...\n"
            ):
                await _stop_goal_from_stream_disconnect(record)
                return resp
            turn_text_parts: list[str] = []
            run_status = "running"
            run_error = ""
            active_tool_previews: dict[str, str] = {}
            seen_tool_events: dict[str, int] = {}
            suppressed_tool_repeats = 0
            suppressed_tool_burst = 0
            tool_window_started_at = time.time()
            tool_window_visible_count = 0
            last_activity_text = f"Hermes run `{hermes_run_id}` started"
            event_iter = _iter_hermes_run_events(hermes_run_id).__aiter__()
            event_task: Optional[asyncio.Task[dict[str, Any]]] = asyncio.create_task(_next_hermes_run_event(event_iter))
            try:
                while event_task is not None:
                    if record.status in TERMINAL_RUN_STATUSES:
                        break
                    done, _pending = await asyncio.wait({event_task}, timeout=GOAL_VISIBLE_PROGRESS_INTERVAL_S)
                    if not done:
                        elapsed = int(time.time() - turn_started_at)
                        record.last_message = (
                            f"Hermes run {hermes_run_id} still running; "
                            f"last event={last_event_name}; elapsed={elapsed}s"
                        )
                        record.updated_at = time.time()
                        suppressed_total = suppressed_tool_repeats + suppressed_tool_burst
                        repeat_note = f" Suppressed {suppressed_total} repetitive/high-frequency tool event(s)." if suppressed_total else ""
                        progress = (
                            f"\n\n⏳ Hermes is still working: turn {turn}, run `{hermes_run_id}`, "
                            f"elapsed {elapsed}s. Latest activity: {last_activity_text}."
                            f"{repeat_note} No assistant text/reasoning has arrived yet.\n"
                        )
                        suppressed_tool_repeats = 0
                        suppressed_tool_burst = 0
                        if not await send_message_delta(progress):
                            await _stop_goal_from_stream_disconnect(record)
                            return resp
                        status_reasoning = (
                            _join_unique_text(reasoning_parts)
                            or f"Hermes run `{hermes_run_id}` is active, but no provider reasoning has arrived yet."
                        )
                        if not await update_reasoning(
                            f"{status_reasoning}\n\nStatus: turn {turn}, elapsed {elapsed}s. Latest activity: {last_activity_text}."
                        ):
                            await _stop_goal_from_stream_disconnect(record)
                            return resp
                        continue
                    try:
                        event_data = event_task.result()
                    except StopAsyncIteration:
                        break
                    event_task = None
                    event_name = str(event_data.get("event") or "")
                    event_status = _normalize_run_status(str(event_data.get("status") or ""))
                    last_event_name = event_name or event_status or "unknown"
                    record.last_message = f"Hermes run {hermes_run_id} event: {last_event_name}"
                    record.updated_at = time.time()
                    if event_name == "reasoning.available":
                        last_activity_text = "Hermes reasoning became available"
                        text = str(event_data.get("text") or "").strip()
                        if text:
                            reasoning_parts.append(f"Turn {turn} · Hermes run {hermes_run_id}\n{text}")
                            if not await update_reasoning(_join_unique_text(reasoning_parts)):
                                await _stop_goal_from_stream_disconnect(record)
                                return resp
                        if not await send_message_delta(f"\n\n🧠 Hermes reasoning arrived for run `{hermes_run_id}`.\n"):
                            await _stop_goal_from_stream_disconnect(record)
                            return resp
                    elif event_name == "message.delta":
                        last_activity_text = "Hermes assistant output is streaming"
                        delta = str(event_data.get("delta") or "")
                        if delta:
                            turn_text_parts.append(delta)
                            if not await send_message_delta(delta):
                                await _stop_goal_from_stream_disconnect(record)
                                return resp
                    elif event_name in {"run.completed", "run.done"} or event_status in SUCCESS_RUN_STATUSES:
                        output = str(event_data.get("output") or "")
                        if output and not turn_text_parts:
                            turn_text_parts.append(output)
                            if not await send_message_delta(output):
                                await _stop_goal_from_stream_disconnect(record)
                                return resp
                        run_status = "done" if event_name == "run.done" or event_status == "done" else "completed"
                        break
                    elif event_name in {"run.failed", "run.cancelled", "run.inactive", "run.paused"} or event_status in NON_SUCCESS_TERMINAL_RUN_STATUSES:
                        run_status = _normalize_run_status(event_name.removeprefix("run.")) if event_name.startswith("run.") else event_status
                        run_error = str(event_data.get("error") or event_name)
                        break
                    elif event_name in {"tool.started", "tool.completed", "tool.failed"}:
                        event_key, tool_line = _format_goal_tool_event(event_data, active_tool_previews)
                        count = seen_tool_events.get(event_key, 0)
                        seen_tool_events[event_key] = count + 1
                        last_activity_text = tool_line
                        if count:
                            suppressed_tool_repeats += 1
                        else:
                            now = time.time()
                            if now - tool_window_started_at > GOAL_TOOL_EVENT_WINDOW_S:
                                if suppressed_tool_burst:
                                    if not await send_message_delta(
                                        f"\n\n↪ Suppressed {suppressed_tool_burst} additional high-frequency Hermes tool event(s); latest: {last_activity_text}\n"
                                    ):
                                        await _stop_goal_from_stream_disconnect(record)
                                        return resp
                                    suppressed_tool_burst = 0
                                tool_window_started_at = now
                                tool_window_visible_count = 0
                            if tool_window_visible_count >= GOAL_TOOL_EVENT_MAX_PER_WINDOW:
                                suppressed_tool_burst += 1
                            else:
                                tool_window_visible_count += 1
                                if not await send_message_delta(f"\n\n{tool_line}\n"):
                                    await _stop_goal_from_stream_disconnect(record)
                                    return resp
                    else:
                        last_activity_text = f"Hermes event `{last_event_name}`"
                        if not await send_message_delta(f"\n\n↪ {last_activity_text} received for run `{hermes_run_id}`.\n"):
                            await _stop_goal_from_stream_disconnect(record)
                            return resp
                    event_task = asyncio.create_task(_next_hermes_run_event(event_iter))
            finally:
                if event_task is not None and not event_task.done():
                    event_task.cancel()
                aclose = getattr(event_iter, "aclose", None)
                if aclose is not None:
                    try:
                        await aclose()
                    except Exception:
                        pass

            if record.status in TERMINAL_RUN_STATUSES:
                if record.status != "done":
                    await send_message_delta(f"\n\n⏹ Goal stream ended: {record.status}. {record.last_message or record.error}")
                break

            if run_status not in SUCCESS_RUN_STATUSES:
                record.status = run_status if run_status in TERMINAL_RUN_STATUSES else "failed"
                record.error = run_error or f"Hermes run {hermes_run_id} ended with {run_status}"
                try:
                    mgr.pause(reason=record.error)
                except Exception:
                    pass
                await send_message_delta(f"\n\n⚠ Hermes goal run ended: {record.error}")
                break

            final_output = "".join(turn_text_parts)
            record.output = final_output
            decision = mgr.evaluate_after_turn(final_output, user_initiated=True)
            record.last_message = str(decision.get("message") or decision.get("reason") or "")
            record.updated_at = time.time()
            if not decision.get("should_continue"):
                record.status = str(decision.get("status") or "completed")
                await send_message_delta(f"\n\n✅ Goal stream ended: {record.status}. {record.last_message or record.output[:500]}")
                break
            next_prompt = str(decision.get("continuation_prompt") or "").strip()
            if not next_prompt:
                record.status = "paused"
                record.error = "Goal judge requested continuation but did not provide a prompt"
                try:
                    mgr.pause(reason=record.error)
                except Exception:
                    pass
                await send_message_delta(f"\n\n⚠ Goal paused: {record.error}")
                break
            await send_message_delta(f"\n\n↪ Goal continuing: {record.last_message or 'next turn requested'}\n")

        if record.status not in TERMINAL_RUN_STATUSES and not mgr.is_active():
            record.status = "inactive"
            await send_message_delta(f"\n\n✅ Goal stream ended: {record.status}. {record.last_message or record.output[:500]}")

        final_text = "".join(final_text_parts)
        reasoning_item = {"id": reasoning_id, "type": "reasoning", "status": "completed", "summary": [{"type": "summary_text", "text": reasoning_text}]}
        message_item = {"id": message_id, "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": final_text}]}
        await send_event("response.output_text.done", {"item_id": message_id, "output_index": 1, "content_index": 0, "text": final_text, "logprobs": []})
        await send_event("response.output_item.done", {"output_index": 0, "item": reasoning_item})
        await send_event("response.output_item.done", {"output_index": 1, "item": message_item})
        await send_event(
            "response.completed",
            {"response": {"id": response_id, "object": "response", "status": "completed", "created_at": _now(), "model": "hermes-agent", "output": [reasoning_item, message_item]}},
        )
    except asyncio.CancelledError:
        if record:
            await _stop_goal_from_stream_disconnect(record)
        raise
    except Exception as exc:
        if record:
            record.status = "failed"
            record.error = str(exc)
            try:
                GoalManager(session_id=session_id).pause(reason=f"bridge goal stream error: {exc}")
            except Exception:
                pass
        await send_event(
            "response.failed",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "failed",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "error": {"message": f"Hermes bridge goal stream failed: {exc}", "type": "bridge_error"},
                    "output": [],
                }
            },
        )
    try:
        await resp.write_eof()
    except Exception:
        pass
    return resp


async def _stop_tui_goal_from_stream_disconnect(
    openwebui_session_id: str,
    tui_session_id: str,
    tui_session_key: str,
    record: Optional[BridgeRun],
) -> None:
    try:
        await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
    except Exception:
        pass
    try:
        GoalManager(session_id=tui_session_key).pause(reason="stopped from OpenWebUI running button")
    except Exception:
        pass
    if record is not None and record.status in ACTIVE_RUN_STATUSES:
        record.status = "cancelled"
        record.last_message = "stopped from OpenWebUI running button"
        record.updated_at = time.time()
    await _cancel_goal_records(openwebui_session_id, "stopped from OpenWebUI running button", clear=False)


async def _write_goal_responses_stream_tui(request: web.Request, session_id: str, arg: str) -> web.StreamResponse:
    """Stream /goal through Hermes TUI events, preserving Hermes' own display model.

    The older bridge path consumed /v1/runs events, which are intentionally
    minimal API lifecycle events.  Hermes' TUI gateway emits the display-facing
    transcript events (tool.start/tool.complete/status.update/message.delta,
    rendered text, reasoning, inline diffs, etc.), so OpenWebUI should mirror
    that stream instead of inventing bridge-specific progress prose.
    """
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)
    response_id = f"resp_bridge_{uuid.uuid4().hex}"
    reasoning_id = f"rs_{uuid.uuid4().hex}"
    message_id = f"msg_{uuid.uuid4().hex}"
    sequence = 0
    final_text_parts: list[str] = []
    reasoning_text_current = ""
    last_reasoning_piece = ""
    reasoning_started = False
    reasoning_item_added = False
    message_delta_seen = False
    record: Optional[BridgeRun] = None
    tui_session_id = ""
    tui_session_key = ""

    async def send_event(event: str, payload: dict[str, Any]) -> bool:
        nonlocal sequence
        payload.setdefault("type", event)
        payload.setdefault("sequence_number", sequence)
        sequence += 1
        return await _safe_sse_write(resp, event, payload)

    async def send_message_delta(delta: str) -> bool:
        if not delta:
            return True
        final_text_parts.append(delta)
        return await send_event(
            "response.output_text.delta",
            {"item_id": message_id, "output_index": 0, "content_index": 0, "delta": delta, "logprobs": []},
        )

    async def send_reasoning_delta(text: str) -> bool:
        nonlocal last_reasoning_piece, reasoning_item_added, reasoning_started, reasoning_text_current
        if not text:
            return True
        if text == last_reasoning_piece:
            return True
        last_reasoning_piece = text
        if not reasoning_text_current:
            reasoning_text_current = text
        elif text.startswith(reasoning_text_current):
            reasoning_text_current = text
        elif not reasoning_text_current.endswith(text):
            reasoning_text_current += text
        if not reasoning_item_added:
            reasoning_item_added = True
            if not await send_event(
                "response.output_item.added",
                {"output_index": 1, "item": {"id": reasoning_id, "type": "reasoning", "status": "in_progress", "summary": []}},
            ):
                return False
        if not reasoning_started:
            reasoning_started = True
            if not await send_event(
                "response.reasoning_summary_part.added",
                {
                    "item_id": reasoning_id,
                    "output_index": 1,
                    "summary_index": 0,
                    "part": {"type": "summary_text", "text": reasoning_text_current},
                },
            ):
                return False
        return await send_event(
            "response.reasoning_summary_part.done",
            {
                "item_id": reasoning_id,
                "output_index": 1,
                "summary_index": 0,
                "part": {"type": "summary_text", "text": reasoning_text_current},
            },
        )

    async def finish_response(status: str = "completed") -> None:
        final_text = "".join(final_text_parts)
        message_item = {
            "id": message_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": final_text}],
        }
        output_items = [message_item]
        await send_event("response.output_text.done", {"item_id": message_id, "output_index": 0, "content_index": 0, "text": final_text, "logprobs": []})
        await send_event("response.output_item.done", {"output_index": 0, "item": message_item})
        if reasoning_item_added or reasoning_text_current:
            reasoning_item = {"id": reasoning_id, "type": "reasoning", "status": "completed", "summary": []}
            if reasoning_text_current:
                reasoning_item["summary"] = [{"type": "summary_text", "text": reasoning_text_current}]
            await send_event("response.output_item.done", {"output_index": 1, "item": reasoning_item})
            output_items.append(reasoning_item)
        await send_event(
            "response.completed",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": status,
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "output": output_items,
                }
            },
        )

    async def finite_response(content: str) -> web.StreamResponse:
        await send_event(
            "response.output_item.added",
            {"output_index": 0, "item": {"id": message_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}},
        )
        await send_message_delta(content)
        await finish_response()
        await resp.write_eof()
        return resp

    event_queue: Optional[asyncio.Queue[dict[str, Any]]] = None
    try:
        tui_session_id, tui_session_key = await _get_tui_session_key(session_id)
        mgr = GoalManager(session_id=tui_session_key)
        lower = arg.strip().lower()

        if not await send_event(
            "response.created",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "in_progress",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "output": [],
                }
            },
        ):
            return resp
        if not arg or lower == "status":
            return await finite_response(mgr.status_line())
        if lower in {"pause", "stop", "done"}:
            state = mgr.pause(reason="user-paused")
            if await _tui_session_running(tui_session_id):
                try:
                    await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
                except Exception:
                    pass
            for active in RUNS.values():
                if active.kind == "goal" and active.session_id == session_id and active.status in ACTIVE_RUN_STATUSES:
                    active.status = "cancelled"
                    active.last_message = "paused by user"
                    active.updated_at = time.time()
            return await finite_response("No goal set." if state is None else f"⏸ Goal paused: {state.goal}")
        if lower == "clear":
            had = mgr.has_goal()
            mgr.clear()
            if await _tui_session_running(tui_session_id):
                try:
                    await TUI_GATEWAY.call("session.interrupt", {"session_id": tui_session_id}, timeout=10.0)
                except Exception:
                    pass
            for active in RUNS.values():
                if active.kind == "goal" and active.session_id == session_id and active.status in ACTIVE_RUN_STATUSES:
                    active.status = "cancelled"
                    active.last_message = "cleared by user"
                    active.updated_at = time.time()
            return await finite_response("✓ Goal cleared." if had else "No active goal.")

        active_record = _active_goal_record(session_id)
        if active_record is not None:
            return await finite_response(
                "A goal stream is already running for this OpenWebUI session. "
                "Use /goal status, /goal pause, /goal clear, or the OpenWebUI stop button before starting another stream."
            )

        if lower == "resume":
            state = mgr.resume()
            if state is None:
                return await finite_response("No goal to resume.")
            prompt = mgr.next_continuation_prompt() or state.goal
        else:
            state = mgr.set(arg)
            prompt = state.goal

        record = _create_goal_stream_record(session_id, prompt)
        record.status = "running"
        record.last_message = f"Hermes TUI session {tui_session_id} streaming"
        record.updated_at = time.time()

        if not await send_event(
            "response.output_item.added",
            {"output_index": 0, "item": {"id": message_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []}},
        ):
            await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
            return resp

        event_queue = TUI_GATEWAY.subscribe(tui_session_id)
        submit_result = await TUI_GATEWAY.call("prompt.submit", {"session_id": tui_session_id, "text": prompt}, timeout=60.0)
        if isinstance(submit_result, dict) and submit_result.get("error"):
            raise RuntimeError(str(submit_result.get("error")))

        inactive_since: Optional[float] = None
        last_visible_event_at = time.time()
        while True:
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=3.0)
            except asyncio.TimeoutError:
                if not await _safe_raw_write(resp, b": hermes-tui-keepalive\n\n"):
                    await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
                    return resp
                if not mgr.is_active():
                    inactive_since = inactive_since or time.time()
                    if time.time() - inactive_since > 1.0:
                        break
                continue

            event = _flatten_tui_event(event)
            event_type = str(event.get("type") or "")
            record.last_message = event_type
            record.updated_at = time.time()
            text, reasoning = _format_tui_event_for_openwebui(event, message_delta_seen=message_delta_seen)
            if event_type == "message.complete" and reasoning_text_current and reasoning:
                reasoning = ""
            if reasoning and not await send_reasoning_delta(reasoning):
                await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
                return resp
            if text:
                if event_type == "message.delta":
                    message_delta_seen = True
                    if not await send_message_delta(text):
                        await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
                        return resp
                else:
                    if not await send_message_delta(f"\n\n{text}\n"):
                        await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
                        return resp
                last_visible_event_at = time.time()

            if event_type == "message.complete":
                message_delta_seen = False
                complete_status = str(event.get("status") or "").strip().lower()
                if complete_status in {"interrupted", "error", "failed"}:
                    try:
                        mgr.pause(reason=complete_status)
                    except Exception:
                        pass
                    record.status = complete_status if complete_status in TERMINAL_RUN_STATUSES else "paused"
                    inactive_since = time.time()
                    continue
                if not mgr.is_active():
                    inactive_since = time.time()
            elif event_type == "message.start":
                message_delta_seen = False
                if final_text_parts and not await send_message_delta("\n\n---\n"):
                    await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
                    return resp
            elif event_type == "status.update" and str(event.get("kind") or "") == "goal" and not mgr.is_active():
                inactive_since = time.time()
            elif time.time() - last_visible_event_at > REQUEST_TIMEOUT_S:
                raise RuntimeError("Hermes TUI event stream timed out")

            if inactive_since is not None and time.time() - inactive_since > 1.0:
                break

        record.status = "completed" if not mgr.is_active() else "inactive"
        await finish_response()
    except asyncio.CancelledError:
        if tui_session_id and tui_session_key:
            await _stop_tui_goal_from_stream_disconnect(session_id, tui_session_id, tui_session_key, record)
        raise
    except Exception as exc:
        if record is not None:
            record.status = "failed"
            record.error = str(exc)
        try:
            await send_event(
                "response.failed",
                {
                    "response": {
                        "id": response_id,
                        "object": "response",
                        "status": "failed",
                        "created_at": _now(),
                        "model": "hermes-agent",
                        "error": {"message": f"Hermes TUI goal stream failed: {exc}", "type": "bridge_error"},
                        "output": [],
                    }
                },
            )
        except Exception:
            pass
    finally:
        if event_queue is not None and tui_session_id:
            TUI_GATEWAY.unsubscribe(tui_session_id, event_queue)
        try:
            await resp.write_eof()
        except Exception:
            pass
    return resp


async def _write_responses_stream_from_run(request: web.Request, body: dict[str, Any], session_id: str) -> web.StreamResponse:
    """Stream OpenAI Responses events backed by Hermes /v1/runs events.

    Hermes' OpenAI-compatible /v1/responses path does not currently expose the
    `reasoning.available` lifecycle event.  The structured /v1/runs event API
    does, so this adapter translates that event into Responses reasoning
    summary events that OpenWebUI already understands.
    """
    resp = web.StreamResponse(status=200, headers=SSE_HEADERS)
    await resp.prepare(request)

    response_id = f"resp_bridge_{uuid.uuid4().hex}"
    reasoning_id = f"rs_{uuid.uuid4().hex}"
    message_id = f"msg_{uuid.uuid4().hex}"
    sequence = 0
    final_text_parts: list[str] = []
    reasoning_parts: list[str] = []

    async def send_event(event: str, payload: dict[str, Any]) -> None:
        nonlocal sequence
        payload.setdefault("type", event)
        payload.setdefault("sequence_number", sequence)
        sequence += 1
        await resp.write(f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))

    try:
        run_id = await _start_hermes_run_for_responses(body, session_id)
        await send_event(
            "response.created",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "in_progress",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "output": [],
                }
            },
        )
        await send_event(
            "response.output_item.added",
            {
                "output_index": 0,
                "item": {"id": message_id, "type": "message", "status": "in_progress", "role": "assistant", "content": []},
            },
        )

        async for event_data in _iter_hermes_run_events(run_id):
            event_name = str(event_data.get("event") or "")
            event_status = _normalize_run_status(str(event_data.get("status") or ""))
            if event_name == "reasoning.available":
                text = str(event_data.get("text") or "")
                if text:
                    reasoning_parts.append(text)
            elif event_name == "message.delta":
                delta = str(event_data.get("delta") or "")
                if delta:
                    final_text_parts.append(delta)
                    await send_event(
                        "response.output_text.delta",
                        {
                            "item_id": message_id,
                            "output_index": 0,
                            "content_index": 0,
                            "delta": delta,
                            "logprobs": [],
                        },
                    )
            elif event_name in {"run.completed", "run.done"} or event_status in SUCCESS_RUN_STATUSES:
                output = str(event_data.get("output") or "")
                if output and not final_text_parts:
                    final_text_parts.append(output)
                    await send_event(
                        "response.output_text.delta",
                        {
                            "item_id": message_id,
                            "output_index": 0,
                            "content_index": 0,
                            "delta": output,
                            "logprobs": [],
                        },
                    )
                break
            elif event_name in {"run.failed", "run.cancelled", "run.inactive", "run.paused"} or event_status in NON_SUCCESS_TERMINAL_RUN_STATUSES:
                error = str(event_data.get("error") or event_name or event_status)
                final_text_parts.append(f"\n\nHermes run ended: {error}")
                break

        final_text = "".join(final_text_parts)
        reasoning_text = _join_unique_text(reasoning_parts)
        include_reasoning = bool(reasoning_text) and not _looks_like_answer_echo(reasoning_text, final_text)
        output_items: list[dict[str, Any]] = []
        message_item = {
            "id": message_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": final_text}],
        }
        await send_event(
            "response.output_text.done",
            {"item_id": message_id, "output_index": 0, "content_index": 0, "text": final_text, "logprobs": []},
        )
        if include_reasoning:
            reasoning_part = {"type": "summary_text", "text": reasoning_text}
            reasoning_item = {"id": reasoning_id, "type": "reasoning", "status": "completed", "summary": [reasoning_part]}
            await send_event(
                "response.output_item.added",
                {"output_index": 1, "item": {"id": reasoning_id, "type": "reasoning", "status": "in_progress", "summary": []}},
            )
            await send_event(
                "response.reasoning_summary_part.added",
                {"item_id": reasoning_id, "output_index": 1, "summary_index": 0, "part": reasoning_part},
            )
            await send_event(
                "response.reasoning_summary_part.done",
                {"item_id": reasoning_id, "output_index": 1, "summary_index": 0, "part": reasoning_part},
            )
            await send_event("response.output_item.done", {"output_index": 1, "item": reasoning_item})
            output_items.append(reasoning_item)
        await send_event("response.output_item.done", {"output_index": 0, "item": message_item})
        output_items.append(message_item)
        await send_event(
            "response.completed",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "completed",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "output": output_items,
                }
            },
        )
    except Exception as exc:
        error_text = f"Hermes bridge responses stream failed: {exc}"
        await send_event(
            "response.failed",
            {
                "response": {
                    "id": response_id,
                    "object": "response",
                    "status": "failed",
                    "created_at": _now(),
                    "model": "hermes-agent",
                    "error": {"message": error_text, "type": "bridge_error"},
                    "output": [],
                }
            },
        )
    await resp.write_eof()
    return resp


async def _maybe_handle_slash(request: web.Request, endpoint: str, body: dict[str, Any]) -> Optional[web.StreamResponse | web.Response]:
    message = _latest_user_message_from_chat(body) if endpoint == "chat" else _latest_user_message_from_responses(body)
    parsed = _parse_slash(message)
    if parsed is None:
        return None
    name, arg = parsed
    session_id = _session_id(request, body, message)
    if name == "goal" and body.get("stream") is True:
        if endpoint == "chat":
            return await _write_chat_stream(
                request,
                "Hermes TUI-mirrored /goal streaming requires the OpenAI Responses API endpoint. "
                "This OpenWebUI bridge/provider is configured for Responses mode; if you see this, reselect the Hermes model or refresh OpenWebUI settings.",
            )
        return await _write_goal_responses_stream_tui(request, session_id, arg)
    content = await _dispatch_slash(session_id, name, arg)
    if endpoint == "chat":
        if body.get("stream") is True:
            return await _write_chat_stream(request, content)
        return _chat_response(content)
    if body.get("stream") is True:
        return await _write_responses_stream(request, content)
    return _responses_response(content)


async def _handle_chat(request: web.Request) -> web.StreamResponse | web.Response:
    auth = _check_auth(request)
    if auth:
        return auth
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": {"message": "Invalid JSON", "type": "invalid_request_error"}}, status=400)
    slash = await _maybe_handle_slash(request, "chat", body)
    if slash is not None:
        return slash
    return await _proxy(request, override_body=json.dumps(body).encode("utf-8"))


async def _handle_responses(request: web.Request) -> web.StreamResponse | web.Response:
    auth = _check_auth(request)
    if auth:
        return auth
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": {"message": "Invalid JSON", "type": "invalid_request_error"}}, status=400)
    slash = await _maybe_handle_slash(request, "responses", body)
    if slash is not None:
        return slash
    if body.get("stream") is True:
        message = _latest_user_message_from_responses(body)
        session_id = _session_id(request, body, message)
        return await _write_responses_stream_from_run(request, body, session_id)
    return await _proxy(request, override_body=json.dumps(body).encode("utf-8"))


HOP_BY_HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length"}


async def _proxy(request: web.Request, *, override_body: Optional[bytes] = None) -> web.StreamResponse | web.Response:
    path_qs = request.rel_url.path_qs
    target = f"{HERMES_BASE}{path_qs}"
    body = override_body if override_body is not None else await request.read()
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
    headers["Authorization"] = f"Bearer {HERMES_API_KEY}"
    timeout = ClientTimeout(total=REQUEST_TIMEOUT_S)
    async with ClientSession(timeout=timeout) as session:
        async with session.request(request.method, target, headers=headers, data=body) as upstream:
            response_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
            if upstream.headers.get("Content-Type", "").startswith("text/event-stream"):
                resp = web.StreamResponse(status=upstream.status, headers=response_headers)
                await resp.prepare(request)
                async for chunk in upstream.content.iter_chunked(8192):
                    await resp.write(chunk)
                await resp.write_eof()
                return resp
            payload = await upstream.read()
            return web.Response(status=upstream.status, body=payload, headers=response_headers)


async def _handle_proxy(request: web.Request) -> web.StreamResponse | web.Response:
    auth = _check_auth(request)
    if auth:
        return auth
    return await _proxy(request)


async def health(_: web.Request) -> web.Response:
    hermes_ok = False
    try:
        data = await _get_json("/health")
        hermes_ok = data.get("status") == "ok"
    except Exception:
        pass
    return web.json_response({"status": "ok", "bridge": "hermes-openwebui", "hermes_ok": hermes_ok, "port": BRIDGE_PORT})


async def commands(request: web.Request) -> web.Response:
    auth = _check_auth(request)
    if auth:
        return auth
    return web.json_response({"object": "hermes.commands", "data": _command_catalog()})


def create_app() -> web.Application:
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.router.add_get("/health", health)
    app.router.add_get("/v1/health", health)
    app.router.add_get("/hermes/commands", commands)
    app.router.add_post("/v1/chat/completions", _handle_chat)
    app.router.add_post("/v1/responses", _handle_responses)
    app.router.add_route("*", "/{tail:.*}", _handle_proxy)
    return app


def main() -> None:
    if BRIDGE_HOST not in {"127.0.0.1", "localhost", "::1"} and BRIDGE_API_KEY == DEFAULT_BRIDGE_KEY:
        raise RuntimeError(
            "Refusing to bind Hermes OpenWebUI bridge outside loopback with the default API key. "
            "Set HERMES_OPENWEBUI_BRIDGE_KEY to a strong secret or bind to 127.0.0.1."
        )
    web.run_app(create_app(), host=BRIDGE_HOST, port=BRIDGE_PORT, print=None)


if __name__ == "__main__":
    main()

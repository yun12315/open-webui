#!/usr/bin/env python3
"""Sync Hermes slash commands into OpenWebUI prompt commands.

OpenWebUI already has a prompt-command table used by the chat input slash
menu.  This script mirrors Hermes' live COMMAND_REGISTRY into that table so
typing "/" in OpenWebUI can show Hermes commands without modifying the
OpenWebUI frontend.

The script is intentionally DB-level because the local OpenWebUI instance may
run with browser/session auth while startup automation has no user token.  It
preserves OpenWebUI's prompt_history entries for the rows it owns.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any


DEFAULT_DB_PATH = Path(__file__).resolve().parent / "backend-data-hermes-fresh" / "webui.db"
DB_PATH = Path(os.getenv("OPENWEBUI_DB", str(DEFAULT_DB_PATH)))
HERMES_REPO = os.getenv("HERMES_REPO", "/root/.hermes/hermes-agent")
WSL_PYTHON = os.getenv("HERMES_WSL_PYTHON", "/root/.hermes/hermes-agent/venv/bin/python3")
SYNC_MARKER = "hermes-openwebui-bridge"
COMMAND_SYNC_MODE = os.getenv("HERMES_OPENWEBUI_COMMAND_SYNC_MODE", "all").strip().lower()
SUPPORTED_BRIDGE_COMMANDS = {
    name.strip()
    for name in os.getenv(
        "HERMES_OPENWEBUI_SUPPORTED_COMMANDS",
        "goal,subgoal,background,agents,tasks,stop,status,yolo,commands,help",
    ).split(",")
    if name.strip()
}
BRIDGE_LOCAL_COMMANDS: list[dict[str, Any]] = [
    {
        "name": "tasks",
        "description": "Show active Hermes bridge tasks and current goal status",
        "category": "Hermes Bridge",
        "aliases": [],
        "args_hint": "",
        "cli_only": False,
        "gateway_only": False,
    }
]


def _load_commands_from_wsl() -> list[dict[str, Any]]:
    code = r'''
import json
import sys
sys.path.insert(0, "{repo}")
from hermes_cli.commands import COMMAND_REGISTRY
rows = []
for cmd in COMMAND_REGISTRY:
    rows.append({{
        "name": cmd.name,
        "description": cmd.description,
        "category": cmd.category,
        "aliases": list(cmd.aliases),
        "args_hint": cmd.args_hint,
        "cli_only": bool(cmd.cli_only),
        "gateway_only": bool(cmd.gateway_only),
    }})
print(json.dumps(rows, ensure_ascii=False))
'''.format(repo=HERMES_REPO.replace('"', '\\"'))
    result = subprocess.run(
        ["wsl.exe", "-d", "Ubuntu", "-u", "root", "--", WSL_PYTHON, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    return json.loads(result.stdout)


def _bridge_supported_commands(commands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name = {str(command["name"]): command for command in commands}
    for local_command in BRIDGE_LOCAL_COMMANDS:
        by_name.setdefault(str(local_command["name"]), local_command)
    rows: list[dict[str, Any]] = []
    names = sorted(by_name) if COMMAND_SYNC_MODE in {"all", "full", "registry"} else sorted(SUPPORTED_BRIDGE_COMMANDS)
    for name in names:
        command = by_name.get(name)
        if command is not None:
            command = dict(command)
            command["bridge_supported"] = name in SUPPORTED_BRIDGE_COMMANDS
            rows.append(command)
    return rows


def _admin_user_id(con: sqlite3.Connection) -> str:
    row = con.execute("SELECT id FROM user WHERE role = 'admin' ORDER BY created_at LIMIT 1").fetchone()
    if row:
        return str(row[0])
    row = con.execute("SELECT id FROM user ORDER BY created_at LIMIT 1").fetchone()
    if row:
        return str(row[0])
    raise RuntimeError("OpenWebUI user table is empty; open OpenWebUI once before syncing prompts")


def _prompt_content(command: dict[str, Any]) -> str:
    name = str(command["name"])
    hint = str(command.get("args_hint") or "").strip()
    if hint and ("text" in hint or "prompt" in hint or "name" in hint or "status" in hint):
        return f"/{name} "
    return f"/{name}"


def _prompt_name(command: dict[str, Any]) -> str:
    hint = str(command.get("args_hint") or "").strip()
    suffix = f" {hint}" if hint else ""
    support = "" if bool(command.get("bridge_supported", True)) else " (Hermes native; bridge fallback)"
    return f"Hermes /{command['name']}{suffix}{support}"


def _upsert_prompt(con: sqlite3.Connection, user_id: str, command: dict[str, Any]) -> tuple[str, bool, bool]:
    now = int(time.time())
    name = str(command["name"])
    prompt_id_row = con.execute("SELECT id, data FROM prompt WHERE command = ?", (name,)).fetchone()
    if prompt_id_row:
        try:
            existing_data = json.loads(prompt_id_row[1] or "{}")
        except Exception:
            existing_data = {}
        if existing_data.get("source") != SYNC_MARKER:
            return str(prompt_id_row[0]), False, True
    prompt_id = str(prompt_id_row[0]) if prompt_id_row else str(uuid.uuid4())
    history_id = str(uuid.uuid4())
    content = _prompt_content(command)
    tags = ["hermes", str(command.get("category") or "Hermes")]
    data = {
        "source": SYNC_MARKER,
        "hermes_command": name,
        "description": command.get("description") or "",
        "args_hint": command.get("args_hint") or "",
        "aliases": command.get("aliases") or [],
        "category": command.get("category") or "Hermes",
        "bridge_supported": bool(command.get("bridge_supported", True)),
    }
    meta = {
        "description": command.get("description") or "",
        "source": SYNC_MARKER,
        "content_type": "hermes_slash_command",
    }
    snapshot = {
        "name": _prompt_name(command),
        "content": content,
        "command": name,
        "data": data,
        "meta": meta,
        "tags": tags,
        "access_grants": [],
    }
    if prompt_id_row:
        con.execute(
            """
            UPDATE prompt
               SET user_id = ?, name = ?, content = ?, data = ?, meta = ?, is_active = 1,
                   version_id = ?, tags = ?, updated_at = ?
             WHERE id = ?
            """,
            (
                user_id,
                _prompt_name(command),
                content,
                json.dumps(data, ensure_ascii=False),
                json.dumps(meta, ensure_ascii=False),
                history_id,
                json.dumps(tags, ensure_ascii=False),
                now,
                prompt_id,
            ),
        )
        created = False
    else:
        con.execute(
            """
            INSERT INTO prompt
                (id, command, user_id, name, content, data, meta, is_active, version_id, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                prompt_id,
                name,
                user_id,
                _prompt_name(command),
                content,
                json.dumps(data, ensure_ascii=False),
                json.dumps(meta, ensure_ascii=False),
                history_id,
                json.dumps(tags, ensure_ascii=False),
                now,
                now,
            ),
        )
        created = True
    con.execute(
        """
        INSERT INTO prompt_history
            (id, prompt_id, parent_id, snapshot, user_id, commit_message, created_at)
        VALUES (?, ?, NULL, ?, ?, ?, ?)
        """,
        (
            history_id,
            prompt_id,
            json.dumps(snapshot, ensure_ascii=False),
            user_id,
            f"Sync Hermes command /{name}",
            now,
        ),
    )
    return prompt_id, created, False


def _deactivate_unsupported_prompts(con: sqlite3.Connection, supported_names: set[str]) -> int:
    if COMMAND_SYNC_MODE in {"all", "full", "registry"}:
        return 0
    rows = con.execute("SELECT id, command, data FROM prompt WHERE is_active = 1").fetchall()
    deactivated = 0
    for prompt_id, command, raw_data in rows:
        try:
            data = json.loads(raw_data or "{}")
        except Exception:
            continue
        if data.get("source") != SYNC_MARKER:
            continue
        if str(command) in supported_names:
            continue
        con.execute("UPDATE prompt SET is_active = 0, updated_at = ? WHERE id = ?", (int(time.time()), prompt_id))
        deactivated += 1
    return deactivated


def main() -> int:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)
    registry_commands = _load_commands_from_wsl()
    registry_unsupported = sum(
        1 for command in registry_commands if str(command.get("name") or "") not in SUPPORTED_BRIDGE_COMMANDS
    )
    commands = _bridge_supported_commands(registry_commands)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA busy_timeout = 10000")
        user_id = _admin_user_id(con)
        created = 0
        updated = 0
        skipped_existing = 0
        for command in commands:
            _prompt_id, was_created, was_skipped = _upsert_prompt(con, user_id, command)
            if was_skipped:
                skipped_existing += 1
            elif was_created:
                created += 1
            else:
                updated += 1
        deactivated = _deactivate_unsupported_prompts(con, {str(command["name"]) for command in commands})
        con.commit()
    print(
        json.dumps(
            {
                "ok": True,
                "created": created,
                "updated": updated,
                "skipped_existing_non_bridge": skipped_existing,
                "deactivated_unsupported": deactivated,
                "total_synced": len(commands),
                "registry_total": len(registry_commands),
                "unsupported_hidden": registry_unsupported,
                "sync_mode": COMMAND_SYNC_MODE,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

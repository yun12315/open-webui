# Hermes OpenWebUI bridge integration

This optional integration lets the OpenWebUI desktop app talk to a Hermes Agent running in WSL without modifying either upstream project.

## What is included

- `hermes-openwebui-bridge.py` - OpenAI-compatible proxy plus Hermes slash-command adapter.
- `sync-hermes-commands-to-openwebui.py` - syncs Hermes slash commands into OpenWebUI prompt commands.
- `auto-select-codex-model.py` - keeps Hermes configured for the newest large Codex model and fallback chain.
- `start-*.ps1`, `start-*.cmd`, and `start-*.sh` - Windows/WSL launch helpers for the local bridge stack.

## Expected layout

Copy this folder to a local runtime directory and create an OpenWebUI Python virtual environment beside the scripts:

```powershell
python -m venv venv
.\venv\Scripts\pip install open-webui aiohttp pyyaml
```

Hermes is expected in WSL at `/root/.hermes/hermes-agent` by default. Override with environment variables when needed.

## Security notes

- The default bridge key is `hermes-openwebui-local-only` and is intended for loopback-only development.
- Set `HERMES_OPENWEBUI_BRIDGE_KEY` to a strong value before exposing anything beyond `127.0.0.1`.
- `WEBUI_SECRET_KEY` is generated at runtime and stored under the local OpenWebUI data directory; it is intentionally not committed.
- Do not commit runtime directories such as `venv/`, `logs/`, `backend-data-*`, desktop caches, or SQLite databases.

## Start

Run this from Windows:

```cmd
start-openwebui-hermes.cmd
```

The launcher starts the Hermes gateway on `8642`, the bridge on `8650`, syncs Hermes slash commands into OpenWebUI, starts OpenWebUI on `8080`, and opens the browser.

## Useful environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `HERMES_WSL_DISTRO` | `Ubuntu` | WSL distro name. |
| `HERMES_WSL_USER` | `root` | WSL user for Hermes. |
| `HERMES_REPO` | `/root/.hermes/hermes-agent` | Hermes source checkout in WSL. |
| `HERMES_OPENWEBUI_BRIDGE_KEY` | `hermes-openwebui-local-only` | Bridge/OpenWebUI API key. |
| `OPENWEBUI_EXE` | `venv\Scripts\open-webui.exe` | OpenWebUI executable path. |
| `OPENWEBUI_DATA_DIR` | `backend-data-hermes-fresh` | Local OpenWebUI data directory. |

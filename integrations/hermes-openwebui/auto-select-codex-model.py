from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime
from importlib import import_module
from pathlib import Path

HERMES_REPO = Path(os.getenv("HERMES_REPO", "/root/.hermes/hermes-agent"))
os.chdir(HERMES_REPO)
sys.path.insert(0, str(HERMES_REPO))

import yaml

_auth_module = import_module("hermes_cli.auth")
_codex_models_module = import_module("hermes_cli.codex_models")
resolve_codex_runtime_credentials = getattr(_auth_module, "resolve_codex_runtime_credentials")
get_codex_model_ids = getattr(_codex_models_module, "get_codex_model_ids")

BASE_URL = 'https://chatgpt.com/backend-api/codex'
PRIMARY_EXCLUDE = ('mini', 'nano', 'spark')
MAX_FALLBACKS = 3
CFG_PATH = Path(os.getenv("HERMES_CONFIG", "/root/.hermes/config.yaml"))


def is_large(model: str) -> bool:
    lowered = model.lower()
    return not any(token in lowered for token in PRIMARY_EXCLUDE)


def pick_primary(models: list[str]) -> str | None:
    large = [m for m in models if is_large(m)]
    if large:
        return large[0]
    return models[0] if models else None


def build_fallbacks(models: list[str], selected: str) -> list[str]:
    preferred = [m for m in models if m != selected and is_large(m)]
    remaining = [m for m in models if m != selected and m not in preferred]
    return (preferred + remaining)[:MAX_FALLBACKS]


creds = resolve_codex_runtime_credentials(refresh_if_expiring=True)
access_token = str(creds.get('api_key') or '').strip()
models = get_codex_model_ids(access_token=access_token)
selected = pick_primary(models)
if not selected:
    raise SystemExit('No Codex models discovered')

fallback_models = build_fallbacks(models, selected)

old_text = CFG_PATH.read_text(encoding='utf-8') if CFG_PATH.exists() else ''
config = yaml.safe_load(old_text) if old_text.strip() else {}
if not isinstance(config, dict):
    config = {}

model_cfg = config.get('model')
if not isinstance(model_cfg, dict):
    model_cfg = {}
config['model'] = model_cfg
model_cfg['provider'] = 'openai-codex'
model_cfg['default'] = selected
model_cfg['base_url'] = BASE_URL

config['fallback_providers'] = [
    {'provider': 'openai-codex', 'model': model, 'base_url': BASE_URL}
    for model in fallback_models
]
config.pop('fallback_model', None)

new_text = yaml.safe_dump(config, sort_keys=False, allow_unicode=True)
if new_text != old_text:
    if CFG_PATH.exists():
        backup = CFG_PATH.with_name(f"config.yaml.bak-auto-codex-{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(CFG_PATH, backup)
    CFG_PATH.write_text(new_text, encoding='utf-8')
    print(f'Codex primary set to: {selected}')
    print(f'Codex fallback chain: {fallback_models}')
    print('Config updated')
else:
    print(f'Codex primary unchanged: {selected}')
    print(f'Codex fallback chain unchanged: {fallback_models}')

"""Configuration helpers for the ACM plugin."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


def _get_config_path() -> Path:
    config_path = os.environ.get("NCATBOT_CONFIG_PATH")
    if config_path:
        return Path(config_path)
    return DEFAULT_CONFIG_PATH


def load_config() -> dict[str, Any]:
    """Load project config from YAML."""
    config_path = _get_config_path()
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    if not isinstance(config, dict):
        return {}
    return config


def get_admins() -> set[str]:
    """Return configured qrating admin QQ ids."""
    config = load_config()
    admins = config.get("admins", [])
    if not admins:
        plugin_config = config.get("plugin", {})
        if isinstance(plugin_config, dict):
            admins = plugin_config.get("admins", [])

    if not isinstance(admins, list):
        return set()

    return {str(admin).strip() for admin in admins if str(admin).strip()}


def is_admin(user_id: str | int | None) -> bool:
    """Check whether the given QQ id is configured as an admin."""
    if user_id is None:
        return False
    return str(user_id).strip() in get_admins()

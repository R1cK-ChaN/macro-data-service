from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILES = (PROJECT_ROOT / ".env",)


@lru_cache(maxsize=None)
def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_env_value(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    for env_file in DEFAULT_ENV_FILES:
        values = _read_env_file(env_file)
        for key in keys:
            value = values.get(key)
            if value:
                return value
    return default


def clear_env_cache() -> None:
    _read_env_file.cache_clear()

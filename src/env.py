from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = "dev"
VALID_PROFILES = frozenset({"dev", "prod"})
DEFAULT_ENV_FILES = (
    PROJECT_ROOT / ".env",
    Path.home() / ".macro-data" / "dev.env",
)


def default_env_files() -> tuple[Path, ...]:
    configured = os.environ.get("MACRO_DATA_ENV_FILES", "").strip()
    if configured:
        return tuple(Path(path).expanduser() for path in configured.split(os.pathsep) if path)
    return DEFAULT_ENV_FILES


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
    for env_file in default_env_files():
        values = _read_env_file(env_file)
        for key in keys:
            value = values.get(key)
            if value:
                return value
    return default


def get_macro_data_profile() -> str:
    profile = get_env_value("MACRO_DATA_PROFILE", default=DEFAULT_PROFILE).strip().lower()
    profile = profile or DEFAULT_PROFILE
    if profile not in VALID_PROFILES:
        valid = ", ".join(sorted(VALID_PROFILES))
        raise ValueError(f"MACRO_DATA_PROFILE must be one of: {valid}")
    return profile


def clear_env_cache() -> None:
    _read_env_file.cache_clear()

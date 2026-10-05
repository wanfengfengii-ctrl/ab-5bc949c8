"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    port: int
    db_path: str
    retention_limit: int
    heartbeat_seconds: float
    max_body_bytes: int


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def load_config() -> Config:
    return Config(
        port=_env_int("APP_PORT", 8080),
        db_path=os.environ.get("DB_PATH", "/data/events.db"),
        # Keep at least one event per channel.
        retention_limit=max(1, _env_int("RETENTION_LIMIT", 100)),
        heartbeat_seconds=float(os.environ.get("HEARTBEAT_SECONDS", "5")),
        max_body_bytes=_env_int("MAX_BODY_BYTES", 10 * 1024 * 1024),
    )

"""Request payload validation."""

from __future__ import annotations

import re
from datetime import datetime

ALLOWED_SEVERITIES = frozenset(
    {"info", "warning", "alarm", "critical", "shutdown"}
)

_EVENT_KEY_RE = re.compile(r"^[^\s].{0,254}$", re.DOTALL)
_MAX_MESSAGE = 10_000


class ValidationError(ValueError):
    """A request payload is structurally invalid."""


def _require_string(obj, field: str, max_len: int) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"field {field!r} must be a non-empty string")
    if len(value) > max_len:
        raise ValidationError(f"field {field!r} exceeds {max_len} characters")
    return value


def validate_batch(raw) -> list[dict]:
    """Validate the POST body; return normalized event dicts in order."""
    if not isinstance(raw, list):
        raise ValidationError("request body must be a JSON array of events")
    if not 1 <= len(raw) <= 50:
        raise ValidationError("batch must contain between 1 and 50 events")

    normalized: list[dict] = []
    for index, item in enumerate(raw):
        where = f"events[{index}]"
        if not isinstance(item, dict):
            raise ValidationError(f"{where} must be an object")

        event_key = _require_string(item, "eventKey", 255)
        if not _EVENT_KEY_RE.match(event_key):
            raise ValidationError(
                f"{where}.eventKey must be 1..255 chars and not start with "
                "whitespace"
            )

        time_value = _require_string(item, "time", 64)
        validate_rfc3339(time_value, where)

        severity = _require_string(item, "severity", 32)
        if severity not in ALLOWED_SEVERITIES:
            raise ValidationError(
                f"{where}.severity must be one of "
                f"{sorted(ALLOWED_SEVERITIES)}"
            )

        message = _require_string(item, "message", _MAX_MESSAGE)

        normalized.append(
            {
                "eventKey": event_key,
                "time": time_value,
                "severity": severity,
                "message": message,
            }
        )
    return normalized


def validate_rfc3339(value: str, where: str = "time") -> None:
    """Accept RFC 3339 timestamps, with or without fractional seconds."""
    candidate = value
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise ValidationError(
            f"{where}.time must be an RFC3339 timestamp, got {value!r}"
        ) from None
    # Bare timestamps with no zone are not RFC3339.
    if parsed.tzinfo is None:
        raise ValidationError(
            f"{where}.time must include a timezone offset (RFC3339)"
        )


def validate_channel(channel: str) -> str:
    if not channel or not re.fullmatch(r"[A-Za-z0-9_.\-]{1,128}", channel):
        raise ValidationError(
            "channel must be 1..128 chars of [A-Za-z0-9_.-]"
        )
    return channel

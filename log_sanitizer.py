"""Helpers to redact sensitive values before logging."""

from __future__ import annotations

import re
from typing import Any

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"\b(?:\+?\d[\d\-\s().]{7,}\d)\b")
BEARER_RE = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._\-]{10,}")
KEY_RE = re.compile(r"(?i)\b(api[_-]?key|token|authorization|secret)\b\s*[:=]\s*[^\s,;]+")


def sanitize_text(value: str) -> str:
    out = value
    out = EMAIL_RE.sub("[REDACTED:email]", out)
    out = PHONE_RE.sub("[REDACTED:phone]", out)
    out = BEARER_RE.sub(r"\1[REDACTED:token]", out)
    out = KEY_RE.sub(r"\1=[REDACTED:key]", out)
    return out


def mask_identifier(value: Any, keep_last: int = 3) -> str:
    s = str(value or "")
    if not s:
        return "unknown"
    if len(s) <= keep_last:
        return "*" * len(s)
    return "*" * (len(s) - keep_last) + s[-keep_last:]


def sanitize_log_fields(fields: dict[str, Any], allowlist: set[str]) -> dict[str, Any]:
    """Return only allow-listed fields, with string values sanitized."""
    cleaned: dict[str, Any] = {}
    for key, value in fields.items():
        if key not in allowlist:
            continue
        if isinstance(value, str):
            cleaned[key] = sanitize_text(value)
        else:
            cleaned[key] = value
    return cleaned

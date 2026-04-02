"""
Pure utility helpers extracted from routers/legacy.py (Patch 7.6).
No side effects, no DB/Redis calls.
"""

import json
from datetime import datetime
from typing import Any


def parse_extra_data(raw: Any) -> dict:
    """Parse extra_data from dict or JSON string into a dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def parse_extra_data_object(raw: Any) -> dict:
    """Alias for parse_extra_data — legacy compatibility."""
    return parse_extra_data(raw)


def parse_json_object(raw: Any) -> dict:
    """Parse a JSON string or return empty dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-format datetime string, returning None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

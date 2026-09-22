"""Small, JSON-friendly helpers shared by the Buzz Layer stages."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from agent_reach.intelligence.schema import iso_sgt, iso_utc, parse_datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def sha256(value: str | bytes | dict[str, Any] | list[Any]) -> str:
    if not isinstance(value, (str, bytes)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def times(value: Any) -> tuple[str | None, str | None]:
    dt = parse_datetime(value)
    return iso_utc(dt), iso_sgt(dt)


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"

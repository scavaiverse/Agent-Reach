"""Conservative anchor matching: same event/mechanism, not same noun."""

from __future__ import annotations

import re
from typing import Any

GENERIC = {"ai", "security", "market", "climate", "outage", "regulation", "today", "news", "technology", "business"}


def _tokens(value: str) -> set[str]:
    return {w.casefold() for w in re.findall(r"[\w\u3400-\u9fff-]+", value or "", flags=re.UNICODE) if len(w) > 2 and w.casefold() not in GENERIC}


def score_candidate(anchor: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    parent = _tokens(str(anchor.get("published_text") or ""))
    text = " ".join(str(record.get(k) or "") for k in ("title", "original_text", "description"))
    candidate = _tokens(text)
    overlap = sorted(parent & candidate)
    exact_entity = any(str(entity).casefold() in text.casefold() for entity in anchor.get("entities", []))
    event_words = sum(1 for word in ("launch", "production", "approval", "talks", "outage", "sales", "shortage", "trial", "policy", "price", "mass") if word in text.casefold())
    score = min(100, len(overlap) * 14 + (22 if exact_entity else 0) + min(event_words * 8, 24))
    direct = len(overlap) >= 2 or (exact_entity and event_words > 0)
    if record.get("source_type") == "video" and not record.get("metadata", {}).get("direct_content"):
        direct = False
        score = min(score, 20)
    return {
        "accepted": bool(direct and score >= 28),
        "mapping_score": score,
        "reason_codes": [*( ["TOKEN_OVERLAP"] if overlap else []), *( ["NAMED_ENTITY"] if exact_entity else []), *( ["EVENT_OR_MECHANISM"] if event_words else [])],
        "rejection_reason": None if direct and score >= 28 else ("VIDEO_DIRECT_CONTENT_MISSING" if record.get("source_type") == "video" and not record.get("metadata", {}).get("direct_content") else "GENERIC_OR_WEAK_ANCHOR_MATCH"),
    }

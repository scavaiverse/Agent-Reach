# -*- coding: utf-8 -*-
"""Deterministic, conservative mapping from buzz topics to OTR contracts."""

from __future__ import annotations

from typing import Any

_RULES: tuple[tuple[str, str, frozenset[str]], ...] = (
    ("/headlines", "Headlines", frozenset({"breaking", "news", "launch", "election", "policy", "regulation", "market", "outage", "earthquake"})),
    ("/today", "Today", frozenset({"today", "latest", "breaking", "update", "current"})),
    ("/risks", "Risks", frozenset({"risk", "security", "attack", "outage", "war", "virus", "earthquake", "flood", "scam", "fraud"})),
    ("/business", "Business", frozenset({"company", "business", "startup", "product", "customer", "market", "stock", "trade", "investment"})),
    ("/investibles", "Investibles", frozenset({"stock", "market", "rate", "inflation", "bank", "energy", "trade"})),
    ("/connections", "Connections", frozenset()),
    ("/graph", "Graph", frozenset()),
    ("/next", "Next", frozenset()),
    ("/learning", "Learning", frozenset()),
    ("/proof", "Proof", frozenset()),
    ("/sources", "Sources", frozenset()),
)


def _topic_text(topic: dict[str, Any]) -> str:
    pieces = [str(topic.get("canonical_title") or "")]
    for signal in topic.get("signals", []):
        if isinstance(signal, dict):
            pieces.extend(str(signal.get(key) or "") for key in ("title", "original_text", "english_translation"))
        else:
            pieces.extend(str(getattr(signal, key, "") or "") for key in ("title", "original_text", "english_translation"))
    return " ".join(pieces).casefold()


def map_topics_to_otr(topics: list[dict[str, Any]]) -> dict[str, Any]:
    """Return only semantically defensible routes and preserve unmatched topics."""

    mappings: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    routes_with_chatter: set[str] = set()
    for rank, topic in enumerate(topics, 1):
        text = _topic_text(topic)
        social_platform_count = int(topic.get("social_platform_count", 0))
        momentum = str(topic.get("momentum_status") or "NEW")
        score = int(topic.get("social_buzz_score", 0))
        routes: list[dict[str, str]] = []
        for path, label, terms in _RULES:
            if path in {"/connections", "/graph"}:
                if social_platform_count >= 2 or int(topic.get("social_independent_source_count", 0)) >= 2:
                    routes.append({"path": path, "label": label, "reason": "cross-platform or independent-source convergence"})
            elif path == "/next":
                if momentum in {"NEW", "ACCELERATING", "RESURGING"} and score >= 35:
                    routes.append({"path": path, "label": label, "reason": "fresh high-buzz or resurging signal"})
            elif path == "/learning":
                if int(topic.get("runs_present", 1)) >= 2 or momentum in {"PERSISTENT", "DECELERATING"}:
                    routes.append({"path": path, "label": label, "reason": "stored trajectory is available"})
            elif path in {"/proof", "/sources"}:
                if topic.get("verification") in {"VERIFIED", "PARTIALLY VERIFIED"}:
                    routes.append({"path": path, "label": label, "reason": "factual verification evidence is retained"})
            elif terms and any(term in text for term in terms):
                routes.append({"path": path, "label": label, "reason": "topic text matches route contract"})
        if routes:
            for route in routes:
                routes_with_chatter.add(route["path"])
            mappings.append(
                {
                    "rank": rank,
                    "topic_id": topic.get("topic_id"),
                    "canonical_title": topic.get("canonical_title"),
                    "social_buzz_score": score,
                    "routes": routes,
                    "mapping_status": "MATCHED",
                }
            )
        else:
            unmatched.append(
                {
                    "rank": rank,
                    "topic_id": topic.get("topic_id"),
                    "canonical_title": topic.get("canonical_title"),
                    "social_buzz_score": score,
                    "status": "UNMATCHED_HIGH_BUZZ_TOPIC",
                    "social_sources": [
                        (getattr(signal, "url", None) if not isinstance(signal, dict) else signal.get("url"))
                        for signal in topic.get("signals", [])
                        if (getattr(signal, "url", None) if not isinstance(signal, dict) else signal.get("url"))
                    ],
                }
            )
    return {
        "mappings": mappings,
        "unmatched_high_buzz_topics": unmatched,
        "routes_with_chatter": sorted(routes_with_chatter),
        "routes_without_chatter": [path for path, _label, _terms in _RULES if path not in routes_with_chatter],
    }

# -*- coding: utf-8 -*-
"""Deterministic momentum labels using the previous stored intelligence run."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .schema import ensure_utc, parse_datetime

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_+.-]{2,}|[\u3400-\u4dbf\u4e00-\u9fff]{2,}", re.I)
_STOP = {"the", "and", "for", "with", "from", "news", "latest", "today", "update", "official"}


def _tokens(value: str) -> set[str]:
    return {token.casefold() for token in _TOKEN_RE.findall(value) if token.casefold() not in _STOP}


def _topic_tokens(topic: dict[str, Any]) -> set[str]:
    values = [str(topic.get("canonical_title") or "")]
    values.extend(str(signal.get("title") or "") for signal in topic.get("signals", []) if isinstance(signal, dict))
    return _tokens(" ".join(values))


def _match(topic: dict[str, Any], previous: list[dict[str, Any]]) -> dict[str, Any] | None:
    current_tokens = _topic_tokens(topic)
    best: tuple[float, dict[str, Any] | None] = (0.0, None)
    for candidate in previous:
        candidate_tokens = _topic_tokens(candidate)
        if not current_tokens or not candidate_tokens:
            continue
        overlap = len(current_tokens & candidate_tokens) / len(current_tokens | candidate_tokens)
        if overlap > best[0]:
            best = (overlap, candidate)
    return best[1] if best[0] >= 0.3 else None


def add_momentum(
    topics: list[dict[str, Any]],
    previous_data: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Attach deterministic momentum labels without inventing a trend history."""

    previous_topics = previous_data.get("topics", []) if isinstance(previous_data, dict) else []
    history_count = int(previous_data.get("history_count", 0)) if isinstance(previous_data, dict) else 0
    if not isinstance(previous_topics, list):
        previous_topics = []
    for current_rank, topic in enumerate(topics, 1):
        previous = _match(topic, [item for item in previous_topics if isinstance(item, dict)])
        previous_rank = None
        if previous is not None:
            previous_rank = next(
                (
                    index
                    for index, candidate in enumerate(previous_topics, 1)
                    if candidate is previous
                ),
                None,
            )
        current_count = len(topic.get("signals", []))
        previous_count = len(previous.get("signals", [])) if previous else 0
        current_latest = ensure_utc(topic.get("latest_signal_utc")) if isinstance(topic.get("latest_signal_utc"), datetime) else parse_datetime(topic.get("latest_signal_utc"))
        previous_latest = parse_datetime(previous.get("latest_signal_utc")) if previous else None
        previous_status = str(previous.get("momentum_status") or "") if previous else ""
        if previous is None:
            status = "NEW"
        elif history_count < 2:
            status = "INSUFFICIENT_HISTORY"
        elif previous_status == "DECELERATING" and current_count > previous_count:
            status = "RESURGING"
        elif current_count > previous_count:
            status = "ACCELERATING"
        elif current_count < previous_count:
            status = "DECELERATING"
        elif current_latest and previous_latest and current_latest > previous_latest:
            status = "PERSISTENT"
        else:
            status = "PERSISTENT"
        topic["momentum_status"] = status
        topic["previous_signal_count"] = previous_count
        topic["signal_count_delta"] = current_count - previous_count
        topic["current_rank"] = current_rank
        topic["previous_rank"] = previous_rank if history_count >= 1 else None
        topic["rank_change"] = (
            previous_rank - current_rank
            if previous_rank is not None and history_count >= 1
            else None
        )
        topic["first_appeared"] = (
            previous.get("first_appeared") or previous.get("first_detected_utc")
            if previous
            else topic.get("first_detected_utc")
        )
        topic["runs_present"] = int(previous.get("runs_present", 1)) + 1 if previous else 1
        topic["peak_rank"] = min(int(previous.get("peak_rank", previous_rank or current_rank)), current_rank) if previous else current_rank
        topic["peak_buzz_score"] = (
            max(
                int(previous.get("peak_buzz_score", previous.get("social_buzz_score", 0))),
                int(topic.get("social_buzz_score", 0)),
            )
            if previous
            else int(topic.get("social_buzz_score", 0))
        )
    return topics

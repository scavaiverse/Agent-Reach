# -*- coding: utf-8 -*-
"""Deterministic social-buzz, consequence, and verification-independent scoring."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any

from .cluster import TopicCluster
from .schema import Signal, ensure_utc

WEIGHTS = {
    "recency": 20,
    "cross_platform_spread": 20,
    "independent_source_diversity": 15,
    "engagement_momentum": 15,
    "real_world_consequence": 15,
    "novelty": 10,
    "geographic_spread": 5,
}

SOCIAL_BUZZ_WEIGHTS = {
    "cross_platform_spread": 25,
    "momentum_velocity": 20,
    "independent_creator_source_spread": 15,
    "engagement": 15,
    "recency": 10,
    "geographic_spread": 5,
    "language_spread": 5,
    "novelty_emergence": 5,
}

SOCIAL_PLATFORMS = frozenset(
    {
        "twitter", "reddit", "youtube", "facebook", "instagram", "xiaohongshu",
        "bilibili", "linkedin", "v2ex", "xueqiu", "xiaoyuzhou", "boss",
    }
)


def is_social_signal(signal: Signal) -> bool:
    """Identify discussion-bearing records without treating news feeds as social buzz."""

    return signal.platform in SOCIAL_PLATFORMS

_CONSEQUENCE_TERMS = {
    "war": 100, "conflict": 95, "attack": 95, "earthquake": 100, "flood": 92,
    "typhoon": 92, "hurricane": 92, "wildfire": 90, "outage": 86, "recall": 80,
    "health": 86, "pandemic": 95, "virus": 86, "regulation": 82, "law": 82,
    "policy": 78, "election": 90, "sanction": 88, "tariff": 82, "market": 70,
    "stock": 68, "rate": 72, "inflation": 75, "bank": 73, "energy": 76,
    "climate": 78, "research": 62, "launch": 58, "security": 82, "cyber": 84,
    "就业": 65, "监管": 82, "地震": 100, "台风": 92, "洪水": 92, "战争": 100,
    "政策": 78, "选举": 90, "経済": 70, "規制": 82, "재난": 94, "정책": 78,
}
_LOW_CONSEQUENCE_TERMS = {"meme", "celebrity", "viral", "trend", "entertainment", "game", "music"}


def _age_hours(signal_time: datetime | None, now: datetime) -> float | None:
    if signal_time is None:
        return None
    return max(0.0, (ensure_utc(now) - ensure_utc(signal_time)).total_seconds() / 3600)


def _source_key(signal: Signal) -> str:
    value = (
        signal.metadata.get("source_key")
        or signal.metadata.get("feed_domain")
        or signal.author
    )
    if not value and signal.url.count("/") >= 2:
        value = signal.url.split("/", 3)[2]
    return str(value or signal.platform).casefold()


def _topic_text(cluster: TopicCluster) -> str:
    return " ".join(
        part for signal in cluster.signals for part in (signal.english_translation, signal.title, signal.original_text) if part
    ).casefold()


def consequence_score(cluster: TopicCluster) -> int:
    text = _topic_text(cluster)
    scores = []
    for term, weight in _CONSEQUENCE_TERMS.items():
        term_lower = term.casefold()
        found = bool(re.search(rf"(?<![a-z0-9]){re.escape(term_lower)}(?![a-z0-9])", text)) if term_lower.isascii() else term_lower in text
        if found:
            scores.append(weight)
    if not scores:
        return 22 if any(term in text for term in _LOW_CONSEQUENCE_TERMS) else 40
    return min(100, max(scores) + min(12, max(0, len(scores) - 1) * 3))


def virality_score(cluster: TopicCluster) -> int:
    signals = [signal for signal in cluster.signals if is_social_signal(signal)]
    platforms = {signal.platform for signal in signals}
    sources = {_source_key(signal) for signal in signals}
    with_metrics = 0
    metric_points = 0.0
    for signal in signals:
        values = [value for value in signal.engagement.to_dict().values() if value is not None]
        if values:
            with_metrics += 1
            # Log scaling prevents large platforms from dominating linearly.
            metric_points += min(12.0, max(math.log10(float(value) + 1) for value in values) * 2.0)
    signal_points = min(30.0, len(signals) * 6.0)
    source_points = min(25.0, len(sources) * 5.0)
    platform_points = min(20.0, len(platforms) * 5.0)
    metric_component = min(25.0, metric_points / max(1, with_metrics) * 2.0) if with_metrics else 0.0
    return int(round(min(100.0, signal_points + source_points + platform_points + metric_component)))


def social_buzz_score(cluster: TopicCluster, now: datetime) -> tuple[int, dict[str, dict[str, Any]]]:
    """Score observable discussion, keeping verification out of the ranking path."""

    signals = [signal for signal in cluster.signals if is_social_signal(signal)]
    platforms = {signal.platform for signal in signals}
    sources = {_source_key(signal) for signal in signals}
    regions = {signal.region for signal in signals if signal.region}
    languages = {signal.language for signal in signals if signal.language}
    recent_burst = sum(1 for signal in signals if (_age_hours(signal.published_at_utc, now) or 999) <= 6)
    latest_social = max((signal.published_at_utc for signal in signals if signal.published_at_utc), default=None)
    latest_age = _age_hours(latest_social, now)
    cross_platform = min(25, len(platforms) * 5)
    # Without a historical time series, this is explicitly a bounded velocity
    # proxy: current social-signal density plus the fresh six-hour burst.
    momentum = min(20, min(10, len(signals) * 2) + min(10, recent_burst * 2))
    independent = min(15, len(sources) * 3)
    engagement = round(virality_score(cluster) * 15 / 100)
    if latest_age is None:
        recency = 0
    elif latest_age <= 3:
        recency = 10
    elif latest_age <= 6:
        recency = 8
    elif latest_age <= 12:
        recency = 6
    elif latest_age <= 24:
        recency = 4
    else:
        recency = 0
    geography = min(5, len(regions))
    language = min(5, len(languages))
    novelty = 5 if latest_age is not None and latest_age <= 6 else 3 if latest_age is not None else 0
    components = {
        "cross_platform_spread": {"points": cross_platform, "max": 25, "social_platform_count": len(platforms)},
        "momentum_velocity": {
            "points": momentum,
            "max": 20,
            "social_signal_count": len(signals),
            "signals_in_last_6h": recent_burst,
            "basis": "bounded current-density and six-hour-burst proxy; historical status is separate",
        },
        "independent_creator_source_spread": {"points": independent, "max": 15, "source_count": len(sources)},
        "engagement": {"points": engagement, "max": 15, "virality_score": virality_score(cluster)},
        "recency": {"points": recency, "max": 10},
        "geographic_spread": {"points": geography, "max": 5, "region_count": len(regions)},
        "language_spread": {"points": language, "max": 5, "language_count": len(languages)},
        "novelty_emergence": {"points": novelty, "max": 5},
    }
    return int(sum(item["points"] for item in components.values())), components


def _recency_points(cluster: TopicCluster, now: datetime) -> int:
    latest = cluster.latest_signal
    age = _age_hours(latest, now)
    if age is None:
        return 0
    if age <= 3:
        return 20
    if age <= 6:
        return 17
    if age <= 12:
        return 13
    if age <= 24:
        return 8
    return 0


def score_cluster(cluster: TopicCluster, now: datetime) -> dict[str, Any]:
    signals = cluster.signals
    platforms = sorted({signal.platform for signal in signals})
    sources = sorted({_source_key(signal) for signal in signals})
    regions = sorted({signal.region for signal in signals if signal.region})
    languages = sorted({signal.language for signal in signals if signal.language})
    social_signals = [signal for signal in signals if is_social_signal(signal)]
    social_platforms = sorted({signal.platform for signal in social_signals})
    social_sources = sorted({_source_key(signal) for signal in social_signals})
    social_regions = sorted({signal.region for signal in social_signals if signal.region})
    social_languages = sorted({signal.language for signal in social_signals if signal.language})
    recency = _recency_points(cluster, now)
    spread = min(20, len(platforms) * 5)
    source_diversity = min(15, len(sources) * 3)
    virality = virality_score(cluster)
    engagement = round(virality * WEIGHTS["engagement_momentum"] / 100)
    consequence = consequence_score(cluster)
    consequence_points = round(consequence * WEIGHTS["real_world_consequence"] / 100)
    novelty = min(10, 3 + len({signal.url for signal in signals if signal.url}) + min(4, len(sources)))
    geography = min(5, len(regions) * 2 + (1 if regions else 0))
    components = {
        "recency": {"points": recency, "max": 20},
        "cross_platform_spread": {"points": spread, "max": 20, "platform_count": len(platforms)},
        "independent_source_diversity": {"points": source_diversity, "max": 15, "source_count": len(sources)},
        "engagement_momentum": {"points": engagement, "max": 15, "virality_score": virality},
        "real_world_consequence": {"points": consequence_points, "max": 15, "consequence_score": consequence},
        "novelty": {"points": novelty, "max": 10},
        "geographic_spread": {"points": geography, "max": 5, "region_count": len(regions)},
    }
    importance = int(sum(item["points"] for item in components.values()))
    buzz, buzz_components = social_buzz_score(cluster, now)
    return {
        "topic_id": cluster.topic_id,
        "canonical_title": cluster.canonical_title,
        "signal_ids": [signal.signal_id for signal in signals],
        "signals": signals,
        "platforms": platforms,
        "platform_count": len(platforms),
        "social_platforms": social_platforms,
        "social_platform_count": len(social_platforms),
        "raw_signal_count": len(signals),
        "social_signal_count": len(social_signals),
        "independent_sources": sources,
        "independent_source_count": len(sources),
        "social_independent_sources": social_sources,
        "social_independent_source_count": len(social_sources),
        "languages": languages,
        "regions": regions,
        "social_languages": social_languages,
        "social_regions": social_regions,
        "first_detected_utc": cluster.first_detected,
        "latest_signal_utc": cluster.latest_signal,
        "event_time_utc": None,
        "virality_score": virality,
        "consequence_score": consequence,
        "importance_score": importance,
        "score_components": components,
        "social_buzz_score": buzz,
        "social_buzz_components": buzz_components,
    }


def score_clusters(clusters: list[TopicCluster], now: datetime) -> list[dict[str, Any]]:
    scored = [score_cluster(cluster, now) for cluster in clusters]
    scored.sort(
        key=lambda topic: (
            topic["social_buzz_score"],
            topic["importance_score"],
            topic["consequence_score"],
            topic["latest_signal_utc"] or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return scored

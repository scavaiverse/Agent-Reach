# -*- coding: utf-8 -*-
"""Build the durable, credential-free source ledger required by OTR."""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import urlsplit

from .cluster import canonicalize_url
from .schema import Signal, iso_sgt, iso_utc


def _domain(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").casefold().removeprefix("www.")
    except ValueError:
        return ""


def _source_group(signal: Signal) -> str:
    value = signal.metadata.get("source_key") or signal.metadata.get("feed_domain")
    if not value:
        value = _domain(signal.url) or signal.author or signal.platform
    return str(value).casefold()


def _source_id(signal: Signal) -> str:
    identity = "|".join(
        (
            signal.platform,
            _source_group(signal),
            canonicalize_url(signal.url),
            signal.author or "",
        )
    )
    return "src-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def build_source_ledger(
    signals: list[Signal],
    topics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return one source-ledger row per accepted signal.

    The ledger deliberately keeps the original URL and text.  A source row is
    evidence metadata, not a claim that the source is correct.
    """

    topic_by_signal: dict[str, dict[str, Any]] = {}
    for topic in topics:
        for signal in topic.get("signals", []):
            topic_by_signal[signal.signal_id] = topic

    rows: list[dict[str, Any]] = []
    for signal in signals:
        topic = topic_by_signal.get(signal.signal_id)
        topic_id = topic.get("topic_id") if topic else signal.topic_cluster
        verification = topic.get("verification") if topic else None
        verification_notes = topic.get("verification_notes", {}) if topic else {}
        url_status = verification_notes.get("url_statuses", {}).get(signal.signal_id, "URL_UNKNOWN")
        if signal.evidence_type in {"primary", "official"}:
            verification_role = "primary_or_official_candidate"
        elif signal.source_type in {"news", "research", "official"}:
            verification_role = "independent_secondary"
        else:
            verification_role = "social_or_community_signal"
        rows.append(
            {
                "source_id": _source_id(signal),
                "signal_id": signal.signal_id,
                "topic_id": topic_id,
                "claim_ids": [f"claim-{topic_id}"] if topic_id else [],
                "platform": signal.platform,
                "publisher": signal.metadata.get("feed_name") or signal.author,
                "author": signal.author,
                "original_url": signal.url or None,
                "canonical_url": canonicalize_url(signal.url) or None,
                "published_at_utc": iso_utc(signal.published_at_utc),
                "published_at_sgt": iso_sgt(signal.published_at_utc),
                "retrieved_at_utc": iso_utc(signal.retrieved_at_utc),
                "retrieved_at_sgt": iso_sgt(signal.retrieved_at_utc),
                "language": signal.language,
                "source_type": signal.source_type,
                "verification_role": verification_role,
                "verification_label": verification,
                "url_status": url_status,
                "independence_group": _source_group(signal),
            }
        )
    return rows

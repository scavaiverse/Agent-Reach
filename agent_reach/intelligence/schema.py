# -*- coding: utf-8 -*-
"""Stable, credential-free schemas used by the intelligence pipeline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

UTC = timezone.utc
SGT = timezone(timedelta(hours=8))


def ensure_utc(value: datetime | None) -> datetime | None:
    """Return an aware UTC datetime, treating naive values as UTC."""

    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_datetime(value: Any) -> datetime | None:
    """Parse common feed/API timestamp forms without guessing local time."""

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, (int, float)):
        # APIs differ between seconds and milliseconds since the epoch.
        seconds = float(value) / 1000 if abs(float(value)) > 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return ensure_utc(datetime.fromisoformat(text))
        except ValueError:
            pass
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y%m%d",
        ):
            try:
                return ensure_utc(datetime.strptime(text, fmt))
            except ValueError:
                continue
    return None


def iso_utc(value: datetime | None) -> str | None:
    value = ensure_utc(value)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z") if value else None


def iso_sgt(value: datetime | None) -> str | None:
    value = ensure_utc(value)
    return value.astimezone(SGT).isoformat(timespec="seconds") if value else None


@dataclass
class Engagement:
    """Raw platform metrics; ``None`` means the platform did not provide it."""

    likes: int | float | None = None
    comments: int | float | None = None
    shares: int | float | None = None
    views: int | float | None = None

    def to_dict(self) -> dict[str, int | float | None]:
        return {
            "likes": self.likes,
            "comments": self.comments,
            "shares": self.shares,
            "views": self.views,
        }


@dataclass
class Signal:
    """One retrieved post, article, video, topic, or other source signal."""

    platform: str
    backend: str
    title: str
    original_text: str
    language: str
    url: str
    published_at_utc: datetime | None
    retrieved_at_utc: datetime
    author: str | None = None
    english_translation: str | None = None
    engagement: Engagement = field(default_factory=Engagement)
    topic_keywords: list[str] = field(default_factory=list)
    region: str | None = None
    source_type: str = "unknown"
    evidence_type: str = "secondary"
    event_time_utc: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    signal_id: str = ""
    topic_cluster: str | None = None
    accepted_in_window: bool | None = None
    rejection_reason: str | None = None

    def __post_init__(self) -> None:
        self.published_at_utc = ensure_utc(self.published_at_utc)
        self.retrieved_at_utc = ensure_utc(self.retrieved_at_utc) or datetime.now(UTC)
        self.event_time_utc = ensure_utc(self.event_time_utc)
        if not self.signal_id:
            identity = "|".join(
                (
                    self.platform,
                    self.backend,
                    self.url,
                    self.author or "",
                    self.title,
                    iso_utc(self.published_at_utc) or "",
                )
            )
            self.signal_id = "sig-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]

    @property
    def published_at_sgt(self) -> str | None:
        return iso_sgt(self.published_at_utc)

    @property
    def retrieved_at_sgt(self) -> str | None:
        return iso_sgt(self.retrieved_at_utc)

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "platform": self.platform,
            "backend": self.backend,
            "title": self.title,
            "original_text": self.original_text,
            "english_translation": self.english_translation,
            "language": self.language,
            "author": self.author,
            "url": self.url,
            "published_at_utc": iso_utc(self.published_at_utc),
            "published_at_sgt": self.published_at_sgt,
            "retrieved_at_utc": iso_utc(self.retrieved_at_utc),
            "retrieved_at_sgt": self.retrieved_at_sgt,
            "event_time_utc": iso_utc(self.event_time_utc),
            "event_time_sgt": iso_sgt(self.event_time_utc),
            "engagement": self.engagement.to_dict(),
            "topic_keywords": self.topic_keywords,
            "region": self.region,
            "source_type": self.source_type,
            "evidence_type": self.evidence_type,
            "accepted_in_window": self.accepted_in_window,
            "rejection_reason": self.rejection_reason,
            "metadata": self.metadata,
            "topic_cluster": self.topic_cluster,
        }

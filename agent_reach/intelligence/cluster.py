# -*- coding: utf-8 -*-
"""Deterministic event clustering and duplicate collapse."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .schema import Signal

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_+.-]{2,}|[\u3400-\u4dbf\u4e00-\u9fff]{2,}", re.I)
_GENERIC = {
    "about", "after", "again", "breaking", "business", "company", "daily", "first",
    "from", "global", "latest", "market", "news", "official", "report", "reports",
    "says", "said", "technology", "today", "update", "video", "world", "yang", "dan",
    "untuk", "berita", "terkini", "newsroom", "latestnews", "ai", "artificial",
    "intelligence", "最新", "新闻", "消息", "视频", "今天", "的", "了", "在", "与",
    "was", "were", "will", "would", "could", "one", "two", "off", "read", "continue",
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december", "berita", "pilihan", "papers",
}


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        query = [(k, v) for k, v in parse_qsl(parts.query) if not k.lower().startswith("utm_")]
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), urlencode(query), ""))
    except ValueError:
        return url


def _text(signal: Signal) -> str:
    return " ".join(
        part for part in (signal.english_translation, signal.title, signal.original_text) if part
    ).casefold()


def _noise_tokens(signal: Signal) -> set[str]:
    source_values = (
        signal.author,
        signal.metadata.get("feed_name"),
        signal.metadata.get("publisher"),
        signal.metadata.get("source_key"),
        signal.region,
    )
    return {token for value in source_values if value for token in _TOKEN_RE.findall(str(value).casefold())}


def tokens(signal: Signal) -> set[str]:
    noise = _noise_tokens(signal)
    return {
        token
        for token in _TOKEN_RE.findall(_text(signal))
        if not token.isdigit() and token not in _GENERIC and token not in noise
    }


def strong_entities(signal: Signal) -> set[str]:
    noise = _noise_tokens(signal)
    values = {value for value in signal.topic_keywords if value.casefold() not in noise}
    values.update(token for token in tokens(signal) if len(token) >= 5)
    return {
        value.casefold()
        for value in values
        if value.casefold() not in _GENERIC and value.casefold() not in noise
    }


def signals_related(left: Signal, right: Signal) -> bool:
    left_url = canonicalize_url(left.url)
    right_url = canonicalize_url(right.url)
    if left_url and right_url and left_url == right_url:
        return True
    left_title = re.sub(r"\W+", " ", left.title.casefold()).strip()
    right_title = re.sub(r"\W+", " ", right.title.casefold()).strip()
    if left_title and left_title == right_title:
        return True
    left_tokens = tokens(left)
    right_tokens = tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = left_tokens & right_tokens
    jaccard = len(overlap) / len(left_tokens | right_tokens)
    shared_entities = strong_entities(left) & strong_entities(right)
    # Require multiple meaningful entities for short multilingual headlines.
    return (len(overlap) >= 2 and jaccard >= 0.30) or (
        len(shared_entities) >= 2 and jaccard >= 0.18
    )


@dataclass
class TopicCluster:
    topic_id: str
    signals: list[Signal]

    @property
    def canonical_title(self) -> str:
        def rank(signal: Signal) -> tuple[int, int, int]:
            primary = 2 if signal.evidence_type in {"primary", "official"} else 0
            source = 1 if signal.source_type in {"official", "news", "research"} else 0
            return primary + source, len(signal.title), len(signal.original_text)

        return max(self.signals, key=rank).title

    @property
    def first_detected(self):
        return min((s.published_at_utc for s in self.signals if s.published_at_utc), default=None)

    @property
    def latest_signal(self):
        return max((s.published_at_utc for s in self.signals if s.published_at_utc), default=None)

    def to_dict(self) -> dict:
        return {
            "topic_id": self.topic_id,
            "canonical_title": self.canonical_title,
            "signal_ids": [signal.signal_id for signal in self.signals],
            "signals": [signal.to_dict() for signal in self.signals],
        }


def cluster_signals(signals: Iterable[Signal]) -> list[TopicCluster]:
    """Union signals representing one underlying development."""

    items = list(signals)
    parent = list(range(len(items)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for index, left in enumerate(items):
        for other_index in range(index + 1, len(items)):
            if signals_related(left, items[other_index]):
                union(index, other_index)

    groups: dict[int, list[Signal]] = {}
    for index, signal in enumerate(items):
        groups.setdefault(find(index), []).append(signal)

    clusters: list[TopicCluster] = []
    ordered_groups = sorted(
        groups.values(),
        key=lambda group: max((s.published_at_utc for s in group if s.published_at_utc), default=None),
        reverse=True,
    )
    for sequence, group in enumerate(ordered_groups, 1):
        topic_id = f"topic-{sequence:03d}"
        for signal in group:
            signal.topic_cluster = topic_id
        group.sort(key=lambda signal: signal.published_at_utc or signal.retrieved_at_utc)
        clusters.append(TopicCluster(topic_id=topic_id, signals=group))
    return clusters

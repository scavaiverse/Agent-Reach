# -*- coding: utf-8 -*-
"""Convert channel-specific records into the common signal schema."""

from __future__ import annotations

import html
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .cluster import canonicalize_url
from .schema import Engagement, Signal, ensure_utc, parse_datetime

_TAG_RE = re.compile(r"<[^>]+>")
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{2,}|\d+(?:\.\d+)?|[\u3400-\u4dbf\u4e00-\u9fff]{2,}")
_TRANSLATION_CACHE: dict[str, str | None] = {}


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = _TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def detect_language(text: str, hint: str | None = None) -> str:
    """Conservative script-based detection; source hints take precedence."""

    if hint and hint not in {"auto", "unknown"}:
        return hint.lower().replace("_", "-")
    if not text:
        return "unknown"
    if re.search(r"[\uac00-\ud7af]", text):
        return "ko"
    if re.search(r"[\u3040-\u30ff]", text):
        return "ja"
    cjk = len(re.findall(r"[\u3400-\u4dbf\u4e00-\u9fff]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    if cjk >= 2 and cjk >= latin:
        return "zh"
    lower = text.casefold()
    if any(word in lower.split() for word in ("yang", "dan", "dengan", "untuk", "berita")):
        return "id"
    if any(word in lower.split() for word in ("yang", "dan", "untuk", "berita", "terkini")):
        return "ms"
    return "en"


def _translate_text(text: str, source_language: str) -> str | None:
    """Best-effort translation using a public endpoint; never invents output."""

    if not text or source_language in {"en", "unknown"}:
        return text if text else None
    key = f"{source_language}:{text[:1200]}"
    if key in _TRANSLATION_CACHE:
        return _TRANSLATION_CACHE[key]
    translated: str | None = None
    try:
        query = urllib.parse.urlencode(
            {
                "client": "gtx",
                "sl": source_language,
                "tl": "en",
                "dt": "t",
                "q": text[:1200],
            }
        )
        req = urllib.request.Request(
            f"https://translate.googleapis.com/translate_a/single?{query}",
            headers={"User-Agent": "agent-reach-intelligence/1.0"},
        )
        with urllib.request.urlopen(req, timeout=8) as response:
            payload = json.loads(response.read(512 * 1024).decode("utf-8"))
        pieces = payload[0] if isinstance(payload, list) else []
        translated = " ".join(str(piece[0]) for piece in pieces if isinstance(piece, list) and piece)
        translated = clean_text(translated) or None
    except Exception:
        translated = None
    _TRANSLATION_CACHE[key] = translated
    return translated


def _keywords(text: str, limit: int = 12) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    stop = {
        "the", "and", "for", "with", "from", "that", "this", "news", "latest",
        "today", "about", "after", "into", "over", "says", "said", "more", "what",
        "how", "why", "yang", "dan", "untuk", "berita", "terkini", "最新", "新闻",
        "的", "了", "在", "与", "及", "是", "有", "中", "에", "의", "最新ニュース",
    }
    for token in _WORD_RE.findall(text):
        key = token.casefold()
        if key in stop or len(key) < 2 or key in seen:
            continue
        seen.add(key)
        result.append(token)
        if len(result) >= limit:
            break
    return result


def _metric(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    return int(number) if number.is_integer() else number


def normalize_records(
    records: Iterable[dict[str, Any]],
    *,
    retrieved_at_utc: datetime,
    multilingual: bool = False,
) -> list[Signal]:
    """Normalize untrusted channel records while preserving original text."""

    normalized: list[Signal] = []
    for record in records:
        title = clean_text(record.get("title") or record.get("name") or "")
        body = clean_text(
            record.get("original_text")
            or record.get("text")
            or record.get("description")
            or record.get("content")
            or title
        )
        combined = " — ".join(part for part in (title, body) if part and part != title)
        language = detect_language(combined, record.get("language"))
        translation = record.get("english_translation")
        if translation is None and multilingual and language not in {"en", "unknown"}:
            translation = _translate_text(combined[:1200], language)
        engagement_data = record.get("engagement") or {}
        if not isinstance(engagement_data, dict):
            engagement_data = {}
        engagement = Engagement(
            likes=_metric(engagement_data.get("likes", record.get("likes", record.get("like")))),
            comments=_metric(engagement_data.get("comments", record.get("comments", record.get("review")))),
            shares=_metric(engagement_data.get("shares", record.get("shares", record.get("reposts")))),
            views=_metric(engagement_data.get("views", record.get("views", record.get("play")))),
        )
        metadata = record.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {"raw_metadata": str(metadata)}
        signal = Signal(
            platform=str(record.get("platform") or "unknown"),
            backend=str(record.get("backend") or "unknown"),
            title=title or body[:240],
            original_text=combined or body,
            english_translation=clean_text(translation) if translation else None,
            language=language,
            author=clean_text(record.get("author") or record.get("channel") or record.get("account")) or None,
            url=str(record.get("url") or ""),
            published_at_utc=parse_datetime(record.get("published_at_utc") or record.get("published_at") or record.get("timestamp")),
            retrieved_at_utc=ensure_utc(retrieved_at_utc),
            engagement=engagement,
            topic_keywords=list(record.get("topic_keywords") or _keywords(translation or combined)),
            region=record.get("region"),
            source_type=str(record.get("source_type") or "unknown"),
            evidence_type=str(record.get("evidence_type") or record.get("source_type") or "secondary"),
            event_time_utc=parse_datetime(record.get("event_time_utc")),
            metadata=metadata,
        )
        normalized.append(signal)
    return normalized


def _duplicate_quality(signal: Signal) -> tuple[int, datetime, int, int]:
    """Prefer the most usable representative when one URL appears repeatedly."""

    metric_count = sum(value is not None for value in signal.engagement.to_dict().values())
    return (
        1 if signal.published_at_utc is not None else 0,
        ensure_utc(signal.published_at_utc) or datetime.min.replace(tzinfo=timezone.utc),
        len(signal.original_text or signal.title),
        metric_count,
    )


def deduplicate_signals(signals: Iterable[Signal]) -> tuple[list[Signal], list[Signal]]:
    """Collapse exact/canonical URL duplicates while preserving rejected raw records.

    Overlapping search terms can return the same source more than once. One
    deterministic representative is retained for ranking; every removed
    record remains in the raw pool with an explicit rejection reason. Different
    URLs discussing one event are left for event clustering.
    """

    unique: list[Signal] = []
    duplicates: list[Signal] = []
    seen: dict[str, tuple[int, Signal]] = {}

    def keys_for(signal: Signal) -> list[str]:
        keys: list[str] = []
        canonical = canonicalize_url(signal.url)
        if canonical:
            keys.append(f"url:{canonical}")
        if signal.signal_id:
            keys.append(f"id:{signal.signal_id}")
        return keys

    def mark_duplicate(signal: Signal, retained: Signal, reason: str) -> None:
        signal.accepted_in_window = False
        signal.rejection_reason = f"duplicate {reason}; retained {retained.signal_id}"
        signal.metadata = dict(signal.metadata)
        signal.metadata["duplicate_of"] = retained.signal_id
        duplicates.append(signal)

    for signal in signals:
        keys = keys_for(signal)
        existing = next((seen[key] for key in keys if key in seen), None)
        if existing is None:
            index = len(unique)
            unique.append(signal)
            for key in keys:
                seen[key] = (index, signal)
            continue

        existing_index, retained = existing
        reason = "canonical URL" if any(key.startswith("url:") and key in seen for key in keys) else "signal ID"
        if _duplicate_quality(signal) > _duplicate_quality(retained):
            mark_duplicate(retained, signal, reason)
            unique[existing_index] = signal
            for key in keys_for(retained):
                if seen.get(key, (None, None))[0] == existing_index:
                    seen.pop(key, None)
            for key in keys:
                seen[key] = (existing_index, signal)
        else:
            mark_duplicate(signal, retained, reason)

    return unique, duplicates


def apply_time_window(
    signals: Iterable[Signal],
    *,
    now_utc: datetime,
    window_hours: float,
    future_tolerance_minutes: int = 0,
) -> tuple[list[Signal], list[Signal]]:
    """Mark and split signals without silently accepting missing/old times."""

    now = ensure_utc(now_utc)
    assert now is not None
    cutoff = now - timedelta(hours=window_hours)
    future_limit = now + timedelta(minutes=future_tolerance_minutes)
    accepted: list[Signal] = []
    rejected: list[Signal] = []
    for signal in signals:
        published = ensure_utc(signal.published_at_utc)
        if published is None:
            signal.accepted_in_window = False
            signal.rejection_reason = "missing publication timestamp"
            rejected.append(signal)
        elif published > future_limit:
            signal.accepted_in_window = False
            signal.rejection_reason = "publication timestamp is in the future"
            rejected.append(signal)
        elif published < cutoff:
            signal.accepted_in_window = False
            signal.rejection_reason = "publication timestamp is outside the requested window"
            rejected.append(signal)
        else:
            signal.accepted_in_window = True
            signal.rejection_reason = None
            accepted.append(signal)
    return accepted, rejected

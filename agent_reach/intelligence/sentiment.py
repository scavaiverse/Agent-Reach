# -*- coding: utf-8 -*-
"""Exhaustive, auditable investible sentiment retrieval.

This lane is separate from Social Buzz ranking. It records the terminal
state for every investible/backend pair and builds meters only from retained,
deduplicated full-content records.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import urlsplit

from agent_reach.channels.v2ex import V2EXChannel
from agent_reach.utils.text import scrub_url_credentials

from .collector import Coverage, _http_json, _run_command
from .investibles import Investible
from .normalize import clean_text
from .schema import Signal, iso_sgt, iso_utc, parse_datetime
from .scoring import SOCIAL_PLATFORMS

UTC = timezone.utc
SENTIMENT_PLATFORM_ORDER = (
    "twitter", "reddit", "youtube", "facebook", "instagram", "xiaohongshu",
    "bilibili", "linkedin", "v2ex", "xueqiu", "xiaoyuzhou", "boss",
)
_BILIBILI_PAGE_SIZE = 20
_YOUTUBE_RESULT_LIMIT = 20
_MAX_AUTHOR_MENTIONS = 2

_POSITIVE_TERMS = (
    "bullish", "buy", "bought", "buying", "gain", "gains", "growth", "strong",
    "positive", "rally", "rise", "rising", "upside", "outperform", "demand",
    "支持", "看多", "利好", "上涨", "增长", "强劲", "買い", "上昇", "긍정",
)
_NEGATIVE_TERMS = (
    "bearish", "sell", "sold", "selling", "loss", "losses", "weak", "negative",
    "fall", "falling", "downside", "crash", "risk", "concern", "cut", "slump",
    "担忧", "看空", "利空", "下跌", "风险", "弱", "売り", "下落", "부정",
)
_UNCERTAIN_TERMS = (
    "rumor", "rumour", "unverified", "alleged", "maybe", "could", "might",
    "uncertain", "screenshot", "传闻", "据称", "未经证实", "うわさ", "未確認",
    "소문", "미확인",
)
_DIRECTIONAL_STOP = {
    "about", "after", "again", "also", "because", "before", "bitcoin", "company",
    "current", "discussion", "ethereum", "from", "gold", "market", "mentions",
    "more", "only", "people", "posts", "price", "social", "that", "this", "today",
    "using", "with", "would", "you", "的", "了", "在", "和", "是", "그리고",
    "one", "two", "three", "four", "five", "consecutive", "latest", "new", "news",
    "official", "video", "watch",
}


@dataclass
class SentimentCollection:
    posts: list[dict[str, Any]]
    retrieval_ledger: list[dict[str, Any]]
    live_validated_platforms: list[str]
    started_at_utc: datetime
    completed_at_utc: datetime


def _canonical_text(value: str) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip()


def _canonical_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value.strip()
    query = "&".join(
        part for part in parsed.query.split("&")
        if part and not part.casefold().startswith(("utm_", "fbclid=", "gclid="))
    )
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.casefold()}://{parsed.netloc.casefold()}{path}" + (f"?{query}" if query else "")


def _slug_id(value: str) -> str:
    return "sent-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:18]


def _domain(value: str) -> str:
    return (urlsplit(value).hostname or "").casefold().removeprefix("www.")


def _is_match(text: str, investible: Investible) -> tuple[bool, bool]:
    """Return (matched, ambiguous_short_ticker)."""

    folded = _canonical_text(text).casefold()
    if not folded:
        return False, False
    long_terms = [term.casefold() for term in (investible.name, *investible.aliases) if len(term.strip()) > 3]
    if any(term in folded for term in long_terms):
        return True, False
    ticker_terms = [term.strip().casefold() for term in investible.ticker.split("/") if term.strip()]
    context = any(term in folded for term in (
        "stock", "share", "shares", "etf", "fund", "market", "price", "invest",
        "crypto", "coin", "token", "oil", "黄金", "股票", "股价", "市场", "基金",
        "ビットコイン", "イーサリアム", "비트코인", "이더리움",
    ))
    for ticker in ticker_terms:
        if len(ticker) > 3 and ticker in folded:
            return True, False
        if len(ticker) <= 3 and re.search(rf"(?<![a-z0-9]){re.escape(ticker)}(?![a-z0-9])", folded):
            if ticker in {"btc", "eth"} or context:
                return True, False
            return True, True
    return False, False


def _base_ledger(
    investible: Investible,
    platform: str,
    backend: str | None,
    start: datetime,
    end: datetime,
    query_variants: Iterable[str] | None = None,
) -> dict[str, Any]:
    return {
        "run_id": None,
        "investible_id": investible.investible_id,
        "investible_name": investible.name,
        "ticker": investible.ticker,
        "platform": platform,
        "backend": backend,
        "research_window_start": iso_utc(start),
        "research_window_end": iso_utc(end),
        "query_variants": list(query_variants or investible.query_variants),
        "query_attempts": [],
        "cursors_or_pages_traversed": [],
        "first_retrieved_at": None,
        "last_retrieved_at": None,
        "posts_discovered": 0,
        "posts_read_full": 0,
        "posts_excluded": 0,
        "duplicates_removed": 0,
        "posts_unreadable": 0,
        "posts_private": 0,
        "posts_deleted": 0,
        "posts_auth_required": 0,
        "retained_posts": 0,
        "terminal_reason": "OTHER_DOCUMENTED_LIMIT",
        "exhaustive_boolean": False,
        "limit_detail": None,
        "errors": [],
        "exclusion_reasons": {},
        "completed_at": None,
    }


def _bump_exclusion(ledger: dict[str, Any], reason: str) -> None:
    ledger["posts_excluded"] += 1
    counts = ledger.setdefault("exclusion_reasons", {})
    counts[reason] = int(counts.get(reason, 0)) + 1


def _record_retrieved_time(ledger: dict[str, Any], published: datetime | None) -> None:
    if published is None:
        return
    value = iso_utc(published)
    current_first = ledger.get("first_retrieved_at")
    current_last = ledger.get("last_retrieved_at")
    if current_first is None or value < current_first:
        ledger["first_retrieved_at"] = value
    if current_last is None or value > current_last:
        ledger["last_retrieved_at"] = value


def _post_record(
    *,
    investible: Investible,
    platform: str,
    backend: str,
    title: str,
    text: str,
    url: str,
    published: datetime | None,
    author: str | None,
    language: str,
    region: str | None,
    engagement: dict[str, Any] | None,
    query_variant: str,
    page: str,
    read_full: bool,
) -> dict[str, Any]:
    canonical = _canonical_url(url)
    return {
        "post_id": _slug_id(f"{investible.investible_id}|{platform}|{canonical}|{author or ''}|{title}"),
        "investible_id": investible.investible_id,
        "platform": platform,
        "backend": backend,
        "title": _canonical_text(title),
        "text": _canonical_text(text) or _canonical_text(title),
        "language": language,
        "author": _canonical_text(author or "") or None,
        "url": url,
        "canonical_url": canonical,
        "published_at": published,
        "region": region,
        "engagement": engagement or {},
        "query_variant": query_variant,
        "page": page,
        "read_full": bool(read_full),
        "source_type": "social_community",
        "evidence_type": "direct_post",
        "metadata": {
            "source_key": f"{platform}:{author or _domain(url) or canonical}",
            "sentiment_investible_id": investible.investible_id,
            "sentiment_query_variant": query_variant,
            "sentiment_page": page,
            "full_content_read": bool(read_full),
        },
    }


class SentimentCollector:
    """Search each investible/backend pair with explicit terminal evidence."""

    def __init__(
        self,
        *,
        investibles: Iterable[Investible],
        coverage: dict[str, Coverage],
        existing_signals: Iterable[Signal],
        retrieved_at_utc: datetime,
        window_hours: float = 24,
        global_timeout_seconds: int = 180,
    ) -> None:
        self.investibles = tuple(investibles)
        self.coverage = coverage
        self.existing_signals = tuple(existing_signals)
        self.end = retrieved_at_utc.astimezone(UTC)
        self.start = self.end - timedelta(hours=window_hours)
        self.global_timeout_seconds = max(1, int(global_timeout_seconds))
        self.deadline = time.monotonic() + self.global_timeout_seconds
        self.topic_cache: dict[int, dict[str, Any]] = {}

    def live_validated_platforms(self) -> list[str]:
        return [
            platform for platform in SENTIMENT_PLATFORM_ORDER
            if (
                platform in self.coverage
                and self.coverage[platform].queried
                and self.coverage[platform].results > 0
                and self.coverage[platform].status in {"OK", "DEGRADED"}
            )
        ]

    def _expired(self) -> bool:
        return time.monotonic() >= self.deadline

    def query_variants_for(self, investible: Investible) -> tuple[str, ...]:
        """Add a small deterministic set of terms discovered in the live corpus."""

        variants = list(investible.query_variants)
        seen = {variant.casefold() for variant in variants}
        for signal in self.existing_signals:
            text = " ".join(
                value for value in (
                    signal.title,
                    signal.original_text,
                    signal.english_translation or "",
                ) if value
            )
            matched, ambiguous = _is_match(text, investible)
            if not matched or ambiguous:
                continue
            for keyword in signal.topic_keywords:
                term = re.sub(r"\s+", " ", str(keyword)).strip()
                if (
                    len(term) < 3
                    or len(term) > 48
                    or term.casefold() in _DIRECTIONAL_STOP
                    or term.casefold() in {"http", "https", "imgur", "com"}
                    or term.casefold() in seen
                ):
                    continue
                variant = f"{investible.name} {term}"
                variants.append(variant)
                seen.add(term.casefold())
                if len(variants) >= len(investible.query_variants) + 4:
                    return tuple(variants)
        return tuple(variants)

    def collect(self) -> SentimentCollection:
        started = datetime.now(UTC)
        posts: list[dict[str, Any]] = []
        ledger_rows: list[dict[str, Any]] = []
        live_platforms = self.live_validated_platforms()
        for investible in self.investibles:
            query_variants = self.query_variants_for(investible)
            for platform in SENTIMENT_PLATFORM_ORDER:
                coverage = self.coverage.get(platform)
                ledger = _base_ledger(
                    investible,
                    platform,
                    coverage.backend if coverage else None,
                    self.start,
                    self.end,
                    query_variants,
                )
                ledger["live_validated"] = platform in live_platforms
                if platform not in live_platforms:
                    ledger["terminal_reason"] = _terminal_for_coverage(coverage)
                    ledger["limit_detail"] = (
                        coverage.failure_reason if coverage and coverage.failure_reason
                        else "Platform was not live-validated for this run."
                    )
                    ledger["posts_auth_required"] = 1 if ledger["terminal_reason"] == "AUTH_REQUIRED" else 0
                    ledger["completed_at"] = iso_utc(datetime.now(UTC))
                    ledger_rows.append(ledger)
                    continue
                if self._expired():
                    ledger["terminal_reason"] = "TIMEOUT"
                    ledger["limit_detail"] = "Sentiment global execution budget expired before this pair started."
                    ledger["completed_at"] = iso_utc(datetime.now(UTC))
                    ledger_rows.append(ledger)
                    continue
                try:
                    if platform == "bilibili":
                        self._collect_bilibili_pair(investible, ledger, posts)
                    elif platform == "youtube":
                        self._collect_youtube_pair(investible, ledger, posts)
                    elif platform == "v2ex":
                        self._collect_v2ex_pair(investible, ledger, posts)
                    else:
                        ledger["terminal_reason"] = "OTHER_DOCUMENTED_LIMIT"
                        ledger["limit_detail"] = "No search/read adapter is registered for this live-validated backend."
                except Exception as exc:  # pair failure must not stop other pairs
                    ledger["terminal_reason"] = "BACKEND_FAILURE"
                    ledger["errors"].append(scrub_url_credentials(exc)[:500])
                ledger["completed_at"] = iso_utc(datetime.now(UTC))
                ledger["exhaustive_boolean"] = ledger["terminal_reason"] in {"EXHAUSTED", "WINDOW_BOUNDARY_REACHED"}
                ledger_rows.append(ledger)
        completed = datetime.now(UTC)
        return SentimentCollection(
            posts=posts,
            retrieval_ledger=ledger_rows,
            live_validated_platforms=live_platforms,
            started_at_utc=started,
            completed_at_utc=completed,
        )

    def _accept_item(
        self,
        *,
        investible: Investible,
        ledger: dict[str, Any],
        posts: list[dict[str, Any]],
        platform: str,
        backend: str,
        title: str,
        text: str,
        url: str,
        published: datetime | None,
        author: str | None,
        language: str,
        region: str | None,
        engagement: dict[str, Any] | None,
        query_variant: str,
        page: str,
        read_full: bool,
        count_discovered: bool = True,
    ) -> None:
        if count_discovered:
            ledger["posts_discovered"] += 1
        _record_retrieved_time(ledger, published)
        matched, ambiguous = _is_match(f"{title} {text}", investible)
        if not matched:
            _bump_exclusion(ledger, "entity_not_present_in_full_text")
            return
        if ambiguous:
            _bump_exclusion(ledger, "ambiguous_short_ticker")
            return
        if published is None:
            _bump_exclusion(ledger, "missing_publication_timestamp")
            return
        if published > self.end:
            _bump_exclusion(ledger, "future_timestamp")
            return
        if published < self.start:
            _bump_exclusion(ledger, "window_boundary")
            return
        if not read_full:
            ledger["posts_unreadable"] += 1
            _bump_exclusion(ledger, "full_content_unavailable")
            return
        ledger["posts_read_full"] += 1
        posts.append(
            _post_record(
                investible=investible,
                platform=platform,
                backend=backend,
                title=title,
                text=text,
                url=url,
                published=published,
                author=author,
                language=language,
                region=region,
                engagement=engagement,
                query_variant=query_variant,
                page=page,
                read_full=read_full,
            )
        )

    @staticmethod
    def _bilibili_items(payload: Any) -> list[dict[str, Any]]:
        result = (payload.get("data") or {}).get("result", []) if isinstance(payload, dict) else []
        for group in result if isinstance(result, list) else []:
            if group.get("result_type") == "video":
                data = group.get("data") or []
                return [item for item in data if isinstance(item, dict)]
        return []

    def _collect_bilibili_pair(
        self,
        investible: Investible,
        ledger: dict[str, Any],
        posts: list[dict[str, Any]],
    ) -> None:
        terminal_reasons: list[str] = []
        for query in self.query_variants_for(investible):
            if self._expired():
                ledger["terminal_reason"] = "TIMEOUT"
                ledger["limit_detail"] = "Sentiment global execution budget expired during Bilibili pagination."
                return
            page = 1
            query_reason = "EXHAUSTED"
            while True:
                if self._expired():
                    ledger["terminal_reason"] = "TIMEOUT"
                    ledger["limit_detail"] = "Sentiment global execution budget expired during Bilibili pagination."
                    return
                request_url = (
                    "https://api.bilibili.com/x/web-interface/search/all/v2"
                    f"?keyword={query.replace(' ', '+')}&page={page}&page_size={_BILIBILI_PAGE_SIZE}"
                )
                ledger["query_attempts"].append(query)
                ledger["cursors_or_pages_traversed"].append(f"query={query}&page={page}")
                try:
                    payload = _http_json(
                        request_url,
                        timeout=max(1, min(8, int(self.deadline - time.monotonic()))),
                    )
                    items = self._bilibili_items(payload)
                except Exception as exc:
                    ledger["errors"].append(
                        f"{query} page {page}: {scrub_url_credentials(exc)[:300]}"
                    )
                    query_reason = "BACKEND_FAILURE"
                    break
                if not items:
                    query_reason = "EXHAUSTED"
                    break
                old_seen = False
                for item in items:
                    published = parse_datetime(item.get("pubdate") or item.get("senddate"))
                    if published and published < self.start:
                        old_seen = True
                    bvid = item.get("bvid") or item.get("arcurl", "").rstrip("/").split("/")[-1]
                    if not bvid:
                        _bump_exclusion(ledger, "missing_source_url")
                        continue
                    self._accept_item(
                        investible=investible,
                        ledger=ledger,
                        posts=posts,
                        platform="bilibili",
                        backend=ledger["backend"] or "B站搜索 API",
                        title=html.unescape(re.sub(r"<[^>]+>", "", str(item.get("title") or ""))),
                        text=item.get("description") or item.get("tag") or item.get("title") or "",
                        url=f"https://www.bilibili.com/video/{bvid}",
                        published=published,
                        author=item.get("author") or item.get("uname"),
                        language="zh",
                        region="China",
                        engagement={
                            "likes": item.get("like"),
                            "comments": item.get("review") or item.get("video_review"),
                            "views": item.get("play"),
                        },
                        query_variant=query,
                        page=f"page:{page}",
                        read_full=bool(item.get("description") or item.get("tag")),
                    )
                if old_seen:
                    query_reason = "WINDOW_BOUNDARY_REACHED"
                    break
                if len(items) < _BILIBILI_PAGE_SIZE:
                    query_reason = "EXHAUSTED"
                    break
                page += 1
            terminal_reasons.append(query_reason)
        if ledger["errors"]:
            ledger["terminal_reason"] = "BACKEND_FAILURE"
            ledger["limit_detail"] = "At least one Bilibili query variant failed; successful variants were retained."
        elif "WINDOW_BOUNDARY_REACHED" in terminal_reasons:
            ledger["terminal_reason"] = "WINDOW_BOUNDARY_REACHED"
            ledger["limit_detail"] = "Every query variant was paginated until an older-than-window result was reached."
        else:
            ledger["terminal_reason"] = "EXHAUSTED"
            ledger["limit_detail"] = "Every query variant reached an empty or short Bilibili page."

    def _collect_youtube_pair(
        self,
        investible: Investible,
        ledger: dict[str, Any],
        posts: list[dict[str, Any]],
    ) -> None:
        for query in self.query_variants_for(investible):
            if self._expired():
                ledger["terminal_reason"] = "TIMEOUT"
                ledger["limit_detail"] = "Sentiment global execution budget expired during YouTube retrieval."
                return
            ledger["query_attempts"].append(query)
            ledger["cursors_or_pages_traversed"].append(
                f"query={query}&result_limit={_YOUTUBE_RESULT_LIMIT}"
            )
            try:
                raw = _run_command(
                    [
                        "yt-dlp",
                        "--dump-single-json",
                        "--skip-download",
                        "--no-warnings",
                        "--socket-timeout",
                        "8",
                        f"ytsearch{_YOUTUBE_RESULT_LIMIT}:{query}",
                    ],
                    timeout=max(1, min(8, int(self.deadline - time.monotonic()))),
                )
                payload = json.loads(raw)
            except Exception as exc:
                ledger["errors"].append(f"{query}: {scrub_url_credentials(exc)[:300]}")
                continue
            for item in payload.get("entries") or []:
                if not isinstance(item, dict):
                    continue
                video_id = item.get("id")
                if not video_id:
                    _bump_exclusion(ledger, "missing_source_url")
                    continue
                published = parse_datetime(item.get("timestamp") or item.get("release_timestamp"))
                if published is None and item.get("upload_date"):
                    published = parse_datetime(str(item["upload_date"]))
                self._accept_item(
                    investible=investible,
                    ledger=ledger,
                    posts=posts,
                    platform="youtube",
                    backend=ledger["backend"] or "yt-dlp",
                    title=item.get("title") or "",
                    text=item.get("description") or "",
                    url=item.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}",
                    published=published,
                    author=item.get("channel") or item.get("uploader"),
                    language="en",
                    region=None,
                    engagement={
                        "likes": item.get("like_count"),
                        "comments": item.get("comment_count"),
                        "views": item.get("view_count"),
                    },
                    query_variant=query,
                    page=f"result_limit:{_YOUTUBE_RESULT_LIMIT}",
                    read_full=bool(item.get("description")),
                )
        if ledger["errors"] and not ledger["posts_read_full"]:
            ledger["terminal_reason"] = "BACKEND_FAILURE"
            ledger["limit_detail"] = "YouTube search failed before a complete result batch was read."
        elif self._expired():
            ledger["terminal_reason"] = "TIMEOUT"
            ledger["limit_detail"] = "Sentiment global execution budget expired before all query variants completed."
        else:
            ledger["terminal_reason"] = "PLATFORM_LIMIT"
            ledger["limit_detail"] = (
                "yt-dlp exposes a bounded search result set without a cursor for exhaustive pagination."
            )

    def _collect_v2ex_pair(
        self,
        investible: Investible,
        ledger: dict[str, Any],
        posts: list[dict[str, Any]],
    ) -> None:
        channel = V2EXChannel()
        for query in self.query_variants_for(investible):
            ledger["query_attempts"].append(query)
            result = channel.search(query)
            if result and isinstance(result[0], dict) and result[0].get("error"):
                ledger["errors"].append(str(result[0]["error"])[:300])
        seen_urls: set[str] = set()
        for signal in self.existing_signals:
            if signal.platform != "v2ex":
                continue
            ledger["posts_discovered"] += 1
            matched, ambiguous = _is_match(f"{signal.title} {signal.original_text}", investible)
            if not matched:
                _bump_exclusion(ledger, "entity_not_present_in_full_text")
                continue
            if ambiguous:
                _bump_exclusion(ledger, "ambiguous_short_ticker")
                continue
            topic_match = re.search(r"/t/(\d+)", signal.url)
            if not topic_match or signal.url in seen_urls:
                ledger["posts_unreadable"] += 1
                _bump_exclusion(ledger, "topic_detail_unavailable")
                continue
            seen_urls.add(signal.url)
            topic_id = int(topic_match.group(1))
            try:
                topic = self.topic_cache.get(topic_id)
                if topic is None:
                    topic = channel.get_topic(topic_id)
                    self.topic_cache[topic_id] = topic
                content = topic.get("content") or ""
                published = parse_datetime(topic.get("created"))
                self._accept_item(
                    investible=investible,
                    ledger=ledger,
                    posts=posts,
                    platform="v2ex",
                    backend=ledger["backend"] or "V2EX API (public)",
                    title=topic.get("title") or signal.title,
                    text=content,
                    url=topic.get("url") or signal.url,
                    published=published,
                    author=(topic.get("author") or None),
                    language="zh",
                    region="China / global community",
                    engagement={"comments": topic.get("replies_count")},
                    query_variant=investible.name,
                    page="existing-live-corpus",
                    read_full=bool(content),
                    count_discovered=False,
                )
            except Exception as exc:
                ledger["errors"].append(f"{signal.url}: {scrub_url_credentials(exc)[:300]}")
                ledger["posts_unreadable"] += 1
                _bump_exclusion(ledger, "topic_detail_read_failed")
        ledger["terminal_reason"] = "OTHER_DOCUMENTED_LIMIT"
        ledger["limit_detail"] = (
            "V2EX's public backend exposes no full-text search or cursor. "
            "The live discovery corpus was checked and matching canonical topics were read in full."
        )


def _terminal_for_coverage(coverage: Coverage | None) -> str:
    if coverage is None:
        return "BACKEND_FAILURE"
    message = f"{coverage.status} {coverage.failure_reason or ''}".casefold()
    if coverage.status == "TIMEOUT" or "timeout" in message:
        return "TIMEOUT"
    if any(marker in message for marker in ("auth", "cookie", "login", "credential", "sign in")):
        return "AUTH_REQUIRED"
    if "rate" in message:
        return "RATE_LIMIT"
    return "BACKEND_FAILURE"


def collect_investible_sentiments(
    *,
    investibles: Iterable[Investible],
    coverage: dict[str, Coverage],
    existing_signals: Iterable[Signal],
    retrieved_at_utc: datetime,
    window_hours: float = 24,
    global_timeout_seconds: int = 180,
) -> SentimentCollection:
    return SentimentCollector(
        investibles=investibles,
        coverage=coverage,
        existing_signals=existing_signals,
        retrieved_at_utc=retrieved_at_utc,
        window_hours=window_hours,
        global_timeout_seconds=global_timeout_seconds,
    ).collect()


def _label_signal(signal: Signal) -> str:
    text = f"{signal.english_translation or ''} {signal.original_text} {signal.title}".casefold()
    positive = sum(term in text for term in _POSITIVE_TERMS)
    negative = sum(term in text for term in _NEGATIVE_TERMS)
    uncertain = sum(term in text for term in _UNCERTAIN_TERMS)
    if positive and negative:
        return "MIXED"
    if uncertain and not positive and not negative:
        return "UNCERTAIN"
    if positive > negative:
        return "POSITIVE"
    if negative > positive:
        return "NEGATIVE"
    return "NEUTRAL"


def _engagement_value(signal: Signal) -> float:
    values = [float(value) for value in signal.engagement.to_dict().values() if value is not None]
    return max(values, default=0.0)


def _signal_source(signal: Signal) -> str:
    return str(
        signal.author
        or signal.metadata.get("source_key")
        or _domain(signal.url)
        or signal.url
        or signal.platform
    ).casefold()


def _keyword_themes(signals: list[Signal]) -> str:
    counter: Counter[str] = Counter()
    for signal in signals:
        for keyword in signal.topic_keywords:
            value = str(keyword).casefold().strip()
            if value and value not in _DIRECTIONAL_STOP and len(value) > 2:
                counter[value] += 1
    themes = [key for key, _ in counter.most_common(2)]
    return ", ".join(themes) if themes else "the current market discussion"


def _sentiment_signal_json(signal: Signal, label: str, role: str | None = None) -> dict[str, Any]:
    payload = signal.to_dict()
    payload["sentiment"] = label
    payload["evidence_role"] = role or label.casefold()
    payload["full_content_read"] = bool(signal.metadata.get("full_content_read"))
    payload["query_variant"] = signal.metadata.get("sentiment_query_variant")
    return payload


def _prose(
    investible: Investible,
    *,
    posts: list[Signal],
    labels: dict[str, str],
    positive_percent: int | None,
    negative_percent: int | None,
    coverage_label: str,
    independent_count: int,
) -> str:
    if positive_percent is None or negative_percent is None:
        return (
            f"The retained discussion around {investible.name} is {coverage_label.lower()} and has "
            f"{independent_count} independent mention source(s). The corpus is too small, ambiguous or "
            "incomplete to support a reliable positive/negative meter. This snapshot describes retained "
            "social discussion, not a forecast or personal financial advice."
        )
    positive = [signal for signal in posts if labels.get(signal.signal_id) == "POSITIVE"]
    negative = [signal for signal in posts if labels.get(signal.signal_id) == "NEGATIVE"]
    if positive_percent >= 60:
        direction = "mostly positive"
    elif negative_percent >= 60:
        direction = "mostly negative"
    else:
        direction = "mixed"
    sentence = f"{investible.name} discussion is {direction} in the current research window."
    if positive and negative:
        sentence += (
            f" Supportive posts focus on {_keyword_themes(positive)}, while the strongest opposing "
            f"theme is {_keyword_themes(negative)}."
        )
    elif positive:
        sentence += f" Supportive posts focus on {_keyword_themes(positive)}."
    elif negative:
        sentence += f" Negative posts focus on {_keyword_themes(negative)}."
    sentence += (
        f" The meter is calculated from retained, deduplicated mentions with {coverage_label.lower()} "
        "coverage. It reflects discussion, not a forecast of price or personal financial advice."
    )
    return sentence


def build_investible_sentiments(
    *,
    investibles: Iterable[Investible],
    posts: Iterable[dict[str, Any]],
    retrieval_ledger: list[dict[str, Any]],
    retrieved_at_utc: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize, deduplicate, classify, and aggregate retained sentiment posts."""

    from .normalize import normalize_records

    raw_posts = list(posts)
    signals = normalize_records(
        raw_posts,
        retrieved_at_utc=retrieved_at_utc,
        multilingual=True,
    )
    ledger_by_pair = {(row["investible_id"], row["platform"]): row for row in retrieval_ledger}
    by_investible: dict[str, list[Signal]] = defaultdict(list)
    for signal in signals:
        investible_id = signal.metadata.get("sentiment_investible_id")
        if investible_id:
            by_investible[str(investible_id)].append(signal)

    outputs: list[dict[str, Any]] = []
    sentiment_ledger: list[dict[str, Any]] = []
    for investible in investibles:
        candidates = sorted(
            by_investible.get(investible.investible_id, []),
            key=lambda signal: (
                signal.published_at_utc or datetime.min.replace(tzinfo=UTC),
                signal.signal_id,
            ),
            reverse=True,
        )
        unique, duplicates = _deduplicate_sentiment_signals(candidates)
        for signal in duplicates:
            pair = ledger_by_pair.get((investible.investible_id, signal.platform))
            if pair:
                pair["duplicates_removed"] += 1
                _bump_exclusion(pair, "duplicate_repost_or_canonical_url")
        author_counts: Counter[tuple[str, str]] = Counter()
        retained: list[Signal] = []
        for signal in unique:
            author_key = (signal.platform, signal.author.casefold()) if signal.author else ("", "")
            if author_key != ("", ""):
                author_counts[author_key] += 1
                if author_counts[author_key] > _MAX_AUTHOR_MENTIONS:
                    pair = ledger_by_pair.get((investible.investible_id, signal.platform))
                    if pair:
                        _bump_exclusion(pair, "single_author_cap")
                    continue
            retained.append(signal)
        labels = {signal.signal_id: _label_signal(signal) for signal in retained}
        platform_max: dict[str, float] = defaultdict(float)
        for signal in retained:
            platform_max[signal.platform] = max(
                platform_max[signal.platform],
                math.log1p(_engagement_value(signal)),
            )
        weights: dict[str, float] = {}
        for signal in retained:
            maximum = platform_max[signal.platform]
            engagement_weight = (
                0.0
                if maximum == 0
                else min(0.5, 0.5 * math.log1p(_engagement_value(signal)) / maximum)
            )
            weights[signal.signal_id] = 1.0 + engagement_weight
        source_ids = {_signal_source(signal) for signal in retained}
        positive_weight = sum(
            weights[signal.signal_id] for signal in retained if labels[signal.signal_id] == "POSITIVE"
        )
        negative_weight = sum(
            weights[signal.signal_id] for signal in retained if labels[signal.signal_id] == "NEGATIVE"
        )
        directional_weight = positive_weight + negative_weight
        independent_count = len(source_ids)
        positive_percent: int | None = None
        negative_percent: int | None = None
        if len(retained) >= 3 and independent_count >= 2 and directional_weight > 0:
            positive_percent = int(round(positive_weight / directional_weight * 100))
            negative_percent = 100 - positive_percent
        ledger_pairs = [
            row for row in retrieval_ledger if row["investible_id"] == investible.investible_id
        ]
        live_pairs = [
            row for row in ledger_pairs
            if row.get(
                "live_validated",
                row["platform"] in SOCIAL_PLATFORMS
                and row["terminal_reason"] not in {"AUTH_REQUIRED", "BACKEND_FAILURE"},
            )
        ]
        if not live_pairs:
            coverage_label = "INSUFFICIENT DATA"
        elif all(row["exhaustive_boolean"] for row in live_pairs):
            coverage_label = "COVERAGE COMPLETE"
        else:
            coverage_label = "COVERAGE PARTIAL"
        meter_status = "METER READY" if positive_percent is not None else "INSUFFICIENT DATA"
        rows_by_label = {
            label: [
                _sentiment_signal_json(signal, label)
                for signal in retained
                if labels[signal.signal_id] == label
            ]
            for label in ("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED", "UNCERTAIN")
        }
        representatives = []
        for label in ("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED", "UNCERTAIN"):
            if rows_by_label[label]:
                representatives.append({**rows_by_label[label][0], "evidence_role": label.casefold()})
        retained_ids = [signal.signal_id for signal in retained]
        for row in ledger_pairs:
            row["retained_posts"] = sum(
                1 for signal in retained if signal.platform == row["platform"]
            )
        output = {
            **investible.to_dict(),
            "coverage": coverage_label,
            "meter_status": meter_status,
            "positive_percent": positive_percent,
            "negative_percent": negative_percent,
            "retained_post_count": len(retained),
            "independent_mention_count": independent_count,
            "sentiment_counts": {label: len(rows_by_label[label]) for label in rows_by_label},
            "retained_signal_ids": retained_ids,
            "representative_sources": representatives[:6],
            "positive_sources": rows_by_label["POSITIVE"][:4],
            "negative_sources": rows_by_label["NEGATIVE"][:4],
            "neutral_mixed_sources": (
                rows_by_label["NEUTRAL"] + rows_by_label["MIXED"] + rows_by_label["UNCERTAIN"]
            )[:4],
            "system_prose": _prose(
                investible,
                posts=retained,
                labels=labels,
                positive_percent=positive_percent,
                negative_percent=negative_percent,
                coverage_label=coverage_label,
                independent_count=independent_count,
            ),
            "retrieved_at_utc": iso_utc(retrieved_at_utc),
            "retrieved_at_sgt": iso_sgt(retrieved_at_utc),
        }
        outputs.append(output)
        sentiment_ledger.append(
            {
                "investible_id": investible.investible_id,
                "name": investible.name,
                "ticker": investible.ticker,
                "coverage": coverage_label,
                "meter_status": meter_status,
                "positive_percent": positive_percent,
                "negative_percent": negative_percent,
                "retained_signal_ids": retained_ids,
                "positive_signal_ids": [
                    signal.signal_id for signal in retained if labels[signal.signal_id] == "POSITIVE"
                ],
                "negative_signal_ids": [
                    signal.signal_id for signal in retained if labels[signal.signal_id] == "NEGATIVE"
                ],
                "neutral_mixed_signal_ids": [
                    signal.signal_id
                    for signal in retained
                    if labels[signal.signal_id] in {"NEUTRAL", "MIXED", "UNCERTAIN"}
                ],
                "independent_source_ids": sorted(source_ids),
                "retrieval_pair_count": len(ledger_pairs),
            }
        )
    retained_ids = {
        signal_id
        for row in sentiment_ledger
        for signal_id in row["retained_signal_ids"]
    }
    sentiment_posts = [
        _sentiment_signal_json(signal, _label_signal(signal))
        for signal in signals
        if signal.signal_id in retained_ids
    ]
    return outputs, sentiment_ledger, sentiment_posts


def _deduplicate_sentiment_signals(signals: list[Signal]) -> tuple[list[Signal], list[Signal]]:
    unique: list[Signal] = []
    duplicates: list[Signal] = []
    seen: dict[str, Signal] = {}
    for signal in signals:
        canonical = _canonical_url(signal.url)
        text_key = re.sub(r"[^a-z0-9\u3400-\u9fff]+", " ", signal.original_text.casefold()).strip()
        key = f"url:{canonical}" if canonical else f"text:{signal.author or ''}:{text_key}"
        retained = seen.get(key)
        if retained is None:
            seen[key] = signal
            unique.append(signal)
            continue
        duplicates.append(signal)
    return unique, duplicates


def render_sentiment_report(
    sentiments: list[dict[str, Any]],
    retrieval_ledger: list[dict[str, Any]],
) -> str:
    lines = ["", "## INVESTIBLES SENTIMENTS", ""]
    for item in sentiments:
        meter = (
            f"{item['positive_percent']}% positive / {item['negative_percent']}% negative"
            if item["positive_percent"] is not None
            else "INSUFFICIENT DATA — no percentage emitted"
        )
        lines.extend(
            [
                f"### {item['name']} ({item['ticker']})",
                f"METER: {meter}",
                f"COVERAGE: {item['coverage']}",
                (
                    f"RETAINED POSTS: {item['retained_post_count']}; "
                    f"INDEPENDENT SOURCES/CREATORS: {item['independent_mention_count']}"
                ),
                item["system_prose"],
                "",
            ]
        )
    lines.extend(
        [
            "### EXHAUSTION LEDGER SUMMARY",
            "",
            f"Investible × platform rows: {len(retrieval_ledger)}",
            f"Exhaustive rows: {sum(bool(row['exhaustive_boolean']) for row in retrieval_ledger)}",
            (
                "The percentage describes only retained, deduplicated full-content posts. "
                "Platform limits and failures remain explicit in sentiment_retrieval_ledger.json."
            ),
        ]
    )
    return "\n".join(lines)

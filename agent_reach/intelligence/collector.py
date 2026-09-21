# -*- coding: utf-8 -*-
"""Live acquisition routed through Agent-Reach's existing backends.

This module intentionally does not implement a replacement scraper. It probes
the registered channel set first, then invokes the documented upstream command
or the channel's public fallback API for channels that are actually available.
"""

from __future__ import annotations

import datetime as dt
import html
import json
import os
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import feedparser

from agent_reach.channels.v2ex import V2EXChannel
from agent_reach.config import Config
from agent_reach.doctor import check_all
from agent_reach.utils.text import scrub_url_credentials

from .schema import UTC, parse_datetime

_MAX_HTTP_BYTES = 5 * 1024 * 1024
_UA = "agent-reach-intelligence/1.0"
_HTTP_TIMEOUT_SECONDS = 8
_COMMAND_TIMEOUT_SECONDS = 8
_GLOBAL_COLLECTION_TIMEOUT_SECONDS = 300


@dataclass
class Coverage:
    platform: str
    backend: str | None
    queried: bool = False
    results: int = 0
    status: str = "NOT QUERIED"
    failure_reason: str | None = None
    authentication: str = "not required / not checked"
    degraded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "backend": self.backend,
            "queried": self.queried,
            "results": self.results,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "authentication": self.authentication,
            "degraded": self.degraded,
        }


@dataclass
class CollectionResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    coverage: dict[str, Coverage] = field(default_factory=dict)
    doctor: dict[str, Any] = field(default_factory=dict)
    retrieved_at_utc: dt.datetime = field(default_factory=lambda: dt.datetime.now(UTC))
    collection_started_at_utc: dt.datetime | None = None
    collection_finished_at_utc: dt.datetime | None = None


RSS_FEEDS: tuple[dict[str, str], ...] = (
    {"name": "BBC World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml", "language": "en", "region": "global", "source_type": "news"},
    {"name": "BBC Technology", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml", "language": "en", "region": "global", "source_type": "news"},
    {"name": "BBC Science", "url": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "language": "en", "region": "global", "source_type": "news"},
    {"name": "NPR News", "url": "https://feeds.npr.org/1001/rss.xml", "language": "en", "region": "US", "source_type": "news"},
    {"name": "The Guardian World", "url": "https://www.theguardian.com/world/rss", "language": "en", "region": "global", "source_type": "news"},
    {"name": "The Guardian Technology", "url": "https://www.theguardian.com/uk/technology/rss", "language": "en", "region": "global", "source_type": "news"},
    {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "language": "en", "region": "global", "source_type": "news"},
    {"name": "The Verge", "url": "https://www.theverge.com/rss/index.xml", "language": "en", "region": "US", "source_type": "news"},
    {"name": "TechCrunch", "url": "https://techcrunch.com/feed/", "language": "en", "region": "US", "source_type": "news"},
    {"name": "Nature", "url": "https://www.nature.com/nature.rss", "language": "en", "region": "global", "source_type": "research"},
    {"name": "NASA Breaking News", "url": "https://www.nasa.gov/rss/dyn/breaking_news.rss", "language": "en", "region": "US", "source_type": "official"},
    {"name": "NHK Japan", "url": "https://www3.nhk.or.jp/rss/news/cat0.xml", "language": "ja", "region": "Japan", "source_type": "news"},
    {"name": "Antara Indonesia", "url": "https://www.antaranews.com/rss/terkini.xml", "language": "id", "region": "Indonesia", "source_type": "news"},
    {"name": "Google News Chinese", "url": "https://news.google.com/rss/search?q=%E7%AA%81%E5%8F%91%E6%96%B0%E9%97%BB+OR+%E7%A7%91%E6%8A%80+OR+%E7%BB%8F%E6%B5%8E&hl=zh-CN&gl=CN&ceid=CN:zh-Hans", "language": "zh", "region": "China", "source_type": "news"},
    {"name": "Google News Malay", "url": "https://news.google.com/rss/search?q=berita+terkini+OR+teknologi+OR+ekonomi&hl=ms-MY&gl=MY&ceid=MY:ms", "language": "ms", "region": "Malaysia", "source_type": "news"},
    {"name": "Google News Indonesian", "url": "https://news.google.com/rss/search?q=berita+terkini+OR+teknologi+OR+ekonomi&hl=id&gl=ID&ceid=ID:id", "language": "id", "region": "Indonesia", "source_type": "news"},
    {"name": "Google News Korean", "url": "https://news.google.com/rss/search?q=%EC%B5%9C%EC%8B%A0+%EB%89%B4%EC%8A%A4+OR+%EA%B8%B0%EC%88%A0+OR+%EA%B2%BD%EC%A0%9C&hl=ko&gl=KR&ceid=KR:ko", "language": "ko", "region": "South Korea", "source_type": "news"},
)

MULTILINGUAL_QUERIES: tuple[dict[str, str], ...] = (
    {"query": "breaking news technology AI markets", "language": "en", "region": "global"},
    {"query": "geopolitics climate disaster science health", "language": "en", "region": "global"},
    {"query": "人工智能 科技 财经 突发新闻", "language": "zh", "region": "China"},
    {"query": "自然灾害 政策 监管 市场", "language": "zh", "region": "China"},
    {"query": "berita terkini teknologi AI ekonomi", "language": "ms", "region": "Malaysia"},
    {"query": "berita terbaru bencana kebijakan pasar", "language": "id", "region": "Indonesia"},
    {"query": "最新ニュース AI 技術 経済 災害", "language": "ja", "region": "Japan"},
    {"query": "최신 뉴스 AI 기술 경제 재난", "language": "ko", "region": "South Korea"},
)


def _read_response_limited(response: Any, *, timeout: int) -> bytes:
    """Read a response with both socket and wall-clock limits.

    A socket timeout alone can still permit an indefinitely slow response when
    the peer sends a few bytes before every timeout. The deadline prevents a
    single feed or API from holding the daily run open indefinitely.
    """
    deadline = time.monotonic() + timeout
    chunks: list[bytes] = []
    total = 0
    while total <= _MAX_HTTP_BYTES:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"response exceeded {timeout}s wall-clock limit")
        chunk = response.read(min(64 * 1024, _MAX_HTTP_BYTES + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    raw = b"".join(chunks)
    if len(raw) > _MAX_HTTP_BYTES:
        raise ValueError("response exceeds safety limit")
    return raw


def _http_json(url: str, *, timeout: int = 20) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        raw = _read_response_limited(response, timeout=timeout)
    return json.loads(raw.decode("utf-8"))


def _http_bytes(url: str, *, timeout: int = 20) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/rss+xml, application/xml, text/xml"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        return _read_response_limited(response, timeout=timeout)


def _feed_datetime(entry: Any) -> dt.datetime | None:
    for field_name in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = entry.get(field_name)
        if parsed:
            try:
                return dt.datetime(*parsed[:6], tzinfo=UTC)
            except (TypeError, ValueError):
                continue
    for field_name in ("published", "updated", "created"):
        parsed = parse_datetime(entry.get(field_name))
        if parsed:
            return parsed
    return None


def _html_title(value: Any) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", str(value or "")))).strip()


def _run_command(args: list[str], *, timeout: int) -> str:
    env = os.environ.copy()
    env.update({"PYTHONIOENCODING": "utf-8", "GH_PAGER": "cat", "PAGER": "cat"})
    completed = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )
    if completed.returncode != 0:
        detail = scrub_url_credentials((completed.stderr or completed.stdout or "").strip())
        raise RuntimeError(detail[:500] or f"command exited {completed.returncode}")
    return completed.stdout


class LiveCollector:
    """Collect from supported sources while retaining truthful coverage facts."""

    DISCOVERY_PLATFORMS = (
        "twitter", "reddit", "youtube", "facebook", "instagram", "xiaohongshu",
        "bilibili", "linkedin", "v2ex", "exa_search", "rss", "github", "web",
        "xueqiu", "xiaoyuzhou", "boss",
    )

    def __init__(
        self,
        *,
        config: Config | None = None,
        multilingual: bool = False,
        global_timeout_seconds: int = _GLOBAL_COLLECTION_TIMEOUT_SECONDS,
    ):
        self.config = config or Config(read_only=True)
        self.multilingual = multilingual
        self.global_timeout_seconds = max(1, int(global_timeout_seconds))
        self._deadline: float | None = None

    def collect(self) -> CollectionResult:
        started = dt.datetime.now(UTC)
        self._deadline = time.monotonic() + self.global_timeout_seconds
        retrieved = dt.datetime.now(UTC)
        doctor = check_all(self.config)
        result = CollectionResult(
            doctor=doctor,
            retrieved_at_utc=retrieved,
            collection_started_at_utc=started,
        )
        for platform in self.DISCOVERY_PLATFORMS:
            check = doctor.get(platform, {})
            result.coverage[platform] = Coverage(
                platform=platform,
                backend=check.get("active_backend"),
                authentication=self._authentication_status(platform, check),
            )

        channels = (
            ("rss", self._collect_rss),
            ("v2ex", self._collect_v2ex),
            ("bilibili", self._collect_bilibili),
            ("youtube", self._collect_youtube),
            ("github", self._collect_github),
        )
        for platform, method in channels:
            if self._expired():
                self._mark_remaining_timeouts(result, platform)
                break
            method(result)
        self._mark_unavailable(result)
        result.collection_finished_at_utc = dt.datetime.now(UTC)
        # Normalize against the end of acquisition so a record published a few
        # seconds after the first channel started is not misclassified as a
        # future signal relative to the run's retrieval timestamp.
        result.retrieved_at_utc = result.collection_finished_at_utc
        return result

    def _remaining(self) -> float:
        if self._deadline is None:
            return float(self.global_timeout_seconds)
        return max(0.0, self._deadline - time.monotonic())

    def _expired(self) -> bool:
        return self._remaining() <= 0

    def _mark_remaining_timeouts(self, result: CollectionResult, first_platform: str) -> None:
        pending = False
        for platform in self.DISCOVERY_PLATFORMS:
            if platform == first_platform:
                pending = True
            if pending and not result.coverage[platform].queried:
                self._set_failure(
                    result,
                    platform,
                    "global collection execution budget exceeded",
                    queried=False,
                    status="TIMEOUT",
                )

    @staticmethod
    def _authentication_status(platform: str, check: dict[str, Any]) -> str:
        message = str(check.get("message") or "").casefold()
        if any(token in message for token in ("auth", "登录", "cookie", "credential", "未安装")):
            return "required or not available"
        if platform in {"twitter", "reddit", "facebook", "instagram", "xiaohongshu", "linkedin", "xueqiu"}:
            return "not verified"
        return "not required / not checked"

    def _set_success(self, result: CollectionResult, platform: str, records: list[dict[str, Any]], backend: str | None = None) -> None:
        coverage = result.coverage[platform]
        coverage.queried = True
        coverage.results += len(records)
        coverage.status = "OK" if records else "DEGRADED"
        coverage.degraded = not bool(records)
        if backend:
            coverage.backend = backend
        result.records.extend(records)

    def _set_failure(self, result: CollectionResult, platform: str, reason: str, *, queried: bool = True, status: str = "UNAVAILABLE") -> None:
        coverage = result.coverage[platform]
        coverage.queried = queried
        coverage.status = status
        coverage.failure_reason = reason[:500]
        coverage.degraded = status != "OK"

    def _collect_rss(self, result: CollectionResult) -> None:
        coverage = result.coverage["rss"]
        coverage.queried = True
        successful_feeds = 0
        errors: list[str] = []
        for feed in RSS_FEEDS:
            if self._expired():
                coverage.status = "TIMEOUT"
                coverage.degraded = True
                coverage.failure_reason = "global collection execution budget exceeded"
                break
            try:
                timeout = max(1, min(_HTTP_TIMEOUT_SECONDS, int(self._remaining())))
                parsed = feedparser.parse(_http_bytes(feed["url"], timeout=timeout))
                feed_domain = urlsplit(feed["url"]).hostname or feed["name"]
                entries = list(parsed.entries)[:40]
                for entry in entries:
                    link = str(entry.get("link") or "")
                    title = _html_title(entry.get("title"))
                    summary = _html_title(entry.get("summary") or entry.get("description"))
                    if not title or not link:
                        continue
                    source = entry.get("source") or {}
                    source_title = _html_title(source.get("title") if isinstance(source, dict) else "")
                    source_link = str(source.get("href") or source.get("url") or "") if isinstance(source, dict) else ""
                    source_domain = urlsplit(source_link).hostname if source_link else ""
                    publisher = entry.get("author") or source_title or feed["name"]
                    result.records.append(
                        {
                            "platform": "rss",
                            "backend": "feedparser",
                            "title": title,
                            "text": summary or title,
                            "language": feed["language"],
                            "author": publisher,
                            "url": link,
                            "published_at": _feed_datetime(entry),
                            "region": feed["region"],
                            "source_type": feed["source_type"],
                            "evidence_type": "official" if feed["source_type"] == "official" else "secondary",
                            "metadata": {
                                "feed_name": feed["name"],
                                "feed_domain": feed_domain,
                                "publisher": publisher,
                                "source_key": (source_domain or source_title or feed_domain).casefold(),
                            },
                        }
                    )
                successful_feeds += 1
            except Exception as exc:
                errors.append(f"{feed['name']}: {scrub_url_credentials(exc)}")
        coverage.results = sum(1 for record in result.records if record.get("platform") == "rss")
        if successful_feeds:
            coverage.status = "OK" if not errors else "DEGRADED"
            coverage.degraded = bool(errors)
            if errors:
                coverage.failure_reason = "; ".join(errors[:4])[:500]
        else:
            coverage.status = "UNAVAILABLE"
            coverage.failure_reason = "; ".join(errors[:4])[:500] or "all RSS feeds returned no data"

    def _collect_v2ex(self, result: CollectionResult) -> None:
        if self._expired():
            self._set_failure(result, "v2ex", "global collection execution budget exceeded", queried=False, status="TIMEOUT")
            return
        check = result.doctor.get("v2ex", {})
        if check.get("status") not in {"ok", "warn"}:
            self._set_failure(result, "v2ex", str(check.get("message") or "doctor marked V2EX unavailable"), queried=False)
            return
        channel = V2EXChannel()
        records: list[dict[str, Any]] = []
        try:
            topics = channel.get_hot_topics(limit=40)
            for node in ("tech", "programmer", "python", "share", "news", "finance"):
                try:
                    topics.extend(channel.get_node_topics(node, limit=20))
                except Exception:
                    continue
            for item in topics:
                topic_id = item.get("id")
                url = item.get("url") or (f"https://www.v2ex.com/t/{topic_id}" if topic_id else "")
                if not url or not item.get("title"):
                    continue
                records.append(
                    {
                        "platform": "v2ex",
                        "backend": "V2EX API (public)",
                        "title": item.get("title"),
                        "text": item.get("content") or item.get("title"),
                        "language": "zh",
                        "author": None,
                        "url": url,
                        "published_at": parse_datetime(item.get("created")),
                        "likes": None,
                        "comments": item.get("replies"),
                        "region": "China / global community",
                        "source_type": "community",
                        "evidence_type": "direct_post",
                        "metadata": {
                            "node": item.get("node_name"),
                            "source_key": f"v2ex:{item.get('node_name') or 'hot'}",
                        },
                    }
                )
            self._set_success(result, "v2ex", records, "V2EX API (public)")
        except Exception as exc:
            self._set_failure(result, "v2ex", scrub_url_credentials(exc))

    def _collect_bilibili(self, result: CollectionResult) -> None:
        check = result.doctor.get("bilibili", {})
        if check.get("status") not in {"ok", "warn"}:
            self._set_failure(result, "bilibili", str(check.get("message") or "doctor marked Bilibili unavailable"), queried=False)
            return
        backend = check.get("active_backend") or "B站搜索 API"
        records: list[dict[str, Any]] = []
        errors: list[str] = []
        for query_info in MULTILINGUAL_QUERIES:
            if self._expired():
                errors.append("global collection execution budget exceeded")
                break
            query = query_info["query"]
            try:
                url = "https://api.bilibili.com/x/web-interface/search/all/v2?" + urllib.parse.urlencode({"keyword": query, "page": 1})
                timeout = max(1, min(_HTTP_TIMEOUT_SECONDS, int(self._remaining())))
                payload = _http_json(url, timeout=timeout)
                for group in (payload.get("data") or {}).get("result", []):
                    if group.get("result_type") != "video":
                        continue
                    for item in (group.get("data") or [])[:10]:
                        bvid = item.get("bvid")
                        if not bvid or not item.get("title"):
                            continue
                        records.append(
                            {
                                "platform": "bilibili",
                                "backend": backend,
                                "title": _html_title(item.get("title")),
                                "text": item.get("description") or item.get("tag") or item.get("title"),
                                "language": query_info["language"],
                                "author": item.get("author") or item.get("uname"),
                                "url": f"https://www.bilibili.com/video/{bvid}",
                                "published_at": parse_datetime(item.get("pubdate") or item.get("senddate")),
                                "engagement": {
                                    "likes": item.get("like"),
                                    "comments": item.get("review") or item.get("video_review"),
                                    "views": item.get("play"),
                                },
                                "region": query_info["region"],
                                "source_type": "social_video",
                                "evidence_type": "direct_post",
                                "metadata": {"query": query, "source_key": f"bilibili:{item.get('author') or item.get('mid') or bvid}"},
                            }
                        )
                    break
            except Exception as exc:
                errors.append(f"{query}: {scrub_url_credentials(exc)}")
        if records:
            self._set_success(result, "bilibili", records, backend)
            if errors:
                result.coverage["bilibili"].status = "TIMEOUT" if "global collection execution budget exceeded" in errors else "DEGRADED"
                result.coverage["bilibili"].degraded = True
                result.coverage["bilibili"].failure_reason = "; ".join(errors[:3])[:500]
        else:
            self._set_failure(result, "bilibili", "; ".join(errors[:3]) or "Bilibili returned no video records")

    def _collect_youtube(self, result: CollectionResult) -> None:
        check = result.doctor.get("youtube", {})
        if check.get("active_backend") != "yt-dlp" or not shutil.which("yt-dlp"):
            self._set_failure(result, "youtube", str(check.get("message") or "yt-dlp is unavailable"), queried=False)
            return
        records: list[dict[str, Any]] = []
        errors: list[str] = []
        for query_info in MULTILINGUAL_QUERIES:
            if self._expired():
                errors.append("global collection execution budget exceeded")
                break
            try:
                timeout = max(1, min(_COMMAND_TIMEOUT_SECONDS, int(self._remaining())))
                raw = _run_command(
                    ["yt-dlp", "--dump-single-json", "--skip-download", "--no-warnings", "--socket-timeout", "8", f"ytsearch6:{query_info['query']}"],
                    timeout=timeout,
                )
                payload = json.loads(raw)
                for item in (payload.get("entries") or [])[:8]:
                    video_id = item.get("id")
                    if not video_id or not item.get("title"):
                        continue
                    published = parse_datetime(item.get("timestamp") or item.get("release_timestamp"))
                    if published is None and item.get("upload_date"):
                        published = parse_datetime(str(item["upload_date"]))
                    records.append(
                        {
                            "platform": "youtube",
                            "backend": "yt-dlp",
                            "title": item.get("title"),
                            "text": item.get("description") or item.get("title"),
                            "language": query_info["language"],
                            "author": item.get("channel") or item.get("uploader"),
                            "url": item.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}",
                            "published_at": published,
                            "engagement": {
                                "likes": item.get("like_count"),
                                "comments": item.get("comment_count"),
                                "views": item.get("view_count"),
                            },
                            "region": query_info["region"],
                            "source_type": "social_video",
                            "evidence_type": "direct_post",
                            "metadata": {"query": query_info["query"], "source_key": f"youtube:{item.get('channel_id') or item.get('channel') or video_id}"},
                        }
                    )
            except Exception as exc:
                errors.append(f"{query_info['query']}: {scrub_url_credentials(exc)}")
        if records:
            self._set_success(result, "youtube", records, "yt-dlp")
            if errors:
                result.coverage["youtube"].status = "TIMEOUT" if "global collection execution budget exceeded" in errors else "DEGRADED"
                result.coverage["youtube"].degraded = True
                result.coverage["youtube"].failure_reason = "; ".join(errors[:3])[:500]
        else:
            self._set_failure(result, "youtube", "; ".join(errors[:3]) or "YouTube returned no records")

    def _collect_github(self, result: CollectionResult) -> None:
        if not shutil.which("gh"):
            self._set_failure(result, "github", "gh CLI is not installed", queried=False)
            return
        records: list[dict[str, Any]] = []
        errors: list[str] = []
        day = (result.retrieved_at_utc - dt.timedelta(hours=24)).date().isoformat()
        for term in ("AI", "security", "outage", "regulation", "climate", "market"):
            if self._expired():
                errors.append("global collection execution budget exceeded")
                break
            try:
                timeout = max(1, min(_COMMAND_TIMEOUT_SECONDS, int(self._remaining())))
                raw = _run_command(
                    [
                        "gh", "search", "issues", f"{term} created:>={day}",
                        "--sort", "updated", "--order", "desc", "--limit", "20",
                        "--json", "title,url,repository,author,createdAt,updatedAt,commentsCount,state",
                    ],
                    timeout=timeout,
                )
                payload = json.loads(raw)
                for item in payload if isinstance(payload, list) else []:
                    repo = item.get("repository") or {}
                    author = item.get("author") or {}
                    created = parse_datetime(item.get("createdAt"))
                    records.append(
                        {
                            "platform": "github",
                            "backend": "gh CLI",
                            "title": item.get("title") or "",
                            "text": item.get("title") or "",
                            "language": "en",
                            "author": author.get("login") if isinstance(author, dict) else author,
                            "url": item.get("url") or "",
                            "published_at": created,
                            "engagement": {"comments": item.get("commentsCount")},
                            "region": "global",
                            "source_type": "developer_community",
                            "evidence_type": "direct_post",
                            "metadata": {
                                "repository": repo.get("nameWithOwner") if isinstance(repo, dict) else repo,
                                "updated_at": item.get("updatedAt"),
                                "source_key": f"github:{repo.get('nameWithOwner') if isinstance(repo, dict) else repo}",
                                "query": term,
                            },
                        }
                    )
            except Exception as exc:
                errors.append(f"{term}: {scrub_url_credentials(exc)}")
        if records:
            self._set_success(result, "github", records, "gh CLI")
            if errors:
                result.coverage["github"].status = "TIMEOUT" if "global collection execution budget exceeded" in errors else "DEGRADED"
                result.coverage["github"].degraded = True
                result.coverage["github"].failure_reason = "; ".join(errors[:3])[:500]
        else:
            self._set_failure(result, "github", "; ".join(errors[:3]) or "GitHub search returned no records")

    def _mark_unavailable(self, result: CollectionResult) -> None:
        for platform in self.DISCOVERY_PLATFORMS:
            coverage = result.coverage[platform]
            if coverage.status == "TIMEOUT":
                continue
            if coverage.queried:
                continue
            if platform == "web":
                coverage.status = "NOT QUERIED"
                coverage.failure_reason = "Jina Reader is read-only; this run uses it only for optional URL verification."
                continue
            check = result.doctor.get(platform, {})
            message = str(check.get("message") or "no usable backend reported by doctor")
            status = "AUTH REQUIRED" if any(token in message.casefold() for token in ("cookie", "login", "登录", "auth")) else "UNAVAILABLE"
            self._set_failure(result, platform, message, queried=False, status=status)


def serialize_coverage(coverage: dict[str, Coverage]) -> list[dict[str, Any]]:
    return [coverage[name].to_dict() for name in coverage]

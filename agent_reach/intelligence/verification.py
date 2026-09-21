# -*- coding: utf-8 -*-
"""Evidence labels, rumour flags, and conservative coordination warnings."""

from __future__ import annotations

import re
import time
from collections import Counter
from typing import Any
from urllib.parse import urlsplit

from .schema import Signal
from .scoring import is_social_signal

_PRIMARY_DOMAINS = {
    "gov", "gov.sg", "gov.uk", "who.int", "un.org", "nasa.gov", "noaa.gov",
    "fda.gov", "europa.eu", "白宫.gov", "openai.com", "anthropic.com", "blog.google",
    "microsoft.com", "apple.com", "nvidia.com", "tesla.com",
}
_REPUTABLE_DOMAINS = {
    "reuters.com", "apnews.com", "bbc.com", "bbc.co.uk", "theguardian.com", "nytimes.com",
    "washingtonpost.com", "ft.com", "bloomberg.com", "wsj.com", "cnbc.com", "aljazeera.com",
    "channelnewsasia.com", "scmp.com", "nikkei.com", "techcrunch.com", "theverge.com",
    "nature.com", "science.org", "arstechnica.com", "wired.com", "npr.org",
}
_RUMOUR_MARKERS = (
    "rumor", "rumour", "unverified", "alleged", "claim", "screenshot", "fake", "hoax",
    "未经证实", "传闻", "疑似", "据称", "うわさ", "未確認", "소문", "미확인",
)
_CONTRADICTION_MARKERS = ("denied", "deny", "false", "not true", "debunk", "否认", "辟谣", "否定")


def _domain(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").casefold().removeprefix("www.")
    except ValueError:
        return ""


def _is_domain_or_subdomain(domain: str, candidates: set[str]) -> bool:
    return any(domain == candidate or domain.endswith("." + candidate) for candidate in candidates)


def _url_status_from_error(error: Exception) -> str:
    message = str(error).casefold()
    if any(marker in message for marker in ("401", "403", "login", "auth", "sign in", "cookie")):
        return "URL_AUTH_REQUIRED"
    if any(marker in message for marker in ("404", "410", "not found", "deleted")):
        return "URL_DELETED"
    if any(marker in message for marker in ("timeout", "timed out", "unreachable", "connection")):
        return "URL_INACCESSIBLE"
    return "URL_UNKNOWN"


def _title_key(signal: Signal) -> str:
    text = signal.english_translation or signal.title or signal.original_text
    return re.sub(r"[^a-z0-9\u3400-\u9fff]+", " ", text.casefold()).strip()


def verify_topics(
    topics: list[dict[str, Any]],
    *,
    perform_reads: bool = False,
    max_reads: int | None = None,
    time_budget_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """Assign labels from source evidence; never turn popularity into proof."""

    web_reader = None
    reads_used = 0
    verification_deadline = (
        time.monotonic() + max(0.0, time_budget_seconds)
        if time_budget_seconds is not None
        else None
    )
    if perform_reads:
        try:
            from agent_reach.channels.web import WebChannel

            web_reader = WebChannel()
        except Exception:
            web_reader = None

    for topic in topics:
        signals: list[Signal] = topic["signals"]
        domains = {_domain(signal.url) for signal in signals if signal.url}
        primary = [
            signal
            for signal in signals
            if signal.evidence_type in {"primary", "official"}
            or _is_domain_or_subdomain(_domain(signal.url), _PRIMARY_DOMAINS)
        ]
        reputable = [
            signal
            for signal in signals
            if signal.source_type in {"news", "research", "official"}
            or _is_domain_or_subdomain(_domain(signal.url), _REPUTABLE_DOMAINS)
        ]
        text = " ".join(signal.original_text for signal in signals).casefold()
        rumor_flags = [marker for marker in _RUMOUR_MARKERS if marker in text]
        contradiction_flags = [marker for marker in _CONTRADICTION_MARKERS if marker in text]
        readable_urls: list[str] = []
        social_evidence_urls = [signal.url for signal in signals if is_social_signal(signal) and signal.url]
        factual_evidence_urls = [
            signal.url
            for signal in signals
            if not is_social_signal(signal)
            and signal.url
            and (
                signal.evidence_type in {"primary", "official"}
                or signal.source_type in {"news", "research", "official"}
                or _is_domain_or_subdomain(_domain(signal.url), _REPUTABLE_DOMAINS)
            )
        ]
        url_statuses: dict[str, str] = {}
        if (
            web_reader is not None
            and (max_reads is None or reads_used < max_reads)
            and (verification_deadline is None or time.monotonic() < verification_deadline)
        ):
            # Read the current source, with one fallback only when the first
            # URL is inaccessible. This bounds the daily verifier while still
            # giving a topic a chance to retain a usable evidence URL.
            preferred = factual_evidence_urls or social_evidence_urls or [signal.url for signal in signals if signal.url]
            candidate_by_url = {signal.url: signal for signal in signals if signal.url}
            candidates = [candidate_by_url[url] for url in preferred if url in candidate_by_url][:1]
            for signal in candidates:
                if max_reads is not None and reads_used >= max_reads:
                    break
                if not signal.url or not signal.url.startswith(("http://", "https://")):
                    continue
                reads_used += 1
                try:
                    content = web_reader.read_bounded(signal.url)
                except Exception as exc:
                    url_statuses[signal.signal_id] = _url_status_from_error(exc)
                    continue
                if content and len(content.strip()) >= 80:
                    readable_urls.append(signal.url)
                    url_statuses[signal.signal_id] = "URL_OK"
                else:
                    url_statuses[signal.signal_id] = "URL_INACCESSIBLE"
                if readable_urls:
                    break
        if contradiction_flags and rumor_flags:
            label = "DISPUTED"
        elif primary and len(domains) >= 2:
            label = "VERIFIED"
        elif primary or len(reputable) >= 2 or len(domains) >= 2:
            label = "PARTIALLY VERIFIED"
        else:
            label = "UNVERIFIED"

        title_counts = Counter(_title_key(signal) for signal in signals if _title_key(signal))
        max_identical = max(title_counts.values(), default=0)
        unique_title_sources = len({
            signal.metadata.get("source_key") or _domain(signal.url) or signal.author or signal.platform
            for signal in signals
        })
        if max_identical >= 5 and unique_title_sources <= 2:
            coordination = "STRONG COORDINATION SIGNAL"
            coordination_reason = "Five or more signals share near-identical wording across two or fewer sources."
        elif max_identical >= 3 and unique_title_sources <= 3:
            coordination = "POSSIBLE COORDINATION"
            coordination_reason = "Repeated wording is concentrated among a small number of sources."
        else:
            coordination = "NORMAL"
            coordination_reason = "No unusual identical-wording concentration detected by the deterministic check."

        topic["verification"] = label
        topic["verification_notes"] = {
            "primary_source_count": len(primary),
            "reputable_source_count": len(reputable),
            "domain_count": len(domains),
            "rumour_flags": rumor_flags,
            "contradiction_flags": contradiction_flags,
            "readable_source_urls": readable_urls,
            "social_buzz_evidence_urls": social_evidence_urls,
            "factual_verification_evidence_urls": factual_evidence_urls,
            "url_statuses": url_statuses,
        }
        topic["rumour_flag"] = bool(rumor_flags)
        topic["coordination_signal"] = coordination
        topic["coordination_reason"] = coordination_reason
    return topics

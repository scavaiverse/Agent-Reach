# -*- coding: utf-8 -*-
"""Human-readable report rendering with explicit evidence and limitations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .collector import Coverage
from .schema import Signal, iso_sgt, iso_utc
from .scoring import is_social_signal


def _signal_excerpt(signal: Signal) -> str:
    text = signal.english_translation or signal.title or signal.original_text
    return " ".join(text.split())[:280]


def _display(value: Any) -> str:
    return str(value) if value is not None else "NULL"


def _summary(topic: dict[str, Any]) -> str:
    signals: list[Signal] = topic["signals"]
    return f"{topic['canonical_title']} — {len(signals)} accepted signal(s) were collected from {topic['platform_count']} platform(s)."


def _momentum(topic: dict[str, Any]) -> str:
    signals: list[Signal] = topic["signals"]
    metric_count = sum(
        1
        for signal in signals
        if any(value is not None for value in signal.engagement.to_dict().values())
    )
    return (
        f"{len(signals)} accepted signal(s), {topic['independent_source_count']} independent source(s), "
        f"and {metric_count} signal(s) with platform-provided engagement metrics. "
        "Raw metrics were not compared linearly across platforms."
    )


def _what_changed(topic: dict[str, Any]) -> str:
    first = topic.get("first_detected_utc")
    latest = topic.get("latest_signal_utc")
    if first and latest:
        return (
            f"New source signals span {iso_utc(first)} to {iso_utc(latest)}. "
            "The stored event time is NULL unless a source supplied a distinct event timestamp."
        )
    return "The topic was detected in the requested window, but a complete source timestamp range was not available."


def _why_matters(topic: dict[str, Any]) -> str:
    score = topic["consequence_score"]
    if score >= 80:
        return "The consequence model flags this as potentially material to public safety, policy, markets, security, or other real-world outcomes; verify the primary source before acting."
    if score >= 60:
        return "The consequence model flags a meaningful technology, economic, scientific, or institutional effect beyond simple audience popularity."
    return "The available evidence shows attention or novelty, but the current consequence signal is limited; it should not be treated as equally important to a public-safety or policy event."


def _watch_next(topic: dict[str, Any]) -> str:
    verification = topic.get("verification")
    if verification in {"UNVERIFIED", "DISPUTED"}:
        return "Watch for a direct original post, official statement, or independent reputable report; do not repeat the claim as established fact."
    return "Watch for new primary-source updates, a change in the latest-signal timestamp, and independent discussion on an additional platform."


def render_report(
    *,
    topics: list[dict[str, Any]],
    coverage: dict[str, Coverage],
    diversity: dict[str, Any],
    connections: list[dict[str, Any]],
    retrieved_at_utc: datetime,
    window_hours: float,
    raw_signal_count: int,
    deduplicated_signal_count: int,
    duplicates_removed: int,
    rejected_signal_count: int,
) -> str:
    lines = [
        "# Cross-Platform Live Intelligence",
        "",
        f"Retrieval time UTC: {iso_utc(retrieved_at_utc)}",
        f"Retrieval time SGT: {iso_sgt(retrieved_at_utc)}",
        f"Window: last {window_hours:g} hours; signals outside the window or without publication timestamps were rejected from ranking.",
        f"Raw signals: {raw_signal_count}; post-dedup: {deduplicated_signal_count}; duplicates removed: {duplicates_removed}; rejected from ranking: {rejected_signal_count}",
        "",
        "## TOP SOCIAL BUZZ TOPICS",
        "",
    ]
    if not topics:
        lines.append("No topics had usable publication timestamps inside the requested window.")
    for index, topic in enumerate(topics, 1):
        signals: list[Signal] = topic["signals"]
        lines.extend(
            [
                f"# {index}. {topic['canonical_title']}",
                "",
                _summary(topic),
                "",
                f"SOCIAL BUZZ SCORE: {topic.get('social_buzz_score', 0)}/100",
                f"TREND STATUS: {topic.get('momentum_status', 'NEW')}",
                f"FIRST DETECTED:\nUTC: {iso_utc(topic.get('first_detected_utc'))}\nSGT: {iso_sgt(topic.get('first_detected_utc'))}",
                f"LATEST SIGNAL:\nUTC: {iso_utc(topic.get('latest_signal_utc'))}\nSGT: {iso_sgt(topic.get('latest_signal_utc'))}",
                f"EVENT TIME:\nUTC: {_display(iso_utc(topic.get('event_time_utc')))}\nSGT: {_display(iso_sgt(topic.get('event_time_utc')))}",
                f"PLATFORMS: {' / '.join(topic['platforms'])}",
                f"PLATFORM COUNT: {topic['platform_count']}",
                f"SOCIAL PLATFORMS: {' / '.join(topic.get('social_platforms', [])) or 'NONE'}",
                f"SOCIAL PLATFORM COUNT: {topic.get('social_platform_count', 0)}",
                f"RAW SIGNALS: {topic.get('raw_signal_count', len(signals))}",
                f"INDEPENDENT SOURCES: {topic['independent_source_count']}",
                f"INDEPENDENT SOCIAL SOURCES/CREATORS: {topic.get('social_independent_source_count', 0)}",
                f"LANGUAGES: {', '.join(topic['languages']) or 'unknown'}",
                f"REGIONS: {', '.join(topic['regions']) or 'unknown'}",
                f"WHY IT IS BUZZING: {topic.get('social_signal_count', 0)} social signal(s), {topic.get('social_platform_count', 0)} platform(s), and a Social Buzz Score of {topic.get('social_buzz_score', 0)}/100.",
                f"WHAT PEOPLE ARE SAYING: {_signal_excerpt(signals[0]) if signals else 'No retained source text.'}",
                f"WHY NOW: {topic.get('social_buzz_components', {}).get('momentum_velocity', {}).get('basis', 'current-window social signal density')}",
                f"WHAT CHANGED: {_what_changed(topic)}",
                f"ENGAGEMENT/MOMENTUM: {_momentum(topic)}",
                f"MOMENTUM STATUS: {topic.get('momentum_status', 'NEW')} (delta {topic.get('signal_count_delta', 0)} vs previous stored run)",
                f"VIRALITY SCORE: {topic['virality_score']}/100",
                f"CONSEQUENCE SCORE: {topic['consequence_score']}/100",
                f"IMPORTANCE SCORE: {topic['importance_score']}/100",
                f"VERIFICATION: {topic.get('verification', 'UNVERIFIED')}",
                f"COORDINATION SIGNAL: {topic.get('coordination_signal', 'NORMAL')} — {topic.get('coordination_reason', '')}",
                f"RUMOUR FLAG: {'YES' if topic.get('rumour_flag') else 'NO'}",
                "",
                "WHY IT MATTERS:",
                _why_matters(topic),
                "",
                "WHAT TO WATCH NEXT:",
                _watch_next(topic),
                "",
                "SOCIAL BUZZ EVIDENCE:",
            ]
        )
        seen_urls: set[str] = set()
        for signal in sorted(
            (item for item in signals if is_social_signal(item)),
            key=lambda item: item.published_at_utc or item.retrieved_at_utc,
            reverse=True,
        ):
            if not signal.url or signal.url in seen_urls:
                continue
            seen_urls.add(signal.url)
            lines.append(f"- {signal.url} ({signal.platform} / {signal.backend}) — {_signal_excerpt(signal)}")
        if not seen_urls:
            lines.append("- No social source URL was returned; this is not a social-buzz publication candidate.")
        lines.append("")
        lines.append("FACTUAL VERIFICATION SOURCES:")
        verification_urls: set[str] = set()
        for signal in sorted(
            (item for item in signals if not is_social_signal(item)),
            key=lambda item: item.published_at_utc or item.retrieved_at_utc,
            reverse=True,
        ):
            if not signal.url or signal.url in verification_urls:
                continue
            verification_urls.add(signal.url)
            lines.append(f"- {signal.url} ({signal.platform} / {signal.backend}) — {_signal_excerpt(signal)}")
        if not verification_urls:
            lines.append("- None retained; the discussion remains eligible but the underlying claim is not independently corroborated.")
        lines.append("")

    lines.extend(["## CONNECTION DISCOVERY", ""])
    if connections:
        for connection in connections:
            lines.append(
                f"- {connection['label']}: {connection['from_topic']} ↔ {connection['to_topic']} — "
                f"shared entities: {', '.join(connection['shared_entities'])}; {connection['basis']}."
            )
    else:
        lines.append("NO VERIFIED CONNECTION: no deterministic shared-entity relationship was strong enough to report between the ranked topics.")
    lines.extend(["", "## PLATFORM COVERAGE", "", "Platform | Backend | Queried? | Results | Status", "--- | --- | --- | ---: | ---"])
    for platform, item in coverage.items():
        backend = item.backend or "NULL"
        queried = "YES" if item.queried else "NO"
        status = item.status
        if item.failure_reason:
            status += f" — {item.failure_reason}"
        lines.append(f"{platform} | {backend} | {queried} | {item.results} | {status}")
    lines.extend(
        [
            "",
            "## DIVERSITY REPORT",
            "",
            f"Successfully queried platforms: {diversity['successfully_queried_platforms']}",
            f"Raw signals: {diversity['raw_signal_count']}",
            f"Post-dedup signals: {diversity.get('deduplicated_signal_count', 'NULL')}",
            f"Duplicates removed: {diversity.get('duplicates_removed', 0)}",
            f"Unique topics before ranking: {diversity['unique_topics_before_ranking']}",
            f"Languages: {diversity['language_count']}",
            f"Countries/regions: {diversity['region_count']}",
            f"Independent sources: {diversity['independent_source_count']}",
            f"Oldest accepted signal: {diversity['oldest_accepted_signal_utc']} UTC / {diversity['oldest_accepted_signal_sgt']} SGT",
            f"Newest accepted signal: {diversity['newest_signal_utc']} UTC / {diversity['newest_signal_sgt']} SGT",
        ]
    )
    return "\n".join(lines)

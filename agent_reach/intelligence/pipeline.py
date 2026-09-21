# -*- coding: utf-8 -*-
"""Pipeline orchestration and safe JSON persistence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .cluster import cluster_signals
from .collector import LiveCollector, serialize_coverage
from .connections import discover_connections
from .investibles import selected_investibles
from .momentum import add_momentum
from .normalize import apply_time_window, deduplicate_signals, normalize_records
from .otr_mapping import map_topics_to_otr
from .report import render_report
from .schema import Signal, iso_sgt, iso_utc, parse_datetime
from .scoring import is_social_signal, score_clusters
from .sentiment import (
    build_investible_sentiments,
    collect_investible_sentiments,
    render_sentiment_report,
)
from .source_ledger import build_source_ledger
from .verification import verify_topics


@dataclass
class IntelligenceRun:
    data: dict[str, Any]
    report: str
    raw_signals: list[dict[str, Any]]
    rejected_signals: list[dict[str, Any]]
    topic_clusters: list[dict[str, Any]]
    source_ledger: list[dict[str, Any]]
    public_payload: dict[str, Any]
    sentiment_retrieval_ledger: list[dict[str, Any]]
    sentiment_ledger: list[dict[str, Any]]
    sentiment_posts: list[dict[str, Any]]

    def write(self, output_dir: str | Path) -> None:
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        _write_json(target / "latest_cross_platform_intelligence.json", self.data)
        _write_json(
            target / "raw_social_signals.json",
            {
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "signals": self.raw_signals,
            },
        )
        _write_json(
            target / "rejected_social_signals.json",
            {
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "signals": self.rejected_signals,
            },
        )
        _write_json(
            target / "topic_clusters.json",
            {
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "clusters": self.topic_clusters,
            },
        )
        _write_json(target / "social_topic_clusters.json", {"retrieval_time_utc": self.data["retrieval_time_utc"], "clusters": self.topic_clusters})
        _write_json(target / "source_ledger.json", {"sources": self.source_ledger})
        _write_json(target / "platform_coverage.json", {"coverage": self.data["platform_coverage"]})
        _write_json(target / "verification_ledger.json", {"topics": _verification_ledger(self.data.get("topics", []))})
        _write_json(
            target / "sentiment_retrieval_ledger.json",
            {
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "live_validated_platforms": self.data.get("sentiment_live_validated_platforms", []),
                "rows": self.sentiment_retrieval_ledger,
            },
        )
        _write_json(
            target / "sentiment_ledger.json",
            {
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "rows": self.sentiment_ledger,
            },
        )
        _write_json(
            target / "sentiment_posts.json",
            {
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "posts": self.sentiment_posts,
            },
        )
        _write_json(
            target / "otr_route_mappings.json",
            {
                "mappings": self.data.get("otr_route_mappings", []),
                "routes_with_chatter": self.data.get("otr_routes_with_chatter", []),
                "routes_without_chatter": self.data.get("otr_routes_without_chatter", []),
            },
        )
        _write_json(target / "otr_research_escalation_queue.json", {"topics": self.data.get("unmatched_high_buzz_topics", [])})
        _write_json(
            target / "top_30_social_buzz.json",
            {
                "schema_version": "otr-social-buzz-1",
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "window_hours": self.data["window_hours"],
                "topics": self.public_payload.get("topics", []),
                "unmatched_high_buzz_topics": self.data.get("unmatched_high_buzz_topics", []),
            },
        )
        _write_json(target / "social_chatter_public.json", self.public_payload)
        _atomic_write_text(target / "latest_cross_platform_intelligence.md", self.report + "\n")
        history_dir = target / "history"
        _write_json(
            history_dir / f"{self.data['run_id']}.json",
            {
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "retrieval_time_sgt": self.data["retrieval_time_sgt"],
                "window_hours": self.data["window_hours"],
                "topics": self.data.get("topics", []),
                "investible_sentiments": self.data.get("investible_sentiments", []),
            },
        )
        momentum_path = target / "momentum_history.json"
        prior_history: list[dict[str, Any]] = []
        if momentum_path.exists():
            try:
                loaded = json.loads(momentum_path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    prior_history = [item for item in loaded if isinstance(item, dict)]
            except (OSError, json.JSONDecodeError):
                prior_history = []
        entry = {
            "run_id": self.data["run_id"],
            "retrieval_time_utc": self.data["retrieval_time_utc"],
            "topics": [
                {
                    "topic_id": topic.get("topic_id"),
                    "canonical_title": topic.get("canonical_title"),
                    "social_buzz_score": topic.get("social_buzz_score", 0),
                    "momentum_status": topic.get("momentum_status", "NEW"),
                    "social_signal_count": topic.get("social_signal_count", 0),
                }
                for topic in self.data.get("topics", [])
            ],
        }
        prior_history = [item for item in prior_history if item.get("run_id") != entry["run_id"]]
        _write_json(momentum_path, prior_history[-29:] + [entry])


def _write_json(path: Path, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default) + "\n"
    _atomic_write_text(path, text)


def _atomic_write_text(path: Path, text: str) -> None:
    """Write beside the target, then atomically promote the complete file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, Signal):
        return value.to_dict()
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def _topic_json(topic: dict[str, Any]) -> dict[str, Any]:
    output = dict(topic)
    output["signals"] = [signal.to_dict() for signal in topic["signals"]]
    output["first_detected_utc"] = iso_utc(topic.get("first_detected_utc"))
    output["latest_signal_utc"] = iso_utc(topic.get("latest_signal_utc"))
    output["event_time_utc"] = iso_utc(topic.get("event_time_utc"))
    output["first_detected_sgt"] = iso_sgt(topic.get("first_detected_utc"))
    output["latest_signal_sgt"] = iso_sgt(topic.get("latest_signal_utc"))
    output["event_time_sgt"] = iso_sgt(topic.get("event_time_utc"))
    return output


def _public_signal(signal: Signal) -> dict[str, Any]:
    """Keep the public site payload evidence-focused and credential-free."""

    return {
        "signal_id": signal.signal_id,
        "platform": signal.platform,
        "backend": signal.backend,
        "title": signal.title,
        "original_text": signal.original_text,
        "english_translation": signal.english_translation,
        "language": signal.language,
        "author": signal.author,
        "url": signal.url,
        "published_at_utc": iso_utc(signal.published_at_utc),
        "published_at_sgt": iso_sgt(signal.published_at_utc),
        "retrieved_at_utc": iso_utc(signal.retrieved_at_utc),
        "retrieved_at_sgt": iso_sgt(signal.retrieved_at_utc),
        "event_time_utc": iso_utc(signal.event_time_utc),
        "event_time_sgt": iso_sgt(signal.event_time_utc),
        "engagement": signal.engagement.to_dict(),
        "region": signal.region,
        "source_type": signal.source_type,
        "evidence_type": signal.evidence_type,
        "evidence_lane": "social_buzz" if is_social_signal(signal) else "factual_verification",
    }


def _verification_ledger(topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, topic in enumerate(topics, 1):
        signals = topic.get("signals", [])
        social_urls = [item.get("url") for item in signals if is_social_signal(_signal_from_dict(item)) and item.get("url")]
        verification_urls = [
            item.get("url")
            for item in signals
            if not is_social_signal(_signal_from_dict(item)) and item.get("url")
        ]
        notes = topic.get("verification_notes", {})
        rows.append(
            {
                "rank": rank,
                "topic_id": topic.get("topic_id"),
                "verification": topic.get("verification", "UNVERIFIED"),
                "social_buzz_evidence_urls": social_urls,
                "factual_verification_evidence_urls": sorted(set(verification_urls + notes.get("readable_source_urls", []))),
                "url_statuses": notes.get("url_statuses", {}),
            }
        )
    return rows


def _signal_from_dict(item: dict[str, Any]) -> Signal:
    """Create the small Signal view needed for evidence-lane classification."""

    return Signal(
        platform=str(item.get("platform") or "unknown"),
        backend=str(item.get("backend") or "unknown"),
        title=str(item.get("title") or ""),
        original_text=str(item.get("original_text") or ""),
        language=str(item.get("language") or "unknown"),
        url=str(item.get("url") or ""),
        published_at_utc=parse_datetime(item.get("published_at_utc")),
        retrieved_at_utc=parse_datetime(item.get("retrieved_at_utc")) or datetime.now(timezone.utc),
        source_type=str(item.get("source_type") or "unknown"),
    )


def _public_topic(topic: dict[str, Any]) -> dict[str, Any]:
    consequence = int(topic.get("consequence_score", 0))
    verification = topic.get("verification", "UNVERIFIED")
    if verification in {"UNVERIFIED", "DISPUTED"}:
        watch_next = "Watch for a direct original post, official statement, or independent reputable report before treating this signal as established fact."
    else:
        watch_next = "Watch for a new primary-source update, an additional independent platform, and a changed latest-signal timestamp."
    if consequence >= 80:
        why_it_matters = "The consequence model flags a potentially material public-safety, policy, market, security, scientific, or institutional effect. Verify before acting."
    elif consequence >= 60:
        why_it_matters = "The available evidence suggests an effect beyond audience attention, but the social layer does not establish causality or a forecast."
    else:
        why_it_matters = "The available evidence shows attention or novelty; its real-world consequence remains limited or uncertain."
    first = iso_utc(topic.get("first_detected_utc"))
    latest = iso_utc(topic.get("latest_signal_utc"))
    social_signals = [signal for signal in topic.get("signals", []) if is_social_signal(signal)]
    verification_signals = [signal for signal in topic.get("signals", []) if not is_social_signal(signal)]
    return {
        "topic_id": topic["topic_id"],
        "title": topic["canonical_title"],
        "summary": f"{len(social_signals)} social signal(s) from {topic.get('social_platform_count', 0)} social platform(s); discussion is evidence of attention, not proof of the underlying claim.",
        "social_buzz_score": topic.get("social_buzz_score", 0),
        "social_buzz_components": topic.get("social_buzz_components", {}),
        "first_detected_utc": first,
        "first_detected_sgt": iso_sgt(topic.get("first_detected_utc")),
        "latest_signal_utc": latest,
        "latest_signal_sgt": iso_sgt(topic.get("latest_signal_utc")),
        "event_time_utc": iso_utc(topic.get("event_time_utc")),
        "event_time_sgt": iso_sgt(topic.get("event_time_utc")),
        "platforms": topic.get("platforms", []),
        "platform_count": topic.get("platform_count", 0),
        "social_platforms": topic.get("social_platforms", []),
        "social_platform_count": topic.get("social_platform_count", 0),
        "raw_signal_count": topic.get("raw_signal_count", len(topic.get("signals", []))),
        "social_signal_count": topic.get("social_signal_count", len(social_signals)),
        "independent_source_count": topic.get("independent_source_count", 0),
        "social_independent_source_count": topic.get("social_independent_source_count", 0),
        "languages": topic.get("languages", []),
        "regions": topic.get("regions", []),
        "social_languages": topic.get("social_languages", []),
        "social_regions": topic.get("social_regions", []),
        "why_trending": f"{len(social_signals)} social signal(s), {topic.get('social_platform_count', 0)} platform(s), and {topic.get('social_independent_source_count', 0)} independent social source(s); ranking is driven by Social Buzz Score.",
        "what_changed": f"Social signals span {first or 'NULL'} to {latest or 'NULL'}; event time remains NULL unless a source supplied one.",
        "engagement_momentum": f"Social Buzz Score is {topic.get('social_buzz_score', 0)}/100; virality is {topic.get('virality_score', 0)}/100 and raw metrics are not compared linearly across platforms.",
        "virality_score": topic.get("virality_score", 0),
        "consequence_score": consequence,
        "importance_score": topic.get("importance_score", 0),
        "score_components": topic.get("score_components", {}),
        "momentum_status": topic.get("momentum_status", "NEW"),
        "previous_signal_count": topic.get("previous_signal_count", 0),
        "signal_count_delta": topic.get("signal_count_delta", 0),
        "current_rank": topic.get("current_rank"),
        "previous_rank": topic.get("previous_rank"),
        "rank_change": topic.get("rank_change"),
        "first_appeared": topic.get("first_appeared"),
        "runs_present": topic.get("runs_present", 1),
        "peak_rank": topic.get("peak_rank"),
        "peak_buzz_score": topic.get("peak_buzz_score", topic.get("social_buzz_score", 0)),
        "verification": verification,
        "discussion_observed": bool(social_signals),
        "what_is_verified": "The existence of this discussion is observable from the retained social/community source signals; the underlying claim may still be unverified.",
        "what_remains_uncertain": "The factual claim, causal interpretation, and forecast remain separate from the measured discussion unless supported by the verification lane.",
        "rumour_flag": bool(topic.get("rumour_flag")),
        "coordination_signal": topic.get("coordination_signal", "NORMAL"),
        "coordination_reason": topic.get("coordination_reason", ""),
        "why_it_matters": why_it_matters,
        "what_to_watch_next": watch_next,
        "social_sources": [_public_signal(signal) for signal in social_signals],
        "verification_sources": [_public_signal(signal) for signal in verification_signals],
        "sources": [_public_signal(signal) for signal in topic.get("signals", [])],
    }


def _public_payload(
    *,
    data: dict[str, Any],
    topics: list[dict[str, Any]],
    coverage: dict,
    source_ledger: list[dict[str, Any]],
    investible_sentiments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Create the safe public hand-off consumed by the OnTheRice site."""

    return {
        "schema_version": "otr-social-chatter-1",
        "run_id": data["run_id"],
        "retrieval_time_utc": data["retrieval_time_utc"],
        "retrieval_time_sgt": data["retrieval_time_sgt"],
        "window_hours": data["window_hours"],
        "raw_signal_count": data["raw_signal_count"],
        "raw_social_signal_count": data.get("diversity", {}).get("raw_social_signal_count", 0),
        "deduplicated_signal_count": data.get("deduplicated_signal_count", data["raw_signal_count"]),
        "post_dedup_social_signal_count": data.get("diversity", {}).get("post_dedup_social_signal_count", 0),
        "duplicates_removed": data.get("duplicates_removed", 0),
        "accepted_signal_count": data["accepted_signal_count"],
        "top_n_requested": data["top_n_requested"],
        "ranking_basis": "SOCIAL_BUZZ_SCORE; verification annotates candidates after discovery and does not remove unverified discussion.",
        "unique_topic_count": data["unique_topic_count"],
        "qualifying_social_cluster_count": data.get("qualifying_social_cluster_count", 0),
        "source_count": len(source_ledger),
        "disclaimer": "Social media chatter is a signal of discussion, not proof, prediction, or causal evidence. OTR verification labels remain explicit.",
        "platform_coverage": [
            {
                "platform": item.platform,
                "backend": item.backend,
                "queried": item.queried,
                "results": item.results,
                "status": item.status,
                "degraded": item.degraded,
            }
            for item in coverage.values()
        ],
        "topics": [_public_topic(topic) for topic in topics],
        "investible_sentiments": investible_sentiments,
        "sentiment_live_validated_platforms": data.get("sentiment_live_validated_platforms", []),
        "sentiment_retrieval_summary": {
            "row_count": data.get("sentiment_retrieval_row_count", 0),
            "exhaustive_row_count": data.get("sentiment_exhaustive_row_count", 0),
            "coverage_rule": (
                "COVERAGE COMPLETE only when every live-validated platform reaches "
                "EXHAUSTED or WINDOW_BOUNDARY_REACHED for that investible."
            ),
        },
        "unmatched_high_buzz_topics": data.get("unmatched_high_buzz_topics", []),
        "otr_route_mappings": data.get("otr_route_mappings", []),
        "otr_routes_with_chatter": data.get("otr_routes_with_chatter", []),
        "otr_routes_without_chatter": data.get("otr_routes_without_chatter", []),
    }


def _diversity(
    *,
    coverage: dict,
    signals: list[Signal],
    deduplicated: list[Signal],
    duplicates_removed: int,
    accepted: list[Signal],
    clusters: list,
) -> dict[str, Any]:
    times = [signal.published_at_utc for signal in accepted if signal.published_at_utc]
    sources = {
        str(signal.metadata.get("source_key") or signal.author or signal.url or signal.platform).casefold()
        for signal in accepted
    }
    successful = sum(1 for item in coverage.values() if item.queried and item.results > 0 and item.status in {"OK", "DEGRADED"})
    social_accepted = [signal for signal in accepted if is_social_signal(signal)]
    social_deduplicated = [signal for signal in deduplicated if is_social_signal(signal)]
    social_clusters = [cluster for cluster in clusters if any(is_social_signal(signal) for signal in cluster.signals)]
    oldest = min(times) if times else None
    newest = max(times) if times else None
    return {
        "successfully_queried_platforms": successful,
        "successfully_queried_social_platforms": sum(
            1
            for item in coverage.values()
            if item.platform in {"twitter", "reddit", "youtube", "facebook", "instagram", "xiaohongshu", "bilibili", "linkedin", "v2ex", "xueqiu", "xiaoyuzhou", "boss"}
            and item.queried
            and item.results > 0
            and item.status in {"OK", "DEGRADED"}
        ),
        "raw_signal_count": len(signals),
        "raw_social_signal_count": sum(is_social_signal(signal) for signal in signals),
        "deduplicated_signal_count": len(deduplicated),
        "post_dedup_social_signal_count": len(social_deduplicated),
        "duplicates_removed": duplicates_removed,
        "accepted_signal_count": len(accepted),
        "accepted_social_signal_count": len(social_accepted),
        "unique_topics_before_ranking": len(clusters),
        "qualifying_social_clusters": len(social_clusters),
        "language_count": len({signal.language for signal in accepted if signal.language}),
        "region_count": len({signal.region for signal in accepted if signal.region}),
        "independent_source_count": len(sources),
        "oldest_accepted_signal_utc": iso_utc(oldest),
        "oldest_accepted_signal_sgt": iso_sgt(oldest),
        "newest_signal_utc": iso_utc(newest),
        "newest_signal_sgt": iso_sgt(newest),
    }


def run_intelligence(
    *,
    window_hours: float = 24,
    top_n: int = 30,
    multilingual: bool = False,
    verify: bool = False,
    output_dir: str | Path = "output",
    global_timeout_seconds: int = 300,
) -> IntelligenceRun:
    if window_hours <= 0:
        raise ValueError("window must be positive")
    if top_n <= 0:
        raise ValueError("top must be positive")
    pipeline_started = time.monotonic()
    sentiment_reserved_budget_seconds = max(1, min(120, int(global_timeout_seconds * 0.35)))
    verification_budget_seconds = max(1, min(60, int(global_timeout_seconds * 0.2)))
    collection_budget_seconds = max(
        1,
        int(global_timeout_seconds)
        - sentiment_reserved_budget_seconds
        - verification_budget_seconds,
    )
    collector = LiveCollector(
        multilingual=multilingual,
        global_timeout_seconds=collection_budget_seconds,
    )
    collection = collector.collect()
    retrieved = collection.retrieved_at_utc
    target = Path(output_dir)
    previous_data: dict[str, Any] | None = None
    previous_path = target / "latest_cross_platform_intelligence.json"
    if previous_path.exists():
        try:
            loaded = json.loads(previous_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                previous_data = loaded
        except (OSError, json.JSONDecodeError):
            previous_data = None
    signals = normalize_records(
        collection.records,
        retrieved_at_utc=retrieved,
        multilingual=multilingual,
    )
    deduplicated, duplicates = deduplicate_signals(signals)
    accepted, rejected = apply_time_window(
        deduplicated,
        now_utc=retrieved,
        window_hours=window_hours,
    )
    rejected = duplicates + rejected
    clusters = cluster_signals(accepted)
    scored = score_clusters(clusters, retrieved)
    scored = add_momentum(scored, previous_data)
    # Verification reads are deliberately limited to the ranked candidates.
    # The full cluster set can be large; reading two external pages for every
    # low-ranked cluster would make a scheduled run unbounded and would not
    # change the deterministic ranking.
    scored = verify_topics(scored, perform_reads=False)
    buzz_candidates = [topic for topic in scored if topic.get("social_signal_count", 0) > 0]
    if verify:
        verified_top = verify_topics(
            buzz_candidates[:top_n],
            perform_reads=True,
            max_reads=min(top_n, 10),
            time_budget_seconds=verification_budget_seconds,
        )
        verified_by_id = {topic["topic_id"]: topic for topic in verified_top}
        scored = [verified_by_id.get(topic["topic_id"], topic) for topic in scored]
        buzz_candidates = [verified_by_id.get(topic["topic_id"], topic) for topic in buzz_candidates]
    top_topics = buzz_candidates[:top_n]
    connections = discover_connections(top_topics)
    otr_mapping = map_topics_to_otr(top_topics)
    coverage = collection.coverage
    if verify:
        web_coverage = coverage.get("web")
        if web_coverage is not None:
            read_count = sum(len(topic.get("verification_notes", {}).get("readable_source_urls", [])) for topic in top_topics)
            web_coverage.queried = read_count > 0
            web_coverage.results = read_count
            web_coverage.status = "OK" if read_count else "DEGRADED"
            web_coverage.degraded = not bool(read_count)
            if not read_count:
                web_coverage.failure_reason = "verification reader returned no readable source pages"
    sentiment_timeout = max(
        1,
        min(
            180,
            int(global_timeout_seconds - (time.monotonic() - pipeline_started)),
        ),
    )
    sentiment_collection = collect_investible_sentiments(
        investibles=selected_investibles(),
        coverage=coverage,
        existing_signals=accepted,
        retrieved_at_utc=retrieved,
        window_hours=window_hours,
        global_timeout_seconds=sentiment_timeout,
    )
    investible_sentiments, sentiment_ledger, sentiment_posts = build_investible_sentiments(
        investibles=selected_investibles(),
        posts=sentiment_collection.posts,
        retrieval_ledger=sentiment_collection.retrieval_ledger,
        retrieved_at_utc=retrieved,
    )
    diversity = _diversity(
        coverage=coverage,
        signals=signals,
        deduplicated=deduplicated,
        duplicates_removed=len(duplicates),
        accepted=accepted,
        clusters=clusters,
    )
    run_material = (
        "|".join(signal.signal_id for signal in signals)
        + "|"
        + "|".join(post.get("post_id", "") for post in sentiment_collection.posts)
        + "|"
        + (iso_utc(retrieved) or "")
    )
    run_id = "run-" + hashlib.sha256(run_material.encode("utf-8")).hexdigest()[:16]
    for row in sentiment_collection.retrieval_ledger:
        row["run_id"] = run_id
    data = {
        "schema_version": "1.0",
        "run_id": run_id,
        "retrieval_time_utc": iso_utc(retrieved),
        "retrieval_time_sgt": iso_sgt(retrieved),
        "window_hours": window_hours,
        "top_n_requested": top_n,
        "multilingual": multilingual,
        "verification_requested": verify,
        "history_count": (int(previous_data.get("history_count", 0)) + 1) if previous_data else 1,
        "global_collection_timeout_seconds": global_timeout_seconds,
        "collection_budget_seconds": collection_budget_seconds,
        "verification_budget_seconds": verification_budget_seconds,
        "sentiment_reserved_budget_seconds": sentiment_reserved_budget_seconds,
        "sentiment_global_timeout_seconds": sentiment_timeout,
        "collection_started_at_utc": iso_utc(collection.collection_started_at_utc),
        "collection_finished_at_utc": iso_utc(collection.collection_finished_at_utc),
        "collection_runtime_seconds": (
            (collection.collection_finished_at_utc - collection.collection_started_at_utc).total_seconds()
            if collection.collection_finished_at_utc and collection.collection_started_at_utc
            else None
        ),
        "doctor": collection.doctor,
        "platform_coverage": serialize_coverage(coverage),
        "raw_signal_count": len(signals),
        "deduplicated_signal_count": len(deduplicated),
        "duplicates_removed": len(duplicates),
        "accepted_signal_count": len(accepted),
        "rejected_signal_count": len(rejected),
        "unique_topic_count": len(clusters),
        "qualifying_social_cluster_count": sum(
            1 for cluster in clusters if any(is_social_signal(signal) for signal in cluster.signals)
        ),
        "diversity": diversity,
        "connections": connections,
        "otr_route_mappings": otr_mapping["mappings"],
        "otr_routes_with_chatter": otr_mapping["routes_with_chatter"],
        "otr_routes_without_chatter": otr_mapping["routes_without_chatter"],
        "unmatched_high_buzz_topics": otr_mapping["unmatched_high_buzz_topics"],
        "topics": [_topic_json(topic) for topic in top_topics],
        "investible_sentiments": investible_sentiments,
        "sentiment_live_validated_platforms": sentiment_collection.live_validated_platforms,
        "sentiment_retrieval_row_count": len(sentiment_collection.retrieval_ledger),
        "sentiment_exhaustive_row_count": sum(
            bool(row["exhaustive_boolean"]) for row in sentiment_collection.retrieval_ledger
        ),
        "sentiment_collection_started_at_utc": iso_utc(sentiment_collection.started_at_utc),
        "sentiment_collection_finished_at_utc": iso_utc(sentiment_collection.completed_at_utc),
        "sentiment_collection_runtime_seconds": (
            sentiment_collection.completed_at_utc - sentiment_collection.started_at_utc
        ).total_seconds(),
        "ranking": {
            "weights": {
                "cross_platform_spread": 25,
                "momentum_velocity": 20,
                "independent_creator_source_spread": 15,
                "engagement": 15,
                "recency": 10,
                "geographic_spread": 5,
                "language_spread": 5,
                "novelty_emergence": 5,
            },
            "primary_score": "social_buzz_score",
            "verification_is_secondary": True,
            "reference_time_utc": iso_utc(retrieved),
            "deterministic_from_stored_inputs": True,
        },
    }
    source_ledger = build_source_ledger(accepted, scored)
    public_payload = _public_payload(
        data=data,
        topics=top_topics,
        coverage=coverage,
        source_ledger=source_ledger,
        investible_sentiments=investible_sentiments,
    )
    data["source_ledger_count"] = len(source_ledger)
    raw_json = [signal.to_dict() for signal in signals]
    rejected_json = [signal.to_dict() for signal in rejected]
    cluster_json = []
    scored_by_id = {topic["topic_id"]: topic for topic in scored}
    for cluster in clusters:
        topic = scored_by_id.get(cluster.topic_id)
        cluster_json.append(
            {
                "topic_id": cluster.topic_id,
                "canonical_title": cluster.canonical_title,
                "signal_ids": [signal.signal_id for signal in cluster.signals],
                "signals": [signal.to_dict() for signal in cluster.signals],
                "ranking": _topic_json(topic) if topic else None,
            }
        )
    report = render_report(
        topics=top_topics,
        coverage=coverage,
        diversity=diversity,
        connections=connections,
        retrieved_at_utc=retrieved,
        window_hours=window_hours,
        raw_signal_count=len(signals),
        deduplicated_signal_count=len(deduplicated),
        duplicates_removed=len(duplicates),
        rejected_signal_count=len(rejected),
    ) + render_sentiment_report(investible_sentiments, sentiment_collection.retrieval_ledger)
    run = IntelligenceRun(
        data=data,
        report=report,
        raw_signals=raw_json,
        rejected_signals=rejected_json,
        topic_clusters=cluster_json,
        source_ledger=source_ledger,
        public_payload=public_payload,
        sentiment_retrieval_ledger=sentiment_collection.retrieval_ledger,
        sentiment_ledger=sentiment_ledger,
        sentiment_posts=sentiment_posts,
    )
    run.write(output_dir)
    return run


def rerank_stored_signals(
    raw_payload: dict[str, Any],
    *,
    top_n: int = 30,
    verify: bool = False,
) -> list[dict[str, Any]]:
    """Re-run filtering, clustering, and ranking from ``raw_social_signals.json``."""

    raw_signals = raw_payload.get("signals") if isinstance(raw_payload, dict) else raw_payload
    if not isinstance(raw_signals, list):
        raise ValueError("stored raw signal payload must contain a signals list")
    reference = parse_datetime(raw_payload.get("retrieval_time_utc")) if isinstance(raw_payload, dict) else None
    if reference is None:
        reference = max(
            (parse_datetime(item.get("retrieved_at_utc")) for item in raw_signals if isinstance(item, dict)),
            default=datetime.now(timezone.utc),
        )
    records = []
    for item in raw_signals:
        if not isinstance(item, dict):
            continue
        records.append(item)
    signals = normalize_records(records, retrieved_at_utc=reference, multilingual=False)
    deduplicated, _ = deduplicate_signals(signals)
    accepted, _ = apply_time_window(deduplicated, now_utc=reference, window_hours=24)
    topics = score_clusters(cluster_signals(accepted), reference)
    topics = verify_topics(topics, perform_reads=False)
    topics = [topic for topic in topics if topic.get("social_signal_count", 0) > 0]
    if verify:
        verified_top = verify_topics(topics[:top_n], perform_reads=True, max_reads=top_n)
        topics = verified_top + topics[top_n:]
    return [_topic_json(topic) for topic in topics[:top_n]]

# -*- coding: utf-8 -*-
"""Pipeline orchestration and safe JSON persistence."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
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
from .pre_run import scan_live_social_pulse
from .prices import collect_price_receipts, resolve_previous_sentiments
from .report import render_report
from .schema import Signal, iso_sgt, iso_utc, parse_datetime
from .scoring import is_social_signal, score_clusters
from .self_healing import SelfHealingCoordinator
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
        output_target = Path(output_dir)
        output_target.mkdir(parents=True, exist_ok=True)
        target = output_target / ".candidates" / self.data["run_id"]
        if target.exists():
            shutil.rmtree(target)
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
            target / "top_50_social_buzz.json",
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
        _write_json(target / "social_pulse_pre_run_scan.json", self.data.get("pre_run_scan", {}))
        _write_json(target / "investible_price_receipts.json", {"run_id": self.data["run_id"], "receipts": self.data.get("investible_price_receipts", [])})
        _write_json(target / "investible_sentiment_current.json", {"run_id": self.data["run_id"], "retrieved_at_utc": self.data["retrieval_time_utc"], "items": self.data.get("investible_sentiments", [])})
        history_path = target / "investible_sentiment_history.json"
        history = []
        previous_history_path = output_target / "investible_sentiment_history.json"
        if previous_history_path.exists():
            try:
                loaded = json.loads(previous_history_path.read_text(encoding="utf-8"))
                history = loaded if isinstance(loaded, list) else loaded.get("runs", [])
            except (OSError, json.JSONDecodeError):
                history = []
        history = [item for item in history if item.get("run_id") != self.data["run_id"]]
        history.append(self.data.get("investible_sentiment_history_entry", {"run_id": self.data["run_id"], "items": self.data.get("investible_sentiments", [])}))
        _write_json(history_path, history[-90:])
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
        previous_momentum_path = output_target / "momentum_history.json"
        if previous_momentum_path.exists():
            try:
                loaded = json.loads(previous_momentum_path.read_text(encoding="utf-8"))
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
        "trend_status": topic.get("momentum_status", "INSUFFICIENT_HISTORY"),
                    "social_signal_count": topic.get("social_signal_count", 0),
                }
                for topic in self.data.get("topics", [])
            ],
        }
        prior_history = [item for item in prior_history if item.get("run_id") != entry["run_id"]]
        _write_json(momentum_path, prior_history[-29:] + [entry])
        _write_json(
            target / "publication_state.json",
            {
                "status": "READY",
                "run_id": self.data["run_id"],
                "retrieval_time_utc": self.data["retrieval_time_utc"],
                "schema_version": "otr-social-pulse-publication-v1",
            },
        )
        _validate_candidate_publication(target, self.data["run_id"])
        _promote_candidate_publication(target, output_target)


def _write_json(path: Path, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default) + "\n"
    _atomic_write_text(path, text)


def _validate_candidate_publication(candidate: Path, run_id: str) -> None:
    required = (
        "latest_cross_platform_intelligence.json",
        "social_chatter_public.json",
        "top_50_social_buzz.json",
        "raw_social_signals.json",
        "social_topic_clusters.json",
        "source_ledger.json",
        "platform_coverage.json",
        "verification_ledger.json",
        "publication_state.json",
    )
    for name in required:
        path = candidate / name
        if not path.is_file():
            raise RuntimeError(f"candidate publication missing {name}")
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"candidate publication has invalid {name}: {exc}") from exc
    for name in ("latest_cross_platform_intelligence.json", "top_50_social_buzz.json", "social_chatter_public.json"):
        payload = json.loads((candidate / name).read_text(encoding="utf-8"))
        if payload.get("run_id") != run_id:
            raise RuntimeError(f"candidate publication run id mismatch in {name}")


def _promote_candidate_publication(candidate: Path, output_target: Path) -> None:
    """Promote a validated candidate and restore the prior set on failure."""

    backup = output_target / ".rollback" / candidate.name
    if backup.exists():
        shutil.rmtree(backup)
    backup.mkdir(parents=True, exist_ok=True)
    promoted: list[Path] = []
    backups: list[tuple[Path, Path]] = []
    files = sorted(
        (path for path in candidate.rglob("*") if path.is_file()),
        key=lambda path: (path.name == "publication_state.json", str(path)),
    )
    try:
        for source in files:
            relative = source.relative_to(candidate)
            destination = output_target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                old = backup / relative
                old.parent.mkdir(parents=True, exist_ok=True)
                os.replace(destination, old)
                backups.append((old, destination))
            os.replace(source, destination)
            promoted.append(destination)
    except Exception:
        for destination in reversed(promoted):
            destination.unlink(missing_ok=True)
        for old, destination in reversed(backups):
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(old, destination)
        raise
    finally:
        if candidate.exists():
            shutil.rmtree(candidate)
        if backup.exists():
            shutil.rmtree(backup)


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
        "original_title": topic["canonical_title"],
        "english_translation": next((signal.english_translation for signal in social_signals if signal.english_translation), None),
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
        "trend_status": topic.get("momentum_status", "INSUFFICIENT_HISTORY"),
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
    previous_investible_sentiments: list[dict[str, Any]] | None = None,
    previous_price_resolution: list[dict[str, Any]] | None = None,
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
        "coverage_label": data.get("coverage_label", "TOP 50 FROM CURRENTLY ACCESSIBLE COVERAGE"),
        "coverage_scope": data.get("coverage_scope", "TOP 50 FROM CURRENTLY ACCESSIBLE COVERAGE"),
        "coverage_audit": data.get("coverage_audit", {}),
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
        "previous_investible_sentiments": previous_price_resolution or [],
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
    top_n: int = 50,
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
    target = Path(output_dir)
    coordinator = SelfHealingCoordinator.begin(target, budget_seconds=global_timeout_seconds)
    pre_run_scan, pre_run_state = coordinator.run_optional(
        "pre_run_scan",
        lambda: scan_live_social_pulse(target),
        {"scan_status": "LIVE_PAGE_SCAN_UNAVAILABLE", "errors": ["pre-run scan did not complete"]},
    )
    coordinator.checkpoint(
        "preflight",
        {"pre_run_scan": pre_run_scan, "pre_run_state": pre_run_state, "top_n": top_n, "window_hours": window_hours},
    )
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
    try:
        collection = collector.collect()
    except Exception as exc:
        coordinator.repair_attempt("collection", change="preserved last-known-good output and closed the collection stage", outcome=f"{type(exc).__name__}: {exc}"[:500])
        coordinator.finish(status="FAILED", summary={"error": str(exc)[:500]})
        raise
    retrieved = collection.retrieved_at_utc
    coordinator.checkpoint(
        "collection",
        {"record_count": len(collection.records), "coverage": serialize_coverage(collection.coverage)},
    )
    for platform, coverage_item in collection.coverage.items():
        coordinator.checkpoint(
            f"platform-{platform}",
            {
                "platform": platform,
                "status": coverage_item.status,
                "queried": coverage_item.queried,
                "results": coverage_item.results,
                "backend": coverage_item.backend,
                "failure_reason": coverage_item.failure_reason,
                "record_count": sum(1 for record in collection.records if record.get("platform") == platform),
            },
        )
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
    verification_state = {"status": "SKIPPED"}
    if verify:
        verified_top, verification_state = coordinator.run_optional(
            "verification",
            lambda: verify_topics(
                buzz_candidates[:top_n],
                perform_reads=True,
                max_reads=min(top_n, 10),
                time_budget_seconds=verification_budget_seconds,
            ),
            buzz_candidates[:top_n],
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
    previous_sentiments: list[dict[str, Any]] = []
    previous_receipts: list[dict[str, Any]] = []
    current_sentiment_path = target / "investible_sentiment_current.json"
    receipts_path = target / "investible_price_receipts.json"
    if current_sentiment_path.exists():
        try:
            previous_sentiments = json.loads(current_sentiment_path.read_text(encoding="utf-8")).get("items", [])
        except (OSError, json.JSONDecodeError):
            previous_sentiments = []
    if receipts_path.exists():
        try:
            previous_receipts = json.loads(receipts_path.read_text(encoding="utf-8")).get("receipts", [])
        except (OSError, json.JSONDecodeError):
            previous_receipts = []
    try:
        sentiment_collection = collect_investible_sentiments(
            investibles=selected_investibles(),
            coverage=coverage,
            existing_signals=accepted,
            retrieved_at_utc=retrieved,
            window_hours=window_hours,
            global_timeout_seconds=sentiment_timeout,
        )
        sentiment_state = {"stage": "sentiment_collection", "status": "COMPLETE"}
        coordinator.checkpoint("sentiment_collection", {"posts": len(sentiment_collection.posts), "rows": len(sentiment_collection.retrieval_ledger)})
    except Exception as exc:
        coordinator.repair_attempt("sentiment_collection", change="preserved completed buzz lanes and emitted an empty auditable sentiment lane", outcome=f"{type(exc).__name__}: {exc}"[:500])
        from .sentiment import SentimentCollection
        now = datetime.now(timezone.utc)
        sentiment_collection = SentimentCollection([], [], [], now, now)
        sentiment_state = {"stage": "sentiment_collection", "status": "DEGRADED", "error": str(exc)[:500]}
        coordinator.checkpoint("sentiment_collection", sentiment_state, status="DEGRADED")
    investible_sentiments, sentiment_ledger, sentiment_posts = build_investible_sentiments(
        investibles=selected_investibles(),
        posts=sentiment_collection.posts,
        retrieval_ledger=sentiment_collection.retrieval_ledger,
        retrieved_at_utc=retrieved,
    )
    price_budget = max(1, min(45, int(global_timeout_seconds - (time.monotonic() - pipeline_started))))
    price_receipts, price_state = coordinator.run_optional(
        "price_receipts",
        lambda: collect_price_receipts(
            selected_investibles(),
            published_at_utc=retrieved,
            retrieved_at_utc=datetime.now(timezone.utc),
            timeout_seconds=price_budget,
        ),
        [],
    )
    previous_price_resolution, resolution_state = coordinator.run_optional(
        "price_resolution",
        lambda: resolve_previous_sentiments(
            previous_sentiments,
            previous_receipts,
            retrieved_at_utc=retrieved,
            timeout_seconds=price_budget,
        ),
        [],
    )
    if not previous_price_resolution and previous_sentiments:
        # A bounded price outage must not erase the prior edition. Preserve
        # each prior record and make the missing resolution explicit.
        previous_price_resolution = []
        for prior in previous_sentiments:
            unresolved = dict(prior)
            unresolved.update(
                {
                    "direction": "UNRESOLVED - PRICE RECEIPT MISSING",
                    "resolved_price": None,
                    "resolved_timestamp_utc": None,
                    "percentage_move": None,
                    "source_url": None,
                }
            )
            previous_price_resolution.append(unresolved)
    for item in investible_sentiments:
        receipt = next((row for row in price_receipts if row.get("investible_id") == item.get("investible_id")), None)
        item["entry_price"] = receipt.get("entry_price") if receipt else None
        item["entry_timestamp_utc"] = receipt.get("entry_timestamp_utc") if receipt else None
        item["entry_price_status"] = receipt.get("status") if receipt else "ENTRY PRICE UNAVAILABLE"
        item["entry_price_source_url"] = receipt.get("source_url") if receipt else None
    region_counts: dict[str, int] = {}
    for topic in top_topics:
        for region in topic.get("social_regions", []) or topic.get("regions", []):
            region_counts[str(region)] = region_counts.get(str(region), 0) + 1
    dominant_region, dominant_count = (max(region_counts.items(), key=lambda item: item[1]) if region_counts else (None, 0))
    coverage_audit = {"region_counts": region_counts, "dominant_region": dominant_region, "dominant_topic_count": dominant_count, "dominant_share": round(dominant_count / len(top_topics), 4) if top_topics else 0, "passes_global_balance": not bool(top_topics) or dominant_count / len(top_topics) <= 0.4}
    coverage_label = "GLOBAL" if coverage_audit["passes_global_balance"] else "COVERAGE_BIASED"
    coverage_scope = "GLOBAL" if coverage_label == "GLOBAL" else "TOP 50 FROM CURRENTLY ACCESSIBLE COVERAGE"
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
        "coverage_label": coverage_label,
        "coverage_scope": coverage_scope,
        "coverage_audit": coverage_audit,
        "pre_run_scan": pre_run_scan,
        "investible_price_receipts": price_receipts,
        "previous_investible_sentiments": previous_price_resolution,
        "verification_requested": verify,
        "self_healing": {
            "coordinator_run_id": coordinator.run_id,
            "pre_run_state": pre_run_state,
            "verification_state": verification_state,
            "price_state": price_state,
            "price_resolution_state": resolution_state,
            "sentiment_state": sentiment_state,
            "remaining_seconds_before_write": coordinator.remaining_seconds,
        },
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
        "previous_investible_sentiments": previous_price_resolution or [],
        "sentiment_live_validated_platforms": sentiment_collection.live_validated_platforms,
        "sentiment_retrieval_row_count": len(sentiment_collection.retrieval_ledger),
        "sentiment_exhaustive_row_count": sum(
            bool(row["exhaustive_boolean"]) for row in sentiment_collection.retrieval_ledger
        ),
        "sentiment_collection_started_at_utc": iso_utc(sentiment_collection.started_at_utc),
        "sentiment_collection_finished_at_utc": iso_utc(sentiment_collection.completed_at_utc),
        "investible_sentiment_history_entry": {"run_id": run_id, "retrieved_at_utc": iso_utc(retrieved), "items": investible_sentiments},
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
        previous_investible_sentiments=previous_sentiments,
        previous_price_resolution=previous_price_resolution,
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
    try:
        run.write(output_dir)
        coordinator.finish(
            status="COMPLETE" if all(
                state.get("status") in {"COMPLETE", "SKIPPED"}
                for state in (pre_run_state, verification_state, price_state, resolution_state, sentiment_state)
            ) else "DEGRADED",
            summary={"final_run_id": run_id, "topic_count": len(top_topics)},
        )
    except Exception as exc:
        coordinator.repair_attempt("write", change="did not promote a new public dataset", outcome=f"{type(exc).__name__}: {exc}"[:500])
        coordinator.finish(status="FAILED", summary={"error": str(exc)[:500]})
        raise
    return run


def rerank_stored_signals(
    raw_payload: dict[str, Any],
    *,
    top_n: int = 50,
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

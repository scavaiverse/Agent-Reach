"""Build one additive, parent-scoped BuzzPack per frozen anchor."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .schema import sha256


def _source(signal: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal_id": signal.get("signal_id"), "platform": signal.get("platform"), "author": signal.get("author"),
        "url": signal.get("url") or None, "original_text": signal.get("original_text"),
        "english_translation": signal.get("english_translation"), "language": signal.get("language"),
        "published_at_utc": signal.get("published_at_utc"), "published_at_sgt": signal.get("published_at_sgt"),
        "raw_receipt_id": signal.get("metadata", {}).get("raw_receipt_id"),
    }


def build_pack(anchor: dict[str, Any], query_pack: dict[str, Any], *, records: list[dict[str, Any]],
               attempted_lanes: list[dict[str, Any]], raw_receipt_ids: list[str], run_id: str,
               git_sha: str | None = None) -> dict[str, Any]:
    unique_by_url: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record.get("url") or record.get("signal_id") or sha256(str(record))
        unique_by_url.setdefault(key, record)
    unique = list(unique_by_url.values())
    languages = sorted({r.get("language") for r in unique if r.get("language")})
    regions = sorted({r.get("region") for r in unique if r.get("region")})
    platforms = sorted({r.get("platform") for r in unique if r.get("platform")})
    dates = sorted(r.get("published_at_utc") for r in unique if r.get("published_at_utc"))
    sources = [_source(r) for r in unique if r.get("url")]
    positive = sum(1 for r in unique if r.get("sentiment") == "POSITIVE")
    negative = sum(1 for r in unique if r.get("sentiment") == "NEGATIVE")
    directional = positive + negative
    if records:
        analysis = (
            f"The retained discussion directly addresses this OTR anchor across {', '.join(platforms) or 'the queried channels'}. "
            f"It is social evidence about the conversation, not proof that the underlying claim is true."
        )
        status = "UNVERIFIED"
    else:
        analysis = "No qualifying social discussion was retained in this scan."
        status = "UNVERIFIED"
    return {
        "buzz_id": "buzz-" + sha256(f"{anchor['anchor_id']}|{anchor['content_hash']}|{run_id}")[:20],
        "edition_id": anchor.get("edition_id"), "parent_anchor_id": anchor.get("anchor_id"),
        "parent_type": anchor.get("anchor_type"), "parent_route": anchor.get("route"),
        "parent_content_hash": anchor.get("content_hash"), "query_pack_id": query_pack.get("query_pack_id"),
        "retrieval_run_id": run_id, "social_git_sha": git_sha, "raw_receipt_ids": raw_receipt_ids,
        "platform_coverage": attempted_lanes, "mapping_features": [], "mapping_score": max((r.get("mapping_score", 0) for r in records), default=0),
        "raw_observation_count": len(records), "unique_conversation_count": len(unique),
        "languages": languages, "regions": regions, "themes": [], "counterthemes": [],
        "sentiment_targets": [], "sentiment_counts": {"POSITIVE": positive, "NEGATIVE": negative, "NON_DIRECTIONAL": len(unique) - directional},
        "representative_sources": sources[:8], "verification_status": status, "verification_sources": [],
        "first_detected": dates[0] if dates else None, "last_scanned": anchor.get("_run_retrieved_at_utc"),
        "generated_analysis": analysis, "previous_buzz_id": None,
        "three_agent_approval_receipts": {"agent_1": None, "agent_2": None, "agent_3": None},
        "sentiment_volume_note": "Corpus B is empty unless directional sentiment is explicitly meaningful for a current edition investible.",
        "searched_empty": not bool(records),
        "query_pack": {"primary_queries": query_pack.get("primary_queries", []), "platform_plan": query_pack.get("platform_plan", {})},
    }

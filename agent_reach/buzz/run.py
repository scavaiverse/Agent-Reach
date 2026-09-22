"""Canonical edition-anchored Buzz run.

This deliberately lives beside, rather than replacing, the older private
discovery/ranking pipeline. Public OTR Buzz starts from live edition anchors.
"""

from __future__ import annotations

import json
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agent_reach.config import Config
from agent_reach.doctor import check_all
from agent_reach.intelligence.normalize import normalize_records
from agent_reach.intelligence.schema import iso_sgt, iso_utc, parse_datetime

from .anchor_relevance import score_candidate
from .anchor_search import execute_lane
from .buzzpack import build_pack
from .edition_anchors import scan_live_edition
from .language_recheck import recheck
from .query_packs import build_query_packs
from .raw_receipts import RawReceiptStore
from .schema import json_dump, sha256, utc_now
from .video_relevance import gate as video_gate


SOCIAL_CHANNELS = ["twitter", "reddit", "youtube", "facebook", "instagram", "xiaohongshu", "bilibili", "linkedin", "v2ex", "github"]


def _write_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json_dump(value), encoding="utf-8")
    temp.replace(path)


def _coverage(doctor: dict[str, Any], lanes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_platform: dict[str, list[dict[str, Any]]] = {}
    for lane in lanes:
        by_platform.setdefault(lane["platform"], []).append(lane)
    rows = []
    for platform in SOCIAL_CHANNELS:
        health = doctor.get(platform, {})
        attempted = by_platform.get(platform, [])
        live = any(x.get("status") == "COMPLETE" and x.get("records") for x in attempted)
        timed_out = any(x.get("status") == "TIMEOUT" for x in attempted)
        statuses = {x.get("status") for x in attempted}
        if timed_out:
            status = "TIMED_OUT"
        elif live:
            status = "LIVE_VALIDATED"
        elif "AUTH_REQUIRED" in statuses or health.get("status") == "warn":
            status = "AUTH_REQUIRED" if platform in {"twitter", "reddit", "facebook", "instagram", "xiaohongshu", "linkedin"} else "DEGRADED"
        elif attempted and "UNAVAILABLE" in statuses:
            status = "UNAVAILABLE"
        else:
            status = "UNQUERIED"
        rows.append({
            "platform": platform, "backend": health.get("active_backend") or ((health.get("backends") or [None])[0]),
            "doctor_status": health.get("status"), "doctor_message": health.get("message"), "attempted": bool(attempted),
            "live_validated": live, "status": status, "results": sum(len(x.get("records") or []) for x in attempted),
            "reason": None if live else (health.get("message") or "No usable current records returned"),
        })
    return rows


def _normalize(raw: dict[str, Any], retrieved: datetime, receipt_id: str) -> list[dict[str, Any]]:
    signal_rows = []
    for item in raw.get("records") or []:
        item = dict(item)
        item["metadata"] = dict(item.get("metadata") or {})
        item["metadata"]["raw_receipt_id"] = receipt_id
        item = recheck(item, translate=False)
        normalized = normalize_records([item], retrieved_at_utc=retrieved, multilingual=False)
        for signal in normalized:
            signal.metadata["raw_receipt_id"] = receipt_id
            signal_row = signal.to_dict()
            signal_row["description"] = item.get("description")
            signal_row["_raw"] = item
            signal_rows.append(signal_row)
    return signal_rows


def run_buzz(*, output_dir: str | Path = "output", base_url: str = "https://new.ontherice.org",
             channel_timeout: float = 15.0, global_timeout: float = 240.0) -> dict[str, Any]:
    started = utc_now()
    run_id = "buzz-" + started.strftime("%Y%m%dT%H%M%SZ") + "-" + sha256(started.isoformat())[:8]
    output = Path(output_dir)
    run_dir = output / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    registry = scan_live_edition(base_url)
    for anchor in registry.get("anchors", []):
        anchor["_run_retrieved_at_utc"] = iso_utc(started)
    query_packs = build_query_packs(registry)
    config = Config(read_only=True)
    doctor = check_all(config)
    store = RawReceiptStore(output, run_id)
    jobs: list[tuple[dict[str, Any], dict[str, Any], str, str]] = []
    pack_by_anchor = {p["anchor_id"]: p for p in query_packs["packs"]}
    for anchor in registry.get("anchors", []):
        pack = pack_by_anchor.get(anchor["anchor_id"])
        if not pack:
            continue
        query = (pack.get("primary_queries") or [anchor["anchor_id"]])[0]
        for platform, plan in pack.get("platform_plan", {}).items():
            if plan.get("applicable"):
                jobs.append((anchor, pack, platform, query))

    lanes: list[dict[str, Any]] = []
    raw_records: list[dict[str, Any]] = []
    deadline = time.monotonic() + global_timeout
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="otr-buzz") as pool:
        futures = {pool.submit(execute_lane, platform, query, doctor=doctor, timeout=channel_timeout): (anchor, pack, platform, query) for anchor, pack, platform, query in jobs}
        for future in as_completed(futures):
            anchor, pack, platform, query = futures[future]
            if time.monotonic() > deadline:
                future.cancel()
                lane = {"platform": platform, "backend": None, "query": query, "status": "TIMEOUT", "records": [], "response": {"reason": "global execution budget exceeded"}}
            else:
                try:
                    lane = future.result(timeout=max(0.1, deadline - time.monotonic()))
                except Exception as exc:  # noqa: BLE001
                    lane = {"platform": platform, "backend": None, "query": query, "status": "ERROR", "records": [], "response": {"reason": str(exc)[:500]}}
            lane["anchor_id"] = anchor["anchor_id"]
            lane["query_pack_id"] = pack["query_pack_id"]
            query_id = sha256(f"{anchor['anchor_id']}|{platform}|{query}")[:24]
            receipt_id = store.write(platform=platform, query_id=query_id, envelope=lane)
            lane["raw_receipt_id"] = receipt_id
            lanes.append(lane)
            for row in lane.get("records") or []:
                row = dict(row)
                row["_anchor_id"] = anchor["anchor_id"]
                row["_query_pack_id"] = pack["query_pack_id"]
                row["_raw_receipt_id"] = receipt_id
                raw_records.append(row)
    store.save_manifest(output, run_id, edition_id=registry.get("edition_id"), retrieved_at_utc=iso_utc(started))

    by_anchor: dict[str, list[dict[str, Any]]] = {}
    rejections: list[dict[str, Any]] = []
    normalized_all: list[dict[str, Any]] = []
    for raw in raw_records:
        anchor = next((a for a in registry.get("anchors", []) if a["anchor_id"] == raw["_anchor_id"]), None)
        if not anchor:
            rejections.append({"raw": raw, "reason": "MISSING_PARENT_ANCHOR"})
            continue
        relevance = score_candidate(anchor, raw)
        video_ok, video_reason = video_gate(anchor, raw)
        raw["mapping_score"] = relevance["mapping_score"]
        raw["reason_codes"] = relevance["reason_codes"]
        raw["video_relevance_reason"] = video_reason
        raw["content_fields_checked"] = (raw.get("metadata") or {}).get("content_fields_checked", [])
        if not relevance["accepted"] or not video_ok:
            rejections.append({"raw": raw, "reason": relevance["rejection_reason"] if not video_ok else relevance["rejection_reason"]})
            continue
        prepared = dict(raw)
        prepared["metadata"] = dict(prepared.get("metadata") or {})
        prepared["metadata"]["raw_receipt_id"] = raw["_raw_receipt_id"]
        normalized = _normalize({"records": [prepared]}, started, raw["_raw_receipt_id"])
        accepted: list[dict[str, Any]] = []
        for signal in normalized:
            published = parse_datetime(signal.get("published_at_utc"))
            if published is None:
                rejections.append({"raw": raw, "reason": "MISSING_PUBLICATION_TIMESTAMP"})
                continue
            age = (started - published).total_seconds()
            if age < 0:
                rejections.append({"raw": raw, "reason": "FUTURE_PUBLICATION_TIMESTAMP"})
                continue
            if age > 24 * 60 * 60:
                rejections.append({"raw": raw, "reason": "OUTSIDE_24_HOUR_WINDOW"})
                continue
            signal["metadata"]["raw_receipt_id"] = raw["_raw_receipt_id"]
            signal["anchor_id"] = anchor["anchor_id"]
            signal["query_pack_id"] = raw["_query_pack_id"]
            signal["mapping_score"] = relevance["mapping_score"]
            signal["reason_codes"] = relevance["reason_codes"]
            accepted.append(signal)
            normalized_all.append(signal)
        by_anchor.setdefault(anchor["anchor_id"], []).extend(accepted)

    coverage = _coverage(doctor, lanes)
    packs: list[dict[str, Any]] = []
    for anchor in registry.get("anchors", []):
        pack = pack_by_anchor[anchor["anchor_id"]]
        applicable = [lane for lane in lanes if lane.get("anchor_id") == anchor["anchor_id"]]
        receipts = sorted({lane["raw_receipt_id"] for lane in applicable if lane.get("raw_receipt_id")})
        pack_obj = build_pack(anchor, pack, records=by_anchor.get(anchor["anchor_id"], []), attempted_lanes=applicable, raw_receipt_ids=receipts, run_id=run_id)
        packs.append(pack_obj)

    source_ledger = []
    for pack in packs:
        for source in pack.get("representative_sources", []):
            source_ledger.append({
                "otr_item": pack["parent_anchor_id"], "topic": pack["buzz_id"], "claim": "social discussion occurred",
                "signal": source.get("signal_id"), "source": source.get("signal_id"), "url": source.get("url"),
                "timestamp": source.get("published_at_utc"), "verification_role": "social_buzz_evidence",
                "raw_receipt_id": source.get("raw_receipt_id"), "url_status": "URL_UNKNOWN",
            })
    mapping = {
        "schema_version": "otr-buzz-mapping-2",
        "matched": [p["parent_anchor_id"] for p in packs if p["raw_observation_count"]],
        "unmatched_high_buzz_topics": [],
        "routes_with_chatter": sorted({p["parent_route"] for p in packs if p["raw_observation_count"]}),
        "routes_without_chatter": sorted({p["parent_route"] for p in packs if not p["raw_observation_count"]}),
    }
    data = {
        "schema_version": "otr-buzz-run-2", "run_id": run_id, "edition_id": registry.get("edition_id"),
        "retrieval_time_utc": iso_utc(started), "retrieval_time_sgt": iso_sgt(started), "research_window_hours": 24,
        "coverage_scope": "current OTR edition anchors only", "registry": registry, "query_packs": query_packs,
        "doctor": doctor, "platform_coverage": coverage, "lanes": lanes, "raw_signals": raw_records,
        "normalized_signals": normalized_all, "rejected_signals": rejections, "buzzpacks": packs,
        "source_ledger": source_ledger, "mapping": mapping,
        "git_freeze": {"social_git_sha": None, "status": "PENDING_FREEZE"},
        "agent_receipts": {"agent_1": None, "agent_2": None, "agent_3": None},
        "acceptance": {"public_top_n_dependency": False, "generic_queries_as_primary": False, "raw_before_normalization": True, "video_gate": True, "anchor_hashes": True},
    }
    _write_atomic(output / "edition_anchor_registry.json", registry)
    _write_atomic(output / "buzz_query_packs.json", query_packs)
    _write_atomic(output / "platform_coverage.json", {"run_id": run_id, "coverage": coverage})
    _write_atomic(output / "raw_social_observations.json", {"run_id": run_id, "signals": raw_records})
    _write_atomic(output / "normalized_social_observations.json", {"run_id": run_id, "signals": normalized_all})
    _write_atomic(output / "rejected_social_observations.json", {"run_id": run_id, "signals": rejections})
    _write_atomic(output / "buzzpacks.json", {"run_id": run_id, "edition_id": registry.get("edition_id"), "buzzpacks": packs})
    _write_atomic(output / "source_ledger.json", {"run_id": run_id, "rows": source_ledger})
    _write_atomic(output / "verification_ledger.json", {"run_id": run_id, "rows": [{"buzz_id": p["buzz_id"], "parent_anchor_id": p["parent_anchor_id"], "verification_status": p["verification_status"], "verification_sources": []} for p in packs]})
    _write_atomic(output / "buzz_mapping.json", mapping)
    _write_atomic(run_dir / "run_manifest.json", {"run_id": run_id, "edition_id": registry.get("edition_id"), "retrieved_at_utc": iso_utc(started), "raw_receipts": store.manifest, "raw_signal_count": len(raw_records), "normalized_signal_count": len(normalized_all), "rejected_signal_count": len(rejections)})
    _write_atomic(output / "latest_buzz_run.json", data)
    return data

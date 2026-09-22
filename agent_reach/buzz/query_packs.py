"""Generate bounded, multilingual, anchor-specific query packs."""

from __future__ import annotations

import re
from typing import Any

from .schema import sha256

GENERIC = {
    "ai", "security", "outage", "regulation", "climate", "market", "news", "today",
    "technology", "business", "world", "risk", "risks", "latest", "update", "story",
}
DEV = {"software", "developer", "developers", "github", "open-source", "open source", "code", "agent", "api", "security", "credential", "permission", "data centre", "data center", "network", "chip", "memory", "semiconductor", "model"}
EVENT = {"launch", "launched", "approval", "approved", "talks", "meeting", "production", "mass", "outage", "strike", "election", "policy", "price", "sales", "shortage", "deficit", "trial", "tests", "testing", "entered", "fell", "rose", "announced", "commitment"}


def _words(text: str) -> list[str]:
    return re.findall(r"[\w\u3400-\u9fff][\w\u3400-\u9fff'’+.-]*", text, flags=re.UNICODE)


def _query_text(anchor: dict[str, Any]) -> str:
    text = re.sub(r"\s+", " ", str(anchor.get("published_text") or "")).strip()
    words = _words(text)
    useful = [w for w in words if w.casefold() not in GENERIC and len(w) > 2]
    chosen = useful[:14] or words[:14]
    if not chosen:
        chosen = [str(anchor.get("section_name") or anchor.get("tab_name") or anchor.get("anchor_id"))]
    query = " ".join(chosen)
    # A route/anchor suffix keeps otherwise short section wording from becoming a
    # free-floating generic search. It is never presented as social evidence.
    if len([w for w in chosen if w.casefold() not in GENERIC]) < 2:
        query = f"{query} {anchor.get('tab_name') or anchor.get('anchor_id')}"
    return query[:240]


def _platform_plan(anchor: dict[str, Any]) -> dict[str, dict[str, Any]]:
    text = str(anchor.get("published_text") or "").casefold()
    has_dev = any(term in text for term in DEV)
    has_event = any(term in text for term in EVENT) or bool(anchor.get("anchor_type") == "headline")
    has_cjk = bool(re.search(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]", text))
    has_consumer = any(term in text for term in {"consumer", "retail", "restaurant", "hotel", "travel", "beauty", "customer", "creator"})
    has_enterprise = any(term in text for term in {"company", "enterprise", "hiring", "executive", "b2b", "customers", "business"})
    return {
        "twitter": {"applicable": has_event, "reason": "fast event/company/policy reaction"},
        "reddit": {"applicable": has_event or has_dev, "reason": "community debate or product/context discussion"},
        "youtube": {"applicable": has_event or has_dev, "reason": "creator commentary or direct event explainer"},
        "facebook": {"applicable": has_consumer or has_event, "reason": "local/community or event reaction"},
        "instagram": {"applicable": has_consumer or has_event, "reason": "consumer/culture/product conversation"},
        "xiaohongshu": {"applicable": has_consumer or has_cjk, "reason": "consumer/lifestyle/Chinese-language discussion"},
        "bilibili": {"applicable": has_cjk or has_dev or has_event, "reason": "Chinese-language technology, culture or event reaction"},
        "linkedin": {"applicable": has_enterprise or has_dev, "reason": "enterprise/professional/company discussion"},
        "v2ex": {"applicable": has_dev or has_cjk, "reason": "developer/technology community discussion"},
        "github": {"applicable": has_dev and not any(term in text for term in {"election", "weather", "restaurant", "celebrity"}), "reason": "developer or implementation evidence is relevant"},
    }


def build_query_packs(registry: dict[str, Any]) -> dict[str, Any]:
    packs: list[dict[str, Any]] = []
    for anchor in registry.get("anchors", []):
        base = _query_text(anchor)
        variants = [base]
        if re.search(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]", anchor.get("published_text", "")):
            native = " ".join(_words(anchor.get("published_text", ""))[:12])
            if native and native != base:
                variants.append(native[:240])
        pack_id = "qp-" + sha256(f"{anchor['anchor_id']}|{anchor['content_hash']}")[:20]
        plan = _platform_plan(anchor)
        anchor["platform_plan"] = plan
        packs.append({
            "query_pack_id": pack_id,
            "edition_id": registry.get("edition_id"),
            "anchor_id": anchor["anchor_id"],
            "parent_content_hash": anchor["content_hash"],
            "primary_queries": variants,
            "multilingual": True,
            "platform_plan": plan,
            "banned_standalone_terms": sorted(GENERIC),
        })
    return {
        "schema_version": "otr-buzz-query-packs-2",
        "edition_id": registry.get("edition_id"),
        "packs": packs,
    }

"""Crawl the live OTR edition and freeze parent anchors before retrieval."""

from __future__ import annotations

import html as html_lib
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any

from .schema import sha256, utc_now

ROUTES = [
    ("/headlines/", "headlines"),
    ("/the-full-immersive/tutorial/", "tutorial"),
    ("/the-full-immersive/today/", "today"),
    ("/the-full-immersive/market-predictions/", "market-predictions"),
    ("/the-full-immersive/risks/", "risks"),
    ("/the-full-immersive/top-three/", "top-three"),
    ("/the-full-immersive/long-term-investment/", "long-term-investment"),
    ("/the-full-immersive/connections/", "connections"),
    ("/the-full-immersive/new-insight/", "new-insight"),
    ("/the-full-immersive/next/", "next"),
    ("/the-full-immersive/predictions-revisited/", "predictions-revisited"),
    ("/the-full-immersive/simple-terms/", "simple-terms"),
    ("/the-full-immersive/sources/", "sources"),
    ("/the-full-immersive/lessons/", "lessons"),
]


def strip_html(value: str) -> str:
    value = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html_lib.unescape(value)).strip()


def _attr(block: str, name: str) -> str | None:
    match = re.search(rf'\b{name}="([^"]*)"', block, flags=re.I)
    return html_lib.unescape(match.group(1)) if match else None


def _release(markup: str) -> str | None:
    return _attr(markup, "data-release-id")


def _published_at(markup: str) -> str | None:
    for pattern in (
        r'data-research-cutoff-sgt="([^"]+)"',
        r'data-issue-date-sgt="([^"]+)"',
        r'Prepared[^<]{0,100}?(20\d\d-\d\d-\d\d[^< ]*)',
    ):
        match = re.search(pattern, markup, flags=re.I)
        if match:
            return match.group(1)
    return None


def _headline_anchors(route: str, markup: str, release: str | None, published: str | None) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    pattern = re.compile(r'<article\b[^>]*class="[^"]*headline-record[^"]*"[^>]*>[\s\S]*?</article>', re.I)
    for index, match in enumerate(pattern.finditer(markup), 1):
        block = match.group(0)
        anchor_id = _attr(block, "id") or f"H{index:02d}"
        topic = _attr(block, "data-topic")
        heading = re.search(r'<h[1-4][^>]*>([\s\S]*?)</h[1-4]>', block, flags=re.I)
        published_text = strip_html(topic or (heading.group(1) if heading else block))
        anchors.append(_make_anchor(
            anchor_id=anchor_id,
            anchor_type="headline",
            route=f"{route}#{anchor_id}",
            canonical_url=f"https://new.ontherice.org{route}#{anchor_id}",
            published_text=published_text,
            release=release,
            published=published,
            rank=index,
            tab_name=None,
            section_name=None,
            raw=block,
        ))
    return anchors


def _editorial_anchors(route: str, tab: str, markup: str, release: str | None, published: str | None) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    matches = list(re.finditer(r'<section\b[^>]*class="[^"]*editorial-section[^"]*"[^>]*>', markup, re.I))
    for ordinal, start_match in enumerate(matches, 1):
        start = start_match.start()
        end = matches[ordinal].start() if ordinal < len(matches) else markup.find("</section></section>", start)
        if end < 0:
            end = min(len(markup), start + 12000)
        block = markup[start:end]
        heading = re.search(r'<h[1-4][^>]*>([\s\S]*?)</h[1-4]>', block, flags=re.I)
        label = strip_html(heading.group(1) if heading else f"{tab} section {ordinal}")
        section_number = _attr(start_match.group(0), "data-editorial-section") or str(ordinal)
        anchor_id = f"EDITORIAL-{tab.upper().replace('-', '_')}-S{ordinal:02d}"
        text = strip_html(block)
        anchors.append(_make_anchor(
            anchor_id=anchor_id,
            anchor_type="editorial_section",
            route=route,
            canonical_url=f"https://new.ontherice.org{route}",
            published_text=text,
            release=release,
            published=published,
            rank=ordinal,
            tab_name=tab,
            section_name=label,
            raw=block,
            section_number=section_number,
        ))
    return anchors


def _make_anchor(*, anchor_id: str, anchor_type: str, route: str, canonical_url: str,
                 published_text: str, release: str | None, published: str | None,
                 rank: int, tab_name: str | None, section_name: str | None,
                 raw: str, section_number: str | None = None) -> dict[str, Any]:
    content_hash = sha256(published_text)
    words = [w for w in re.findall(r"[\w\u3400-\u9fff-]+", published_text, flags=re.UNICODE) if len(w) > 2]
    entities = sorted({w for w in words if re.search(r"[A-Z\u3400-\u9fff]", w)})[:24]
    return {
        "edition_id": release or "UNKNOWN_EDITION",
        "release_id": release or "UNKNOWN_EDITION",
        "published_at_sgt": published,
        "anchor_id": anchor_id,
        "anchor_type": anchor_type,
        "route": route,
        "canonical_url": canonical_url,
        "headline_order_or_rank": rank,
        "tab_name": tab_name,
        "section_name": section_name,
        "section_number": section_number,
        "published_text": published_text,
        "content_hash": content_hash,
        "source_ids": sorted(set(re.findall(r"\bH\d{2}\b", raw))),
        "entities": entities,
        "investibles": [],
        "products": [],
        "locations": [],
        "time_terms": [],
        "claims": [],
        "mechanisms": [],
        "risks": [],
        "predictions": [],
        "break_conditions": [],
        "keywords": words[:40],
        "exclusions": [],
        "research_depth": "targeted",
        "platform_plan": {},
    }


def _fetch(base_url: str, route: str, timeout: float) -> tuple[int, str]:
    stamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    url = f"{base_url.rstrip('/')}{route}?buzzscan={stamp}"
    request = urllib.request.Request(url, headers={"User-Agent": "OTR-Buzz-Agent/2.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return int(response.status), response.read().decode("utf-8", errors="replace")


def scan_live_edition(base_url: str = "https://new.ontherice.org", timeout: float = 20.0) -> dict[str, Any]:
    """Return a frozen, live-derived anchor registry; failures are recorded."""
    retrieved = utc_now()
    route_receipts: list[dict[str, Any]] = []
    anchors: list[dict[str, Any]] = []
    for route, tab in ROUTES:
        try:
            status, markup = _fetch(base_url, route, timeout)
            release = _release(markup)
            published = _published_at(markup)
            route_receipts.append({"route": route, "status": status, "bytes": len(markup), "release_id": release})
            if tab == "headlines":
                anchors.extend(_headline_anchors(route, markup, release, published))
            else:
                anchors.extend(_editorial_anchors(route, tab, markup, release, published))
        except Exception as exc:  # noqa: BLE001 - the registry records partial runs
            route_receipts.append({"route": route, "status": "ERROR", "error": str(exc)[:500]})
    edition_ids = sorted({a["edition_id"] for a in anchors if a.get("edition_id") != "UNKNOWN_EDITION"})
    edition_id = edition_ids[0] if len(edition_ids) == 1 else (edition_ids[0] if edition_ids else "UNKNOWN_EDITION")
    for anchor in anchors:
        anchor["edition_id"] = edition_id
        anchor["release_id"] = edition_id
    return {
        "schema_version": "otr-buzz-anchors-2",
        "retrieved_at_utc": retrieved.isoformat().replace("+00:00", "Z"),
        "retrieved_at_sgt": retrieved.astimezone().isoformat(),
        "base_url": base_url,
        "edition_id": edition_id,
        "route_receipts": route_receipts,
        "anchors": anchors,
    }

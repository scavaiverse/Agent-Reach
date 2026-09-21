# -*- coding: utf-8 -*-
"""Read-only baseline capture for the public Social Pulse page."""

from __future__ import annotations

import json
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def scan_live_social_pulse(
    output_dir: str | Path,
    *,
    live_url: str = "https://new.ontherice.org/social-pulse/",
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    """Capture the current public page before a new run; never infer baseline state."""

    captured = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    result: dict[str, Any] = {
        "schema_version": "social-pulse-pre-run-scan-1",
        "captured_at_utc": captured,
        "live_url": live_url,
        "scan_status": "LIVE_PAGE_SCAN_UNAVAILABLE",
        "errors": [],
        "previous_publish_id": None,
        "previous_publish_time": None,
        "reader_tabs": [],
        "topic_ids": [],
        "investible_ids": [],
        "sentiment_values": [],
        "entry_prices": [],
        "sources": [],
        "timestamps": [],
    }
    html = ""
    try:
        request = urllib.request.Request(live_url, headers={"User-Agent": "agent-reach-social-pulse-scan/2.0"})
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            html = response.read(4 * 1024 * 1024).decode("utf-8", errors="replace")
        result["scan_status"] = "LIVE_PAGE_SCAN_OK"
    except Exception as exc:
        result["errors"].append(f"live page unavailable: {type(exc).__name__}: {exc}"[:500])
        prior = Path(output_dir) / "social_pulse_pre_run_scan.json"
        if prior.exists():
            try:
                old = json.loads(prior.read_text(encoding="utf-8"))
                for key in ("previous_publish_id", "previous_publish_time", "reader_tabs", "topic_ids", "investible_ids", "sentiment_values", "entry_prices", "sources", "timestamps"):
                    if key in old:
                        result[key] = old[key]
                result["fallback_snapshot"] = str(prior)
            except (OSError, json.JSONDecodeError):
                result["errors"].append("immutable pre-run snapshot was unreadable")
        _write(Path(output_dir) / "social_pulse_pre_run_scan.json", result)
        return result

    run_match = re.search(r'data-social-pulse-run="([^"]+)"', html)
    result["previous_publish_id"] = run_match.group(1) if run_match else None
    retrieval = re.search(r"Retrieval time(?:</[^>]+>|\s|&nbsp;)*([^<]{8,80})", html, re.I)
    if retrieval:
        result["previous_publish_time"] = re.sub(r"\s+", " ", retrieval.group(1)).strip()
    result["reader_tabs"] = re.findall(r'<button[^>]+role="tab"[^>]*>(.*?)</button>', html, re.I | re.S)
    result["reader_tabs"] = [re.sub(r"<[^>]+>", "", item).strip() for item in result["reader_tabs"]]
    result["topic_ids"] = re.findall(r'data-topic-id="([^"]+)"', html, re.I)
    result["investible_ids"] = re.findall(r'data-investible-id="([^"]+)"', html, re.I)
    result["sentiment_values"] = re.findall(r"(?:positive|negative)[^0-9]{0,20}(\d{1,3})%", html, re.I)
    result["entry_prices"] = re.findall(r"(?:entry price|price)[^<]{0,80}", html, re.I)[:100]
    result["sources"] = sorted(set(re.findall(r'https?://[^\"<> ]+', html)))[:500]
    result["timestamps"] = sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}[^<\" ]*", html)))[:100]
    if not result["reader_tabs"]:
        result["errors"].append("no reader tabs detected in the reachable baseline page")
    if len(result["reader_tabs"]) < 3:
        result["errors"].append("baseline predates the canonical three-tab contract")
    if not result["entry_prices"]:
        result["errors"].append("baseline contains no frozen entry-price fields")
    _write(Path(output_dir) / "social_pulse_pre_run_scan.json", result)
    return result

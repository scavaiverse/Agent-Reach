"""Audit one stored Agent-Reach intelligence run without making network calls."""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

from agent_reach.intelligence.cluster import canonicalize_url
from agent_reach.intelligence.pipeline import rerank_stored_signals
from agent_reach.intelligence.schema import parse_datetime


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def audit(output_dir: Path) -> dict[str, object]:
    latest = _load(output_dir / "latest_cross_platform_intelligence.json")
    raw = _load(output_dir / "raw_social_signals.json")
    rejected = _load(output_dir / "rejected_social_signals.json")
    clusters = _load(output_dir / "topic_clusters.json")
    ledger = _load(output_dir / "source_ledger.json")
    public = _load(output_dir / "social_chatter_public.json")
    retrieval_ledger = _load(output_dir / "sentiment_retrieval_ledger.json")
    sentiment_ledger = _load(output_dir / "sentiment_ledger.json")
    sentiment_posts = _load(output_dir / "sentiment_posts.json")
    pre_run_scan = _load(output_dir / "social_pulse_pre_run_scan.json")
    top50 = _load(output_dir / "top_50_social_buzz.json")
    price_receipts = _load(output_dir / "investible_price_receipts.json")
    sentiment_current = _load(output_dir / "investible_sentiment_current.json")
    sentiment_history = _load(output_dir / "investible_sentiment_history.json")

    signals = raw["signals"]
    accepted = [signal for signal in signals if signal.get("accepted_in_window") is True]
    rejected_signals = rejected["signals"]
    retrieval = parse_datetime(latest["retrieval_time_utc"])
    assert retrieval is not None
    cutoff = retrieval - timedelta(hours=latest["window_hours"])
    for signal in accepted:
        published = parse_datetime(signal.get("published_at_utc"))
        assert published is not None, f"accepted signal missing timestamp: {signal.get('signal_id')}"
        assert cutoff <= published <= retrieval, f"timestamp outside window: {signal.get('signal_id')}"
        assert urlsplit(signal.get("url") or "").scheme in {"http", "https"}
        assert signal.get("original_text")

    assert len(signals) == latest["raw_signal_count"]
    assert len(accepted) == latest["accepted_signal_count"]
    assert len(rejected_signals) == latest["rejected_signal_count"]
    assert len(clusters["clusters"]) == latest["unique_topic_count"]
    assert len(ledger["sources"]) == latest["source_ledger_count"]
    expected_topics = min(latest["top_n_requested"], latest.get("qualifying_social_cluster_count", latest["unique_topic_count"]))
    assert len(latest["topics"]) == expected_topics
    assert latest["top_n_requested"] == 50
    assert top50["run_id"] == latest["run_id"]
    assert pre_run_scan.get("scan_status") in {"LIVE_PAGE_SCAN_OK", "LIVE_PAGE_SCAN_UNAVAILABLE"}
    assert isinstance(price_receipts.get("receipts"), list)
    assert sentiment_current.get("run_id") == latest["run_id"]
    assert isinstance(sentiment_history, list) and sentiment_history
    assert all(topic.get("social_signal_count", 0) > 0 for topic in latest["topics"])
    assert "doctor" not in public
    assert "cookie" not in json.dumps(public, ensure_ascii=False).casefold()
    assert "bearer" not in json.dumps(public, ensure_ascii=False).casefold()

    accepted_ids = {signal["signal_id"] for signal in accepted}
    cluster_ids = {cluster["topic_id"] for cluster in clusters["clusters"]}
    ledger_ids = {source["signal_id"] for source in ledger["sources"]}
    assert ledger_ids == accepted_ids, "source ledger has orphaned or missing signal rows"
    assert all(source["topic_id"] in cluster_ids for source in ledger["sources"] if source["topic_id"])

    url_statuses = {"URL_OK", "URL_REDIRECTED", "URL_AUTH_REQUIRED", "URL_INACCESSIBLE", "URL_DELETED", "URL_UNKNOWN"}
    assert all(source["url_status"] in url_statuses for source in ledger["sources"])
    for topic in latest["topics"]:
        assert topic["signals"], f"empty published topic: {topic['topic_id']}"
        assert any(signal.get("url") for signal in topic["signals"]), f"topic has no evidence URL: {topic['topic_id']}"
        assert sum(item["points"] for item in topic["score_components"].values()) == topic["importance_score"]
        assert topic["social_buzz_score"] == sum(
            item["points"] for item in topic["social_buzz_components"].values()
        )
        assert topic.get("social_sources") or any(signal.get("url") for signal in topic["signals"])

    canonical_urls = [canonicalize_url(signal.get("url") or "") for signal in accepted if signal.get("url")]
    duplicate_urls_removed = len(canonical_urls) - len(set(canonical_urls))
    assert duplicate_urls_removed == 0, "accepted dataset contains duplicate canonical URLs"
    first = rerank_stored_signals(raw, top_n=latest["top_n_requested"], verify=False)
    second = rerank_stored_signals(raw, top_n=latest["top_n_requested"], verify=False)
    assert first == second, "reranking the stored dataset was not deterministic"
    assert [topic["topic_id"] for topic in first] == [topic["topic_id"] for topic in latest["topics"]]

    retrieval_rows = retrieval_ledger.get("rows", [])
    assert isinstance(retrieval_rows, list)
    sentiment_ledger_rows = sentiment_ledger.get("rows", [])
    sentiment_post_rows = sentiment_posts.get("posts", [])
    assert len(retrieval_rows) == len(sentiment_ledger_rows) * 12
    required_retrieval_fields = {
        "run_id", "investible_id", "platform", "backend", "research_window_start",
        "research_window_end", "query_variants", "cursors_or_pages_traversed",
        "first_retrieved_at", "last_retrieved_at", "posts_discovered", "posts_read_full",
        "posts_excluded", "duplicates_removed", "posts_unreadable", "posts_private",
        "posts_deleted", "posts_auth_required", "retained_posts", "terminal_reason",
        "exhaustive_boolean", "limit_detail", "errors", "completed_at",
    }
    assert all(required_retrieval_fields <= set(row) for row in retrieval_rows)
    allowed_terminals = {
        "EXHAUSTED", "WINDOW_BOUNDARY_REACHED", "PLATFORM_LIMIT", "AUTH_REQUIRED",
        "TIMEOUT", "RATE_LIMIT", "BACKEND_FAILURE", "OTHER_DOCUMENTED_LIMIT",
    }
    assert all(row["terminal_reason"] in allowed_terminals for row in retrieval_rows)
    sentiment_post_ids = {post["signal_id"] for post in sentiment_post_rows}
    for item in latest.get("investible_sentiments", []):
        retained = set(item.get("retained_signal_ids", []))
        assert retained <= sentiment_post_ids, f"sentiment item has orphaned signal ids: {item['investible_id']}"
        positive = item.get("positive_percent")
        negative = item.get("negative_percent")
        if positive is not None or negative is not None:
            assert positive is not None and negative is not None
            assert positive + negative == 100
        assert "NON_DIRECTIONAL" in item.get("sentiment_counts", {})
        rows = [row for row in retrieval_rows if row["investible_id"] == item["investible_id"]]
        live_rows = [row for row in rows if row["platform"] in retrieval_ledger.get("live_validated_platforms", [])]
        if item["coverage"] == "COVERAGE COMPLETE":
            assert live_rows and all(row["exhaustive_boolean"] for row in live_rows)
        assert not (
            item["coverage"] == "COVERAGE COMPLETE"
            and any(row["terminal_reason"] not in {"EXHAUSTED", "WINDOW_BOUNDARY_REACHED"} for row in live_rows)
        )
    assert retrieval_ledger.get("retrieval_time_utc") == latest.get("retrieval_time_utc")

    credential_markers = ("art_v1_", "gho_", "Bearer ", "cookie:")
    output_text = "\n".join(path.read_text(encoding="utf-8") for path in output_dir.glob("*.json"))
    assert not any(marker.casefold() in output_text.casefold() for marker in credential_markers)

    return {
        "raw_signal_count": len(signals),
        "accepted_signal_count": len(accepted),
        "rejected_signal_count": len(rejected_signals),
        "unique_topic_count": len(clusters["clusters"]),
        "source_ledger_count": len(ledger["sources"]),
        "duplicate_urls_removed": duplicate_urls_removed,
        "top50_deterministic": True,
        "pre_run_scan_status": pre_run_scan.get("scan_status"),
        "price_receipts": len(price_receipts.get("receipts", [])),
        "sentiment_retrieval_rows": len(retrieval_rows),
        "sentiment_posts": len(sentiment_post_rows),
        "source_integrity": "PASS",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="output")
    args = parser.parse_args()
    print(json.dumps(audit(Path(args.output)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

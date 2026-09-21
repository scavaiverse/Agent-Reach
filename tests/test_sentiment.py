from __future__ import annotations

from datetime import datetime, timedelta, timezone

from agent_reach.intelligence.collector import Coverage
from agent_reach.intelligence.investibles import Investible
from agent_reach.intelligence.sentiment import (
    SentimentCollector,
    build_investible_sentiments,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 21, 8, tzinfo=UTC)
BITCOIN = Investible(
    "bitcoin",
    "Bitcoin",
    "BTC",
    ("BTC-USD", "比特币", "ビットコイン"),
    "crypto",
)


def coverage(platform: str, *, results: int = 1, status: str = "OK") -> dict[str, Coverage]:
    return {
        platform: Coverage(
            platform,
            "test-backend",
            queried=True,
            results=results,
            status=status,
        )
    }


def item(index: int, *, text: str = "Bitcoin bullish growth", author: str = "creator"):
    return {
        "platform": "bilibili",
        "backend": "test-backend",
        "title": text,
        "description": text,
        "text": text,
        "language": "en",
        "author": author,
        "bvid": f"BV{index}",
        "pubdate": (NOW - timedelta(hours=1)).timestamp(),
        "engagement": {"views": index * 100},
    }


def test_query_variants_preserve_native_aliases():
    assert "BTC" in BITCOIN.query_variants
    assert "比特币" in BITCOIN.query_variants
    assert "ビットコイン" in BITCOIN.query_variants


def test_bilibili_paginates_beyond_page_one(monkeypatch):
    page_calls = []

    def fake_json(url, *, timeout):
        page_calls.append(url)
        if "&page=1&" in url:
            rows = [item(1)]
            rows.extend(item(index, text="unrelated video") for index in range(2, 21))
            return {"data": {"result": [{"result_type": "video", "data": rows}]}}
        return {"data": {"result": [{"result_type": "video", "data": []}]}}

    monkeypatch.setattr("agent_reach.intelligence.sentiment._http_json", fake_json)
    run = SentimentCollector(
        investibles=(BITCOIN,),
        coverage=coverage("bilibili"),
        existing_signals=(),
        retrieved_at_utc=NOW,
        global_timeout_seconds=10,
    ).collect()
    row = next(row for row in run.retrieval_ledger if row["platform"] == "bilibili")
    assert any("page=2" in value for value in page_calls)
    assert row["terminal_reason"] == "EXHAUSTED"
    assert row["cursors_or_pages_traversed"][-1].endswith("page=2")
    assert row["posts_read_full"] >= 1
    assert len(run.posts) >= 1


def test_bilibili_stops_at_window_boundary(monkeypatch):
    old = item(1)
    old["pubdate"] = (NOW - timedelta(hours=25)).timestamp()
    monkeypatch.setattr(
        "agent_reach.intelligence.sentiment._http_json",
        lambda *args, **kwargs: {"data": {"result": [{"result_type": "video", "data": [old]}]}},
    )
    run = SentimentCollector(
        investibles=(BITCOIN,),
        coverage=coverage("bilibili"),
        existing_signals=(),
        retrieved_at_utc=NOW,
        global_timeout_seconds=10,
    ).collect()
    row = next(row for row in run.retrieval_ledger if row["platform"] == "bilibili")
    assert row["terminal_reason"] == "WINDOW_BOUNDARY_REACHED"
    assert row["exhaustive_boolean"] is True
    assert row["retained_posts"] == 0


def test_one_failed_platform_does_not_stop_another(monkeypatch):
    monkeypatch.setattr(
        "agent_reach.intelligence.sentiment._http_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("backend down")),
    )
    monkeypatch.setattr(
        "agent_reach.intelligence.sentiment._run_command",
        lambda *args, **kwargs: '{"entries":[]}',
    )
    run = SentimentCollector(
        investibles=(BITCOIN,),
        coverage={
            "bilibili": Coverage("bilibili", "bili", queried=True, results=1, status="OK"),
            "youtube": Coverage("youtube", "yt-dlp", queried=True, results=1, status="OK"),
        },
        existing_signals=(),
        retrieved_at_utc=NOW,
        global_timeout_seconds=10,
    ).collect()
    rows = {row["platform"]: row for row in run.retrieval_ledger}
    assert rows["bilibili"]["terminal_reason"] == "BACKEND_FAILURE"
    assert rows["youtube"]["terminal_reason"] == "PLATFORM_LIMIT"


def _ledger(*, terminal_reason: str = "OTHER_DOCUMENTED_LIMIT"):
    return [
        {
            "run_id": "run-test",
            "investible_id": "bitcoin",
            "investible_name": "Bitcoin",
            "ticker": "BTC",
            "platform": "bilibili",
            "backend": "test",
            "terminal_reason": terminal_reason,
            "exhaustive_boolean": terminal_reason in {"EXHAUSTED", "WINDOW_BOUNDARY_REACHED"},
            "duplicates_removed": 0,
            "posts_excluded": 0,
            "posts_unreadable": 0,
            "retained_posts": 0,
        }
    ]


def _post(index: int, text: str, author: str):
    return {
        "investible_id": "bitcoin",
        "platform": "bilibili",
        "backend": "test",
        "title": text,
        "text": text,
        "language": "en",
        "author": author,
        "url": f"https://www.bilibili.com/video/BV{index}",
        "published_at": NOW - timedelta(hours=index),
        "engagement": {"views": index * 100},
        "metadata": {
            "source_key": f"bilibili:{author}",
            "sentiment_investible_id": "bitcoin",
            "sentiment_query_variant": "Bitcoin",
            "full_content_read": True,
        },
        "source_type": "social_community",
        "evidence_type": "direct_post",
    }


def test_meter_is_traced_and_duplicate_retained():
    posts = [
        _post(1, "Bitcoin bullish growth", "a"),
        _post(2, "Bitcoin bearish risk", "b"),
        _post(3, "Bitcoin bullish demand", "c"),
        _post(3, "Bitcoin bullish demand repost", "c"),
    ]
    retrieval_ledger = _ledger()
    results, ledger, retained_posts = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=posts,
        retrieval_ledger=retrieval_ledger,
        retrieved_at_utc=NOW,
    )
    result = results[0]
    assert result["meter_status"] == "METER READY"
    assert result["positive_percent"] + result["negative_percent"] == 100
    assert result["coverage"] == "COVERAGE PARTIAL"
    assert retrieval_ledger[0]["duplicates_removed"] == 0
    assert result["retained_post_count"] == 4
    assert any(post.get("metadata", {}).get("duplicate_repost_flag") for post in retained_posts)
    assert set(result["retained_signal_ids"]) == {post["signal_id"] for post in retained_posts}


def test_insufficient_data_suppresses_percentage():
    results, _, _ = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=[_post(1, "Bitcoin bullish growth", "a")],
        retrieval_ledger=_ledger(),
        retrieved_at_utc=NOW,
    )
    assert results[0]["meter_status"] == "INSUFFICIENT DIRECTIONAL DATA"
    assert results[0]["positive_percent"] is None
    assert "too small" in results[0]["system_prose"]


def test_platform_limit_can_never_be_coverage_complete():
    results, _, _ = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=[
            _post(1, "Bitcoin bullish growth", "a"),
            _post(2, "Bitcoin bullish demand", "b"),
            _post(3, "Bitcoin bearish risk", "c"),
        ],
        retrieval_ledger=_ledger(terminal_reason="PLATFORM_LIMIT"),
        retrieved_at_utc=NOW,
    )
    assert results[0]["positive_percent"] is not None
    assert results[0]["coverage"] == "COVERAGE PARTIAL"


def test_same_stored_corpus_reproduces_meter_and_prose():
    posts = [
        _post(1, "Bitcoin bullish growth", "a"),
        _post(2, "Bitcoin bearish risk", "b"),
        _post(3, "Bitcoin bullish demand", "c"),
    ]
    first = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=posts,
        retrieval_ledger=_ledger(),
        retrieved_at_utc=NOW,
    )[0][0]
    second = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=posts,
        retrieval_ledger=_ledger(),
        retrieved_at_utc=NOW,
    )[0][0]
    assert first["positive_percent"] == second["positive_percent"]
    assert first["negative_percent"] == second["negative_percent"]
    assert first["system_prose"] == second["system_prose"]


def test_system_prose_omits_generic_numeric_fragments():
    result = build_investible_sentiments(
        investibles=(BITCOIN,),
        posts=[
            _post(1, "Bitcoin bearish three consecutive sessions", "a"),
            _post(2, "Bitcoin bearish three consecutive sessions", "b"),
            _post(3, "Bitcoin bullish demand", "c"),
        ],
        retrieval_ledger=_ledger(),
        retrieved_at_utc=NOW,
    )[0][0]
    assert "three, consecutive" not in result["system_prose"]

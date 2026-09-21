from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from agent_reach.intelligence.cluster import cluster_signals
from agent_reach.intelligence.collector import CollectionResult, Coverage, LiveCollector
from agent_reach.intelligence.momentum import add_momentum
from agent_reach.intelligence.normalize import (
    apply_time_window,
    deduplicate_signals,
    normalize_records,
)
from agent_reach.intelligence.pipeline import _atomic_write_text
from agent_reach.intelligence.schema import Engagement, Signal, iso_sgt, iso_utc, parse_datetime
from agent_reach.intelligence.scoring import score_clusters
from agent_reach.intelligence.source_ledger import build_source_ledger
from agent_reach.intelligence.verification import verify_topics

UTC = timezone.utc


def make_signal(title: str, *, platform: str = "rss", url: str = "https://example.com/1", published=None, **kwargs):
    return Signal(
        platform=platform,
        backend=kwargs.pop("backend", "test"),
        title=title,
        original_text=kwargs.pop("original_text", title),
        language=kwargs.pop("language", "en"),
        url=url,
        published_at_utc=published or datetime(2026, 9, 21, 2, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 9, 21, 4, tzinfo=UTC),
        author=kwargs.pop("author", "source"),
        source_type=kwargs.pop("source_type", "news"),
        evidence_type=kwargs.pop("evidence_type", "secondary"),
        engagement=kwargs.pop("engagement", Engagement()),
        metadata=kwargs.pop("metadata", {"source_key": url.split('/')[2]}),
        **kwargs,
    )


def test_timestamp_filtering_rejects_old_future_and_missing():
    now = datetime(2026, 9, 21, 12, tzinfo=UTC)
    signals = [
        make_signal("inside", published=now - timedelta(hours=4)),
        make_signal("old", published=now - timedelta(hours=25)),
        make_signal("future", published=now + timedelta(hours=2)),
        make_signal("missing", published=None),
    ]
    signals[-1].published_at_utc = None
    accepted, rejected = apply_time_window(signals, now_utc=now, window_hours=24)
    assert [signal.title for signal in accepted] == ["inside"]
    assert {signal.rejection_reason for signal in rejected} == {
        "publication timestamp is outside the requested window",
        "publication timestamp is in the future",
        "missing publication timestamp",
    }


def test_timestamp_filtering_rejects_even_small_future_skew():
    now = datetime(2026, 9, 21, 12, tzinfo=UTC)
    accepted, rejected = apply_time_window(
        [make_signal("slightly future", published=now + timedelta(minutes=1))],
        now_utc=now,
        window_hours=24,
    )
    assert accepted == []
    assert rejected[0].rejection_reason == "publication timestamp is in the future"


def test_timezone_conversion_is_explicit():
    value = datetime(2026, 9, 21, 4, 0, tzinfo=UTC)
    assert iso_utc(value) == "2026-09-21T04:00:00Z"
    assert iso_sgt(value) == "2026-09-21T12:00:00+08:00"


def test_compact_platform_date_is_parsed_as_utc_date():
    assert iso_utc(parse_datetime("20260921")) == "2026-09-21T00:00:00Z"


def test_multilingual_normalization_preserves_original_and_translation(monkeypatch):
    monkeypatch.setattr(
        "agent_reach.intelligence.normalize._translate_text",
        lambda text, language: "AI policy update",
    )
    signals = normalize_records(
        [
            {
                "platform": "bilibili",
                "backend": "B站搜索 API",
                "title": "人工智能政策更新",
                "text": "原始中文正文",
                "language": "zh",
                "url": "https://www.bilibili.com/video/BV1",
                "published_at": "2026-09-21T02:00:00Z",
            }
        ],
        retrieved_at_utc=datetime(2026, 9, 21, 4, tzinfo=UTC),
        multilingual=True,
    )
    assert signals[0].title == "人工智能政策更新"
    assert signals[0].original_text == "原始中文正文"
    assert signals[0].english_translation == "AI policy update"
    assert signals[0].language == "zh"


def test_duplicate_detection_collapses_same_event_but_not_generic_overlap():
    first = make_signal("OpenAI launches new safety model", platform="rss", url="https://a.example/story")
    second = make_signal("OpenAI launches new safety model", platform="youtube", url="https://youtube.com/watch?v=1")
    unrelated = make_signal("OpenAI posts a cooking recipe", platform="v2ex", url="https://v2ex.com/t/2")
    clusters = cluster_signals([first, second, unrelated])
    assert sorted(len(cluster.signals) for cluster in clusters) == [1, 2]


def test_clustering_does_not_merge_unrelated_syndicated_headlines():
    titles = [
        "Atletico Madrid wins derby and beats Real Madrid",
        "Juventus wins 2-0 over Atalanta",
        "Manchester City won 5-3 over Sunderland",
        "Fresh air that Yogyakarta longs for",
    ]
    signals = [
        make_signal(
            title,
            url=f"https://news.google.com/rss/{index}",
            author="ANTARA News Yogyakarta",
            metadata={"source_key": "antara:yogyakarta", "publisher": "ANTARA News Yogyakarta"},
        )
        for index, title in enumerate(titles, 1)
    ]
    clusters = cluster_signals(signals)
    assert sorted(len(cluster.signals) for cluster in clusters) == [1, 1, 1, 1]


def test_duplicate_canonical_urls_are_removed_before_ranking():
    first = make_signal("Same source item", url="https://example.com/story?utm_source=feed")
    second = make_signal("Same source item", url="https://EXAMPLE.com/story/")
    unique, duplicates = deduplicate_signals([first, second])
    assert len(unique) == 1
    assert len(duplicates) == 1
    assert duplicates[0].accepted_in_window is False
    assert "duplicate canonical URL" in (duplicates[0].rejection_reason or "")


def test_date_words_do_not_merge_unrelated_current_headlines():
    astro = make_signal(
        "10 Berita Pilihan - 20 September 2026 - Astro Awani",
        platform="rss",
        url="https://news.google.com/rss/articles/astro",
        author="Astro Awani",
        metadata={"source_key": "astroawani", "publisher": "Astro Awani"},
    )
    github = make_signal(
        "Latest 15 Papers - September 21, 2026",
        platform="github",
        url="https://github.com/example/repo/issues/245",
        author="repository",
        source_type="developer_community",
        evidence_type="direct_post",
        metadata={"source_key": "github:example/repo"},
    )
    assert len(cluster_signals([astro, github])) == 2


def test_scoring_has_separate_virality_consequence_and_independent_sources():
    signals = [
        make_signal(
            "Government announces earthquake emergency response",
            platform="rss",
            url="https://reuters.com/story",
            author="Reuters",
            source_type="news",
            engagement=Engagement(comments=20),
        ),
        make_signal(
            "Government announces earthquake emergency response",
            platform="v2ex",
            url="https://v2ex.com/t/1",
            author="community",
            source_type="community",
        ),
    ]
    topic = score_clusters(cluster_signals(signals), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    assert topic["independent_source_count"] == 2
    assert topic["virality_score"] >= 0
    assert topic["consequence_score"] >= 90
    assert sum(item["points"] for item in topic["score_components"].values()) == topic["importance_score"]


def test_social_buzz_is_primary_and_news_only_is_not_social_buzz():
    news = make_signal(
        "War policy development",
        platform="rss",
        url="https://reuters.com/war-policy",
        source_type="news",
    )
    social = make_signal(
        "Community discusses a new game trend",
        platform="v2ex",
        url="https://v2ex.com/t/42",
        source_type="community",
    )
    topics = score_clusters(
        cluster_signals([news, social]),
        datetime(2026, 9, 21, 4, tzinfo=UTC),
    )
    assert topics[0]["social_platforms"] == ["v2ex"]
    assert topics[0]["social_buzz_score"] > topics[1]["social_buzz_score"]
    assert sum(item["points"] for item in topics[0]["social_buzz_components"].values()) == topics[0]["social_buzz_score"]


def test_null_engagement_is_retained_and_does_not_crash():
    signal = make_signal("No metrics supplied", engagement=Engagement())
    topic = score_clusters(cluster_signals([signal]), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    assert topic["signals"][0].engagement.to_dict() == {
        "likes": None,
        "comments": None,
        "shares": None,
        "views": None,
    }


def test_verification_labels_unverified_and_disputed():
    unverified = {
        "topic_id": "topic-001",
        "signals": [make_signal("Unverified screenshot claim", source_type="community", original_text="unverified screenshot")],
    }
    verify_topics([unverified])
    assert unverified["verification"] == "UNVERIFIED"
    disputed = {
        "topic_id": "topic-002",
        "signals": [make_signal("Claim denied", source_type="news", original_text="rumor claim denied")],
    }
    verify_topics([disputed])
    assert disputed["verification"] == "DISPUTED"


def test_unavailable_platforms_are_not_reported_as_queried(monkeypatch):
    doctor = {
        "twitter": {"status": "warn", "message": "Twitter CLI 未安装", "active_backend": None},
        "web": {"status": "ok", "message": "Jina Reader", "active_backend": "Jina Reader"},
    }
    collector = LiveCollector()
    result = CollectionResult(doctor=doctor)
    result.coverage = {
        "twitter": Coverage("twitter", None),
        "web": Coverage("web", "Jina Reader"),
    }
    collector.DISCOVERY_PLATFORMS = ("twitter", "web")
    collector._mark_unavailable(result)
    assert result.coverage["twitter"].queried is False
    assert result.coverage["twitter"].status == "UNAVAILABLE"
    assert result.coverage["web"].status == "NOT QUERIED"


def test_malformed_platform_response_is_degraded(monkeypatch):
    collector = LiveCollector()
    result = CollectionResult(
        doctor={"bilibili": {"status": "ok", "active_backend": "B站搜索 API"}},
        coverage={"bilibili": Coverage("bilibili", "B站搜索 API")},
    )
    monkeypatch.setattr("agent_reach.intelligence.collector._http_json", lambda *args, **kwargs: {"data": None})
    collector._collect_bilibili(result)
    assert result.coverage["bilibili"].queried is True
    assert result.coverage["bilibili"].status == "UNAVAILABLE"


def test_source_ledger_preserves_direct_url_and_independence_group():
    signal = make_signal(
        "Official policy update",
        url="https://example.gov/update?utm_source=test",
        source_type="official",
        evidence_type="official",
        metadata={"source_key": "example.gov"},
    )
    topic = score_clusters(cluster_signals([signal]), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    verify_topics([topic])
    ledger = build_source_ledger([signal], [topic])
    assert ledger[0]["original_url"].endswith("utm_source=test")
    assert ledger[0]["canonical_url"] == "https://example.gov/update"
    assert ledger[0]["independence_group"] == "example.gov"
    assert ledger[0]["topic_id"] == topic["topic_id"]


def test_momentum_transitions_are_explicit():
    signal = make_signal("Policy update", url="https://example.gov/update")
    topic = score_clusters(cluster_signals([signal]), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    add_momentum([topic], {"topics": []})
    assert topic["momentum_status"] == "NEW"
    previous = {"history_count": 2, "topics": [{"canonical_title": topic["canonical_title"], "signals": [{"title": "Policy update"}], "latest_signal_utc": "2026-09-21T03:00:00Z", "momentum_status": "DECELERATING"}]}
    second = score_clusters(cluster_signals([signal, make_signal("Policy update", url="https://example.gov/update-2")]), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    add_momentum([second], previous)
    assert second["momentum_status"] in {"ACCELERATING", "RESURGING"}


def test_momentum_does_not_claim_acceleration_with_one_prior_run():
    signal = make_signal("Policy update", url="https://example.gov/update")
    topic = score_clusters(cluster_signals([signal]), datetime(2026, 9, 21, 4, tzinfo=UTC))[0]
    add_momentum(
        [topic],
        {
            "history_count": 1,
            "topics": [{"canonical_title": topic["canonical_title"], "signals": [{"title": "Policy update"}]}],
        },
    )
    assert topic["momentum_status"] == "INSUFFICIENT_HISTORY"


def test_verification_reads_only_one_current_source_per_topic(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "agent_reach.channels.web.WebChannel.read_bounded",
        lambda _reader, url: calls.append(url) or ("verified page " * 30),
    )
    topic = {
        "topic_id": "topic-read-budget",
        "signals": [
            make_signal("Current source", url="https://example.com/current"),
            make_signal("Supporting source", url="https://example.org/support"),
            make_signal("Community source", url="https://example.net/community"),
        ],
    }
    verify_topics([topic], perform_reads=True)
    assert calls == ["https://example.com/current"]
    assert topic["verification_notes"]["url_statuses"]
    assert next(iter(topic["verification_notes"]["url_statuses"].values())) == "URL_OK"


def test_global_timeout_marks_unfinished_channels(monkeypatch):
    doctor = {platform: {"status": "ok", "active_backend": "test"} for platform in LiveCollector.DISCOVERY_PLATFORMS}
    monkeypatch.setattr("agent_reach.intelligence.collector.check_all", lambda _config: doctor)
    collector = LiveCollector(global_timeout_seconds=1)

    def expire_after_first(result):
        result.coverage["rss"].queried = True
        result.coverage["rss"].status = "TIMEOUT"
        collector._deadline = time.monotonic() - 1

    monkeypatch.setattr(collector, "_collect_rss", expire_after_first)
    result = collector.collect()
    assert result.coverage["v2ex"].status == "TIMEOUT"
    assert result.coverage["github"].status == "TIMEOUT"
    assert result.collection_finished_at_utc is not None


def test_atomic_artifact_write_leaves_no_temp_file(tmp_path):
    target = tmp_path / "latest.json"
    _atomic_write_text(target, '{"ok": true}\n')
    assert target.read_text(encoding="utf-8") == '{"ok": true}\n'
    assert list(tmp_path.glob("*.tmp")) == []

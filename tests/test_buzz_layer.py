from __future__ import annotations

import json

from agent_reach.buzz.anchor_relevance import score_candidate
from agent_reach.buzz.buzzpack import build_pack
from agent_reach.buzz.edition_anchors import _editorial_anchors, _headline_anchors
from agent_reach.buzz.language_recheck import recheck
from agent_reach.buzz.query_packs import build_query_packs
from agent_reach.buzz.raw_receipts import RawReceiptStore
from agent_reach.buzz.video_relevance import gate


def test_live_markup_freezes_headline_and_section_content_hashes():
    markup = '<body data-release-id="R1"><article class="headline-record" id="H01" data-topic="CXMT LPDDR5X mass production"><h2>ignored</h2></article></body>'
    headlines = _headline_anchors("/headlines/", markup, "R1", "2026-09-22T08:00:00+08:00")
    assert headlines[0]["anchor_id"] == "H01"
    assert headlines[0]["content_hash"]
    editorial = '<section class="editorial-section" data-editorial-section="1"><header><h2>What changed</h2></header><p>CXMT entered production.</p></section></section>'
    sections = _editorial_anchors("/the-full-immersive/today/", "today", editorial, "R1", None)
    assert sections[0]["anchor_id"] == "EDITORIAL-TODAY-S01"


def test_query_pack_is_anchor_specific_and_has_platform_plan():
    registry = {"edition_id": "R1", "anchors": [{"anchor_id": "H01", "content_hash": "abc", "anchor_type": "headline", "published_text": "CXMT LPDDR5X entered mass production in China", "entities": ["CXMT"]}]}
    pack = build_query_packs(registry)["packs"][0]
    assert "cxmt" in pack["primary_queries"][0].casefold()
    assert pack["platform_plan"]["youtube"]["applicable"] is True


def test_relevance_and_video_gate_reject_generic_video():
    anchor = {"published_text": "CXMT LPDDR5X entered mass production", "entities": ["CXMT"]}
    record = {"title": "AI news roundup", "description": "A broad technology roundup", "source_type": "video", "metadata": {"direct_content": True, "content_fields_checked": ["title", "description"]}}
    assert score_candidate(anchor, record)["accepted"] is False
    assert gate(anchor, record)[0] is False


def test_language_recheck_overrides_stale_upstream_label():
    value = recheck({"title": "长鑫存储 LPDDR5X 量产", "language": "en"}, translate=False)
    assert value["language"] == "zh"
    assert value["language_upstream"] == "en"


def test_raw_receipt_hash_and_pack_empty_state(tmp_path):
    store = RawReceiptStore(tmp_path, "run-1")
    receipt = store.write(platform="v2ex", query_id="q1", envelope={"query": "CXMT", "records": []})
    store.save_manifest(tmp_path, "run-1")
    manifest = json.loads((tmp_path / "runs" / "run-1" / "raw_manifest.json").read_text())
    assert receipt == "raw-v2ex-q1"
    assert manifest["receipts"][0]["sha256"]
    pack = build_pack({"edition_id": "R1", "anchor_id": "H01", "anchor_type": "headline", "route": "/headlines/#H01", "content_hash": "hash", "_run_retrieved_at_utc": "2026-09-22T00:00:00Z"}, {"query_pack_id": "qp-1"}, records=[], attempted_lanes=[], raw_receipt_ids=[receipt], run_id="run-1")
    assert pack["searched_empty"] is True
    assert pack["parent_content_hash"] == "hash"

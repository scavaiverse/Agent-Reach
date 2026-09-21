# -*- coding: utf-8 -*-
"""Conservative relationship discovery between ranked topics."""

from __future__ import annotations

from typing import Any

from .cluster import strong_entities

_CONNECTION_STOP = {
    "about", "after", "against", "announced", "announcement", "including", "officially",
    "reading", "started", "these", "there", "which", "their", "other", "country",
    "countries", "government", "minister", "official", "officials", "provide", "reported",
    "reports", "relation", "relations", "trade", "united", "world", "global", "latest",
    "today", "news", "update", "technology", "business", "company", "market", "people",
}


def _usable_shared_entity(value: str) -> bool:
    normalized = value.casefold().strip()
    if normalized in _CONNECTION_STOP or len(normalized) < 5:
        return False
    return any(character.isalpha() for character in normalized)


def discover_connections(topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relationships: list[dict[str, Any]] = []
    entity_sets = {
        topic["topic_id"]: set().union(*(strong_entities(signal) for signal in topic["signals"]))
        for topic in topics
    }
    for index, left in enumerate(topics):
        for right in topics[index + 1 :]:
            shared = sorted(
                entity
                for entity in entity_sets[left["topic_id"]] & entity_sets[right["topic_id"]]
                if _usable_shared_entity(entity)
            )
            if not shared:
                continue
            left_domains = {
                signal.url.split("/", 3)[2].casefold()
                for signal in left["signals"]
                if signal.url.count("/") >= 2
            }
            right_domains = {
                signal.url.split("/", 3)[2].casefold()
                for signal in right["signals"]
                if signal.url.count("/") >= 2
            }
            label = "DIRECT CONNECTION" if len(shared) >= 3 and left_domains & right_domains else "PLAUSIBLE CONNECTION"
            relationships.append(
                {
                    "from_topic": left["topic_id"],
                    "to_topic": right["topic_id"],
                    "label": label,
                    "shared_entities": shared[:8],
                    "basis": "shared named entities in independently collected signals",
                }
            )
    return relationships[:20]

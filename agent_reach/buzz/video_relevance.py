"""Strict publication gate for social video URLs."""

from __future__ import annotations

from typing import Any


def gate(anchor: dict[str, Any], record: dict[str, Any]) -> tuple[bool, str]:
    if record.get("source_type") != "video":
        return True, "not_a_video"
    metadata = record.get("metadata") or {}
    checked = metadata.get("content_fields_checked") or []
    if "description" not in checked or not record.get("description"):
        return False, "title_only_or_description_missing"
    text = f"{record.get('title') or ''} {record.get('description') or ''}".casefold()
    entities = [str(x).casefold() for x in anchor.get("entities", []) if len(str(x)) > 2]
    if entities and not any(entity in text for entity in entities):
        return False, "distinctive_anchor_entity_not_in_direct_content"
    return True, "title_and_description_confirm_direct_anchor_relation"

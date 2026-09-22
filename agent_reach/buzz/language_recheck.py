"""Re-detect language from retrieved text, then translate only accepted records."""

from __future__ import annotations

from typing import Any

from agent_reach.intelligence.normalize import _translate_text, detect_language


def recheck(record: dict[str, Any], *, translate: bool = True) -> dict[str, Any]:
    original = " ".join(str(record.get(k) or "") for k in ("title", "original_text", "description")).strip()
    language = detect_language(original, None)
    record = dict(record)
    record["language_detected"] = language
    record["language_upstream"] = record.get("language")
    record["language"] = language
    if translate and language not in {"en", "unknown"}:
        translated = _translate_text(original[:1200], language)
        record["english_translation"] = translated
        if not translated:
            record["translation_status"] = "TRANSLATION_PENDING"
    return record

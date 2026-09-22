"""Append-only raw upstream envelopes and hashes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .schema import json_dump, sha256


class RawReceiptStore:
    def __init__(self, output_dir: str | Path, run_id: str) -> None:
        self.root = Path(output_dir) / "runs" / run_id / "raw" / "platform"
        self.root.mkdir(parents=True, exist_ok=True)
        self.manifest: list[dict[str, Any]] = []

    def write(self, *, platform: str, query_id: str, envelope: dict[str, Any]) -> str:
        folder = self.root / platform
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{query_id}.json"
        payload = json_dump(envelope)
        path.write_text(payload, encoding="utf-8")
        digest = sha256(payload)
        receipt_id = f"raw-{platform}-{query_id}"
        self.manifest.append({"receipt_id": receipt_id, "platform": platform, "query_id": query_id, "path": str(path), "sha256": digest})
        return receipt_id

    def save_manifest(self, output_dir: str | Path, run_id: str, **meta: Any) -> None:
        target = Path(output_dir) / "runs" / run_id
        target.mkdir(parents=True, exist_ok=True)
        (target / "raw_manifest.json").write_text(json_dump({"run_id": run_id, "receipts": self.manifest, **meta}), encoding="utf-8")

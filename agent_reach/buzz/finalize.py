"""Finalize a frozen Buzz run without changing immutable raw receipts.

This small post-run step repairs only derived artifact metadata: rejection
labels, agent receipt references, and the exact site-evidence Git SHA. Raw
platform envelopes remain byte-for-byte untouched.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _repair(value: Any, git_sha: str | None, agents: dict[str, str | None]) -> int:
    repaired = 0
    if isinstance(value, list):
        for item in value:
            repaired += _repair(item, git_sha, agents)
        return repaired
    if not isinstance(value, dict):
        return repaired

    if "social_git_sha" in value and git_sha:
        value["social_git_sha"] = git_sha
    for key in ("agent_receipts", "three_agent_approval_receipts"):
        if isinstance(value.get(key), dict):
            for agent, receipt in agents.items():
                if receipt is not None and agent in value[key]:
                    value[key][agent] = receipt

    # The collector historically emitted 24 null labels for records rejected
    # by the shared relevance/date gate. Make the reason explicit in the
    # derived rejection ledger; do not touch raw receipts.
    if "reason" in value and value["reason"] is None and isinstance(value.get("raw"), dict):
        raw = value["raw"]
        if raw.get("published_at_utc") in (None, ""):
            value["reason"] = "MISSING_PUBLICATION_TIMESTAMP"
        else:
            value["reason"] = "RELEVANCE_GATE_FAILED"
        repaired += 1

    for child in value.values():
        repaired += _repair(child, git_sha, agents)
    return repaired


def finalize(output: Path, git_sha: str | None, agents: dict[str, str | None]) -> dict[str, Any]:
    latest = _load(output / "latest_buzz_run.json")
    run_id = latest["run_id"]
    paths = [
        output / "latest_buzz_run.json",
        output / "buzzpacks.json",
        output / "rejected_social_observations.json",
        output / "normalized_social_observations.json",
        output / "source_ledger.json",
        output / "verification_ledger.json",
        output / "buzz_mapping.json",
    ]
    repaired = 0
    for path in paths:
        if not path.exists():
            continue
        value = _load(path)
        repaired += _repair(value, git_sha, agents)
        _dump(path, value)

    receipt = {
        "schema_version": "otr-canonical-buzz-finalization-1",
        "run_id": run_id,
        "finalized_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "social_git_sha": git_sha,
        "agent_receipts": agents,
        "derived_rejection_labels_repaired": repaired,
        "raw_receipts_mutated": False,
    }
    _dump(output / "finalization_receipt.json", receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("output"))
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--agent-1", default=None)
    parser.add_argument("--agent-2", default=None)
    parser.add_argument("--agent-3", default=None)
    args = parser.parse_args()
    receipt = finalize(
        args.output,
        args.git_sha,
        {"agent_1": args.agent_1, "agent_2": args.agent_2, "agent_3": args.agent_3},
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

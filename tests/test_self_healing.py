from __future__ import annotations

import json
import time
from pathlib import Path

from agent_reach.intelligence.self_healing import SelfHealingCoordinator


def test_stale_lease_is_removed_before_new_run(tmp_path: Path):
    state = tmp_path / "self_healing"
    state.mkdir(parents=True)
    (state / "active_lease.json").write_text(
        json.dumps({"run_id": "old", "status": "RUNNING", "expires_at_epoch": time.time() - 10}),
        encoding="utf-8",
    )
    coordinator = SelfHealingCoordinator.begin(tmp_path, budget_seconds=10)
    assert coordinator.run_id.startswith("coord-")
    assert not (state / "active_lease.json").exists() or json.loads((state / "active_lease.json").read_text())['run_id'] == coordinator.run_id
    coordinator.finish(status="COMPLETE")


def test_repair_circuit_opens_on_third_same_root_cause(tmp_path: Path):
    coordinator = SelfHealingCoordinator.begin(tmp_path, budget_seconds=10)
    first = coordinator.repair_attempt("translation", change="bounded fallback", outcome="failed")
    second = coordinator.repair_attempt("translation", change="bounded fallback", outcome="failed")
    third = coordinator.repair_attempt("translation", change="bounded fallback", outcome="failed")
    assert first["circuit"] == "CLOSED"
    assert second["circuit"] == "CLOSED"
    assert third["circuit"] == "OPEN"
    coordinator.finish(status="DEGRADED")


def test_optional_stage_fails_open_and_checkpoints(tmp_path: Path):
    coordinator = SelfHealingCoordinator.begin(tmp_path, budget_seconds=10)
    value, state = coordinator.run_optional("optional-index", lambda: (_ for _ in ()).throw(RuntimeError("broken")), {"fallback": True})
    assert value == {"fallback": True}
    assert state["status"] == "DEGRADED"
    assert (tmp_path / "self_healing" / "runs" / coordinator.run_id / "optional-index.json").exists()
    coordinator.finish(status="DEGRADED")

# -*- coding: utf-8 -*-
"""Bounded run coordination, checkpoints, leases, and repair circuits.

This module deliberately coordinates the existing Agent-Reach acquisition
layers. It does not fabricate a result or silently convert a failed lane into
success. Every state transition is an atomic, JSON-readable record keyed by a
run id so a later run can resume or safely degrade.
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")
UTC = timezone.utc


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass
class SelfHealingCoordinator:
    output_dir: Path
    run_id: str
    deadline_monotonic: float
    lease_path: Path
    checkpoint_dir: Path
    max_repair_attempts: int = 3

    @classmethod
    def begin(cls, output_dir: str | Path, *, budget_seconds: int) -> "SelfHealingCoordinator":
        output = Path(output_dir)
        state_dir = output / "self_healing"
        state_dir.mkdir(parents=True, exist_ok=True)
        cls._cleanup_stale_leases(state_dir)
        lease_path = state_dir / "active_lease.json"
        if lease_path.exists():
            try:
                active = json.loads(lease_path.read_text(encoding="utf-8"))
                if active.get("status") == "RUNNING" and float(active.get("expires_at_epoch", 0)) > time.time():
                    raise RuntimeError(f"active Social Pulse run lease exists: {active.get('run_id', 'unknown')}")
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                lease_path.unlink(missing_ok=True)
        run_id = "coord-" + uuid.uuid4().hex[:16]
        lease = {
            "run_id": run_id,
            "owner": f"{socket.gethostname()}:{os.getpid()}",
            "started_at_utc": _now(),
            "heartbeat_at_utc": _now(),
            "expires_at_epoch": time.time() + max(1, budget_seconds),
            "status": "RUNNING",
        }
        _atomic_json(lease_path, lease)
        checkpoint_dir = state_dir / "runs" / run_id
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        coordinator = cls(output, run_id, time.monotonic() + max(1, budget_seconds), lease_path, checkpoint_dir)
        coordinator.checkpoint("preflight", {"status": "STARTED", "run_id": run_id})
        return coordinator

    @staticmethod
    def _cleanup_stale_leases(state_dir: Path) -> None:
        lease_path = state_dir / "active_lease.json"
        if not lease_path.exists():
            return
        try:
            lease = json.loads(lease_path.read_text(encoding="utf-8"))
            expired = float(lease.get("expires_at_epoch", 0)) < time.time()
            if expired or lease.get("status") != "RUNNING":
                lease_path.unlink(missing_ok=True)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            # A malformed optional lease must not deadlock the next run.
            lease_path.unlink(missing_ok=True)

    @property
    def remaining_seconds(self) -> int:
        return max(0, int(self.deadline_monotonic - time.monotonic()))

    def heartbeat(self, **extra: Any) -> None:
        try:
            lease = json.loads(self.lease_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lease = {"run_id": self.run_id}
        lease.update({"heartbeat_at_utc": _now(), "remaining_seconds": self.remaining_seconds, **extra})
        _atomic_json(self.lease_path, lease)

    def checkpoint(self, stage: str, payload: Any, *, status: str = "COMPLETE") -> Path:
        record = {
            "run_id": self.run_id,
            "stage": stage,
            "status": status,
            "written_at_utc": _now(),
            "payload": payload,
        }
        path = self.checkpoint_dir / f"{stage}.json"
        _atomic_json(path, record)
        self.heartbeat(stage=stage, status=status)
        return path

    def latest_checkpoint(self, stage: str) -> dict[str, Any] | None:
        """Return the newest valid checkpoint for crash recovery inspection."""

        candidates = sorted(
            (self.output_dir / "self_healing" / "runs").glob(f"*/{stage}.json"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(record, dict) and record.get("status") in {"COMPLETE", "DEGRADED"}:
                    return record
            except (OSError, json.JSONDecodeError):
                continue
        return None

    def repair_attempt(self, root_cause: str, *, change: str, outcome: str) -> dict[str, Any]:
        path = self.output_dir / "self_healing" / "repair_log.json"
        rows: list[dict[str, Any]] = []
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                rows = loaded if isinstance(loaded, list) else []
            except (OSError, json.JSONDecodeError):
                rows = []
        attempts = sum(1 for row in rows if row.get("root_cause") == root_cause) + 1
        row = {
            "run_id": self.run_id,
            "root_cause": root_cause,
            "attempt": attempts,
            "change": change,
            "outcome": outcome,
            "timestamp_utc": _now(),
            "circuit": "OPEN" if attempts >= self.max_repair_attempts else "CLOSED",
        }
        rows.append(row)
        _atomic_json(path, rows[-500:])
        return row

    def run_optional(self, name: str, function: Callable[[], T], fallback: T) -> tuple[T, dict[str, Any]]:
        """Run an optional stage without allowing its fault to erase good data."""

        if self.remaining_seconds <= 0:
            state = {"stage": name, "status": "TIMED_OUT", "error": "run budget exhausted"}
            self.checkpoint(name, state, status="TIMED_OUT")
            return fallback, state
        try:
            value = function()
            state = {"stage": name, "status": "COMPLETE"}
            self.checkpoint(name, state)
            return value, state
        except Exception as exc:  # optional lanes are fail-open by design
            error = f"{type(exc).__name__}: {exc}"[:500]
            repair = self.repair_attempt(name, change="bounded optional-stage recovery", outcome=error)
            state = {"stage": name, "status": "DEGRADED", "error": error, "repair": repair}
            self.checkpoint(name, state, status="DEGRADED")
            return fallback, state

    def finish(self, *, status: str, summary: dict[str, Any] | None = None) -> None:
        self.checkpoint("final", {"status": status, **(summary or {})}, status=status)
        try:
            lease = json.loads(self.lease_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lease = {"run_id": self.run_id}
        lease.update({"status": status, "finished_at_utc": _now(), "heartbeat_at_utc": _now()})
        _atomic_json(self.lease_path, lease)
        self.lease_path.unlink(missing_ok=True)

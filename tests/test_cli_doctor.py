from __future__ import annotations

from typing import Any

from cao_control_plane.cli import _doctor


class _DoctorDatabase:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts

    def integrity_check(self) -> dict[str, Any]:
        return {"ok": True, "result": ["ok"]}

    def fetchone(self, query: str) -> dict[str, int]:
        if "effect_operations WHERE status = 'started'" in query:
            return {"count": self.counts.get("started_effects", 0)}
        if "effect_operations WHERE status = 'unknown'" in query:
            return {"count": self.counts.get("unknown_effects", 0)}
        if "message_deliveries WHERE state = 'dispatched'" in query:
            return {"count": self.counts.get("unknown_deliveries", 0)}
        if "message_deliveries WHERE state = 'dead'" in query:
            return {"count": self.counts.get("dead_deliveries", 0)}
        if "a2a_push_deliveries WHERE state = 'dead'" in query:
            return {"count": self.counts.get("dead_pushes", 0)}
        raise AssertionError(f"unexpected doctor query: {query}")


class _DoctorService:
    def __init__(self, counts: dict[str, int]) -> None:
        self.db = _DoctorDatabase(counts)

    def expire_runtime_leases(self) -> int:
        return 0

    def list_principals(self) -> list[dict[str, Any]]:
        return []

    def list_runtimes(self) -> list[dict[str, Any]]:
        return []


def test_doctor_blocks_unknown_outcomes_and_degrades_terminal_history(settings) -> None:
    result = _doctor(
        _DoctorService(
            {
                "started_effects": 1,
                "unknown_effects": 2,
                "unknown_deliveries": 3,
                "dead_deliveries": 4,
                "dead_pushes": 5,
            }
        ),
        settings,
    )

    assert result["ok"] is False
    assert result["degraded"] is True
    assert result["blocking_conditions"] == [
        "unknown_effect_outcome",
        "unknown_delivery_outcome",
        "dead_push_delivery",
    ]
    assert result["degraded_conditions"] == [
        "effect_in_progress",
        "dead_delivery_history",
    ]
    assert result["unresolved_effect_operations"] == 3


def test_doctor_reports_expected_dead_delivery_history_without_blocking(settings) -> None:
    result = _doctor(_DoctorService({"dead_deliveries": 2}), settings)

    assert result["ok"] is True
    assert result["degraded"] is True
    assert result["blocking_conditions"] == []
    assert result["degraded_conditions"] == ["dead_delivery_history"]

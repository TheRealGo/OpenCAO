"""Display-only timestamp formatting for Dashboard presentation surfaces.

Canonical Dashboard timestamps remain ISO-8601 instants in UTC.  This module
derives bounded, human-facing text at the last presentation boundary, using
the host operating system's current timezone unless a timezone is supplied by
a deterministic test.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, tzinfo
from typing import Any

_CANONICAL_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_SAFE_ZONE_LABEL = re.compile(r"^[A-Za-z][A-Za-z0-9:+-]{0,15}$")
_INTERNAL_RUNTIME_FIELDS = frozenset(
    {
        "cooldown_until",
        "provider_condition",
        "provider_retry_after_at",
        "provider_retry_after_at_display",
    }
)


def local_timestamp_display(value: object, *, timezone: tzinfo | None = None) -> str | None:
    """Return one safe local timestamp, or ``None`` for non-canonical input."""

    if (
        not isinstance(value, str)
        or len(value) > 64
        or _CANONICAL_TIMESTAMP.fullmatch(value) is None
    ):
        return None
    try:
        instant = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        local = instant.astimezone(timezone) if timezone is not None else instant.astimezone()
    except (OverflowError, ValueError):
        return None

    rendered = local.strftime("%Y-%m-%d %H:%M:%S")
    if "." in value and local.microsecond:
        rendered += f".{local.microsecond:06d}".rstrip("0")
    return f"{rendered} {_safe_zone_label(local)}"


def native_dashboard_snapshot(
    snapshot: Mapping[str, Any], *, timezone: tzinfo | None = None
) -> dict[str, Any]:
    """Add local display fields to a native snapshot without changing its UTC data."""

    rendered = copy.deepcopy(dict(snapshot))
    operator = rendered.get("operator")
    if not isinstance(operator, dict):
        return rendered

    visited: set[int] = set()

    def add_work_items(value: object) -> None:
        if not isinstance(value, list):
            return
        for item in value:
            if not isinstance(item, dict) or id(item) in visited:
                continue
            visited.add(id(item))
            # Provider failure and scheduling state is an internal runtime
            # circuit, not part of the native human/model presentation.
            for key in _INTERNAL_RUNTIME_FIELDS:
                item.pop(key, None)
            availability = item.get("availability")
            if isinstance(availability, dict):
                for key in _INTERNAL_RUNTIME_FIELDS:
                    availability.pop(key, None)
            item["latest_reported_at_display"] = local_timestamp_display(
                item.get("latest_reported_at"), timezone=timezone
            )
            for field in (
                "runtime_heartbeat_at",
                "last_worker_activity_at",
                "last_artifact_at",
                "status_requested_at",
                "status_response_due_at",
                "status_responded_at",
                "cao_supervision_updated_at",
                "completed_at",
            ):
                item[f"{field}_display"] = local_timestamp_display(
                    item.get(field), timezone=timezone
                )

    add_work_items(operator.get("work_items"))
    add_work_items(operator.get("recently_completed"))
    for category in (
        "needs_attention",
        "cao_processing",
        "user_confirmation",
        "stopped_or_failed",
        "working",
        "ready",
        "inactive_workers",
    ):
        workers = operator.get(category)
        if not isinstance(workers, list):
            continue
        for worker in workers:
            if isinstance(worker, dict):
                add_work_items(worker.get("current_work_items"))
    return rendered


def _safe_zone_label(value: datetime) -> str:
    label = value.tzname()
    if isinstance(label, str) and _SAFE_ZONE_LABEL.fullmatch(label):
        return label
    offset = value.utcoffset()
    if offset is None:
        return "UTC"
    return _offset_label(offset)


def _offset_label(offset: timedelta) -> str:
    seconds = int(offset.total_seconds())
    if seconds == 0:
        return "UTC"
    sign = "+" if seconds >= 0 else "-"
    minutes = abs(seconds) // 60
    hours, remainder = divmod(minutes, 60)
    return f"UTC{sign}{hours:02d}:{remainder:02d}"

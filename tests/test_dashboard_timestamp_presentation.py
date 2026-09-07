from __future__ import annotations

import copy
from zoneinfo import ZoneInfo

from cao_control_plane.dashboard_presentation import (
    local_timestamp_display,
    native_dashboard_snapshot,
)


def test_known_utc_instant_is_displayed_as_jst_without_changing_canonical_value() -> None:
    canonical = "2026-08-12T00:00:00Z"
    assert local_timestamp_display(canonical, timezone=ZoneInfo("Asia/Tokyo")) == (
        "2026-08-12 09:00:00 JST"
    )

    snapshot = {
        "format": "cao-dashboard-read-model/v1",
        "snapshot_digest": "a" * 64,
        "operator": {
            "working": [
                {
                    "current_work_items": [
                        {"latest_reported_at": canonical, "work_title": "Safe title"}
                    ]
                }
            ],
            "needs_attention": [],
            "ready": [],
            "inactive_workers": [],
            "work_items": [],
        },
    }
    original = copy.deepcopy(snapshot)
    rendered = native_dashboard_snapshot(snapshot, timezone=ZoneInfo("Asia/Tokyo"))
    item = rendered["operator"]["working"][0]["current_work_items"][0]

    assert snapshot == original
    assert item["latest_reported_at"] == canonical
    assert item["latest_reported_at_display"] == "2026-08-12 09:00:00 JST"
    assert rendered["snapshot_digest"] == snapshot["snapshot_digest"]


def test_native_dashboard_resource_omits_internal_runtime_circuit_fields() -> None:
    reported_at = "2026-08-12T00:00:00Z"
    internal_timestamp = "2099-12-31T23:59:59Z"
    item = {
        "latest_reported_at": reported_at,
        "provider_condition": "rate_limited",
        "provider_retry_after_at": internal_timestamp,
        "provider_retry_after_at_display": "internal display",
        "cooldown_until": internal_timestamp,
        "availability": {
            "latest_reported_at": "available",
            "provider_condition": "available",
            "provider_retry_after_at": "available",
            "cooldown_until": "available",
        },
    }
    snapshot = {
        "format": "cao-dashboard-read-model/v1",
        "snapshot_digest": "b" * 64,
        "operator": {
            "working": [{"current_work_items": [copy.deepcopy(item)]}],
            "needs_attention": [],
            "ready": [],
            "inactive_workers": [],
            "work_items": [copy.deepcopy(item)],
        },
    }
    original = copy.deepcopy(snapshot)

    rendered = native_dashboard_snapshot(snapshot, timezone=ZoneInfo("Asia/Tokyo"))

    assert snapshot == original
    assert rendered["snapshot_digest"] == snapshot["snapshot_digest"]
    assert internal_timestamp not in str(rendered)
    assert "rate_limited" not in str(rendered)
    for rendered_item in (
        rendered["operator"]["working"][0]["current_work_items"][0],
        rendered["operator"]["work_items"][0],
    ):
        assert rendered_item["latest_reported_at"] == reported_at
        assert rendered_item["latest_reported_at_display"] == "2026-08-12 09:00:00 JST"
        assert not {
            "cooldown_until",
            "provider_condition",
            "provider_retry_after_at",
            "provider_retry_after_at_display",
        } & rendered_item.keys()
        assert "provider_condition" not in rendered_item["availability"]
        assert "provider_retry_after_at" not in rendered_item["availability"]
        assert "cooldown_until" not in rendered_item["availability"]


def test_dst_zone_uses_standard_timezone_rules_for_winter_and_summer() -> None:
    new_york = ZoneInfo("America/New_York")

    assert (
        local_timestamp_display("2026-01-15T12:00:00Z", timezone=new_york)
        == "2026-01-15 07:00:00 EST"
    )
    assert (
        local_timestamp_display("2026-07-15T12:00:00Z", timezone=new_york)
        == "2026-07-15 08:00:00 EDT"
    )


def test_invalid_timestamp_is_not_echoed_as_display_text() -> None:
    private_value = "/Users/owner/private/credential-token"

    assert local_timestamp_display(private_value, timezone=ZoneInfo("Asia/Tokyo")) is None
    assert private_value not in str(
        native_dashboard_snapshot(
            {"operator": {"working": [{"current_work_items": [{"latest_reported_at": None}]}]}},
            timezone=ZoneInfo("Asia/Tokyo"),
        )
    )

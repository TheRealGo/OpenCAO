from __future__ import annotations

from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.database import utc_now
from cao_control_plane.delivery_lane import (
    cao_notification_dispatch_head_sql,
    cao_notification_inbox_visible_sql,
)
from cao_control_plane.errors import AuthorizationError, ConflictError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    MessageKind,
    ReportInput,
    ReviewInput,
    WorkAssignment,
)
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.service import ControlPlane

PAST = "2000-01-01T00:00:00Z"
FUTURE = "2100-01-01T00:00:00Z"


def _attached(service: ControlPlane, suffix: str) -> dict[str, Any]:
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"notification-lane-{suffix}",
            project_digest="b" * 64,
        ),
    )
    return service.authenticate(str(attachment["context_token"]))


def _report(
    system: dict[str, Any],
    actor: dict[str, Any],
    suffix: str,
    *,
    kind: str = "completion_claim",
) -> dict[str, Any]:
    service: ControlPlane = system["service"]
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Notification lane {suffix}",
            objective="Make an independent result available to its owning conversation.",
            acceptance=["The result remains observable without incorporating another Work."],
            completion_contract="no_artifact_expected",
            idempotency_key=f"notification-work-{suffix}",
        ),
    )
    attempt = work["current_attempt"]
    return service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=kind,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The bounded result is available for review.",
            trajectory="complete" if kind == "completion_claim" else "stalled",
            evidence=[{"check": "no-external-effect", "result": "observed"}],
            idempotency_key=f"notification-report-{suffix}",
        ),
    )


def _message_id(service: ControlPlane, work_id: str, kind: str) -> str:
    row = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = ? "
        "ORDER BY sequence DESC LIMIT 1",
        (work_id, kind),
    )
    assert row is not None
    return str(row["id"])


def _set_delivery(service: ControlPlane, message_id: str, state: str) -> None:
    service.db.execute(
        "UPDATE message_deliveries SET state = ?, lease_until = NULL, owner_token = '', "
        "next_attempt_at = ?, delivered_at = ?, acknowledged_at = ?, handled_at = ? "
        "WHERE message_id = ?",
        (
            state,
            PAST,
            PAST if state in {"delivered", "acknowledged", "handled"} else None,
            PAST if state in {"acknowledged", "handled"} else None,
            PAST if state == "handled" else None,
            message_id,
        ),
    )


@pytest.fixture
def notification_lane(system: dict[str, Any]) -> dict[str, Any]:
    service: ControlPlane = system["service"]
    actor = _attached(service, "one")
    first = _report(system, actor, "review")
    boundary = first["open_boundaries"][0]
    service.acknowledge(
        actor, AckInput(message_ids=[_message_id(service, first["id"], "completion_claim")])
    )
    turn = service.acquire_reasoner_turn(
        actor,
        first["id"],
        boundary_id=boundary["id"],
        expected_generation=first["generation"],
        idempotency_key="notification-review-turn",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=first["generation"],
            kind="accept",
            reason="The no-artifact result can proceed to review.",
        ),
    )
    reviewed = service.review(
        actor,
        ReviewInput(
            attempt_id=first["current_attempt"]["id"],
            verdict="ok",
            summary="The result meets the bounded no-artifact contract.",
            idempotency_key="notification-self-review",
        ),
    )
    assert reviewed["state"] == "waiting_user"
    review_id = _message_id(service, first["id"], "review")
    _set_delivery(service, review_id, "delivered")
    review_row = service.db.fetchone(
        "SELECT m.sender_id, d.recipient_id FROM messages AS m "
        "JOIN message_deliveries AS d ON d.message_id = m.id WHERE m.id = ?",
        (review_id,),
    )
    assert review_row is not None and review_row["sender_id"] == review_row["recipient_id"]

    blocked = _report(system, actor, "blocker", kind="blocker")
    completed = _report(system, actor, "completion")
    return {
        **system,
        "actor": actor,
        "review_id": review_id,
        "blocker_id": _message_id(service, blocked["id"], "blocker"),
        "completion_id": _message_id(service, completed["id"], "completion_claim"),
        "completed_work": completed,
    }


def _dispatch_eligible(service: ControlPlane, message_id: str) -> bool:
    return (
        service.db.fetchone(
            f"SELECT 1 FROM messages AS m JOIN message_deliveries AS d ON d.message_id = m.id "
            f"WHERE m.id = ? AND d.state = 'queued' AND {cao_notification_dispatch_head_sql()}",
            (message_id, utc_now()),
        )
        is not None
    )


def test_mixed_work_notifications_remain_readable_without_prior_incorporation(
    notification_lane: dict[str, Any],
) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    expected = [lane["review_id"], lane["blocker_id"], lane["completion_id"]]

    inbox = service.get_inbox(lane["actor"])
    assert [item["id"] for item in inbox["items"]] == expected
    assert [item["kind"] for item in inbox["items"]] == ["review", "blocker", "completion_claim"]
    first_page = service.get_inbox(lane["actor"], limit=1)
    assert first_page["next_cursor"] is not None
    second_page = service.get_inbox(lane["actor"], after=first_page["next_cursor"], limit=2)
    assert [item["id"] for item in second_page["items"]] == expected[1:]

    service.acknowledge(lane["actor"], AckInput(message_ids=[lane["completion_id"]]))
    with pytest.raises(ConflictError, match="boundary must be disposed"):
        service.mark_message_handled(lane["actor"], lane["completion_id"], evidence="Read result.")
    history_ids = {
        item["id"] for item in service.get_inbox(lane["actor"], include_acknowledged=True)["items"]
    }
    assert set(expected) <= history_ids
    assert len(service.get_inbox(lane["actor"])["items"]) == 2


def test_acknowledged_history_includes_handled_without_changing_delivery_or_attachment(
    notification_lane: dict[str, Any],
) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    _set_delivery(service, lane["review_id"], "handled")
    _set_delivery(service, lane["blocker_id"], "acknowledged")
    before = [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")]

    inbox = service.get_inbox(lane["actor"])
    assert [item["id"] for item in inbox["items"]] == [lane["completion_id"]]
    history = {
        item["id"]: item["delivery_state"]
        for item in service.get_inbox(lane["actor"], include_acknowledged=True)["items"]
    }
    assert history[lane["review_id"]] == "handled"
    assert history[lane["blocker_id"]] == "acknowledged"
    assert history[lane["completion_id"]] == "queued"
    assert _dispatch_eligible(service, lane["completion_id"])
    assert [dict(row) for row in service.db.fetchall("SELECT * FROM message_deliveries")] == before

    other = _attached(service, "history-other")
    other_history = service.get_inbox(other, include_acknowledged=True)
    assert not (set(history) & {item["id"] for item in other_history["items"]})


@pytest.mark.parametrize("predecessor_state", ["delivered", "acknowledged", "handled", "dead"])
def test_completed_transport_does_not_hold_later_work(
    notification_lane: dict[str, Any], predecessor_state: str
) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    _set_delivery(service, lane["review_id"], predecessor_state)
    dispatcher = Dispatcher(service, lane["settings"])

    claimed = dispatcher._claim_delivery()
    assert claimed is not None and claimed["message_id"] == lane["blocker_id"]
    _set_delivery(service, lane["blocker_id"], "delivered")
    claimed = dispatcher._claim_delivery()
    assert claimed is not None and claimed["message_id"] == lane["completion_id"]
    prior = service.db.fetchone(
        "SELECT state, attempts FROM message_deliveries WHERE message_id = ?",
        (lane["review_id"],),
    )
    assert prior is not None and prior["state"] == predecessor_state and prior["attempts"] == 0


def test_notification_backoff_preserves_submission_order_without_hiding_inbox(
    notification_lane: dict[str, Any],
) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    service.db.execute(
        "UPDATE message_deliveries SET attempts = 1, next_attempt_at = ? WHERE message_id = ?",
        (FUTURE, lane["blocker_id"]),
    )
    assert not _dispatch_eligible(service, lane["completion_id"])
    assert len(service.get_inbox(lane["actor"])["items"]) == 3
    dispatcher = Dispatcher(service, lane["settings"])
    assert dispatcher._claim_delivery() is None

    service.db.execute(
        "UPDATE message_deliveries SET next_attempt_at = ? WHERE message_id = ?",
        (PAST, lane["blocker_id"]),
    )
    claimed = dispatcher._claim_delivery()
    assert claimed is not None and claimed["message_id"] == lane["blocker_id"]
    assert dispatcher._claim_delivery() is None
    _set_delivery(service, lane["blocker_id"], "delivered")
    claimed = dispatcher._claim_delivery()
    assert claimed is not None and claimed["message_id"] == lane["completion_id"]


@pytest.mark.parametrize(
    ("lease_until", "owner", "runtime_state", "eligible"),
    [
        (PAST, "prior-owner", "ready", True),
        (None, "", "busy", True),
        (FUTURE, "prior-owner", "ready", False),
        (PAST, "prior-owner", "busy", False),
    ],
)
def test_dispatch_uncertainty_stays_visible_without_replay(
    notification_lane: dict[str, Any],
    lease_until: str | None,
    owner: str,
    runtime_state: str,
    eligible: bool,
) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched', owner_token = ?, lease_until = ? "
        "WHERE message_id = ?",
        (owner, lease_until, lane["blocker_id"]),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = ? WHERE id = "
        "(SELECT runtime_session_id FROM message_deliveries WHERE message_id = ?)",
        (runtime_state, lane["blocker_id"]),
    )
    before = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?", (lane["blocker_id"],)
    )
    assert _dispatch_eligible(service, lane["completion_id"]) is eligible
    assert lane["blocker_id"] in {item["id"] for item in service.get_inbox(lane["actor"])["items"]}
    if eligible:
        with pytest.raises(ConflictError, match="unknown CAO handoff"):
            service.acknowledge(lane["actor"], AckInput(message_ids=[lane["blocker_id"]]))
        claimed = Dispatcher(service, lane["settings"])._claim_delivery()
        assert claimed is not None and claimed["message_id"] == lane["completion_id"]
    after = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?", (lane["blocker_id"],)
    )
    assert before is not None and after is not None and dict(after) == dict(before)


def test_notifications_keep_exact_attachment_authority(notification_lane: dict[str, Any]) -> None:
    lane = notification_lane
    service: ControlPlane = lane["service"]
    other_actor = _attached(service, "two")
    assert service.get_inbox(other_actor)["items"] == []
    with pytest.raises(AuthorizationError, match="not bound to this CAO conversation"):
        service.acknowledge(other_actor, AckInput(message_ids=[lane["completion_id"]]))
    other_work = _report(lane, other_actor, "other-attachment")
    other_id = _message_id(service, other_work["id"], "completion_claim")
    assert _dispatch_eligible(service, other_id)
    assert [item["id"] for item in service.get_inbox(other_actor)["items"]] == [other_id]


@pytest.mark.parametrize("head_state", ["delivered", "acknowledged"])
def test_worker_command_handling_fifo_is_unchanged(system: dict[str, Any], head_state: str) -> None:
    service: ControlPlane = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Worker command order",
            objective="Apply commands in their execution order.",
            acceptance=["A later command cannot bypass an unhandled command."],
        ),
    )
    assignment_id = _message_id(service, work["id"], "assignment")
    instruction = service.send_message(
        system["cao"],
        [system["worker"]["id"]],
        kind=MessageKind.INSTRUCTION,
        payload={"summary": "Continue the same bounded work."},
        work_item_id=work["id"],
        attempt_id=work["current_attempt"]["id"],
        goal_version=work["goal_version"],
    )
    _set_delivery(service, assignment_id, head_state)

    assert instruction["id"] not in {
        item["id"]
        for item in service.get_inbox(system["worker"], include_acknowledged=True)["items"]
    }
    with pytest.raises(ConflictError, match="unhandled predecessor"):
        service.acknowledge(system["worker"], AckInput(message_ids=[instruction["id"]]))
    assert Dispatcher(service, system["settings"])._claim_delivery() is None
    _set_delivery(service, assignment_id, "handled")
    assert [item["id"] for item in service.get_inbox(system["worker"])["items"]] == [
        instruction["id"]
    ]


@pytest.mark.parametrize("unsafe_alias", ["d; SELECT 1", "m.x", "d --", ""])
def test_notification_predicates_accept_only_static_sql_aliases(unsafe_alias: str) -> None:
    with pytest.raises(ValueError, match="static identifiers"):
        cao_notification_dispatch_head_sql(message_alias=unsafe_alias)
    with pytest.raises(ValueError, match="static identifiers"):
        cao_notification_inbox_visible_sql(delivery_alias=unsafe_alias)

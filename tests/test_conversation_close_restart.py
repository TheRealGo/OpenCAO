from __future__ import annotations

import json

import pytest
from test_worker_thread_lifecycle_core import _attached, _prepare_runtime_recovery

from cao_control_plane.database import Database
from cao_control_plane.errors import AuthorizationError
from cao_control_plane.goal_packets import canonical_digest
from cao_control_plane.models import CloseCAOConversationInput


def _closed_boundary(system):
    service = system["service"]
    actor = _attached(system, suffix="close-restart")
    ids, work, boundary, _ = _prepare_runtime_recovery(
        system, actor, ordinal=901, reason="runtime_dispatch_failed", acquire_turn=False,
    )
    result = service.close_cao_conversation(
        actor, CloseCAOConversationInput(idempotency_key="close-open-boundary"),
    )
    assert result["work_items_canceled"] == 1
    assert result["managed_workers_stopped"] == 0
    assert service.get_work(work["id"])["state"] == "canceled"
    assert service.db.fetchone(
        "SELECT state FROM managed_worker_threads WHERE id=?", (ids["thread_id"],),
    )["state"] == "active"
    turn = service.db.fetchone(
        "SELECT * FROM reasoner_turns WHERE boundary_id=?", (boundary["id"],),
    )
    return actor, dict(turn), dict(service.db.fetchone(
        "SELECT * FROM boundaries WHERE id=?", (boundary["id"],),
    ))


def test_conversation_close_with_open_boundary_survives_restart(system):
    _actor, turn, boundary = _closed_boundary(system)
    decision = system["service"].db.fetchone(
        "SELECT * FROM boundary_dispositions WHERE reasoner_turn_id=?", (turn["id"],),
    )
    assert turn["input_digest"] == canonical_digest({
        "boundary_id": boundary["id"], "boundary_input_digest": boundary["input_digest"],
        "goal_packet_digest": boundary["goal_packet_digest"],
        "task_packet_digest": boundary["task_packet_digest"], "generation": turn["generation"],
    })
    assert turn["result_digest"] == canonical_digest({
        "boundary_id": boundary["id"], "disposition_id": decision["id"],
        "request_digest": decision["request_digest"],
        "goal_packet_digest": boundary["goal_packet_digest"],
        "task_packet_digest": boundary["task_packet_digest"],
    })
    reopened = Database(system["settings"])
    assert dict(reopened.fetchone("SELECT * FROM reasoner_turns WHERE id=?", (turn["id"],))) == turn


def test_explicit_conversation_close_fences_its_own_unexpired_decision_lease(system):
    service = system["service"]
    actor = _attached(system, suffix="close-own-lease")
    ids, work, boundary, lease = _prepare_runtime_recovery(
        system, actor, ordinal=902, reason="runtime_dispatch_failed", acquire_turn=True,
    )
    assert lease is not None
    other_actor = _attached(system, suffix="close-unrelated-lease")
    _, other_work, _, other_lease = _prepare_runtime_recovery(
        system, other_actor, ordinal=903, reason="runtime_dispatch_failed", acquire_turn=True,
    )
    assert other_lease is not None
    result = service.close_cao_conversation(
        actor, CloseCAOConversationInput(idempotency_key="close-with-own-decision-lease"),
    )
    assert result["work_items_canceled"] == 1
    assert result["managed_workers_stopped"] == 0
    assert service.get_work(work["id"])["state"] == "canceled"
    assert service.db.fetchone(
        "SELECT state FROM reasoner_turns WHERE id=?", (lease.turn_id,),
    )["state"] == "abandoned"
    assert service.db.fetchone(
        "SELECT kind FROM boundary_dispositions WHERE boundary_id=?", (boundary["id"],),
    )["kind"] == "cancel"
    assert service.db.fetchone(
        "SELECT state FROM managed_worker_threads WHERE id=?", (ids["thread_id"],),
    )["state"] == "active"
    assert service.db.fetchone(
        "SELECT state FROM reasoner_turns WHERE id=?", (other_lease.turn_id,),
    )["state"] == "leased"
    assert service.get_work(other_work["id"])["state"] != "canceled"
    with pytest.raises(AuthorizationError):
        service.dispose_boundary(actor, boundary["id"], lease)
    reopened = Database(system["settings"])
    assert reopened.fetchone(
        "SELECT state FROM cao_session_attachments WHERE id=?", (actor["_cao_attachment_id"],),
    )["state"] == "revoked"


@pytest.mark.parametrize("receipt", [
    "legacy", "corrupt_input", "corrupt_result", "ordinary_marker", "real_lease",
    "wrong_kind", "wrong_reason", "instruction", "wrong_owner", "wrong_generation",
    "wrong_time", "missing_close_receipt", "wrong_close_digest", "wrong_close_result",
    "mixed_digest", "wrong_attachment", "wrong_close_generation", "wrong_close_actor",
])
def test_only_exact_legacy_conversation_close_receipt_survives_restart(system, receipt):
    service = system["service"]
    actor, turn, boundary = _closed_boundary(system)
    decision = service.db.fetchone(
        "SELECT * FROM boundary_dispositions WHERE reasoner_turn_id=?", (turn["id"],),
    )
    # Reproduce both historical digest fields without changing their audit origin.
    service.db.execute("UPDATE reasoner_turns SET input_digest=?, result_digest=? WHERE id=?", (
        boundary["input_digest"], decision["request_digest"], turn["id"],
    ))
    if receipt in {
        "corrupt_input", "corrupt_result", "ordinary_marker", "real_lease", "wrong_time",
        "mixed_digest", "wrong_attachment", "wrong_close_generation",
    }:
        field, value = {
            "corrupt_input": ("input_digest", "0" * 64),
            "corrupt_result": ("result_digest", "0" * 64),
            "ordinary_marker": ("idempotency_key", "ordinary"),
            "real_lease": ("lease_token_digest", "0" * 64),
            "wrong_time": ("completed_at", "2000-01-01T00:00:00Z"),
            "mixed_digest": ("input_digest", turn["input_digest"]),
            "wrong_attachment": ("idempotency_key", f"conversation-close:cat_other:0:{boundary['id']}"),
            "wrong_close_generation": ("idempotency_key", f"conversation-close:{actor['_cao_attachment_id']}:{actor['_cao_attachment_generation'] + 1}:{boundary['id']}"),
        }[receipt]
        service.db.execute(f"UPDATE reasoner_turns SET {field}=? WHERE id=?", (value, turn["id"]))
    elif receipt in {"wrong_kind", "wrong_reason", "instruction", "wrong_owner", "wrong_generation"}:
        field, value = {
            "wrong_kind": ("kind", "correct"),
            "wrong_reason": ("reason", "Another decision"),
            "instruction": ("instruction", "Continue the Worker"),
            "wrong_owner": ("decided_by", system["user"]["id"]),
            "wrong_generation": ("generation", turn["generation"] + 1),
        }[receipt]
        service.db.execute(f"UPDATE boundary_dispositions SET {field}=? WHERE id=?", (value, decision["id"]))
    else:
        operation = f"close_cao_conversation:{actor['_cao_attachment_id']}:{actor['_cao_attachment_generation']}"
        if receipt == "missing_close_receipt":
            service.db.execute("DELETE FROM idempotency_results WHERE operation=?", (operation,))
        elif receipt == "wrong_close_digest":
            service.db.execute("UPDATE idempotency_results SET request_digest=? WHERE operation=?", ("0" * 64, operation))
        elif receipt == "wrong_close_result":
            service.db.execute("UPDATE idempotency_results SET result_json=? WHERE operation=?", (json.dumps({"status": "closed"}), operation))
        elif receipt == "wrong_close_actor":
            service.db.execute("UPDATE idempotency_results SET actor_id=? WHERE operation=?", (system["user"]["id"], operation))
    if receipt != "legacy":
        with pytest.raises(RuntimeError, match=r"stored reasoner.*digest is invalid"):
            Database(system["settings"])
        return
    before = dict(service.db.fetchone("SELECT * FROM reasoner_turns WHERE id=?", (turn["id"],)))
    before_decision = dict(service.db.fetchone("SELECT * FROM boundary_dispositions WHERE id=?", (decision["id"],)))
    for _ in range(2):
        reopened = Database(system["settings"])
        assert dict(reopened.fetchone("SELECT * FROM reasoner_turns WHERE id=?", (turn["id"],))) == before
        assert dict(reopened.fetchone("SELECT * FROM boundary_dispositions WHERE id=?", (decision["id"],))) == before_decision
        assert dict(reopened.fetchone("SELECT * FROM boundaries WHERE id=?", (boundary["id"],))) == boundary
        # Diagnostic event retention must not rewrite or invalidate the receipt.
        service.db.execute("DELETE FROM events WHERE event_type IN ('boundary.disposed','cao.conversation_closed')")
    renewed = _attached(system, suffix="close-restart")
    assert renewed["_cao_attachment_generation"] > actor["_cao_attachment_generation"]
    reopened = Database(system["settings"])
    assert dict(reopened.fetchone("SELECT * FROM reasoner_turns WHERE id=?", (turn["id"],))) == before

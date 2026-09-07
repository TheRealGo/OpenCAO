from __future__ import annotations

import json
from typing import Any

import pytest
from test_worker_output_delivery import _capture_fixture, _event, _read_request

from cao_control_plane.errors import ConflictError
from cao_control_plane.models import MessageKind

_OLD_TIMESTAMP = "2000-01-01T00:00:00Z"


def _ordinary_expired_message(service: Any, actor: dict[str, Any]) -> str:
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=actor["id"],
            recipient_id=actor["id"],
            kind=MessageKind.SYSTEM,
            payload={"action": "retention_test", "summary": "Unrelated expired notification."},
            idempotency_key="retention:unrelated-message",
        )
        connection.execute(
            "UPDATE messages SET created_at = ? WHERE id = ?",
            (_OLD_TIMESTAMP, message["id"]),
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled' WHERE message_id = ?",
            (message["id"],),
        )
    return str(message["id"])


def _age_handled_references(service: Any, message_ids: set[str]) -> None:
    with service.db.transaction() as connection:
        for message_id in message_ids:
            connection.execute(
                "UPDATE messages SET created_at = ? WHERE id = ?",
                (_OLD_TIMESTAMP, message_id),
            )
            connection.execute(
                "UPDATE message_deliveries SET state = 'handled', owner_token = '', "
                "lease_until = NULL WHERE message_id = ?",
                (message_id,),
            )


def test_prune_retains_capture_reservation_source_without_any_output_receipt(system) -> None:
    service = system["service"]
    actor, _, binding = _capture_fixture(system)
    source_id = binding["delivery_message_id"]
    _age_handled_references(service, {source_id})
    unrelated_id = _ordinary_expired_message(service, actor)
    assert service.db.fetchone("SELECT 1 FROM worker_output_receipts") is None

    result = service.db.prune(event_days=1, message_days=1)

    assert result["messages"] == 1
    assert service.db.fetchone("SELECT 1 FROM messages WHERE id = ?", (source_id,)) is not None
    assert service.db.fetchone("SELECT 1 FROM messages WHERE id = ?", (unrelated_id,)) is None
    assert service.db.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.parametrize(
    ("work_state", "attempt_state"),
    [
        ("active", "working"),
        ("waiting_supervisor", "waiting_supervisor"),
        ("completed", "completed"),
        ("canceled", "canceled"),
        ("failed", "failed"),
    ],
)
def test_prune_preserves_only_exact_capture_sources_and_outbox_references(
    system, work_state: str, attempt_state: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    terminal = service.observe_worker_output(
        **binding,
        event=_event(kind="turn_end", item_id="", text="", status="completed"),
    )
    assert output["notification_message_id"]
    references = {
        message_id
        for message_id in (
            binding["delivery_message_id"],
            output["notification_message_id"],
            terminal["notification_message_id"],
        )
        if message_id
    }
    _age_handled_references(service, references)
    # Retention is independent of semantic lifecycle. Historical receipt
    # references must remain intact even after explicit terminal settlement.
    service.db.execute("UPDATE work_items SET state = ? WHERE id = ?", (work_state, work["id"]))
    service.db.execute(
        "UPDATE attempts SET state = ? WHERE id = ?", (attempt_state, binding["attempt_id"])
    )
    unrelated_id = _ordinary_expired_message(service, actor)
    before = [dict(row) for row in service.db.fetchall("SELECT * FROM worker_output_receipts")]

    result = service.db.prune(event_days=1, message_days=1)

    assert result["messages"] == 1
    assert all(
        service.db.fetchone("SELECT 1 FROM messages WHERE id = ?", (message_id,)) is not None
        for message_id in references
    )
    assert service.db.fetchone("SELECT 1 FROM messages WHERE id = ?", (unrelated_id,)) is None
    assert [
        dict(row) for row in service.db.fetchall("SELECT * FROM worker_output_receipts")
    ] == before
    assert service.db.fetchall("PRAGMA foreign_key_check") == []


def test_prune_does_not_resolve_or_retry_unknown_capture_input(system) -> None:
    service = system["service"]
    actor, _, binding = _capture_fixture(system)
    service.observe_worker_output(**binding, event=_event(complete=False))
    service.db.execute(
        "UPDATE messages SET created_at = ? WHERE id = ?",
        (_OLD_TIMESTAMP, binding["delivery_message_id"]),
    )
    service.db.execute(
        "UPDATE message_deliveries SET owner_token = '', lease_until = NULL, "
        "last_error = 'runtime_dispatch_failed' WHERE message_id = ?",
        (binding["delivery_message_id"],),
    )
    before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?",
            (binding["delivery_message_id"],),
        )
    )
    assert before["state"] == "dispatched"
    _ordinary_expired_message(service, actor)

    assert service.db.prune(event_days=1, message_days=1)["messages"] == 1

    after = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?",
            (binding["delivery_message_id"],),
        )
    )
    assert after == before


@pytest.mark.parametrize("work_state", ["active", "completed"])
def test_prune_retains_exact_output_read_proof_and_idempotency_only(
    system, work_state: str
) -> None:
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    output = service.observe_worker_output(**binding, event=_event())
    first_request = _read_request(work, output, max_bytes=8)
    first = service.read_worker_output(actor, first_request)
    second = service.read_worker_output(actor, _read_request(work, output, byte_offset=8))
    audits = {first["audit_event_sequence"], second["audit_event_sequence"]}
    service.db.execute("UPDATE work_items SET state = ? WHERE id = ?", (work_state, work["id"]))

    with service.db.transaction() as connection:
        row = connection.execute(
            "SELECT data_json FROM events WHERE sequence = ?", (first["audit_event_sequence"],)
        ).fetchone()
        assert row is not None
        exact = json.loads(row["data_json"])
        prunable = {
            service._event(
                connection, "test.unrelated.retention", "test", "bounded", actor["id"], {}
            )
        }
        for changed in (
            {"extra": "not-read-authority"},
            {"output_id": "wout_unrelated"},
            {"digest": "f" * 64},
            {"total_bytes": output["byte_count"] + 1},
            {"byte_offset": -1},
            {"byte_count": output["byte_count"] + 1},
            {"supervisor_attachment_id": "cat_unrelated"},
            {"supervisor_attachment_generation": "not-an-integer"},
        ):
            prunable.add(
                service._event(
                    connection,
                    "worker_output.read",
                    "attempt",
                    binding["attempt_id"],
                    actor["id"],
                    exact | changed,
                )
            )
        for aggregate_type, aggregate_id, actor_id in (
            ("work_item", binding["attempt_id"], actor["id"]),
            ("attempt", "att_unrelated", actor["id"]),
            ("attempt", binding["attempt_id"], system["worker"]["id"]),
        ):
            prunable.add(
                service._event(
                    connection, "worker_output.read", aggregate_type, aggregate_id, actor_id, exact
                )
            )
        for sequence in audits | prunable:
            connection.execute(
                "UPDATE events SET created_at = ? WHERE sequence = ?", (_OLD_TIMESTAMP, sequence)
            )

    pruned = service.db.prune(event_days=1, message_days=1)

    assert pruned["events"] == len(prunable)
    assert all(
        service.db.fetchone("SELECT 1 FROM events WHERE sequence = ?", (sequence,)) is not None
        for sequence in audits
    )
    assert all(
        service.db.fetchone("SELECT 1 FROM events WHERE sequence = ?", (sequence,)) is None
        for sequence in prunable
    )
    replay = service.read_worker_output(actor, first_request)
    assert replay["audit_event_sequence"] == first["audit_event_sequence"]
    assert replay["content"] == first["content"]
    with pytest.raises(ConflictError, match="idempotency key was reused"):
        service.read_worker_output(actor, first_request.model_copy(update={"max_bytes": 4}))

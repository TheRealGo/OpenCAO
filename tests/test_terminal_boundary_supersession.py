from __future__ import annotations

import json
import sqlite3

import pytest
from test_boundary_authority_pruning import _completion_boundary

from cao_control_plane.models import BoundaryDispositionInput, BoundaryInput
from cao_control_plane.projection import verify_projection
from cao_control_plane.service import _digest


def _terminal(system, monkeypatch, *, kind="cancel", legacy=False):
    service = system["service"]
    work, completion = _completion_boundary(system, key="terminal-siblings")
    attempt = work["current_attempt"]
    failure = service.record_boundary(
        system["cao"],
        BoundaryInput(
            source_event_id="runtime-failed-after-completion",
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            kind="failure",
            summary="The provider failed after capturing the completion claim.",
            runtime_state="failed",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"], work["id"], boundary_id=failure["id"],
        expected_generation=work["generation"], idempotency_key="terminal-sibling-turn",
    )
    with monkeypatch.context() as patch:
        if legacy:
            patch.setattr(
                "cao_control_plane.service.supersede_terminal_work_boundaries",
                lambda *args, **kwargs: None,
            )
        request = BoundaryDispositionInput(
                turn_id=turn["id"], lease_token=turn["lease_token"],
                expected_generation=work["generation"], kind="cancel",
                reason="The exact task is terminal; preserve the captured evidence.",
        )
        disposition = service.dispose_boundary(system["cao"], failure["id"], request)
    if kind == "fail":
        # Seed pre-upgrade evidence. New service operations cannot fail unfinished Work.
        legacy_request = BoundaryDispositionInput(**(request.model_dump() | {"kind": "fail"}))
        request_digest = _digest(legacy_request.model_dump(mode="json"))
        with service.db.transaction() as connection:
            connection.execute(
                "UPDATE boundary_dispositions SET kind = 'fail', request_digest = ? WHERE id = ?",
                (request_digest, disposition["id"]),
            )
            connection.execute("UPDATE work_items SET state = 'failed' WHERE id = ?", (work["id"],))
            connection.execute("UPDATE attempts SET state = 'failed' WHERE id = ?", (attempt["id"],))
            connection.execute(
                "UPDATE events SET data_json = json_set(data_json, '$.kind', 'fail') "
                "WHERE event_type = 'boundary.disposed' AND aggregate_id = ?", (work["id"],),
            )
            connection.execute("UPDATE reasoner_turns SET result_digest = ? WHERE id = ?", (
                _digest({"boundary_id": failure["id"], "disposition_id": disposition["id"],
                         "request_digest": request_digest, "goal_packet_digest": turn["goal_packet_digest"],
                         "task_packet_digest": turn["task_packet_digest"]}), turn["id"],
            ))
    return service, work, completion, disposition


@pytest.mark.parametrize("kind", ["fail", "cancel"])
@pytest.mark.parametrize("legacy", [False, True])
def test_terminal_disposition_closes_earlier_actionability_with_exact_authority(
    system, monkeypatch, kind, legacy
):
    service, work, completion, disposition = _terminal(
        system, monkeypatch, kind=kind, legacy=legacy
    )
    before = dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
    messages = [dict(row) for row in service.db.fetchall("SELECT * FROM messages ORDER BY id")]
    if legacy:
        assert not verify_projection(service.db).healthy
        with service.db.transaction() as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version > 46")
            connection.execute("UPDATE metadata SET value = '46' WHERE key = 'schema_version'")
            connection.execute("PRAGMA user_version = 46")
        service.db.initialize()
    proof = service.db.fetchone(
        "SELECT * FROM boundary_supersessions WHERE boundary_id = ?", (completion["id"],)
    )
    assert proof is not None and proof["reason"] == "work_terminal_disposition"
    closing = service.db.fetchone(
        "SELECT * FROM events WHERE sequence = ?", (proof["superseding_event_sequence"],)
    )
    assert closing["event_type"] == "boundary.disposed"
    assert json.loads(closing["data_json"])["disposition_id"] == disposition["id"]
    assert service.get_work(work["id"], system["cao"])["open_boundaries"] == []
    assert verify_projection(service.db).healthy
    assert dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))) == before
    assert [dict(row) for row in service.db.fetchall("SELECT * FROM messages ORDER BY id")] == messages
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM reviews")["n"] == 0
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM boundary_dispositions")["n"] == 1
    service.db.initialize()
    assert dict(service.db.fetchone(
        "SELECT * FROM boundary_supersessions WHERE boundary_id = ?", (completion["id"],)
    )) == dict(proof)


@pytest.mark.parametrize("broken_proof", ["duplicate_event", "wrong_generation", "wrong_work"])
def test_ambiguous_terminal_authority_remains_visible_and_cannot_be_fabricated(
    system, monkeypatch, broken_proof
):
    service, work, completion, disposition = _terminal(system, monkeypatch, legacy=True)
    closing = service.db.fetchone(
        "SELECT * FROM events WHERE event_type = 'boundary.disposed' AND aggregate_id = ?",
        (work["id"],),
    )
    if broken_proof == "duplicate_event":
        with service.db.transaction() as connection:
            connection.execute(
                "INSERT INTO events(id, event_type, aggregate_type, aggregate_id, actor_id, "
                "data_json, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                ("evt_duplicate_terminal", closing["event_type"], closing["aggregate_type"],
                 closing["aggregate_id"], closing["actor_id"], closing["data_json"], closing["created_at"]),
            )
    elif broken_proof == "wrong_generation":
        service.db.execute("UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],))
    else:
        service.db.execute("UPDATE events SET aggregate_id = 'wrk_unrelated' WHERE sequence = ?", (closing["sequence"],))
    service.db.initialize()
    assert service.db.fetchone(
        "SELECT * FROM boundary_supersessions WHERE boundary_id = ?", (completion["id"],)
    ) is None
    assert not verify_projection(service.db).healthy
    opening = service.db.fetchone(
        "SELECT sequence FROM events WHERE event_type = 'boundary.recorded' "
        "AND json_extract(data_json, '$.boundary_id') = ?", (completion["id"],),
    )
    with pytest.raises(sqlite3.IntegrityError, match="exact binding"):
        service.db.execute(
            "INSERT INTO boundary_supersessions VALUES(?, ?, ?, 'work_terminal_disposition', ?)",
            (completion["id"], opening["sequence"], closing["sequence"], disposition["created_at"]),
        )

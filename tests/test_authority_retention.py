from __future__ import annotations

import json
from typing import Any

import pytest
from test_verified_artifact_content_read import (
    _attached_cao,
    _read_arguments,
    _reported_artifact,
)

from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import MCPServer

_OLD_TIMESTAMP = "2000-01-01T00:00:00Z"


def test_prune_retains_artifact_content_read_idempotency_authority(
    system: dict[str, Any],
) -> None:
    attachment, actor = _attached_cao(
        system,
        thread_id="artifact-retention",
        project_marker="e",
    )
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=b"durable verified artifact content",
        media_type="text/plain",
        key="authority-retention",
    )
    service = system["service"]
    server = MCPServer(service)
    arguments = _read_arguments(
        work,
        artifact,
        max_bytes=8,
        idempotency_key="artifact-retention-exact-read",
    )
    first = server.call_tool(actor, "cao_read_artifact", arguments)
    with service.db.transaction() as connection:
        audit = connection.execute(
            "SELECT data_json FROM events WHERE sequence = ?",
            (first["audit_event_sequence"],),
        ).fetchone()
        assert audit is not None
        forged_data = json.loads(str(audit["data_json"]))
        forged_data.pop("media_type")
        forged_data["worker_supplied_extra"] = "must-not-extend-retention"
        forged_sequence = service._event(
            connection,
            "artifact.content_read",
            "artifact",
            artifact["id"],
            actor["id"],
            forged_data,
        )
        wrong_actor_sequence = service._event(
            connection,
            "artifact.content_read",
            "artifact",
            artifact["id"],
            system["worker"]["id"],
            json.loads(str(audit["data_json"])),
        )
        connection.execute(
            "UPDATE events SET created_at = ? WHERE sequence = ?",
            (_OLD_TIMESTAMP, first["audit_event_sequence"]),
        )
        unrelated_sequence = service._event(
            connection,
            "test.unrelated.retention",
            "test",
            artifact["id"],
            actor["id"],
            {},
        )
        connection.execute(
            "UPDATE events SET created_at = ? WHERE sequence IN (?, ?, ?)",
            (
                _OLD_TIMESTAMP,
                unrelated_sequence,
                forged_sequence,
                wrong_actor_sequence,
            ),
        )

    pruned = service.db.prune(event_days=1, message_days=36_500)

    assert pruned["events"] >= 1
    remaining_prunable_sequences = {
        int(row["sequence"])
        for row in service.db.fetchall(
            "SELECT sequence FROM events WHERE sequence IN (?, ?, ?)",
            (unrelated_sequence, forged_sequence, wrong_actor_sequence),
        )
    }
    assert remaining_prunable_sequences == set(), {
        "unrelated": unrelated_sequence,
        "missing_media": forged_sequence,
        "wrong_actor": wrong_actor_sequence,
    }
    assert (
        service.db.fetchone(
            "SELECT sequence FROM events WHERE sequence = ?",
            (first["audit_event_sequence"],),
        )
        is not None
    )
    replay = server.call_tool(actor, "cao_read_artifact", arguments)
    assert replay["audit_event_sequence"] == first["audit_event_sequence"]
    assert replay["content"] == first["content"]
    with pytest.raises(ConflictError) as changed:
        server.call_tool(
            actor,
            "cao_read_artifact",
            {**arguments, "max_bytes": 4},
        )
    assert changed.value.details == {"reason_code": "artifact_content_idempotency_conflict"}

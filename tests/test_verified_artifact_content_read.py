from __future__ import annotations

import base64
import hashlib
import json
import threading
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from pydantic import ValidationError as PydanticValidationError

from cao_control_plane.database import utc_now
from cao_control_plane.errors import AuthorizationError, ConflictError, NotFoundError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    ArtifactContentReadInput,
    ArtifactInput,
    CompletionContract,
    ReportInput,
    ReportKind,
    WorkAssignment,
)


def _attached_cao(
    system: dict[str, Any], *, thread_id: str, project_marker: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id=thread_id,
            project_digest=project_marker * 64,
        ),
    )
    return attachment, system["service"].authenticate(attachment["context_token"])


def _reported_artifact(
    system: dict[str, Any],
    attachment: dict[str, Any],
    *,
    content: bytes,
    media_type: str,
    key: str,
    report_kind: ReportKind = ReportKind.COMPLETION_CLAIM,
    source_uri: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
            title=f"Verified artifact read {key}",
            objective="Produce one exact result for attachment-scoped review.",
            acceptance=["The CAO can inspect the verified content without a raw path."],
            completion_contract=CompletionContract.COMPLETION_REQUIRED,
            idempotency_key=f"assign:{key}",
        ),
    )
    attempt = work["current_attempt"]
    digest = hashlib.sha256(content).hexdigest()
    uri = source_uri or (
        "data:application/octet-stream;base64," + base64.b64encode(content).decode("ascii")
    )
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=report_kind,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The exact artifact is ready for bounded CAO inspection.",
            trajectory=("complete" if report_kind == ReportKind.COMPLETION_CLAIM else "advancing"),
            artifacts=[
                ArtifactInput(
                    name="result",
                    uri=uri,
                    media_type=media_type,
                    digest=digest,
                )
            ],
            idempotency_key=f"report:{key}",
        ),
    )
    artifact = next(item for item in reported["artifacts"] if item["attempt_id"] == attempt["id"])
    return reported, artifact


def _read_arguments(
    work: dict[str, Any], artifact: dict[str, Any], **overrides: Any
) -> dict[str, Any]:
    values = {
        "work_item_id": work["id"],
        "attempt_id": work["current_attempt"]["id"],
        "artifact_id": artifact["id"],
        "expected_digest": artifact["digest"],
        "expected_media_type": artifact["media_type"],
        "byte_offset": 0,
        "max_bytes": 65_536,
    }
    values.update(overrides)
    values.setdefault(
        "idempotency_key",
        "read:" + artifact["id"] + f":{values['byte_offset']}:{values['max_bytes']}",
    )
    return values


def test_attached_cao_reads_utf8_chunks_with_path_free_durable_audit(system) -> None:
    attachment, actor = _attached_cao(system, thread_id="artifact-reader", project_marker="a")
    content = "ab日語cd".encode()
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=content,
        media_type="text/plain; charset=UTF-8",
        key="utf8-chunks",
    )
    server = MCPServer(system["service"])

    first = server.call_tool(
        actor,
        "cao_read_artifact",
        _read_arguments(work, artifact, max_bytes=5),
    )
    replayed = server.call_tool(
        actor,
        "cao_read_artifact",
        _read_arguments(work, artifact, max_bytes=5),
    )
    assert replayed == first
    with pytest.raises(ConflictError) as replay_conflict:
        server.call_tool(
            actor,
            "cao_read_artifact",
            _read_arguments(
                work,
                artifact,
                max_bytes=6,
                idempotency_key=_read_arguments(work, artifact, max_bytes=5)["idempotency_key"],
            ),
        )
    assert replay_conflict.value.details == {"reason_code": "artifact_content_idempotency_conflict"}
    assert first["content"] == "ab日"
    assert first["byte_offset"] == 0
    assert first["byte_count"] == 5
    assert first["total_bytes"] == len(content)
    assert first["next_byte_offset"] == 5
    assert first["complete"] is False
    assert first["chunk_digest"] == hashlib.sha256(content[:5]).hexdigest()

    second = server.call_tool(
        actor,
        "cao_read_artifact",
        _read_arguments(work, artifact, byte_offset=5, max_bytes=4),
    )
    assert second["content"] == "語c"
    assert second["byte_count"] == 4
    assert second["next_byte_offset"] == 9

    serialized = json.dumps({"first": first, "second": second})
    assert "owner-private-artifact:" not in serialized
    assert "artifact-archive" not in serialized
    assert str(system["settings"].state_dir) not in serialized

    audit = system["service"].db.fetchone(
        "SELECT sequence, aggregate_type, aggregate_id, data_json FROM events WHERE sequence = ?",
        (first["audit_event_sequence"],),
    )
    assert audit is not None
    assert audit["aggregate_type"] == "artifact"
    assert audit["aggregate_id"] == artifact["id"]
    audit_data = json.loads(str(audit["data_json"]))
    assert audit_data["supervisor_attachment_id"] == attachment["id"]
    assert audit_data["supervisor_attachment_generation"] == actor["_cao_attachment_generation"]
    assert audit_data["chunk_digest"] == first["chunk_digest"]
    assert "content" not in audit_data
    assert "uri" not in audit_data
    assert "path" not in audit_data
    assert str(system["settings"].state_dir) not in str(audit_data)
    audit_count = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'artifact.content_read' AND aggregate_id = ?",
        (artifact["id"],),
    )
    assert audit_count is not None and audit_count["count"] == 2


def test_artifact_read_rejects_secret_spanning_requested_chunk_without_authority_event(
    system,
) -> None:
    attachment, actor = _attached_cao(
        system,
        thread_id="artifact-secret-scan",
        project_marker="c",
    )
    secret = str(attachment["context_token"])
    prefix = "ordinary review text: "
    content = f"{prefix}{secret}\ntrailing ordinary text".encode()
    requested_bytes = len(prefix.encode()) + 5
    assert requested_bytes < len(prefix.encode()) + len(secret.encode())
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=content,
        media_type="text/plain; charset=utf-8",
        key="secret-spans-chunk",
    )
    idempotency_key = "read:secret-spans-chunk"
    request = ArtifactContentReadInput.model_validate(
        _read_arguments(
            work,
            artifact,
            max_bytes=requested_bytes,
            idempotency_key=idempotency_key,
        )
    )

    with pytest.raises(ConflictError) as rejected:
        MCPServer(system["service"]).call_tool(
            actor,
            "cao_read_artifact",
            request.model_dump(mode="json"),
        )

    assert rejected.value.details == {
        "reason_code": "artifact_content_contains_control_plane_secret"
    }
    rendered_error = str(rejected.value)
    assert secret not in rendered_error
    assert content.decode() not in rendered_error
    assert str(system["settings"].state_dir) not in rendered_error
    audit = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'artifact.content_read' AND aggregate_id = ?",
        (artifact["id"],),
    )
    assert audit is not None and audit["count"] == 0
    idempotency_authority = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM idempotency_results "
        "WHERE actor_id = ? AND idempotency_key = ?",
        (actor["id"], idempotency_key),
    )
    assert idempotency_authority is not None
    assert idempotency_authority["count"] == 0


def test_artifact_read_rejects_generic_credentials_outside_requested_chunk(
    system,
) -> None:
    attachment, actor = _attached_cao(
        system,
        thread_id="artifact-generic-credential-scan",
        project_marker="d",
    )
    token_prefix = "ordinary review text: "
    github_token = "github_pat_" + "G" * 40
    private_key_prefix = "ordinary first page.\n"
    private_key_lead_in = "more non-sensitive review notes.\n"
    cases = (
        (
            "github-token-spans-chunk",
            f"{token_prefix}{github_token}\ntrailing ordinary text",
            len(token_prefix.encode()) + 7,
            len(token_prefix.encode()),
        ),
        (
            "private-key-outside-chunk",
            private_key_prefix + private_key_lead_in + "-----BEGIN PRIVATE KEY-----\nnot-returned",
            len(private_key_prefix.encode()),
            len((private_key_prefix + private_key_lead_in).encode()),
        ),
    )

    for key, text_content, requested_bytes, credential_start in cases:
        content = text_content.encode()
        if key == "github-token-spans-chunk":
            assert (
                credential_start < requested_bytes < credential_start + len(github_token.encode())
            )
        else:
            assert requested_bytes <= credential_start
        work, artifact = _reported_artifact(
            system,
            attachment,
            content=content,
            media_type="text/plain; charset=utf-8",
            key=key,
        )
        idempotency_key = f"read:{key}"

        with pytest.raises(ConflictError) as rejected:
            MCPServer(system["service"]).call_tool(
                actor,
                "cao_read_artifact",
                _read_arguments(
                    work,
                    artifact,
                    max_bytes=requested_bytes,
                    idempotency_key=idempotency_key,
                ),
            )

        assert rejected.value.details == {
            "reason_code": "artifact_content_contains_generic_credential"
        }
        rendered_error = str(rejected.value)
        assert text_content not in rendered_error
        assert str(system["settings"].state_dir) not in rendered_error
        audit = system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'artifact.content_read' AND aggregate_id = ?",
            (artifact["id"],),
        )
        assert audit is not None and audit["count"] == 0
        idempotency_authority = system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM idempotency_results "
            "WHERE actor_id = ? AND idempotency_key = ?",
            (actor["id"], idempotency_key),
        )
        assert idempotency_authority is not None
        assert idempotency_authority["count"] == 0


def test_artifact_read_allows_descriptive_credential_words(system) -> None:
    attachment, actor = _attached_cao(
        system,
        thread_id="artifact-credential-description",
        project_marker="e",
    )
    text_content = (
        "This document describes credential rotation, token scope, password length, "
        "Authorization bearer schemes, API key policy, and private key format. "
        "It contains no credential values."
    )
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=text_content.encode(),
        media_type="text/plain",
        key="descriptive-credential-words",
    )

    result = MCPServer(system["service"]).call_tool(
        actor,
        "cao_read_artifact",
        _read_arguments(work, artifact, idempotency_key="read:credential-description"),
    )

    assert result["content"] == text_content
    assert result["complete"] is True


def test_artifact_read_denies_foreign_or_unattached_cao_without_audit(system) -> None:
    attachment, owner = _attached_cao(system, thread_id="artifact-owner", project_marker="a")
    _foreign_attachment, foreign = _attached_cao(
        system, thread_id="artifact-foreign", project_marker="b"
    )
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=b"attachment private result",
        media_type="text/plain",
        key="attachment-fence",
    )
    arguments = _read_arguments(work, artifact)
    before = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM events WHERE event_type = 'artifact.content_read'"
    )
    assert before is not None

    with pytest.raises(NotFoundError):
        MCPServer(system["service"]).call_tool(foreign, "cao_read_artifact", arguments)
    with pytest.raises(AuthorizationError):
        system["service"].read_artifact_content(
            system["cao"], ArtifactContentReadInput.model_validate(arguments)
        )
    assert "cao_read_artifact" not in {
        tool["name"] for tool in MCPServer(system["service"]).tools_for(system["cao"])
    }

    after = system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM events WHERE event_type = 'artifact.content_read'"
    )
    assert after is not None and after["count"] == before["count"]
    assert (
        MCPServer(system["service"]).call_tool(owner, "cao_read_artifact", arguments)["content"]
        == "attachment private result"
    )


def test_artifact_read_rechecks_current_attachment_generation(system) -> None:
    attachment, actor = _attached_cao(system, thread_id="artifact-generation", project_marker="a")
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=b"generation-fenced result",
        media_type="text/plain",
        key="generation-fence",
    )
    service = system["service"]
    next_generation = int(actor["_cao_attachment_generation"]) + 1
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE cao_session_attachments SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_attachment_connections SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_connection_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials SET generation = ? WHERE id = ?",
            (next_generation, actor["_cao_conversation_credential_id"]),
        )

    stale_request = ArtifactContentReadInput.model_validate(
        _read_arguments(work, artifact, idempotency_key="read:stale-generation")
    )
    with pytest.raises(AuthorizationError):
        service.read_artifact_content(actor, stale_request)

    current_actor = dict(actor)
    current_actor["_cao_attachment_generation"] = next_generation
    current = service.read_artifact_content(
        current_actor,
        stale_request.model_copy(update={"idempotency_key": "read:current-generation"}),
    )
    assert current["content"] == "generation-fenced result"
    event = service.db.fetchone(
        "SELECT data_json FROM events WHERE sequence = ?",
        (current["audit_event_sequence"],),
    )
    assert event is not None
    assert (
        json.loads(str(event["data_json"]))["supervisor_attachment_generation"] == next_generation
    )


def test_artifact_read_rejects_when_reattach_commits_after_role_precheck(
    system, monkeypatch
) -> None:
    attachment, actor = _attached_cao(
        system,
        thread_id="artifact-generation-race",
        project_marker="a",
    )
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=b"transaction-fenced result",
        media_type="text/plain",
        key="generation-race",
    )
    service = system["service"]
    request = ArtifactContentReadInput.model_validate(
        _read_arguments(work, artifact, idempotency_key="read:generation-race")
    )
    role_checked = threading.Event()
    continue_operation = threading.Event()
    original_require_role = service._require_role

    def gated_require_role(candidate: dict[str, Any], *roles: Any) -> None:
        original_require_role(candidate, *roles)
        if candidate is actor:
            role_checked.set()
            if not continue_operation.wait(timeout=10):
                raise AssertionError("artifact read role-check gate timed out")

    monkeypatch.setattr(service, "_require_role", gated_require_role)
    content_reads = 0
    original_read = service.owner_private_artifact_preservation.read_verified_text_chunk

    def tracked_content_read(**arguments: Any):
        nonlocal content_reads
        content_reads += 1
        return original_read(**arguments)

    monkeypatch.setattr(
        service.owner_private_artifact_preservation,
        "read_verified_text_chunk",
        tracked_content_read,
    )
    outcome: dict[str, Any] = {}

    def run_read() -> None:
        try:
            outcome["result"] = service.read_artifact_content(actor, request)
        except BaseException as error:
            outcome["error"] = error

    operation = threading.Thread(target=run_read, daemon=True)
    operation.start()
    assert role_checked.wait(timeout=10)
    before_audit = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'artifact.content_read' AND aggregate_id = ?",
        (artifact["id"],),
    )
    assert before_audit is not None
    source = service.db.fetchone(
        "SELECT runtime_session_id FROM cao_session_attachments WHERE id = ?",
        (actor["_cao_attachment_id"],),
    )
    assert source is not None
    now = utc_now()
    try:
        with service.db.transaction() as connection:
            connection.execute(
                "UPDATE cao_session_attachments SET state = 'failed', updated_at = ? WHERE id = ?",
                (now, actor["_cao_attachment_id"]),
            )
            connection.execute(
                "UPDATE runtime_sessions SET state = 'failed', updated_at = ? WHERE id = ?",
                (now, source["runtime_session_id"]),
            )
            connection.execute(
                "UPDATE cao_conversation_credentials SET state = 'revoked', "
                "revoked_at = ?, updated_at = ? WHERE attachment_id = ?",
                (now, now, actor["_cao_attachment_id"]),
            )
        attachment_before = dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?",
                (actor["_cao_attachment_id"],),
            )
        )
        wake_before = dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?",
                (source["runtime_session_id"],),
            )
        )
        work_before = dict(
            service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))
        )
        worker_before = dict(
            service.db.fetchone(
                "SELECT * FROM principals WHERE id = ?",
                (work_before["assigned_worker_id"],),
            )
        )
        prior_connection_generation = int(
            service.db.fetchone(
                "SELECT MAX(connection_generation) AS value "
                "FROM cao_attachment_connections WHERE attachment_id = ?",
                (actor["_cao_attachment_id"],),
            )["value"]
        )
        connected = attach_cao_session_with_peer(
            service,
            current_cao_session_attachment(
                native_thread_id="artifact-generation-race",
                project_digest="a" * 64,
            ),
        )
        connected_actor = service.authenticate(connected["context_token"])
        assert connected_actor["_cao_attachment_id"] == actor["_cao_attachment_id"]
        assert connected_actor["_cao_attachment_generation"] == actor["_cao_attachment_generation"]
        assert connected["runtime_session_id"] != source["runtime_session_id"]
        assert connected["runtime"]["state"] == "ready"
        assert connected["runtime"]["native_session_id"] == "artifact-generation-race"
        assert connected["connection_generation"] == prior_connection_generation + 1
        assert (
            dict(
                service.db.fetchone(
                    "SELECT * FROM runtime_sessions WHERE id = ?",
                    (source["runtime_session_id"],),
                )
            )
            == wake_before
        )
        assert (
            dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
            == work_before
        )
        assert (
            dict(
                service.db.fetchone(
                    "SELECT * FROM principals WHERE id = ?",
                    (work_before["assigned_worker_id"],),
                )
            )
            == worker_before
        )
        current_attachment = dict(
            service.db.fetchone(
                "SELECT * FROM cao_session_attachments WHERE id = ?",
                (actor["_cao_attachment_id"],),
            )
        )
        assert current_attachment["generation"] == attachment_before["generation"]
        assert current_attachment["runtime_session_id"] == connected["runtime_session_id"]
        old_credential = service.db.fetchone(
            "SELECT state FROM cao_conversation_credentials WHERE id = ?",
            (actor["_cao_conversation_credential_id"],),
        )
        assert old_credential is not None and old_credential["state"] == "revoked"
    finally:
        continue_operation.set()
    operation.join(timeout=10)
    assert not operation.is_alive()

    assert "result" not in outcome
    assert isinstance(outcome.get("error"), AuthorizationError)
    assert content_reads == 0
    after_audit = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'artifact.content_read' AND aggregate_id = ?",
        (artifact["id"],),
    )
    assert after_audit is not None
    assert after_audit["count"] == before_audit["count"]


def test_artifact_read_rejects_digest_corruption_and_non_boundary_offsets(system) -> None:
    attachment, actor = _attached_cao(system, thread_id="artifact-integrity", project_marker="a")
    content = "A日本".encode()
    work, artifact = _reported_artifact(
        system,
        attachment,
        content=content,
        media_type="text/plain",
        key="integrity",
    )
    service = system["service"]
    request = ArtifactContentReadInput.model_validate(_read_arguments(work, artifact))

    with pytest.raises(ConflictError) as mismatch:
        service.read_artifact_content(
            actor,
            request.model_copy(update={"expected_digest": "0" * 64}),
        )
    assert mismatch.value.details == {"reason_code": "artifact_content_digest_mismatch"}

    with pytest.raises(ConflictError) as offset:
        service.read_artifact_content(
            actor,
            request.model_copy(update={"byte_offset": 2, "max_bytes": 4}),
        )
    assert offset.value.details == {"reason_code": "artifact_content_offset_not_utf8_boundary"}
    with pytest.raises(ConflictError) as media_type:
        service.read_artifact_content(
            actor,
            request.model_copy(
                update={
                    "expected_media_type": "application/json",
                    "idempotency_key": "read:wrong-media-type",
                }
            ),
        )
    assert media_type.value.details == {"reason_code": "artifact_content_media_type_mismatch"}

    digest = artifact["digest"]
    archive = system["settings"].state_dir / "artifact-archive-v1" / digest[:2] / digest
    archive.write_bytes(b"X" * len(content))
    with pytest.raises(ConflictError) as corrupted:
        service.read_artifact_content(actor, request)
    assert corrupted.value.details == {
        "reason_code": "artifact_preservation_archive_digest_mismatch"
    }


def test_artifact_read_rejects_unverified_binary_oversize_and_invalid_chunk(
    system, tmp_path: Path
) -> None:
    attachment, actor = _attached_cao(system, thread_id="artifact-bounds", project_marker="a")
    unverified_work, unverified = _reported_artifact(
        system,
        attachment,
        content=b"progress-only artifact",
        media_type="text/plain",
        key="unverified",
        report_kind=ReportKind.ARTIFACT,
    )
    with pytest.raises(ConflictError) as unverified_error:
        system["service"].read_artifact_content(
            actor,
            ArtifactContentReadInput.model_validate(_read_arguments(unverified_work, unverified)),
        )
    assert unverified_error.value.details == {"reason_code": "artifact_content_manifest_unverified"}

    binary_work, binary = _reported_artifact(
        system,
        attachment,
        content=b"binary-declared",
        media_type="application/octet-stream",
        key="binary",
    )
    with pytest.raises(ConflictError) as binary_error:
        system["service"].read_artifact_content(
            actor,
            ArtifactContentReadInput.model_validate(_read_arguments(binary_work, binary)),
        )
    assert binary_error.value.details == {"reason_code": "artifact_content_media_type_unsupported"}

    invalid_utf8_work, invalid_utf8 = _reported_artifact(
        system,
        attachment,
        content=b"\xff\xfe",
        media_type="text/plain",
        key="invalid-utf8",
    )
    with pytest.raises(ConflictError) as utf8_error:
        system["service"].read_artifact_content(
            actor,
            ArtifactContentReadInput.model_validate(
                _read_arguments(invalid_utf8_work, invalid_utf8)
            ),
        )
    assert utf8_error.value.details == {"reason_code": "artifact_content_not_utf8"}

    source = tmp_path / "oversize-result.txt"
    oversized_content = b"x" * (1_048_576 + 1)
    source.write_bytes(oversized_content)
    oversized_work, oversized = _reported_artifact(
        system,
        attachment,
        content=oversized_content,
        media_type="text/plain",
        key="oversize",
        source_uri=str(source.resolve()),
    )
    with pytest.raises(ConflictError) as oversized_error:
        system["service"].read_artifact_content(
            actor,
            ArtifactContentReadInput.model_validate(_read_arguments(oversized_work, oversized)),
        )
    assert oversized_error.value.details == {"reason_code": "artifact_content_too_large"}

    with pytest.raises(PydanticValidationError):
        ArtifactContentReadInput.model_validate(
            _read_arguments(binary_work, binary, max_bytes=65_537)
        )
    with pytest.raises(PydanticValidationError):
        ArtifactContentReadInput.model_validate(
            _read_arguments(binary_work, binary, artifact_id="../private")
        )
    tool = next(
        item
        for item in MCPServer(system["service"]).tools_for(actor)
        if item["name"] == "cao_read_artifact"
    )
    assert tool["inputSchema"]["properties"]["max_bytes"] == {
        "type": "integer",
        "minimum": 4,
        "maximum": 65_536,
    }
    assert "idempotency_key" in tool["inputSchema"]["required"]
    assert tool["annotations"] == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }

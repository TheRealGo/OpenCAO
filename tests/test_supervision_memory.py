from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.database import Database, utc_now
from cao_control_plane.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ValidationError,
)
from cao_control_plane.models import (
    MemoryReadInput,
    MemorySearchInput,
    MemoryWriteInput,
    ReportInput,
    WorkAssignment,
)
from cao_control_plane.service import ControlPlane
from cao_control_plane.supervision_memory import (
    read_memory,
    remember_memory,
    search_memories,
    search_memories_tx,
)


def _actor(system, suffix: str, *, project: str = "c" * 64) -> dict[str, Any]:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"supervision-memory-{suffix}", project_digest=project
        ),
    )
    return service.authenticate(str(attachment["context_token"]))


def _work(system, actor, suffix: str) -> dict[str, Any]:
    return system["service"].assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Memory source {suffix}",
            objective="Preserve scoped operational experience without creating new authority.",
            acceptance=["Relevant experience can be recalled with exact provenance."],
            idempotency_key=f"memory-source-{suffix}",
        ),
    )


@pytest.fixture
def memory_case(system):
    actor = _actor(system, "original")
    return {**system, "actor": actor, "work": _work(system, actor, "original")}


def _write(case, **changes) -> MemoryWriteInput:
    return MemoryWriteInput.model_validate(
        {
            "work_item_id": case["work"]["id"],
            "primary_abstraction": "Verified dependency adjustment",
            "cue_anchors": ["progress recall", "shared prerequisite"],
            "value": "A complete observed outcome. richvalueneedle is deliberately not indexed.",
            "idempotency_key": "memory-initial-write",
            **changes,
        }
    )


def _remember(case, **changes):
    return remember_memory(case["service"], case["actor"], _write(case, **changes))


def _search(case, query: str, **changes):
    return search_memories(
        case["service"], case["actor"], MemorySearchInput(query=query, **changes)
    )


def _read(case, memory, **changes):
    return read_memory(
        case["service"],
        case["actor"],
        MemoryReadInput.model_validate(
            {
                "memory_id": memory["memory_id"],
                "expected_revision": memory["revision"],
                **changes,
            }
        ),
    )


def test_value_is_not_a_search_surface_and_search_is_a_pure_read(memory_case) -> None:
    case = memory_case
    memory = _remember(case)
    service = case["service"]
    before = service.db.commit_generation()
    events = service.db.fetchone("SELECT COUNT(*) FROM events")[0]
    assert _search(case, "richvalueneedle")["memories"] == []
    results = _search(case, "progress recall")
    assert results["total"] == 1 and results["next_offset"] is None
    found = results["memories"][0]
    assert found["memory_id"] == memory["memory_id"]
    assert found["matched_by"] == ["cue"]
    assert found["matched_cues"] == ["progress recall"]
    assert found["source_work_item_id"] == case["work"]["id"]
    assert "richvalueneedle" not in json.dumps(results)
    with service.db.connection_scope() as connection:
        connection.execute("PRAGMA query_only=ON")
        assert (
            search_memories_tx(
                connection, case["actor"], MemorySearchInput(query="progress recall")
            )
            == results
        )
    assert service.db.commit_generation() == before
    assert service.db.fetchone("SELECT COUNT(*) FROM events")[0] == events


def test_older_nonadjacent_memory_is_recalled_amid_unrelated_later_entries(memory_case) -> None:
    case = memory_case
    original = _remember(case)
    for number in range(9):
        _remember(
            case,
            primary_abstraction=f"Unrelated inventory {number}",
            cue_anchors=[f"unrelatedcue{number}"],
            value="An unrelated bounded observation.",
            idempotency_key=f"unrelated-memory-{number}",
        )
    result = _search(case, "progress recall")
    assert [row["memory_id"] for row in result["memories"]] == [original["memory_id"]]


def test_full_unicode_value_is_paginated_without_display_truncation_or_output_read(
    memory_case,
) -> None:
    case = memory_case
    value = "過去の試行と観測結果。\n" * 150 + "The original complete ending."
    memory = _remember(case, value=value)
    parts, offset = [], 0
    while True:
        page = _read(case, memory, character_offset=offset, max_chars=137)
        assert page["revision"] == 1 and page["untrusted"] is True
        assert page["total_characters"] == len(value)
        parts.append(page["content"])
        if page["complete"]:
            assert page["next_character_offset"] is None
            break
        offset = page["next_character_offset"]
    assert "".join(parts) == value and len(value) > 280
    assert memory["value_digest"] == hashlib.sha256(value.encode()).hexdigest()
    assert (
        case["service"].db.fetchone(
            "SELECT COUNT(*) FROM events WHERE event_type='worker_output.read'"
        )[0]
        == 0
    )
    assert case["service"].db.fetchone(
        "SELECT COUNT(*) FROM events WHERE event_type='supervision_memory.read'"
    )[0] == len(parts)


def test_revisions_and_old_cues_are_retained_and_reads_pin_an_immutable_revision(
    memory_case,
) -> None:
    case = memory_case
    original = _remember(case)
    updated = _remember(
        case,
        memory_id=original["memory_id"],
        expected_revision=1,
        primary_abstraction="Revised prerequisite strategy",
        cue_anchors=["alternate recall"],
        value="A changed strategy with its full observed result.",
        idempotency_key="memory-updated",
    )
    assert updated["revision"] == 2
    assert {"progress recall", "alternate recall"} <= set(updated["cue_anchors"])
    assert _read(case, original)["content"] == _write(case).value
    assert _read(case, updated)["content"] == "A changed strategy with its full observed result."
    assert _search(case, "progress recall")["memories"][0]["revision"] == 2
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 2


def test_repeated_write_is_idempotent_and_changed_parameters_do_not_rewrite_history(
    memory_case,
) -> None:
    case = memory_case
    first = _remember(case)
    assert _remember(case) == first
    with pytest.raises(ConflictError):
        _remember(case, value="Different value with the same receipt key.")
    with pytest.raises(ConflictError):
        _remember(
            case,
            memory_id=first["memory_id"],
            expected_revision=0,
            idempotency_key="stale-revision",
        )
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 1


@pytest.mark.parametrize(
    "primary", ["verified DEPENDENCY adjustment", "  Verified \n dependency\t adjustment  "]
)
def test_duplicate_primary_create_returns_exact_merge_target_without_writing(
    memory_case, primary
) -> None:
    case = memory_case
    original = _remember(case)
    before = case["service"].db.commit_generation()
    with pytest.raises(ConflictError) as failure:
        _remember(
            case,
            primary_abstraction=primary,
            value="A second observation that must be explicitly merged.",
            idempotency_key="duplicate-primary-create",
        )
    assert failure.value.details == {
        "reason_code": "memory_primary_conflict",
        "memory_id": original["memory_id"],
        "current_revision": 1,
        "retryable": False,
    }
    assert case["service"].db.commit_generation() == before
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 1
    assert _remember(case) == original


def test_duplicate_primary_is_scoped_to_original_conversation_or_shared_project(
    memory_case,
) -> None:
    case = memory_case
    conversation = _remember(case)
    project = _remember(case, scope="project", idempotency_key="shared-primary")
    other_actor = _actor(case, "same-primary-other")
    other = {
        **case,
        "actor": other_actor,
        "work": _work(case, other_actor, "same-primary-other"),
    }
    independent = _remember(other, idempotency_key="independent-primary")
    assert len({conversation["memory_id"], project["memory_id"], independent["memory_id"]}) == 3
    with pytest.raises(ConflictError) as failure:
        _remember(other, scope="project", idempotency_key="duplicate-shared-primary")
    assert failure.value.details["memory_id"] == project["memory_id"]
    assert "source_work_item_id" not in failure.value.details
    updated = _remember(
        other,
        scope="project",
        memory_id=project["memory_id"],
        expected_revision=1,
        idempotency_key="merge-shared-primary",
    )
    assert updated["revision"] == 2
    assert updated["source_work_item_id"] == other["work"]["id"]


def test_update_cannot_rename_an_entry_onto_another_primary(memory_case) -> None:
    case = memory_case
    original = _remember(case)
    alternative = _remember(
        case,
        primary_abstraction="Alternative observation",
        idempotency_key="alternative-primary",
    )
    with pytest.raises(ConflictError) as failure:
        _remember(
            case,
            memory_id=alternative["memory_id"],
            expected_revision=1,
            idempotency_key="rename-primary-collision",
        )
    assert failure.value.details["memory_id"] == original["memory_id"]
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 2


def test_concurrent_revision_cas_commits_one_value(memory_case) -> None:
    case = memory_case
    first = _remember(case)
    barrier = Barrier(2)

    def update(number):
        barrier.wait()
        try:
            return _remember(
                case,
                memory_id=first["memory_id"],
                expected_revision=1,
                value=f"A distinct observed correction {number}.",
                idempotency_key=f"concurrent-memory-{number}",
            )
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(update, (1, 2)))
    assert sum(result is not None for result in results) == 1
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 2


def test_conversation_scope_survives_reconnection_but_not_another_attachment(memory_case) -> None:
    case = memory_case
    original = _remember(case)
    other = {**case, "actor": _actor(case, "other")}
    assert _search(other, "progress recall")["memories"] == []
    with pytest.raises(NotFoundError):
        _read(other, original)
    with pytest.raises(NotFoundError):
        _search(other, "unrelated", related_to=original["memory_id"])
    reconnected = {**case, "actor": _actor(case, "original")}
    assert _search(reconnected, "progress recall")["total"] == 1
    # Another authentic bridge need not revoke the old one. Exercise a
    # genuinely revoked bearer separately instead of inferring connection death.
    assert _search(case, "progress recall")["total"] == 1
    case["service"].db.execute(
        "UPDATE cao_conversation_credentials SET state='revoked' WHERE id=?",
        (case["actor"]["_cao_conversation_credential_id"],),
    )
    with pytest.raises(AuthorizationError):
        _search(case, "progress recall")


def test_explicit_project_scope_shares_value_without_foreign_source_work_authority(
    memory_case,
) -> None:
    case = memory_case
    memory = _remember(case, scope="project")
    other = {**case, "actor": _actor(case, "project-reader")}
    found = _search(other, "progress recall")["memories"][0]
    assert "source_work_item_id" not in found
    assert _read(other, memory)["content"] == _write(case).value
    with pytest.raises(AuthorizationError):
        _remember(
            other,
            memory_id=memory["memory_id"],
            expected_revision=1,
            scope="project",
            idempotency_key="foreign-work-write",
        )
    foreign_project = {**case, "actor": _actor(case, "foreign-project", project="d" * 64)}
    assert _search(foreign_project, "progress recall")["memories"] == []
    with pytest.raises(NotFoundError):
        _read(foreign_project, memory)


def test_scope_cannot_be_silently_broadened_by_update(memory_case) -> None:
    case = memory_case
    memory = _remember(case)
    before = case["service"].db.commit_generation()
    with pytest.raises(ConflictError):
        _remember(
            case,
            memory_id=memory["memory_id"],
            expected_revision=1,
            scope="project",
            idempotency_key="scope-rewrite",
        )
    assert case["service"].db.commit_generation() == before


def test_related_cues_expand_to_another_primary_without_searching_values(memory_case) -> None:
    case = memory_case
    seed = _remember(case)
    related = _remember(
        case,
        primary_abstraction="Alternative interpretation",
        cue_anchors=["shared prerequisite"],
        value="Rich unrelated words stay outside search.",
        idempotency_key="related-memory",
    )
    assert _search(case, "no-direct-lexical-match")["memories"] == []
    result = _search(case, "no-direct-lexical-match", related_to=seed["memory_id"])
    assert [row["memory_id"] for row in result["memories"]] == [related["memory_id"]]
    assert result["memories"][0]["matched_by"] == ["cue-related"]
    assert result["memories"][0]["matched_cues"] == ["shared prerequisite"]


def test_pagination_has_no_implicit_recency_cutoff(memory_case) -> None:
    case = memory_case
    ids = {
        _remember(
            case,
            primary_abstraction=f"Shared recall {number}",
            cue_anchors=["paging cue"],
            idempotency_key=f"page-{number}",
        )["memory_id"]
        for number in range(5)
    }
    found, offset = set(), 0
    while True:
        page = _search(case, "paging cue", limit=2, offset=offset)
        assert page["total"] == 5
        found.update(row["memory_id"] for row in page["memories"])
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]
    assert found == ids


@pytest.mark.parametrize("field", ["primary_abstraction", "cue_anchors", "value"])
def test_unsafe_memory_is_rejected_atomically_before_index_or_value_storage(
    memory_case, field
) -> None:
    case = memory_case
    text = "/private/unsafe-memory-locator"
    before = case["service"].db.commit_generation()
    with pytest.raises(ValidationError):
        _remember(case, **{field: [text] if field == "cue_anchors" else text})
    assert case["service"].db.commit_generation() == before
    assert case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0


@pytest.mark.parametrize("actor_key", ["cao", "worker", "user"])
def test_principal_bootstrap_and_non_cao_roles_cannot_use_attachment_memory(
    memory_case, actor_key
) -> None:
    case = {**memory_case, "actor": memory_case[actor_key]}
    with pytest.raises(AuthorizationError):
        _search(case, "progress recall")
    with pytest.raises(AuthorizationError):
        _remember(case)


@pytest.mark.parametrize(
    "table,field",
    [
        ("supervision_memories", "scope"),
        ("supervision_memory_revisions", "value"),
        ("supervision_memory_cues", "cue_text"),
    ],
)
def test_memory_identity_values_and_cues_are_immutable(memory_case, table, field) -> None:
    _remember(memory_case)
    database = memory_case["service"].db
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(f"UPDATE {table} SET {field}={field}")
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(f"DELETE FROM {table}")


def test_restart_preserves_all_memory_rows_and_exact_goal_provenance(memory_case) -> None:
    case = memory_case
    original = _remember(case)
    database = case["service"].db
    before = {
        table: [dict(row) for row in database.fetchall(f"SELECT * FROM {table}")]
        for table in (
            "supervision_memories",
            "supervision_memory_revisions",
            "supervision_memory_cues",
        )
    }
    reopened = Database(case["settings"])
    reopened.initialize()
    assert before == {
        table: [dict(row) for row in reopened.fetchall(f"SELECT * FROM {table}")]
        for table in before
    }
    restarted = {**case, "service": ControlPlane(reopened, case["settings"])}
    assert _read(restarted, original)["content"] == _write(case).value
    revision = before["supervision_memory_revisions"][0]
    assert revision["source_work_item_id"] == case["work"]["id"]
    assert revision["source_goal_version"] == case["work"]["goal_version"]
    assert (
        revision["source_goal_packet_digest"]
        == case["work"]["current_attempt"]["goal_packet_digest"]
    )
    assert reopened.fetchone("PRAGMA foreign_key_check") is None


def test_history_cursor_backfill_and_append_are_stable_and_immutable(memory_case) -> None:
    case = memory_case
    service, work = case["service"], case["work"]
    attempt = work["current_attempt"]
    report = service.report(
        case["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            summary="A scoped next decision is needed.",
            expected_goal_version=work["goal_version"],
            expected_generation=work["generation"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            idempotency_key="memory-history-boundary",
        ),
    )
    boundary_id = report["open_boundaries"][0]["id"]
    before = dict(
        service.db.fetchone("SELECT * FROM supervision_history WHERE boundary_id=?", (boundary_id,))
    )
    assert before["sequence"] > 0
    # Model the real pre-v44 source: historical Boundaries exist, but no
    # permanent cursor ledger or append trigger has yet been installed.
    service.db.execute("DROP TRIGGER supervision_history_append")
    service.db.execute("DROP TABLE supervision_history")
    service.db.initialize()
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM supervision_history WHERE boundary_id=?", (boundary_id,)
            )
        )
        == before
    )
    with pytest.raises(sqlite3.IntegrityError):
        service.db.execute("UPDATE supervision_history SET sequence=sequence")
    with pytest.raises(sqlite3.IntegrityError):
        service.db.execute("DELETE FROM supervision_history")


@pytest.mark.parametrize("lifecycle", ["active", "superseded", "deleted"])
def test_legacy_lifecycle_is_preserved_and_only_active_entries_are_searched(
    memory_case, lifecycle
) -> None:
    case = memory_case
    actor, service = case["actor"], case["service"]
    memory_id, value = (
        f"mem_legacy_{lifecycle}",
        "Historical experience is untrusted evidence only.",
    )
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "INSERT INTO supervision_memories VALUES(?,?,?,?,?,'legacy',?,?,?,1,?,?)",
            (
                memory_id,
                actor["id"],
                actor["_cao_attachment_id"],
                actor["_cao_project_digest"],
                "conversation",
                lifecycle,
                "a" * 64,
                hashlib.sha256(lifecycle.encode()).hexdigest(),
                now,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO supervision_memory_revisions VALUES(?,1,?,?,?,NULL,NULL,NULL,?,?,?)",
            (
                memory_id,
                "Preserved legacy recall",
                value,
                hashlib.sha256(value.encode()).hexdigest(),
                actor["_cao_attachment_id"],
                actor["id"],
                now,
            ),
        )
        connection.execute(
            "INSERT INTO supervision_memory_cues VALUES(?,?,?,1,?)",
            (memory_id, "historical anchor", "Historical anchor", now),
        )
    result = _search(case, "legacy recall")
    assert result["total"] == (1 if lifecycle == "active" else 0)
    read = _read(case, {"memory_id": memory_id, "revision": 1})
    assert read["content"] == value and read["untrusted"] is True
    assert read["lifecycle_state"] == lifecycle
    if lifecycle != "active":
        with pytest.raises(ConflictError):
            _remember(
                case,
                memory_id=memory_id,
                expected_revision=1,
                idempotency_key=f"legacy-rewrite-{lifecycle}",
            )
        assert service.db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 1
        return
    with pytest.raises(ConflictError) as duplicate:
        _remember(
            case,
            primary_abstraction="  PRESERVED  legacy recall ",
            idempotency_key="duplicate-active-legacy",
        )
    assert duplicate.value.details["memory_id"] == memory_id
    updated = _remember(
        case,
        memory_id=memory_id,
        expected_revision=1,
        primary_abstraction="Preserved legacy recall",
        value="New experience explicitly merged with its historical source.",
        idempotency_key="merge-active-legacy",
    )
    assert updated["kind"] == "legacy" and updated["revision"] == 2
    assert updated["lifecycle_state"] == "active"
    assert "Historical anchor" in updated["cue_anchors"]
    assert _read(case, {"memory_id": memory_id, "revision": 1})["content"] == value
    identity = service.db.fetchone("SELECT * FROM supervision_memories WHERE id=?", (memory_id,))
    assert identity["origin_source_digest"] == "a" * 64
    assert identity["origin_id_digest"] == hashlib.sha256(lifecycle.encode()).hexdigest()
    revision = service.db.fetchone(
        "SELECT * FROM supervision_memory_revisions WHERE memory_id=? AND revision=2", (memory_id,)
    )
    assert revision["source_work_item_id"] == case["work"]["id"]
    assert revision["source_goal_version"] == case["work"]["goal_version"]
    assert (
        revision["source_goal_packet_digest"]
        == case["work"]["current_attempt"]["goal_packet_digest"]
    )
    with pytest.raises(ConflictError) as stale:
        _remember(
            case,
            memory_id=memory_id,
            expected_revision=1,
            idempotency_key="stale-legacy-revision",
        )
    assert stale.value.details["reason_code"] == "memory_revision_conflict"
    assert service.db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_goal_version", 999),
        ("source_goal_packet_digest", "f" * 64),
        ("revision", 3),
    ],
)
def test_revision_insertion_rejects_wrong_goal_or_skipped_revision(
    memory_case, field, value
) -> None:
    case = memory_case
    memory = _remember(case)
    database = case["service"].db
    original = dict(
        database.fetchone(
            "SELECT * FROM supervision_memory_revisions WHERE memory_id=?", (memory["memory_id"],)
        )
    )
    invalid = {**original, "revision": 2, field: value}
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(
            "INSERT INTO supervision_memory_revisions VALUES("
            + ",".join("?" for _ in invalid)
            + ")",
            tuple(invalid.values()),
        )
    assert database.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 1


def test_current_revision_cannot_commit_without_its_append_only_value(memory_case) -> None:
    case = memory_case
    actor, database = case["actor"], case["service"].db
    now = utc_now()
    before = database.commit_generation()
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(
            "INSERT INTO supervision_memories(id,owner_principal_id,origin_attachment_id,project_digest,scope,kind,lifecycle_state,current_revision,created_at,updated_at) VALUES('mem_partial',?,?,?,'conversation','curated','active',1,?,?)",
            (actor["id"], actor["_cao_attachment_id"], actor["_cao_project_digest"], now, now),
        )
    assert database.commit_generation() == before
    assert database.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0


def test_unsafe_query_is_not_echoed_or_written_to_events(memory_case) -> None:
    case = memory_case
    before = case["service"].db.commit_generation()
    with pytest.raises(ValidationError):
        _search(case, "/private/unsafe-memory-locator")
    assert case["service"].db.commit_generation() == before


@pytest.mark.parametrize("tampering", ["wrong_digest", "unsafe_content"])
def test_read_fails_closed_for_corrupt_or_unsafe_stored_value(memory_case, tampering) -> None:
    case = memory_case
    memory = _remember(case)
    database = case["service"].db
    database.execute("DROP TRIGGER supervision_memory_revisions_immutable_update")
    value = (
        "Changed without an integrity update."
        if tampering == "wrong_digest"
        else "/private/unsafe-memory-locator"
    )
    digest = (
        memory["value_digest"]
        if tampering == "wrong_digest"
        else hashlib.sha256(value.encode()).hexdigest()
    )
    database.execute(
        "UPDATE supervision_memory_revisions SET value=?,value_digest=?", (value, digest)
    )
    with pytest.raises(ConflictError if tampering == "wrong_digest" else ValidationError):
        _read(case, memory)


def test_partial_memory_migration_rejects_missing_foreign_key_atomically(memory_case) -> None:
    database = memory_case["service"].db
    original = database.fetchone(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='supervision_memory_cues'"
    )[0]
    database.execute("DROP TABLE supervision_memory_cues")
    database.execute(original.replace("REFERENCES supervision_memories(id) ON DELETE RESTRICT", ""))
    database.execute("UPDATE metadata SET value='43' WHERE key='schema_version'")
    database.execute("PRAGMA user_version=43")
    with pytest.raises(RuntimeError, match="memory key contract is incompatible"):
        Database(memory_case["settings"])
    assert database.fetchone("SELECT value FROM metadata WHERE key='schema_version'")[0] == "43"
    assert database.fetchone("PRAGMA user_version")[0] == 43

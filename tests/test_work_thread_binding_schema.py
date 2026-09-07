from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.database import SCHEMA_VERSION, Database, utc_after, utc_now
from cao_control_plane.goal_packets import (
    build_goal_packet,
    build_task_packet,
    canonical_json,
    goal_packet_digest,
    task_packet_digest,
)
from cao_control_plane.service import worker_mcp_tool_contract_digest

_BINDING_TRIGGERS = (
    "work_items_managed_thread_pair_insert",
    "work_items_managed_thread_pair_update",
    "work_items_managed_thread_exact_insert",
    "work_items_managed_thread_exact_update",
    "work_items_managed_thread_rebind_update",
    "attempts_managed_thread_exact_insert",
    "attempts_managed_thread_identity_immutable",
    "managed_worker_threads_nonterminal_work_update",
    "managed_worker_threads_nonterminal_work_delete",
)


def _attached_actor(system: dict[str, Any]) -> dict[str, Any]:
    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id="work-thread-binding-schema",
            project_digest="b" * 64,
        ),
    )
    return system["service"].authenticate(str(attachment["context_token"]))


def _seed_managed_lane(
    system: dict[str, Any],
    actor: dict[str, Any],
    suffix: str,
) -> dict[str, str]:
    ids = {
        "principal_id": f"prn_binding_{suffix}",
        "runtime_id": f"run_binding_{suffix}",
        "enrollment_id": f"enr_binding_{suffix}",
        "spec_id": f"mws_binding_{suffix}",
        "thread_id": f"mwt_binding_{suffix}",
        "epoch_id": f"mwe_binding_{suffix}",
    }
    now = utc_now()
    digest = hashlib.sha256(suffix.encode()).hexdigest()
    with system["service"].db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO principals(
                id, name, role, token_hash, enabled, operator_scope,
                operator_label, metadata_json, created_at, updated_at
            ) VALUES(?, ?, 'worker', ?, 1, 'production', ?, '{}', ?, ?)
            """,
            (
                ids["principal_id"],
                f"binding-worker-{suffix}",
                f"discarded-hash-{suffix}",
                f"Binding Worker {suffix}",
                now,
                now,
            ),
        )
        # Runtime/enrollment health is deliberately unavailable. Logical Work
        # admission below must depend on the durable lane, not this projection.
        connection.execute(
            """
            INSERT INTO runtime_sessions(
                id, principal_id, adapter, endpoint, native_session_id, state,
                lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
            ) VALUES(?, ?, 'codex-app-server', '', '', 'missing', ?, ?, '{}', ?, ?)
            """,
            (ids["runtime_id"], ids["principal_id"], utc_after(86400), now, now, now),
        )
        connection.execute(
            """
            INSERT INTO worker_enrollments(
                id, principal_id, runtime_session_id, state, generation, managed,
                required_tools_digest, discovered_tools_digest, protocol_version,
                heartbeat_sequence, discovered_at, heartbeat_at, lease_expires_at,
                revoked_at, created_at, updated_at
            ) VALUES(?, ?, ?, 'stale', 1, 1, ?, '', '', 0,
                     NULL, NULL, ?, NULL, ?, ?)
            """,
            (
                ids["enrollment_id"],
                ids["principal_id"],
                ids["runtime_id"],
                worker_mcp_tool_contract_digest(),
                utc_after(86400),
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_specs(
                id, attachment_id, attachment_generation, principal_id,
                runtime_session_id, enrollment_id, worker_profile_id, adapter,
                workspace_ref, requested_model, effective_model,
                requested_reasoning_effort, effective_reasoning_effort,
                provider_scope_digest, catalog_target_id, state,
                policy_binding_digest, input_digest, idempotency_key,
                created_at, updated_at, stopped_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'test-profile', 'codex-app-server', ?,
                     'test-model', 'test-model', 'high', 'high', '', '', 'enabled',
                     ?, ?, ?, ?, ?, NULL)
            """,
            (
                ids["spec_id"],
                actor["_cao_attachment_id"],
                actor["_cao_attachment_generation"],
                ids["principal_id"],
                ids["runtime_id"],
                ids["enrollment_id"],
                f"workspace-{suffix}",
                digest,
                digest,
                f"binding-{suffix}",
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_threads(
                id, managed_spec_id, state, generation,
                created_at, updated_at, archived_at
            ) VALUES(?, ?, 'active', 1, ?, ?, NULL)
            """,
            (ids["thread_id"], ids["spec_id"], now, now),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_thread_epochs(
                id, thread_id, generation, connection_generation,
                runtime_session_id, enrollment_id, created_at, retired_at
            ) VALUES(?, ?, 1, 1, ?, ?, ?, NULL)
            """,
            (
                ids["epoch_id"],
                ids["thread_id"],
                ids["runtime_id"],
                ids["enrollment_id"],
                now,
            ),
        )
    return ids


def _insert_work(
    system: dict[str, Any],
    actor: dict[str, Any],
    lane: dict[str, str],
    work_id: str,
    *,
    attempts: tuple[tuple[str, str | None], ...],
    include_binding_columns: bool,
    binding: tuple[str, int] | None,
    supervisor_attachment_id: str | None,
    state: str = "active",
    operator_scope: str = "production",
) -> None:
    now = utc_now()
    title = f"Work {work_id}"
    objective = f"Objective {work_id}"
    metadata = {"byte_preservation_fixture": work_id}
    metadata_json = canonical_json(metadata)
    with system["service"].db.transaction() as connection:
        attachment_packet: dict[str, Any] | None = None
        if supervisor_attachment_id is not None:
            attachment = connection.execute(
                """
                SELECT id, principal_id, runtime_session_id, native_thread_id,
                       project_digest, generation
                FROM cao_session_attachments WHERE id = ?
                """,
                (supervisor_attachment_id,),
            ).fetchone()
            assert attachment is not None
            attachment_packet = {
                "attachment_id": str(attachment["id"]),
                "supervisor_id": str(attachment["principal_id"]),
                "runtime_session_id": str(attachment["runtime_session_id"]),
                "native_thread_id": str(attachment["native_thread_id"]),
                "project_digest": str(attachment["project_digest"]),
                "generation": int(attachment["generation"]),
            }
        goal_packet = build_goal_packet(
            work_item_id=work_id,
            version=1,
            title=title,
            objective=objective,
            maturity="defined",
            acceptance=(),
            non_goals=(),
            priority=50,
            requester_id=str(system["user"]["id"]),
            supervisor_id=str(actor["id"]),
            metadata=metadata,
            reason="schema36 fixture",
            created_by=str(actor["id"]),
            source_intent_id=None,
            source_directive_id=None,
            correlation_id="",
            prior_version=None,
            supervisor_attachment=attachment_packet,
        )
        goal_digest = goal_packet_digest(goal_packet)
        columns = [
            "id",
            "title",
            "goal_version",
            "state",
            "priority",
            "created_by",
            "requester_id",
            "supervisor_id",
            "supervisor_attachment_id",
            "assigned_worker_id",
            "operator_scope",
            "attention_owner",
            "generation",
            "metadata_json",
            "created_at",
            "updated_at",
        ]
        values: list[Any] = [
            work_id,
            title,
            1,
            state,
            50,
            actor["id"],
            system["user"]["id"],
            actor["id"],
            supervisor_attachment_id,
            lane["principal_id"],
            operator_scope,
            "worker" if state == "active" else "none",
            1,
            metadata_json,
            now,
            now,
        ]
        if include_binding_columns:
            columns.extend(["managed_worker_thread_id", "managed_worker_thread_generation"])
            values.extend(binding if binding is not None else (None, None))
        connection.execute(
            f"INSERT INTO work_items({', '.join(columns)}) "
            f"VALUES({', '.join('?' for _ in columns)})",
            tuple(values),
        )
        connection.execute(
            """
            INSERT INTO goal_revisions(
                work_item_id, version, title, objective, maturity,
                acceptance_json, non_goals_json, priority, requester_id,
                supervisor_id, metadata_json, packet_json, packet_digest,
                reason, created_by, correlation_id,
                supervisor_attachment_generation,
                supervisor_runtime_session_id, created_at
            ) VALUES(?, 1, ?, ?, 'defined', '[]', '[]', 50, ?, ?, ?, ?, ?,
                     'schema36 fixture', ?, '', ?, ?, ?)
            """,
            (
                work_id,
                title,
                objective,
                system["user"]["id"],
                actor["id"],
                metadata_json,
                canonical_json(goal_packet),
                goal_digest,
                actor["id"],
                (
                    actor["_cao_attachment_generation"]
                    if supervisor_attachment_id is not None
                    else None
                ),
                (
                    attachment_packet["runtime_session_id"]
                    if attachment_packet is not None
                    else None
                ),
                now,
            ),
        )
        for number, (worker_id, runtime_id) in enumerate(attempts, start=1):
            attempt_id = f"att_{work_id}_{number}"
            task_digest = task_packet_digest(
                build_task_packet(
                    goal_packet_digest_value=goal_digest,
                    work_item_id=work_id,
                    goal_version=1,
                    attempt_id=attempt_id,
                    attempt_number=number,
                    worker_id=worker_id,
                    runtime_session_id=runtime_id,
                    supervisor_attachment=attachment_packet,
                )
            )
            connection.execute(
                """
                INSERT INTO attempts(
                    id, work_item_id, attempt_number, worker_id,
                    runtime_session_id, goal_version, goal_packet_digest,
                    task_packet_digest, state, trajectory, evidence_confidence,
                    completion_claim_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?, 'untracked', 'unknown', ?, ?, ?)
                """,
                (
                    attempt_id,
                    work_id,
                    number,
                    worker_id,
                    runtime_id,
                    goal_digest,
                    task_digest,
                    "assigned" if state == "active" else "canceled",
                    f"completion-evidence:\x00:{work_id}:{number}",
                    now,
                    now,
                ),
            )
        if attempts:
            connection.execute(
                """
                INSERT INTO artifacts(
                    id, work_item_id, attempt_id, producer_id, name, uri,
                    media_type, digest, metadata_json, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'application/octet-stream', ?, ?, ?)
                """,
                (
                    f"art_{work_id}",
                    work_id,
                    f"att_{work_id}_{len(attempts)}",
                    attempts[-1][0],
                    f"artifact-{work_id}",
                    f"artifact-uri:\x00:{work_id}",
                    f"artifact-digest:\x00:{work_id}",
                    f"artifact-metadata:\x00:{work_id}",
                    now,
                ),
            )


def _drop_binding_guards(connection: sqlite3.Connection) -> None:
    for trigger in _BINDING_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    connection.execute("DROP INDEX IF EXISTS work_items_managed_thread_generation_state_idx")


def _set_schema_version_35(connection: sqlite3.Connection) -> None:
    connection.execute("UPDATE metadata SET value = '35' WHERE key = 'schema_version'")
    connection.execute("DELETE FROM schema_migrations WHERE version = 36")
    connection.execute("PRAGMA user_version = 35")


def _downgrade_work_bindings_to_v35(system: dict[str, Any]) -> None:
    with system["service"].db.transaction() as connection:
        _drop_binding_guards(connection)
        connection.execute("ALTER TABLE work_items DROP COLUMN managed_worker_thread_generation")
        connection.execute("ALTER TABLE work_items DROP COLUMN managed_worker_thread_id")
        _set_schema_version_35(connection)


def _payload_snapshot(connection: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    rows: list[tuple[Any, ...]] = []
    for table, columns in (
        ("work_items", ("metadata_json",)),
        ("goal_revisions", ("metadata_json", "packet_json", "packet_digest")),
        (
            "attempts",
            ("goal_packet_digest", "task_packet_digest", "completion_claim_json"),
        ),
        ("artifacts", ("uri", "digest", "metadata_json")),
    ):
        expressions = ", ".join(f"hex(CAST({column} AS BLOB))" for column in columns)
        rows.extend(
            tuple(row)
            for row in connection.execute(
                f"SELECT '{table}', rowid, {expressions} FROM {table} ORDER BY rowid"
            )
        )
    return tuple(rows)


def test_schema36_enforces_managed_work_lane_without_runtime_health(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    lane = _seed_managed_lane(system, actor, "fresh")
    _insert_work(
        system,
        actor,
        lane,
        "wrk_binding_fresh",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=True,
        binding=(lane["thread_id"], 1),
        supervisor_attachment_id=str(actor["_cao_attachment_id"]),
    )

    bound = system["service"].db.fetchone(
        """
        SELECT managed_worker_thread_id, managed_worker_thread_generation
        FROM work_items WHERE id = 'wrk_binding_fresh'
        """
    )
    assert bound is not None
    assert tuple(bound) == (lane["thread_id"], 1)

    with pytest.raises(sqlite3.IntegrityError, match="exact active Worker thread"):
        _insert_work(
            system,
            actor,
            lane,
            "wrk_binding_missing",
            attempts=(),
            include_binding_columns=True,
            binding=None,
            supervisor_attachment_id=str(actor["_cao_attachment_id"]),
        )

    with (
        pytest.raises(sqlite3.IntegrityError, match="Attempt worker or runtime"),
        system["service"].db.transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO attempts(
                id, work_item_id, attempt_number, worker_id,
                runtime_session_id, goal_version, goal_packet_digest,
                task_packet_digest, state, trajectory, evidence_confidence,
                created_at, updated_at
            ) VALUES(
                'att_binding_wrong_runtime', 'wrk_binding_fresh', 2, ?, ?, 1,
                'goal', 'task', 'assigned', 'untracked', 'unknown', ?, ?
            )
            """,
            (
                lane["principal_id"],
                system["runtime"]["id"],
                utc_now(),
                utc_now(),
            ),
        )

    with pytest.raises(sqlite3.IntegrityError, match="logical thread generation"):
        system["service"].db.execute(
            """
            UPDATE work_items
            SET managed_worker_thread_generation = 2
            WHERE id = 'wrk_binding_fresh'
            """
        )

    for statement in (
        "UPDATE managed_worker_threads SET state = 'archived' WHERE id = ?",
        "DELETE FROM managed_worker_threads WHERE id = ?",
    ):
        with pytest.raises(sqlite3.IntegrityError, match="nonterminal Work"):
            system["service"].db.execute(statement, (lane["thread_id"],))

    with system["service"].db.transaction() as connection:
        connection.execute(
            "UPDATE work_items SET state = 'canceled' WHERE id = 'wrk_binding_fresh'"
        )
        connection.execute("DELETE FROM managed_worker_threads WHERE id = ?", (lane["thread_id"],))
    preserved = system["service"].db.fetchone(
        """
        SELECT managed_worker_thread_id, managed_worker_thread_generation
        FROM work_items WHERE id = 'wrk_binding_fresh'
        """
    )
    assert preserved is not None
    assert tuple(preserved) == (lane["thread_id"], 1)


def test_schema36_rejects_managed_attempt_worker_for_unmanaged_work(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    managed_lane = _seed_managed_lane(system, actor, "attempt-worker-mismatch")
    owner = system["service"].db.fetchone(
        "SELECT operator_scope FROM principals WHERE id = ?",
        (system["worker"]["id"],),
    )
    assert owner is not None
    unmanaged_lane = {
        "principal_id": str(system["worker"]["id"]),
        "runtime_id": str(system["runtime"]["id"]),
    }
    _insert_work(
        system,
        actor,
        unmanaged_lane,
        "wrk_unmanaged_attempt_owner",
        attempts=((unmanaged_lane["principal_id"], unmanaged_lane["runtime_id"]),),
        include_binding_columns=True,
        binding=None,
        supervisor_attachment_id=str(actor["_cao_attachment_id"]),
        operator_scope=str(owner["operator_scope"]),
    )

    with (
        pytest.raises(sqlite3.IntegrityError, match="Attempt worker or runtime"),
        system["service"].db.transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO attempts(
                id, work_item_id, attempt_number, worker_id,
                runtime_session_id, goal_version, goal_packet_digest,
                task_packet_digest, state, trajectory, evidence_confidence,
                created_at, updated_at
            ) VALUES(
                'att_unmanaged_attempt_owner_2', 'wrk_unmanaged_attempt_owner', 2,
                ?, ?, 1, 'goal', 'task', 'assigned', 'untracked', 'unknown', ?, ?
            )
            """,
            (
                managed_lane["principal_id"],
                unmanaged_lane["runtime_id"],
                utc_now(),
                utc_now(),
            ),
        )


def test_schema36_v35_backfill_is_exact_latest_only_and_byte_preserving(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    lane = _seed_managed_lane(system, actor, "migration")
    historical_lane = _seed_managed_lane(system, actor, "historical")
    deleted_lane = _seed_managed_lane(system, actor, "deleted")
    _downgrade_work_bindings_to_v35(system)

    attachment_id = str(actor["_cao_attachment_id"])
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_exact",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_no_attempt",
        attempts=(),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_latest_missing",
        attempts=(
            (lane["principal_id"], lane["runtime_id"]),
            (lane["principal_id"], None),
        ),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_unmapped_runtime",
        attempts=((lane["principal_id"], system["runtime"]["id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_worker_mismatch",
        attempts=((system["worker"]["id"], lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_backfill_attachment_mismatch",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=None,
    )
    with system["service"].db.transaction() as connection:
        now = utc_now()
        connection.execute(
            "UPDATE managed_worker_thread_epochs SET retired_at = ? WHERE id = ?",
            (now, historical_lane["epoch_id"]),
        )
        connection.execute(
            "UPDATE managed_worker_threads SET state = 'archived', archived_at = ? WHERE id = ?",
            (now, historical_lane["thread_id"]),
        )
        connection.execute(
            "UPDATE managed_worker_specs SET state = 'stopped', stopped_at = ? WHERE id = ?",
            (now, historical_lane["spec_id"]),
        )
    _insert_work(
        system,
        actor,
        historical_lane,
        "wrk_backfill_historical",
        attempts=((historical_lane["principal_id"], historical_lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
        state="canceled",
    )
    _insert_work(
        system,
        actor,
        deleted_lane,
        "wrk_backfill_deleted_thread",
        attempts=((deleted_lane["principal_id"], deleted_lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
        state="canceled",
    )
    with system["service"].db.transaction() as connection:
        connection.execute(
            "DELETE FROM managed_worker_threads WHERE id = ?",
            (deleted_lane["thread_id"],),
        )

    with system["service"].db.connect() as connection:
        before = _payload_snapshot(connection)

    migrated = Database(system["settings"])
    with migrated.connect() as connection:
        rows = {
            str(row["id"]): (
                row["managed_worker_thread_id"],
                row["managed_worker_thread_generation"],
            )
            for row in connection.execute(
                """
                SELECT id, managed_worker_thread_id,
                       managed_worker_thread_generation
                FROM work_items WHERE id LIKE 'wrk_backfill_%'
                ORDER BY id
                """
            )
        }
        assert rows["wrk_backfill_exact"] == (lane["thread_id"], 1)
        assert rows["wrk_backfill_historical"] == (historical_lane["thread_id"], 1)
        for work_id in (
            "wrk_backfill_no_attempt",
            "wrk_backfill_latest_missing",
            "wrk_backfill_unmapped_runtime",
            "wrk_backfill_worker_mismatch",
            "wrk_backfill_attachment_mismatch",
            "wrk_backfill_deleted_thread",
        ):
            assert rows[work_id] == (None, None)
        assert _payload_snapshot(connection) == before
        migration = connection.execute(
            "SELECT description FROM schema_migrations WHERE version = 36"
        ).fetchone()
        assert migration is not None
        index = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'index'
              AND name = 'work_items_managed_thread_generation_state_idx'
            """
        ).fetchone()
        assert index is not None and "WHERE managed_worker_thread_id IS NOT NULL" in str(
            index["sql"]
        )
        plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT id FROM work_items
            WHERE managed_worker_thread_id = ?
              AND managed_worker_thread_generation = ?
              AND state = 'active'
            ORDER BY updated_at DESC
            """,
            (lane["thread_id"], 1),
        ).fetchall()
        assert any(
            "work_items_managed_thread_generation_state_idx" in str(row["detail"]) for row in plan
        )
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    migrated.initialize()
    with migrated.connect() as connection:
        assert _payload_snapshot(connection) == before
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 36"
            ).fetchone()[0]
            == 1
        )


def test_schema36_reopen_never_infers_a_post_cutover_null_binding(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    lane = _seed_managed_lane(system, actor, "reopen-null")
    _insert_work(
        system,
        actor,
        lane,
        "wrk_binding_reopen_null",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=True,
        binding=(lane["thread_id"], 1),
        supervisor_attachment_id=str(actor["_cao_attachment_id"]),
    )
    with system["service"].db.transaction() as connection:
        _drop_binding_guards(connection)
        connection.execute(
            """
            UPDATE work_items
            SET managed_worker_thread_id = NULL,
                managed_worker_thread_generation = NULL
            WHERE id = 'wrk_binding_reopen_null'
            """
        )
        before = _payload_snapshot(connection)
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    reopened = Database(system["settings"])
    with reopened.connect() as connection:
        work = connection.execute(
            """
            SELECT managed_worker_thread_id, managed_worker_thread_generation
            FROM work_items WHERE id = 'wrk_binding_reopen_null'
            """
        ).fetchone()
        assert work is not None and tuple(work) == (None, None)
        assert _payload_snapshot(connection) == before

    with (
        pytest.raises(sqlite3.IntegrityError, match="Attempt worker or runtime"),
        reopened.transaction() as connection,
    ):
        connection.execute(
            """
            INSERT INTO attempts(
                id, work_item_id, attempt_number, worker_id,
                runtime_session_id, goal_version, goal_packet_digest,
                task_packet_digest, state, trajectory, evidence_confidence,
                created_at, updated_at
            ) VALUES(
                'att_binding_reopen_null_2', 'wrk_binding_reopen_null', 2, ?, ?, 1,
                'goal', 'task', 'assigned', 'untracked', 'unknown', ?, ?
            )
            """,
            (lane["principal_id"], lane["runtime_id"], utc_now(), utc_now()),
        )


def test_pre_pair_schema36_backfills_once_and_preserves_unresolved_null(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    lane = _seed_managed_lane(system, actor, "pre-pair-36")
    with system["service"].db.transaction() as connection:
        _drop_binding_guards(connection)
        connection.execute("ALTER TABLE work_items DROP COLUMN managed_worker_thread_generation")
        connection.execute("ALTER TABLE work_items DROP COLUMN managed_worker_thread_id")
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    attachment_id = str(actor["_cao_attachment_id"])
    _insert_work(
        system,
        actor,
        lane,
        "wrk_pre_pair_36_exact",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    _insert_work(
        system,
        actor,
        lane,
        "wrk_pre_pair_36_unresolved",
        attempts=(
            (lane["principal_id"], lane["runtime_id"]),
            (lane["principal_id"], None),
        ),
        include_binding_columns=False,
        binding=None,
        supervisor_attachment_id=attachment_id,
    )
    with system["service"].db.connect() as connection:
        before = _payload_snapshot(connection)

    first_reopen = Database(system["settings"])
    with first_reopen.connect() as connection:
        rows = {
            str(row["id"]): (
                row["managed_worker_thread_id"],
                row["managed_worker_thread_generation"],
            )
            for row in connection.execute(
                """
                SELECT id, managed_worker_thread_id,
                       managed_worker_thread_generation
                FROM work_items WHERE id LIKE 'wrk_pre_pair_36_%'
                """
            )
        }
        assert rows["wrk_pre_pair_36_exact"] == (lane["thread_id"], 1)
        assert rows["wrk_pre_pair_36_unresolved"] == (None, None)
        assert _payload_snapshot(connection) == before

    # Once the pair exists at schema36, making the latest Attempt look exact
    # cannot grant a previously unresolved Work lifecycle authority.
    with first_reopen.transaction() as connection:
        _drop_binding_guards(connection)
        connection.execute("DELETE FROM attempts WHERE id = 'att_wrk_pre_pair_36_unresolved_2'")
    second_reopen = Database(system["settings"])
    with second_reopen.connect() as connection:
        row = connection.execute(
            """
            SELECT managed_worker_thread_id, managed_worker_thread_generation
            FROM work_items WHERE id = 'wrk_pre_pair_36_unresolved'
            """
        ).fetchone()
        assert row is not None and tuple(row) == (None, None)


def test_schema36_rejects_partial_schema_without_advancing_ledger(
    system: dict[str, Any],
) -> None:
    _downgrade_work_bindings_to_v35(system)
    with system["service"].db.transaction() as connection:
        connection.execute("ALTER TABLE work_items ADD COLUMN managed_worker_thread_id TEXT")

    with pytest.raises(RuntimeError, match="binding schema is partial"):
        Database(system["settings"])

    with system["service"].db.connect() as connection:
        columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(work_items)")}
        assert "managed_worker_thread_id" in columns
        assert "managed_worker_thread_generation" not in columns
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 35
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 36"
            ).fetchone()[0]
            == 0
        )


def test_schema36_rejects_conflicting_binding_without_mutating_history(
    system: dict[str, Any],
) -> None:
    actor = _attached_actor(system)
    lane = _seed_managed_lane(system, actor, "conflict")
    _insert_work(
        system,
        actor,
        lane,
        "wrk_binding_conflict",
        attempts=((lane["principal_id"], lane["runtime_id"]),),
        include_binding_columns=True,
        binding=(lane["thread_id"], 1),
        supervisor_attachment_id=str(actor["_cao_attachment_id"]),
    )
    with system["service"].db.transaction() as connection:
        _drop_binding_guards(connection)
        connection.execute(
            """
            UPDATE work_items
            SET managed_worker_thread_id = 'mwt_conflicting_history',
                managed_worker_thread_generation = 99
            WHERE id = 'wrk_binding_conflict'
            """
        )
        _set_schema_version_35(connection)
        before = _payload_snapshot(connection)

    with pytest.raises(RuntimeError, match="binding conflicts with its Attempt"):
        Database(system["settings"])

    with system["service"].db.connect() as connection:
        row = connection.execute(
            """
            SELECT managed_worker_thread_id, managed_worker_thread_generation
            FROM work_items WHERE id = 'wrk_binding_conflict'
            """
        ).fetchone()
        assert row is not None
        assert tuple(row) == ("mwt_conflicting_history", 99)
        assert _payload_snapshot(connection) == before
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 35
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 36"
            ).fetchone()[0]
            == 0
        )

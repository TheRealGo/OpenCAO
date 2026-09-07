"""Immutable operator scope attribution for durable Control Plane events."""

from __future__ import annotations

import sqlite3

_UNCLASSIFIED = "unclassified"
_OPERATOR_SCOPES = frozenset({"production", "acceptance-test", "system", _UNCLASSIFIED})

_EVENT_SCOPE_QUERIES: dict[str, str] = {
    "work_item": "SELECT operator_scope FROM work_items WHERE id = ?",
    "attempt": (
        "SELECT work.operator_scope FROM attempts AS attempt "
        "JOIN work_items AS work ON work.id = attempt.work_item_id "
        "WHERE attempt.id = ?"
    ),
    "message": (
        "SELECT work.operator_scope FROM messages AS message "
        "JOIN work_items AS work ON work.id = message.work_item_id "
        "WHERE message.id = ?"
    ),
    "managed_worker_spec": (
        "SELECT principal.operator_scope FROM managed_worker_specs AS spec "
        "JOIN principals AS principal ON principal.id = spec.principal_id "
        "WHERE spec.id = ?"
    ),
    "managed_worker_thread": (
        "WITH target(id) AS (VALUES (?)) "
        "SELECT COALESCE("
        "(SELECT principal.operator_scope "
        "FROM managed_worker_threads AS thread "
        "JOIN managed_worker_specs AS spec ON spec.id = thread.managed_spec_id "
        "JOIN principals AS principal ON principal.id = spec.principal_id "
        "JOIN target ON target.id = thread.id LIMIT 1), "
        "(SELECT work.operator_scope FROM work_items AS work "
        "JOIN target ON target.id = work.managed_worker_thread_id "
        "WHERE work.operator_scope <> 'unclassified' "
        "ORDER BY work.created_at, work.id LIMIT 1)) AS operator_scope"
    ),
    "runtime": (
        "SELECT principal.operator_scope FROM runtime_sessions AS runtime "
        "JOIN principals AS principal ON principal.id = runtime.principal_id "
        "WHERE runtime.id = ?"
    ),
    "worker_enrollment": (
        "SELECT principal.operator_scope FROM worker_enrollments AS enrollment "
        "JOIN principals AS principal ON principal.id = enrollment.principal_id "
        "WHERE enrollment.id = ?"
    ),
    "principal": (
        "SELECT operator_scope FROM principals WHERE id = ? AND role = 'worker'"
    ),
    "effect_operation": (
        "SELECT work.operator_scope FROM effect_operations AS effect "
        "JOIN work_items AS work ON work.id = effect.cleanup_work_item_id "
        "WHERE effect.id = ?"
    ),
    "a2a_task": (
        "SELECT work.operator_scope FROM a2a_task_map AS task "
        "JOIN work_items AS work ON work.id = task.work_item_id "
        "WHERE task.task_id = ?"
    ),
}


def normalize_event_operator_scope(value: object) -> str:
    """Return one persisted scope enum, failing closed for every other value."""

    normalized = str(value)
    return normalized if normalized in _OPERATOR_SCOPES else _UNCLASSIFIED


def resolve_event_operator_scope_tx(
    connection: sqlite3.Connection,
    aggregate_type: str,
    aggregate_id: str,
) -> str:
    """Resolve scope while the event aggregate still exists, failing closed."""

    query = _EVENT_SCOPE_QUERIES.get(aggregate_type)
    if query is None:
        return _UNCLASSIFIED
    row = connection.execute(query, (aggregate_id,)).fetchone()
    if row is None:
        return _UNCLASSIFIED
    return normalize_event_operator_scope(row["operator_scope"])


def backfill_event_operator_scopes_tx(connection: sqlite3.Connection) -> None:
    """Best-effort attribution for legacy events whose aggregates remain durable."""

    for aggregate_type, query in _EVENT_SCOPE_QUERIES.items():
        correlated_query = query.replace("?", "event.aggregate_id")
        connection.execute(
            f"""
            UPDATE events AS event
            SET operator_scope = COALESCE(({correlated_query}), 'unclassified')
            WHERE event.aggregate_type = ?
              AND event.operator_scope = 'unclassified'
            """,
            (aggregate_type,),
        )

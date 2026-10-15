"""Stable, process-loaded contract for CAO conversation MCP connections."""

from __future__ import annotations

import re
import sqlite3

# Bump this only when bridge behavior changes in a way that the complete MCP
# tool catalog digest cannot represent. A long-lived Python bridge retains this
# value in memory, so reading newer package files cannot impersonate a current
# connection contract.
CAO_CONVERSATION_PROXY_ABI_VERSION = 2


def cao_automatic_wake_ready_sql(*, attachment_alias: str = "attachment") -> str:
    """Use admitted connection evidence to gate automatic conversation wakes.

    Durable supervision authority and the ability to consume an automatic wake
    are independent. A catalog rollover invalidates the latter until an authentic
    bridge presents the current contract for this exact attachment generation.
    Lease expiry alone does not invalidate a process-loaded catalog: a cold
    conversation may reacquire its credential when awakened. Revoked, stale or
    failed connections provide no such evidence. This predicate does not govern
    inbox visibility, command authority, Worker execution or lifecycle admission.
    """

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", attachment_alias):
        raise ValueError("attachment SQL alias must be a static identifier")
    return f"""
        EXISTS (
            SELECT 1 FROM cao_attachment_connections AS wake_connection
            JOIN metadata AS wake_catalog
              ON wake_catalog.key = 'conversation_mcp_catalog_digest'
             AND wake_catalog.value = wake_connection.proxy_catalog_digest
            JOIN metadata AS wake_abi
              ON wake_abi.key = 'conversation_mcp_proxy_abi_version'
             AND wake_abi.value = CAST(wake_connection.proxy_abi_version AS TEXT)
            WHERE wake_connection.attachment_id = {attachment_alias}.id
              AND wake_connection.principal_id = {attachment_alias}.principal_id
              AND wake_connection.generation = {attachment_alias}.generation
              AND wake_connection.state IN ('active', 'expired')
        )
    """


def cao_automatic_wake_ready_tx(connection: sqlite3.Connection, attachment_id: str | None) -> bool:
    if attachment_id is None:
        # Non-conversation legacy supervision does not use a conversation MCP bridge.
        return True
    return (
        connection.execute(
            f"SELECT 1 FROM cao_session_attachments AS attachment "
            f"WHERE attachment.id = ? AND {cao_automatic_wake_ready_sql()}",
            (attachment_id,),
        ).fetchone()
        is not None
    )

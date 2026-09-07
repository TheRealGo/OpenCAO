"""Owner-local, read-only-source migration of preserved operational memory.

The retired memory CLI/database is never a runtime fallback. Import creates
scoped historical evidence in the canonical store and leaves its source intact.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .database import utc_now
from .errors import AuthorizationError, ConflictError, ValidationError
from .models import PrincipalRole
from .security import canonical_json
from .supervision_control import evidence_text
from .supervision_memory import require_memory_safe_payload

if TYPE_CHECKING:
    from .service import ControlPlane


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _source_json(text: Any, field: str) -> Any:
    try:
        return json.loads(text)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"legacy memory {field} is invalid") from error


def _source_snapshot(source: Path) -> dict[str, Any]:
    """Read only a bounded, owner-only legacy file; never initialize it."""

    try:
        source = source.expanduser().absolute()
        before = source.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > 64 * 1024 * 1024
        ):
            raise ValueError("invalid source")
        with closing(sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("invalid source")
            schema = connection.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if schema is None or str(schema[0]) != "1":
                raise ValueError("invalid schema")
            snapshot = {
                "entries": [
                    dict(row)
                    for row in connection.execute("SELECT * FROM memory_entries ORDER BY id")
                ],
                "cues": [
                    dict(row)
                    for row in connection.execute(
                        "SELECT cue_text, memory_id FROM cue_links ORDER BY memory_id, cue_text"
                    )
                ],
                "episodes": [
                    dict(row) for row in connection.execute("SELECT * FROM episodes ORDER BY id")
                ],
            }
            if len(snapshot["entries"]) > 10000:
                raise ValueError("source too large")
        after = source.lstat()
        if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ):
            raise ValueError("source changed")
    except (OSError, sqlite3.Error, ValueError, KeyError) as error:
        raise ValidationError("legacy memory source is invalid or changed") from error
    return snapshot


def _legacy_value(entry: dict[str, Any], episode: dict[str, Any] | None) -> str:
    """Preserve detailed content, but do not export private locators or credentials."""

    source_refs = (
        _source_json(entry["source_refs_json"], "source references")
        if "source_refs_json" in entry
        else entry.get("source_refs", [])
    )
    if not isinstance(source_refs, list) or any(not isinstance(ref, str) for ref in source_refs):
        raise ValidationError("legacy memory source references are invalid")
    value: dict[str, Any] = {
        "historical_evidence_only": True,
        "value": evidence_text(entry.get("value")),
        "memory_type": evidence_text(entry.get("memory_type")),
        "legacy_scope": evidence_text(entry.get("scope")),
        "observed_at": evidence_text(entry.get("updated_at")),
        "snapshot_recorded_at": evidence_text(entry.get("ts")),
        "source_refs_digest": _digest(source_refs),
    }
    if episode is not None:
        value["episode"] = {
            key: evidence_text(episode.get(key))
            for key in ("kind", "observed_at", "summary", "raw_text")
        }
    return canonical_json(value)


def _prepare_entry(
    entry: dict[str, Any], episodes: dict[str, dict[str, Any]], cues: list[str]
) -> dict[str, Any]:
    """Validate the same lossless migration in both dry-run and apply modes."""

    lifecycle = entry.get("lifecycle_state")
    if lifecycle not in {"active", "superseded", "deleted"}:
        raise ValidationError("legacy memory lifecycle is invalid")
    history = _source_json(entry.get("history_json"), "revision history")
    if not isinstance(history, list) or any(not isinstance(row, dict) for row in history):
        raise ValidationError("legacy memory revision history is invalid")
    episode_id = entry.get("source_episode_id")
    if episode_id and str(episode_id) not in episodes:
        raise ValidationError("legacy memory episode reference is missing")
    versions: list[tuple[str, str]] = []
    retained_cues: dict[str, tuple[str, int]] = {}
    for revision, historical in enumerate([*history, entry], 1):
        primary_source = historical.get("primary_abstraction") or historical.get("primary")
        if not isinstance(primary_source, str) or not isinstance(historical.get("value"), str):
            raise ValidationError("legacy memory revision content is invalid")
        primary = evidence_text(primary_source)
        if not primary or len(primary) > 512 or not historical["value"].strip():
            raise ValidationError("legacy memory revision content is outside its bounds")
        # Old snapshots do not retain an episode ID. Never attach the current
        # episode to a past value: its actual source is unknown, not interchangeable.
        episode = episodes.get(str(episode_id)) if revision == len(history) + 1 else None
        value = _legacy_value(historical, episode)
        if len(value) > 64000:
            raise ValidationError("legacy memory value exceeds its full-value bound")
        require_memory_safe_payload({"primary_abstraction": primary, "value": value})
        versions.append((primary, value))
        revision_cues = historical.get("cues", []) if revision <= len(history) else cues
        if not isinstance(revision_cues, list) or any(
            not isinstance(cue, str) for cue in revision_cues
        ):
            raise ValidationError("legacy memory cue anchors are invalid")
        for cue in revision_cues:
            text = evidence_text(cue)
            if not text or len(text) > 256:
                raise ValidationError("legacy memory cue anchor is outside its bounds")
            require_memory_safe_payload(text)
            retained_cues.setdefault(" ".join(text.split()).casefold(), (text, revision))
    return {"lifecycle": lifecycle, "versions": versions, "cues": retained_cues}


def import_legacy_memories(
    service: ControlPlane,
    actor: dict[str, Any],
    *,
    source: Path,
    project_digest: str,
    attachment_id: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Import once into one explicit existing project using owner-local authority."""

    if actor.get("_cao_conversation_credential_id") or actor.get("_cao_runtime_credential_id"):
        raise AuthorizationError("legacy memory import requires owner-local CAO authority")
    service._require_role(actor, PrincipalRole.CAO)
    if re.fullmatch(r"[0-9a-f]{64}", project_digest) is None:
        raise ValidationError("memory import requires an exact project digest")
    # Authorize the destination before reading a source path.
    with service.db.connection_scope() as connection:
        target = connection.execute(
            "SELECT 1 FROM cao_session_attachments WHERE id = ? AND principal_id = ? "
            "AND project_digest = ? AND state = 'active' LIMIT 1",
            (attachment_id, actor["id"], project_digest),
        ).fetchone()
        if target is None:
            raise AuthorizationError("memory import destination is not an attached owner project")
    snapshot = _source_snapshot(source)
    source_digest = _digest(snapshot)
    entries = snapshot["entries"]
    episodes = {str(row["id"]): row for row in snapshot["episodes"]}
    cues: dict[str, list[str]] = {}
    for cue in snapshot["cues"]:
        if not isinstance(cue["cue_text"], str):
            raise ValidationError("legacy memory cue anchors are invalid")
        cues.setdefault(str(cue["memory_id"]), []).append(cue["cue_text"])
    prepared = {
        str(entry["id"]): _prepare_entry(entry, episodes, cues.get(str(entry["id"]), []))
        for entry in entries
    }
    if set(cues) - set(prepared):
        raise ValidationError("legacy memory cue reference is missing")
    report: dict[str, Any] = {
        "dry_run": dry_run,
        "source_digest": source_digest,
        "source_entries": len(entries),
        "source_episodes": len(episodes),
        "source_cue_links": len(snapshot["cues"]),
        "active_entries": sum(row["lifecycle_state"] == "active" for row in entries),
        "source_preserved": True,
        "imported": 0,
        "already_imported": 0,
    }
    if dry_run:
        return report
    with service.db.transaction() as connection:
        if (
            connection.execute(
                "SELECT 1 FROM cao_session_attachments WHERE id = ? AND principal_id = ? "
                "AND project_digest = ? AND state = 'active' LIMIT 1",
                (attachment_id, actor["id"], project_digest),
            ).fetchone()
            is None
        ):
            raise AuthorizationError("memory import destination attachment changed")
        for entry in entries:
            origin_id_digest = _digest(str(entry["id"]))
            memory_id = (
                "mem_"
                + _digest(
                    {
                        "owner": actor["id"],
                        "project": project_digest,
                        "source": source_digest,
                        "origin": origin_id_digest,
                    }
                )[:32]
            )
            existing = connection.execute(
                "SELECT origin_source_digest, origin_id_digest FROM supervision_memories "
                "WHERE id = ?",
                (memory_id,),
            ).fetchone()
            if existing is not None:
                if (existing["origin_source_digest"], existing["origin_id_digest"]) != (
                    source_digest,
                    origin_id_digest,
                ):
                    raise ConflictError("memory import identity conflicts with existing provenance")
                report["already_imported"] += 1
                continue
            prepared_entry = prepared[str(entry["id"])]
            versions = prepared_entry["versions"]
            now = utc_now()
            connection.execute(
                """
                INSERT INTO supervision_memories(
                    id, owner_principal_id, origin_attachment_id, project_digest, scope,
                    kind, lifecycle_state, origin_source_digest, origin_id_digest,
                    current_revision, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'project', 'legacy', ?, ?, ?, 1, ?, ?)
                """,
                (
                    memory_id,
                    actor["id"],
                    attachment_id,
                    project_digest,
                    prepared_entry["lifecycle"],
                    source_digest,
                    origin_id_digest,
                    now,
                    now,
                ),
            )
            for revision, (primary, value) in enumerate(versions, 1):
                connection.execute(
                    """
                    INSERT INTO supervision_memory_revisions(
                        memory_id, revision, primary_abstraction, value, value_digest,
                        source_work_item_id, source_goal_version, source_goal_packet_digest,
                        created_attachment_id, created_by, created_at
                    ) VALUES(?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?)
                    """,
                    (
                        memory_id,
                        revision,
                        primary,
                        value,
                        hashlib.sha256(value.encode("utf-8")).hexdigest(),
                        attachment_id,
                        actor["id"],
                        now,
                    ),
                )
                if revision > 1:
                    connection.execute(
                        "UPDATE supervision_memories SET current_revision = ?, updated_at = ? "
                        "WHERE id = ? AND current_revision = ?",
                        (revision, now, memory_id, revision - 1),
                    )
            # Old cues remain alternative paths; they never overwrite the value.
            for normalized, (text, introduced_revision) in prepared_entry["cues"].items():
                connection.execute(
                    "INSERT INTO supervision_memory_cues "
                    "(memory_id, normalized_cue, cue_text, introduced_revision, created_at) "
                    "VALUES(?, ?, ?, ?, ?) "
                    "ON CONFLICT(memory_id, normalized_cue) DO NOTHING",
                    (memory_id, normalized, text, introduced_revision, now),
                )
            report["imported"] += 1
        service._event(
            connection,
            "memory.legacy_imported",
            "memory_import",
            source_digest,
            actor["id"],
            {
                "source_digest": source_digest,
                "imported": report["imported"],
                "already_imported": report["already_imported"],
            },
        )
    return report

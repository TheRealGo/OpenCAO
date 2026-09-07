from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from test_supervision_memory import _actor

from cao_control_plane.errors import AuthorizationError, ValidationError
from cao_control_plane.memory_import import import_legacy_memories
from cao_control_plane.models import MemoryReadInput, MemorySearchInput


def _legacy(path: Path, *, invalid: bool = False) -> None:
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript("""
            CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
            INSERT INTO meta VALUES('schema_version', '1');
            CREATE TABLE memory_entries(
                id TEXT PRIMARY KEY, primary_abstraction TEXT, value TEXT,
                memory_type TEXT, scope TEXT, lifecycle_state TEXT,
                history_json TEXT, source_refs_json TEXT, source_episode_id TEXT,
                updated_at TEXT
            );
            CREATE TABLE cue_links(cue_text TEXT, memory_id TEXT);
            CREATE TABLE episodes(
                id TEXT PRIMARY KEY, kind TEXT, observed_at TEXT, summary TEXT, raw_text TEXT
            );
        """)
        connection.execute(
            "INSERT INTO memory_entries VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "older-memory",
                "Earlier diagnostic lesson",
                "Detailed remembered experience. " * 30,
                "decision",
                "supervision",
                "active",
                json.dumps(
                    [
                        {
                            "primary": "Earlier diagnostic lesson",
                            "value": "A rejected earlier approach.",
                            "cues": ["older diagnostic cue"],
                            "ts": "1999-12-31T00:00:00Z",
                            "source_refs": ["older-observation"],
                        }
                    ]
                ),
                "[]",
                "episode-one",
                "2000-01-01T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO memory_entries VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "retired-memory",
                "Retired diagnostic lesson",
                "No longer current.",
                "decision",
                "supervision",
                "invalid" if invalid else "deleted",
                "[]",
                "[]",
                None,
                "2000-01-01T00:00:00Z",
            ),
        )
        connection.execute("INSERT INTO cue_links VALUES('diagnostic cue', 'older-memory')")
        connection.execute(
            "INSERT INTO episodes VALUES(?, ?, ?, ?, ?)",
            (
                "episode-one",
                "observation",
                "2000-01-01T00:00:00Z",
                "An earlier failed attempt.",
                "The private evidence was at /private/fixture/evidence.txt.",
            ),
        )
    path.chmod(0o600)


@pytest.fixture
def import_case(system: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    actor = _actor(system, "legacy-memory-import")
    source = tmp_path / "preserved-memory.sqlite3"
    _legacy(source)
    return {**system, "actor": actor, "source": source}


def _import(case: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    args = {
        "source": case["source"],
        "project_digest": case["actor"]["_cao_project_digest"],
        "attachment_id": case["actor"]["_cao_attachment_id"],
        "dry_run": False,
        **overrides,
    }
    return import_legacy_memories(case["service"], case["cao"], **args)


def test_dry_run_reads_but_preserves_both_stores(import_case: dict[str, Any]) -> None:
    before = hashlib.sha256(import_case["source"].read_bytes()).hexdigest()
    result = _import(import_case, dry_run=True)
    assert result["source_entries"] == 2 and result["active_entries"] == 1
    assert result["imported"] == 0 and result["source_preserved"] is True
    assert import_case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0
    assert hashlib.sha256(import_case["source"].read_bytes()).hexdigest() == before


def test_import_preserves_rich_revisions_inactive_records_and_source(
    import_case: dict[str, Any],
) -> None:
    service = import_case["service"]
    before = hashlib.sha256(import_case["source"].read_bytes()).hexdigest()
    result = _import(import_case)
    assert result["imported"] == 2
    assert service.db.fetchone("SELECT COUNT(*) FROM supervision_memory_revisions")[0] == 3
    found = service.search_memories(import_case["actor"], MemorySearchInput(query="diagnostic cue"))
    assert found["total"] == 1
    entry = found["memories"][0]
    assert entry["kind"] == "legacy" and entry["revision"] == 2
    read = service.read_memory(
        import_case["actor"],
        MemoryReadInput(
            memory_id=entry["memory_id"],
            expected_revision=2,
        ),
    )
    assert len(read["content"]) > 280 and read["untrusted"] is True
    assert "/private/fixture" not in read["content"]
    assert "historical_evidence_only" in read["content"]
    older = service.read_memory(
        import_case["actor"],
        MemoryReadInput(
            memory_id=entry["memory_id"],
            expected_revision=1,
        ),
    )
    assert "rejected earlier approach" in older["content"]
    past = json.loads(older["content"])
    current = json.loads(read["content"])
    assert "episode" not in past and "episode" in current
    assert past["snapshot_recorded_at"] == "1999-12-31T00:00:00Z"
    assert not past["observed_at"]
    assert past["source_refs_digest"] != current["source_refs_digest"]
    assert "older diagnostic cue" in entry["cue_anchors"]
    assert hashlib.sha256(import_case["source"].read_bytes()).hexdigest() == before


def test_replaying_import_is_idempotent(import_case: dict[str, Any]) -> None:
    first = _import(import_case)
    second = _import(import_case)
    assert first["source_digest"] == second["source_digest"]
    assert second["imported"] == 0 and second["already_imported"] == 2
    assert import_case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 2


def test_destination_is_authorized_before_source_is_opened(import_case: dict[str, Any]) -> None:
    with pytest.raises(AuthorizationError):
        _import(import_case, project_digest="d" * 64, source=Path("/missing/memory.sqlite3"))


def test_conversation_token_cannot_invoke_owner_import(import_case: dict[str, Any]) -> None:
    with pytest.raises(AuthorizationError):
        import_legacy_memories(
            import_case["service"],
            import_case["actor"],
            source=import_case["source"],
            project_digest=import_case["actor"]["_cao_project_digest"],
            attachment_id=import_case["actor"]["_cao_attachment_id"],
        )


def test_non_private_source_is_rejected(import_case: dict[str, Any]) -> None:
    import_case["source"].chmod(0o644)
    with pytest.raises(ValidationError, match="source is invalid"):
        _import(import_case)


def test_invalid_record_rolls_back_entire_import(
    import_case: dict[str, Any], tmp_path: Path
) -> None:
    invalid = tmp_path / "invalid-memory.sqlite3"
    _legacy(invalid, invalid=True)
    with pytest.raises(ValidationError, match="lifecycle"):
        _import(import_case, source=invalid)
    assert import_case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0


@pytest.mark.parametrize("dry_run", [True, False])
@pytest.mark.parametrize("history", ["{broken", "null", "{}", '[{"value":"lost primary"}]'])
def test_invalid_history_is_not_silently_discarded(import_case, dry_run, history) -> None:
    with closing(sqlite3.connect(import_case["source"])) as connection, connection:
        connection.execute(
            "UPDATE memory_entries SET history_json = ? WHERE id = 'older-memory'", (history,)
        )
    before = hashlib.sha256(import_case["source"].read_bytes()).hexdigest()
    with pytest.raises(ValidationError, match="revision"):
        _import(import_case, dry_run=dry_run)
    assert import_case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0
    assert hashlib.sha256(import_case["source"].read_bytes()).hexdigest() == before


@pytest.mark.parametrize("dry_run", [True, False])
def test_out_of_bounds_cue_cannot_be_ignored(import_case, dry_run) -> None:
    with closing(sqlite3.connect(import_case["source"])) as connection, connection:
        connection.execute("INSERT INTO cue_links VALUES(?, 'older-memory')", ("c" * 257,))
    with pytest.raises(ValidationError, match="cue anchor"):
        _import(import_case, dry_run=dry_run)
    assert import_case["service"].db.fetchone("SELECT COUNT(*) FROM supervision_memories")[0] == 0


def test_short_lived_runtime_cannot_invoke_owner_import(import_case) -> None:
    with pytest.raises(AuthorizationError, match="owner-local"):
        import_legacy_memories(
            import_case["service"],
            {**import_case["cao"], "_cao_runtime_credential_id": "runtime-fixture"},
            source=Path("/missing/memory.sqlite3"),
            project_digest=import_case["actor"]["_cao_project_digest"],
            attachment_id=import_case["actor"]["_cao_attachment_id"],
        )

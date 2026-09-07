from __future__ import annotations

import sqlite3

import pytest

from cao_control_plane.database import SCHEMA_VERSION, Database


def _restore_v39_cutover_schema(database: Database, *, occupied: str | None = None) -> None:
    with database.connection_scope() as connection:
        connection.executescript(
            """
            BEGIN EXCLUSIVE;
            ALTER TABLE control_authority RENAME TO control_authority_v40;
            CREATE TABLE control_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                mode TEXT NOT NULL CHECK(mode IN ('canonical', 'shadow', 'sealed')),
                generation INTEGER NOT NULL,
                legacy_snapshot_digest TEXT NOT NULL DEFAULT '',
                fence_evidence_digest TEXT NOT NULL DEFAULT '',
                fence_generation INTEGER NOT NULL DEFAULT 0,
                activated_at TEXT,
                updated_at TEXT NOT NULL
            );
            INSERT INTO control_authority(
                singleton, mode, generation, legacy_snapshot_digest,
                fence_evidence_digest, fence_generation, activated_at, updated_at
            )
            SELECT singleton, mode, generation, '', '', 0, activated_at, updated_at
            FROM control_authority_v40;
            DROP TABLE control_authority_v40;

            CREATE TABLE legacy_import_snapshots (
                id TEXT PRIMARY KEY,
                snapshot_digest TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE legacy_entity_mappings (
                snapshot_id TEXT NOT NULL,
                entity_kind TEXT NOT NULL
            );
            CREATE TABLE cutover_canonical_manifest (
                snapshot_id TEXT NOT NULL,
                entity_kind TEXT NOT NULL
            );
            CREATE TABLE cutover_verifications (
                id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL
            );
            CREATE TABLE user_acceptances (
                id TEXT PRIMARY KEY,
                work_item_id TEXT NOT NULL
            );
            DELETE FROM schema_migrations WHERE version = 40;
            UPDATE metadata SET value = '39' WHERE key = 'schema_version';
            PRAGMA user_version = 39;
            COMMIT;
            """
        )
        if occupied == "cutover":
            connection.execute(
                "INSERT INTO legacy_import_snapshots(id, snapshot_digest, created_at) "
                "VALUES('snapshot', 'digest', '2026-08-28T00:00:00Z')"
            )
        elif occupied == "acceptance":
            connection.execute(
                "INSERT INTO user_acceptances(id, work_item_id) VALUES('acceptance', 'work')"
            )
        elif occupied == "scope":
            connection.execute(
                """
                INSERT INTO principals(
                    id, name, role, token_hash, enabled, operator_scope,
                    operator_label, metadata_json, created_at, updated_at
                ) VALUES(
                    'retired-worker', 'retired-worker', 'worker', 'hash', 1,
                    'unclassified', '', '{}',
                    '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z'
                )
                """
            )
            triggers = connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'trigger' "
                "AND name LIKE 'principals_operator_%'"
            ).fetchall()
            for trigger in triggers:
                connection.execute(f"DROP TRIGGER {trigger['name']}")
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute(
                "UPDATE principals SET operator_scope = 'migration', "
                "operator_label = 'Retired Worker' WHERE id = 'retired-worker'"
            )
            connection.execute("PRAGMA ignore_check_constraints = OFF")


def test_schema40_removes_empty_cutover_tables_and_legacy_authority_columns(settings):
    database = Database(settings)
    _restore_v39_cutover_schema(database)

    migrated = Database(settings)

    assert migrated.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    assert migrated.fetchone(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    )["value"] == str(SCHEMA_VERSION)
    columns = {
        row["name"] for row in migrated.fetchall("PRAGMA table_info(control_authority)")
    }
    assert columns == {"singleton", "mode", "generation", "activated_at", "updated_at"}
    for table in (
        "user_acceptances",
        "legacy_import_snapshots",
        "legacy_entity_mappings",
        "cutover_canonical_manifest",
        "cutover_verifications",
    ):
        assert migrated.fetchone(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?",
            (table,),
        ) is None
    with pytest.raises(sqlite3.IntegrityError), migrated.transaction() as connection:
        connection.execute("UPDATE control_authority SET mode = 'shadow' WHERE singleton = 1")


@pytest.mark.parametrize("occupied", ["cutover", "acceptance", "scope"])
def test_schema40_refuses_to_discard_retired_evidence(settings, occupied: str):
    database = Database(settings)
    _restore_v39_cutover_schema(database, occupied=occupied)

    with pytest.raises(RuntimeError, match="will not discard retired compatibility evidence"):
        Database(settings)

    with database.connection_scope() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 39
        assert connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()["value"] == "39"
        count_queries = {
            "cutover": "SELECT COUNT(*) FROM legacy_import_snapshots",
            "acceptance": "SELECT COUNT(*) FROM user_acceptances",
            "scope": "SELECT COUNT(*) FROM principals WHERE operator_scope = 'migration'",
        }
        assert connection.execute(count_queries[occupied]).fetchone()[0] == 1

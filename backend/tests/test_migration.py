"""
Tests for SQLite schema migration system and physical schema verification.
"""

import sqlite3
import pytest
from pathlib import Path

try:
    from backend.migrations.runner import (
        backup_database,
        run_migration_file,
        run_all_migrations,
        ensure_migrations_table,
        get_applied_versions,
    )
    from backend.migrations import migration_001_unresolved_5col_unique as m001
    from backend.migrations import migration_002_temporal_event_extraction as m002
except ModuleNotFoundError:
    from migrations.runner import (
        backup_database,
        run_migration_file,
        run_all_migrations,
        ensure_migrations_table,
        get_applied_versions,
    )
    from migrations import migration_001_unresolved_5col_unique as m001
    from migrations import migration_002_temporal_event_extraction as m002


@pytest.fixture
def old_schema_db(tmp_path):
    """Creates a temporary database with the legacy 3-column constraint and sample records."""
    db_path = tmp_path / "old_schema.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    # Create dummy parent tables for foreign keys
    c.execute("CREATE TABLE source_records (id INTEGER PRIMARY KEY, dataset_name TEXT)")
    c.execute("CREATE TABLE ingestion_batches (id INTEGER PRIMARY KEY, status TEXT)")
    c.execute("INSERT INTO source_records VALUES (1, 'KafkaSQLite'), (2, 'KafkaSQLite')")
    c.execute("INSERT INTO ingestion_batches VALUES (10, 'COMPLETED')")

    # Create legacy unresolved_relationships table (3-column constraint)
    c.execute("""
        CREATE TABLE unresolved_relationships (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL,
            source_table VARCHAR NOT NULL,
            source_record_id INTEGER,
            source_id VARCHAR NOT NULL,
            target_type VARCHAR NOT NULL,
            target_id VARCHAR NOT NULL,
            reason TEXT NOT NULL,
            batch_id INTEGER,
            detected_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            CONSTRAINT _unresolved_rel_uc UNIQUE (dataset_name, source_table, source_id),
            FOREIGN KEY(source_record_id) REFERENCES source_records (id),
            FOREIGN KEY(batch_id) REFERENCES ingestion_batches (id)
        )
    """)
    c.execute("CREATE INDEX ix_unresolved_relationships_source_table ON unresolved_relationships (source_table)")
    c.execute("CREATE INDEX ix_unresolved_relationships_source_id ON unresolved_relationships (source_id)")
    c.execute("CREATE INDEX ix_unresolved_relationships_source_record_id ON unresolved_relationships (source_record_id)")
    c.execute("CREATE INDEX ix_unresolved_relationships_batch_id ON unresolved_relationships (batch_id)")

    # Insert sample records with specific IDs to verify primary key preservation
    c.execute("""
        INSERT INTO unresolved_relationships (id, dataset_name, source_table, source_record_id, source_id, target_type, target_id, reason, batch_id)
        VALUES (42, 'KafkaSQLite', 'issue_link', 1, 'LINK-1', 'issue', 'TARGET-A', 'Missing target', 10),
               (99, 'KafkaSQLite', 'change_set_link', 2, 'CS-1', 'commit', 'COMMIT-X', 'Missing commit', 10)
    """)

    conn.commit()
    conn.close()
    return db_path


def test_backup_creation_and_integrity(old_schema_db, tmp_path):
    backup_dir = tmp_path / "custom_backups"
    b1 = backup_database(old_schema_db, backup_dir=backup_dir)

    assert b1.exists()
    assert b1.parent == backup_dir
    assert b1.stat().st_size > 0

    # Verify integrity of backup
    conn = sqlite3.connect(str(b1))
    c = conn.cursor()
    c.execute("PRAGMA integrity_check")
    assert c.fetchone()[0] == "ok"
    conn.close()

    # Ensure second backup does not overwrite
    b2 = backup_database(old_schema_db, backup_dir=backup_dir)
    assert b2 != b1
    assert b2.exists()
    assert b1.exists()


def test_migration_upgrades_schema_and_preserves_data(old_schema_db):
    # Verify pre-condition: old schema is needed
    conn = sqlite3.connect(str(old_schema_db))
    assert m001.is_needed(conn) is True
    conn.close()

    # Run migration
    applied = run_migration_file(old_schema_db, m001)
    assert applied is True

    # Inspect post-condition: schema upgraded
    conn = sqlite3.connect(str(old_schema_db))
    c = conn.cursor()

    # Check unique index columns
    c.execute("PRAGMA index_list(unresolved_relationships)")
    unique_indexes = [idx for idx in c.fetchall() if idx[2] == 1]
    assert len(unique_indexes) >= 1
    idx_name = unique_indexes[0][1]

    c.execute(f"PRAGMA index_info({idx_name})")
    cols = [row[2] for row in c.fetchall()]
    assert set(cols) == {"dataset_name", "source_table", "source_id", "target_type", "target_id"}

    # Check preserved rows and IDs
    c.execute("SELECT id, source_id, target_id FROM unresolved_relationships ORDER BY id")
    rows = c.fetchall()
    assert len(rows) == 2
    assert rows[0] == (42, "LINK-1", "TARGET-A")
    assert rows[1] == (99, "CS-1", "COMMIT-X")

    # Check secondary indexes
    c.execute("PRAGMA index_list(unresolved_relationships)")
    all_idx = {idx[1] for idx in c.fetchall()}
    assert "ix_unresolved_relationships_source_table" in all_idx
    assert "ix_unresolved_relationships_source_id" in all_idx
    assert "ix_unresolved_relationships_source_record_id" in all_idx
    assert "ix_unresolved_relationships_batch_id" in all_idx

    # Check foreign keys and integrity
    c.execute("PRAGMA foreign_key_check")
    assert c.fetchall() == []
    c.execute("PRAGMA integrity_check")
    assert c.fetchone()[0] == "ok"

    # Check schema_migrations
    versions = get_applied_versions(conn)
    assert "001" in versions

    conn.close()


def test_multi_target_unresolved_coexist_in_migrated_db(old_schema_db):
    run_migration_file(old_schema_db, m001)

    conn = sqlite3.connect(str(old_schema_db))
    c = conn.cursor()

    # Insert two records with identical dataset_name, source_table, source_id but different target_id
    c.execute("""
        INSERT INTO unresolved_relationships (dataset_name, source_table, source_record_id, source_id, target_type, target_id, reason, batch_id)
        VALUES ('KafkaSQLite', 'issue_link', 1, 'MULTI-LINK', 'issue', 'TARGET-1', 'Missing endpoint 1', 10),
               ('KafkaSQLite', 'issue_link', 1, 'MULTI-LINK', 'issue', 'TARGET-2', 'Missing endpoint 2', 10)
    """)
    conn.commit()

    # Verify both records coexist
    c.execute("SELECT target_id FROM unresolved_relationships WHERE source_id = 'MULTI-LINK'")
    targets = {row[0] for row in c.fetchall()}
    assert targets == {"TARGET-1", "TARGET-2"}

    # Attempt exact duplicate of the first multi-target record
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("""
            INSERT INTO unresolved_relationships (dataset_name, source_table, source_record_id, source_id, target_type, target_id, reason, batch_id)
            VALUES ('KafkaSQLite', 'issue_link', 1, 'MULTI-LINK', 'issue', 'TARGET-1', 'Duplicate', 10)
        """)

    conn.close()


def test_migration_is_repeatable_and_safe(old_schema_db):
    # Pass 1
    assert run_migration_file(old_schema_db, m001) is True

    # Pass 2: should be a safe no-op
    assert run_migration_file(old_schema_db, m001) is False

    # Check that is_needed returns False
    conn = sqlite3.connect(str(old_schema_db))
    assert m001.is_needed(conn) is False
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM unresolved_relationships")
    assert c.fetchone()[0] == 2
    conn.close()


def test_migration_002_creates_tables_and_indexes(old_schema_db):
    conn = sqlite3.connect(str(old_schema_db))
    assert m002.is_needed(conn) is True
    conn.close()

    applied = run_migration_file(old_schema_db, m002)
    assert applied is True

    conn = sqlite3.connect(str(old_schema_db))
    c = conn.cursor()

    # Verify tables created
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in c.fetchall()}
    assert {"extraction_runs", "event_candidates", "temporal_events", "event_evidence"}.issubset(tables)

    # Verify unique index on event_candidates: (dataset_name, candidate_fingerprint)
    c.execute("PRAGMA index_list(event_candidates)")
    c_indexes = c.fetchall()
    assert any(idx[2] == 1 for idx in c_indexes)

    # Verify unique index on temporal_events: (dataset_name, event_key)
    c.execute("PRAGMA index_list(temporal_events)")
    e_indexes = c.fetchall()
    assert any(idx[2] == 1 for idx in e_indexes)

    # Verify integrity
    c.execute("PRAGMA integrity_check")
    assert c.fetchone()[0] == "ok"
    conn.close()


def test_migration_002_repeatable(old_schema_db):
    assert run_migration_file(old_schema_db, m002) is True
    assert run_migration_file(old_schema_db, m002) is False

    conn = sqlite3.connect(str(old_schema_db))
    assert m002.is_needed(conn) is False
    conn.close()


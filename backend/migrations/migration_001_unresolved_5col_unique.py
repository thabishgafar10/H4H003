"""
Migration 001: unresolved_relationships_5col_unique

Upgrades the unresolved_relationships table to use a 5-column composite unique constraint:
    (dataset_name, source_table, source_id, target_type, target_id)

Preserves:
- All primary keys (id)
- All 24 existing unresolved relationship rows
- Both foreign keys (source_record_id, batch_id)
- All 4 secondary non-unique indexes
"""

import sqlite3

VERSION = "001"
NAME = "unresolved_relationships_5col_unique"


def is_needed(conn: sqlite3.Connection) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='unresolved_relationships'")
    if not cursor.fetchone():
        return False

    cursor.execute("PRAGMA index_list(unresolved_relationships)")
    indexes = cursor.fetchall()
    for idx in indexes:
        is_unique = bool(idx[2])
        if is_unique:
            idx_name = idx[1]
            cursor.execute(f"PRAGMA index_info({idx_name})")
            cols = [row[2] for row in cursor.fetchall()]
            if set(cols) == {"dataset_name", "source_table", "source_id", "target_type", "target_id"}:
                return False  # Already has the 5-column unique constraint
    return True


def upgrade(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    # 1. Create table with 5-column composite uniqueness constraint
    cursor.execute("""
        CREATE TABLE unresolved_relationships_new (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
            source_table VARCHAR NOT NULL,
            source_record_id INTEGER,
            source_id VARCHAR NOT NULL,
            target_type VARCHAR NOT NULL,
            target_id VARCHAR NOT NULL,
            reason TEXT NOT NULL,
            batch_id INTEGER,
            detected_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            CONSTRAINT _unresolved_rel_uc UNIQUE (dataset_name, source_table, source_id, target_type, target_id),
            FOREIGN KEY(source_record_id) REFERENCES source_records (id),
            FOREIGN KEY(batch_id) REFERENCES ingestion_batches (id)
        )
    """)

    # 2. Copy existing data preserving primary key IDs and all columns
    cursor.execute("""
        INSERT INTO unresolved_relationships_new (
            id, dataset_name, source_table, source_record_id, source_id,
            target_type, target_id, reason, batch_id, detected_at
        )
        SELECT id, dataset_name, source_table, source_record_id, source_id,
               target_type, target_id, reason, batch_id, detected_at
        FROM unresolved_relationships
    """)

    # 3. Drop existing table
    cursor.execute("DROP TABLE unresolved_relationships")

    # 4. Rename new table to original name
    cursor.execute("ALTER TABLE unresolved_relationships_new RENAME TO unresolved_relationships")

    # 5. Recreate all secondary indexes
    cursor.execute("CREATE INDEX ix_unresolved_relationships_source_table ON unresolved_relationships (source_table)")
    cursor.execute("CREATE INDEX ix_unresolved_relationships_source_id ON unresolved_relationships (source_id)")
    cursor.execute("CREATE INDEX ix_unresolved_relationships_source_record_id ON unresolved_relationships (source_record_id)")
    cursor.execute("CREATE INDEX ix_unresolved_relationships_batch_id ON unresolved_relationships (batch_id)")


"""
Migration 004: Multi-Project Onboarding, Source Connections, Documents & Ingestion Runs.
Adds first-class multi-tenant project isolation while safely bootstrapping
the existing Apache Kafka dataset as the default baseline project.
"""

import sqlite3
from app.core.logging_config import logger

VERSION = "004"
NAME = "multiproject_onboarding"


def is_needed(conn: sqlite3.Connection) -> bool:
    """Checks whether the projects table or columns already exist."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='projects'")
    has_projects_table = cursor.fetchone() is not None
    return not has_projects_table


def _column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def upgrade(conn: sqlite3.Connection) -> None:
    """Applies Migration 004 transactionally."""
    cursor = conn.cursor()
    logger.info("Executing Migration 004: Multi-Project Onboarding...")

    # 1. Create projects table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) NOT NULL,
            slug VARCHAR(100) NOT NULL UNIQUE,
            description TEXT,
            domain VARCHAR(100),
            status VARCHAR(50) NOT NULL DEFAULT 'READY',
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            updated_at DATETIME DEFAULT (CURRENT_TIMESTAMP)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_projects_slug ON projects (slug)")

    # 2. Create source_connections table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            name VARCHAR(255) NOT NULL,
            config TEXT NOT NULL,
            auth_type VARCHAR(50) NOT NULL DEFAULT 'NONE',
            credentials_encrypted TEXT,
            status VARCHAR(50) NOT NULL DEFAULT 'CONNECTED',
            last_sync_at DATETIME,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            updated_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_source_connections_project_id ON source_connections (project_id)")

    # 3. Create documents table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            source_connection_id INTEGER,
            filename VARCHAR(255) NOT NULL,
            file_type VARCHAR(50) NOT NULL,
            source_url VARCHAR(1000),
            file_size INTEGER NOT NULL,
            sha256_hash VARCHAR(64) NOT NULL,
            page_count INTEGER,
            extracted_text TEXT NOT NULL,
            metadata_json TEXT,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
            FOREIGN KEY (source_connection_id) REFERENCES source_connections (id) ON DELETE SET NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_documents_project_id ON documents (project_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_documents_sha256 ON documents (sha256_hash)")

    # 4. Create ingestion_runs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            source_connection_id INTEGER,
            source_system VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
            started_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            completed_at DATETIME,
            records_discovered INTEGER DEFAULT 0,
            records_created INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            error_summary TEXT,
            checkpoint_data TEXT,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
            FOREIGN KEY (source_connection_id) REFERENCES source_connections (id) ON DELETE SET NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_ingestion_runs_project_id ON ingestion_runs (project_id)")

    # 5. Add project_id column to existing tables if missing
    project_scopable_tables = [
        "source_records",
        "features",
        "temporal_events",
        "extraction_runs",
    ]
    for tbl in project_scopable_tables:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tbl}'")
        if cursor.fetchone() is not None:
            if not _column_exists(cursor, tbl, "project_id"):
                cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN project_id INTEGER REFERENCES projects(id)")
                cursor.execute(f"CREATE INDEX IF NOT EXISTS ix_{tbl}_project_id ON {tbl} (project_id)")

    # 6. Seed default Apache Kafka project (id=1) for 100% backward compatibility
    cursor.execute("SELECT id FROM projects WHERE slug='kafka' OR id=1")
    kafka_proj = cursor.fetchone()
    if not kafka_proj:
        cursor.execute("""
            INSERT INTO projects (id, name, slug, description, domain, status)
            VALUES (1, 'Apache Kafka', 'kafka', 'High-throughput distributed event streaming platform and real-world benchmark.', 'DISTRIBUTED_SYSTEMS', 'READY')
        """)
        cursor.execute("""
            INSERT INTO source_connections (project_id, source_type, name, config, auth_type, status)
            VALUES (1, 'LEGACY_DATASET', 'Apache Kafka SQLite Archive', '{"dataset_name": "KafkaSQLite"}', 'NONE', 'CONNECTED')
        """)

    # 7. Migrate existing unassigned records to project_id = 1
    for tbl in project_scopable_tables:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tbl}'")
        if cursor.fetchone() is not None:
            if _column_exists(cursor, tbl, "project_id"):
                cursor.execute(f"UPDATE {tbl} SET project_id = 1 WHERE project_id IS NULL")

    conn.commit()
    logger.info("Migration 004 applied successfully.")

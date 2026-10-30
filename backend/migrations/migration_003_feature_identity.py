"""
Migration 003: Feature Identity and Entity Resolution
Additive migration creating:
- features
- feature_aliases
- feature_record_links
"""

import sqlite3
from typing import Set

VERSION = "003"
NAME = "feature_identity_and_entity_resolution"


def get_existing_tables(conn: sqlite3.Connection) -> Set[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cursor.fetchall()}


def is_needed(conn: sqlite3.Connection) -> bool:
    tables = get_existing_tables(conn)
    required = {"features", "feature_aliases", "feature_record_links"}
    return not required.issubset(tables)


def upgrade(conn: sqlite3.Connection) -> None:
    tables = get_existing_tables(conn)

    # 1. Create features table
    if "features" not in tables:
        conn.execute("""
            CREATE TABLE features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
                canonical_name VARCHAR NOT NULL,
                normalized_name VARCHAR NOT NULL,
                description TEXT,
                feature_category VARCHAR NOT NULL DEFAULT 'CORE',
                first_observed_at DATETIME,
                last_observed_at DATETIME,
                identity_confidence FLOAT NOT NULL DEFAULT 1.0,
                resolution_status VARCHAR NOT NULL DEFAULT 'CONFIRMED',
                created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
                updated_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
                CONSTRAINT _feature_canonical_uc UNIQUE (dataset_name, normalized_name)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_features_dataset ON features (dataset_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_features_canonical_name ON features (canonical_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_features_normalized_name ON features (normalized_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_features_category ON features (feature_category)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_features_status ON features (resolution_status)")

    # 2. Create feature_aliases table
    if "feature_aliases" not in tables:
        conn.execute("""
            CREATE TABLE feature_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feature_id INTEGER NOT NULL,
                alias_name VARCHAR NOT NULL,
                normalized_alias VARCHAR NOT NULL,
                alias_type VARCHAR NOT NULL DEFAULT 'ALTERNATIVE_NAME',
                source_record_id INTEGER,
                valid_from DATETIME,
                valid_until DATETIME,
                confidence FLOAT NOT NULL DEFAULT 1.0,
                verification_status VARCHAR NOT NULL DEFAULT 'CONFIRMED',
                created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
                CONSTRAINT _feature_alias_uc UNIQUE (feature_id, normalized_alias),
                FOREIGN KEY (feature_id) REFERENCES features (id) ON DELETE CASCADE,
                FOREIGN KEY (source_record_id) REFERENCES source_records (id) ON DELETE SET NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_feature_aliases_feature_id ON feature_aliases (feature_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_feature_aliases_normalized_alias ON feature_aliases (normalized_alias)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_feature_aliases_type ON feature_aliases (alias_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_feature_aliases_status ON feature_aliases (verification_status)")

    # 3. Create feature_record_links table
    if "feature_record_links" not in tables:
        conn.execute("""
            CREATE TABLE feature_record_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feature_id INTEGER NOT NULL,
                record_type VARCHAR NOT NULL,
                record_id VARCHAR NOT NULL,
                relationship_type VARCHAR NOT NULL,
                confidence FLOAT NOT NULL DEFAULT 1.0,
                source_evidence TEXT,
                evidence_hash VARCHAR,
                source_record_id INTEGER,
                verification_status VARCHAR NOT NULL DEFAULT 'CONFIRMED',
                created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
                CONSTRAINT _feature_record_link_uc UNIQUE (feature_id, record_type, record_id, relationship_type),
                FOREIGN KEY (feature_id) REFERENCES features (id) ON DELETE CASCADE,
                FOREIGN KEY (source_record_id) REFERENCES source_records (id) ON DELETE SET NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fr_links_feature_id ON feature_record_links (feature_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fr_links_record ON feature_record_links (record_type, record_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fr_links_rel_type ON feature_record_links (relationship_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fr_links_status ON feature_record_links (verification_status)")


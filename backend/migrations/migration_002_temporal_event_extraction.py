"""
Migration 002: temporal_event_extraction

Adds tables for temporal event extraction:
- extraction_runs
- event_candidates
- temporal_events
- event_evidence
"""

import sqlite3

VERSION = "002"
NAME = "temporal_event_extraction"


def is_needed(conn: sqlite3.Connection) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}
    required_tables = {"extraction_runs", "event_candidates", "temporal_events", "event_evidence"}
    return not required_tables.issubset(existing_tables)


def upgrade(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    # 1. extraction_runs
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS extraction_runs (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
            status VARCHAR NOT NULL DEFAULT 'RUNNING',
            extraction_mode VARCHAR NOT NULL DEFAULT 'DETERMINISTIC',
            extractor_version VARCHAR NOT NULL DEFAULT '1.0.0',
            started_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            completed_at DATETIME,
            records_scanned INTEGER DEFAULT 0,
            candidates_created INTEGER DEFAULT 0,
            events_accepted INTEGER DEFAULT 0,
            events_rejected INTEGER DEFAULT 0,
            events_needing_review INTEGER DEFAULT 0,
            error_message TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_extraction_runs_dataset_name ON extraction_runs (dataset_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_extraction_runs_status ON extraction_runs (status)")

    # 2. event_candidates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_candidates (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
            event_type VARCHAR NOT NULL,
            event_time DATETIME,
            subject VARCHAR NOT NULL,
            description TEXT,
            actor VARCHAR,
            extractor_type VARCHAR NOT NULL,
            extractor_version VARCHAR NOT NULL DEFAULT '1.0.0',
            confidence_band VARCHAR NOT NULL,
            validation_status VARCHAR NOT NULL DEFAULT 'PENDING',
            rejection_reason TEXT,
            candidate_fingerprint VARCHAR NOT NULL,
            extraction_run_id INTEGER,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            CONSTRAINT _candidate_fingerprint_uc UNIQUE (dataset_name, candidate_fingerprint),
            FOREIGN KEY (extraction_run_id) REFERENCES extraction_runs (id) ON DELETE CASCADE
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_dataset_name ON event_candidates (dataset_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_event_type ON event_candidates (event_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_event_time ON event_candidates (event_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_validation_status ON event_candidates (validation_status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_candidate_fingerprint ON event_candidates (candidate_fingerprint)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_candidates_extraction_run_id ON event_candidates (extraction_run_id)")

    # 3. temporal_events
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_events (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
            event_key VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL,
            event_time DATETIME,
            subject VARCHAR NOT NULL,
            description TEXT,
            actor VARCHAR,
            issue_id VARCHAR,
            commit_hash VARCHAR,
            extraction_run_id INTEGER,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            CONSTRAINT _temporal_event_key_uc UNIQUE (dataset_name, event_key),
            FOREIGN KEY (extraction_run_id) REFERENCES extraction_runs (id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_dataset_name ON temporal_events (dataset_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_event_key ON temporal_events (event_key)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_event_type ON temporal_events (event_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_event_time ON temporal_events (event_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_issue_id ON temporal_events (issue_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_commit_hash ON temporal_events (commit_hash)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_temporal_events_extraction_run_id ON temporal_events (extraction_run_id)")

    # 4. event_evidence
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_evidence (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER,
            event_id INTEGER,
            source_record_id INTEGER,
            evidence_role VARCHAR NOT NULL DEFAULT 'PRIMARY',
            evidence_excerpt TEXT NOT NULL,
            evidence_hash VARCHAR NOT NULL,
            created_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY (candidate_id) REFERENCES event_candidates (id) ON DELETE CASCADE,
            FOREIGN KEY (event_id) REFERENCES temporal_events (id) ON DELETE CASCADE,
            FOREIGN KEY (source_record_id) REFERENCES source_records (id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_evidence_candidate_id ON event_evidence (candidate_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_evidence_event_id ON event_evidence (event_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_evidence_source_record_id ON event_evidence (source_record_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_event_evidence_evidence_hash ON event_evidence (evidence_hash)")

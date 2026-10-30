"""
Database Migration Runner for SQLite
Handles safe, versioned, repeatable migrations with automated backups and verification.
"""

import os
import shutil
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))
root_dir = backend_dir.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from app.core.logging_config import logger


def get_default_db_path() -> Path:
    from app.core.config import settings
    return Path(settings.APP_DB_PATH)


def ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            applied_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            checksum VARCHAR
        )
    """)
    conn.commit()


def backup_database(db_path: Path, backup_dir: Optional[Path] = None) -> Path:
    """
    Create a timestamped backup of the SQLite database and verify its integrity.
    Never overwrites an existing backup.
    """
    if backup_dir is None:
        backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    random_suffix = uuid.uuid4().hex[:6]
    backup_filename = f"{db_path.stem}_backup_{timestamp}_{random_suffix}{db_path.suffix}"
    backup_path = backup_dir / backup_filename

    if backup_path.exists():
        raise FileExistsError(f"Backup file already exists: {backup_path}")

    # Use SQLite online backup API to ensure a clean, consistent snapshot
    src_conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    dst_conn = sqlite3.connect(str(backup_path))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

    # Verify backup with PRAGMA integrity_check
    verify_conn = sqlite3.connect(str(backup_path))
    try:
        cursor = verify_conn.cursor()
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError(f"Backup integrity check failed for {backup_path}: {result}")
    finally:
        verify_conn.close()

    logger.info(f"Verified database backup created at: {backup_path}")
    return backup_path


def get_applied_versions(conn: sqlite3.Connection) -> List[str]:
    ensure_migrations_table(conn)
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM schema_migrations ORDER BY version")
    return [row[0] for row in cursor.fetchall()]


def run_migration_file(db_path: Path, migration_module) -> bool:
    """
    Run a single migration safely.
    Returns True if migration was executed, False if already applied/not needed.
    """
    version = getattr(migration_module, "VERSION")
    name = getattr(migration_module, "NAME")

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_migrations_table(conn)
        applied = get_applied_versions(conn)

        if version in applied:
            logger.info(f"Migration {version}_{name} is already recorded as applied.")
            # Double check if physical schema is still needed
            if hasattr(migration_module, "is_needed") and not migration_module.is_needed(conn):
                return False

        if hasattr(migration_module, "is_needed"):
            if not migration_module.is_needed(conn):
                logger.info(f"Migration {version}_{name} schema check indicates it is already applied.")
                if version not in applied:
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                        (version, name)
                    )
                    conn.commit()
                return False
    finally:
        conn.close()

    # Create verified backup before executing migration
    backup_path = backup_database(db_path)

    # Execute migration with transactional safety
    conn = sqlite3.connect(str(db_path))
    try:
        # SQLite constraint recreation requires foreign_keys = OFF during the table swap
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN TRANSACTION")

        migration_module.upgrade(conn)

        # Record migration
        conn.execute(
            "INSERT OR REPLACE INTO schema_migrations (version, name) VALUES (?, ?)",
            (version, name)
        )
        conn.commit()

        # Turn foreign keys back on and verify integrity
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_key_check")
        fk_errors = cursor.fetchall()
        if fk_errors:
            raise RuntimeError(f"Foreign key check failed after migration {version}: {fk_errors}")

        cursor.execute("PRAGMA integrity_check")
        integrity = cursor.fetchone()
        if not integrity or integrity[0] != "ok":
            raise RuntimeError(f"Integrity check failed after migration {version}: {integrity}")

        logger.info(f"Successfully applied migration {version}_{name}")
        return True
    except Exception as e:
        logger.error(f"Migration {version}_{name} failed: {e}. Restoring from backup...")
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
        # Restore from verified backup
        shutil.copyfile(backup_path, db_path)
        raise e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_all_migrations(db_path: Optional[Path] = None) -> List[str]:
    """Run all pending migrations in order."""
    if db_path is None:
        db_path = get_default_db_path()

    if not db_path.exists():
        logger.info(f"Database {db_path} does not exist yet; skipping migrations.")
        return []

    try:
        from backend.migrations import migration_001_unresolved_5col_unique as m001
        from backend.migrations import migration_002_temporal_event_extraction as m002
        from backend.migrations import migration_003_feature_identity as m003
    except ModuleNotFoundError:
        from migrations import migration_001_unresolved_5col_unique as m001
        from migrations import migration_002_temporal_event_extraction as m002
        from migrations import migration_003_feature_identity as m003

    migrations = [m001, m002, m003]
    applied_list = []

    for m in migrations:
        was_applied = run_migration_file(db_path, m)
        if was_applied:
            applied_list.append(m.VERSION)

    return applied_list


if __name__ == "__main__":
    db_file = get_default_db_path()
    print(f"Running migrations against: {db_file}")
    applied = run_all_migrations(db_file)
    print(f"Migrations applied: {applied}")

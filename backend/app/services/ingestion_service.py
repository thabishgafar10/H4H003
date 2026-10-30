import sqlite3
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink
from app.models.ingestion_batch import IngestionBatch
from app.core.logging_config import logger


class IngestionService:
    def __init__(self, db: Session, source_db_path: str):
        self.db = db
        self.source_db_path = source_db_path

    def run_ingestion(self):
        batch = IngestionBatch(dataset_name="KafkaSQLite", status="RUNNING")
        self.db.add(batch)
        self.db.commit()

        try:
            conn = sqlite3.connect(f"file:{self.source_db_path}?mode=ro", uri=True)
            cursor = conn.cursor()

            # Import Order: parent tables before child tables
            tables = [
                "meta",
                "issue",
                "change_set",
                "issue_comment",
                "issue_component",
                "issue_fix_version",
                "issue_link",
                "change_set_link",
                "code_change",
            ]

            for table in tables:
                self._process_table(cursor, table, batch)

            conn.close()
            batch.status = "COMPLETED"
            batch.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            return batch
        except Exception as e:
            batch.status = "FAILED"
            batch.error_message = str(e)
            self.db.commit()
            raise e

    def _process_table(self, cursor, table, batch):
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]

        for row in rows:
            data = dict(zip(column_names, row))
            record_id = self._ingest_record(table, data, batch)
            self._map_to_application_models(table, data, record_id, batch)

    def _make_source_id(self, table: str, data: dict) -> str:
        """
        Produce a stable, human-readable source record ID for each table.
        Falls back to a deterministic hash of the full row when no natural key exists.
        """
        if table == "issue":
            return str(data["issue_id"])
        elif table == "change_set":
            return str(data["commit_hash"])
        elif table == "meta":
            return str(data.get("key", ""))
        elif table == "issue_component":
            return f"{data.get('issue_id')}::{data.get('component')}"
        elif table == "issue_fix_version":
            return f"{data.get('issue_id')}::{data.get('fix_version')}"
        elif table == "issue_comment":
            # Comments have no surrogate key; use composite of issue_id + username + created_date
            return f"{data.get('issue_id')}::{data.get('username')}::{data.get('created_date')}"
        elif table == "issue_link":
            return (
                f"{data.get('source_issue_id')}::{data.get('target_issue_id')}"
                f"::{data.get('name')}"
            )
        elif table == "change_set_link":
            return f"{data.get('commit_hash')}::{data.get('issue_id')}"
        elif table == "code_change":
            return f"{data.get('commit_hash')}::{data.get('file_path')}::{data.get('change_type')}"
        else:
            # Generic fallback: SHA-256 of sorted row
            row_json = json.dumps(data, sort_keys=True, default=str)
            return hashlib.sha256(row_json.encode()).hexdigest()[:32]

    def _ingest_record(self, table: str, data: dict, batch) -> int:
        record_json = json.dumps(data, sort_keys=True, default=str)
        record_hash = hashlib.sha256(record_json.encode()).hexdigest()
        source_id = self._make_source_id(table, data)

        existing = self.db.query(SourceRecord).filter_by(
            dataset_name="KafkaSQLite",
            source_table=table,
            source_record_id=source_id,
        ).first()

        if existing:
            if existing.source_record_hash == record_hash:
                batch.records_skipped += 1
                return existing.id
            else:
                existing.source_record_hash = record_hash
                existing.raw_data = data
                batch.records_updated += 1
                self.db.flush()
                return existing.id

        sr = SourceRecord(
            dataset_name="KafkaSQLite",
            source_table=table,
            source_record_id=source_id,
            source_record_hash=record_hash,
            raw_data=data,
            ingestion_batch_id=batch.id,
            # Preserve source timestamps where available
            source_created_at=self._parse_date(
                data.get("created_date") or data.get("committed_date")
            ),
            source_updated_at=self._parse_date(data.get("updated_date")),
        )
        self.db.add(sr)
        self.db.flush()
        batch.records_inserted += 1
        batch.records_processed += 1
        return sr.id

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse ISO 8601 date strings. Timezone offsets are preserved.
        Returns None for missing or unparseable values.
        Original string is always stored unchanged in raw_data.
        """
        if not date_str:
            return None
        # Normalise the trailing Z to explicit UTC offset
        normalised = date_str.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalised)
        except ValueError:
            pass
        # Secondary fallback formats
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        logger.warning(f"Could not parse date string: {date_str!r}")
        return None

    # ------------------------------------------------------------------
    # Application-model mapping
    # ------------------------------------------------------------------

    def _map_to_application_models(self, table: str, data: dict, record_id: int, batch) -> None:
        try:
            if table == "issue":
                self._upsert_issue(data, record_id)

            elif table == "change_set":
                self._upsert_commit(data, record_id)

            elif table == "issue_comment":
                self._insert_issue_comment(data, record_id, batch)

            elif table == "issue_component":
                self._insert_issue_component(data, record_id, batch)

            elif table == "issue_fix_version":
                self._insert_issue_fix_version(data, record_id, batch)

            elif table == "issue_link":
                self._insert_issue_link(data, record_id, batch)

            elif table == "change_set_link":
                self._insert_commit_issue_link(data, record_id, batch)

            elif table == "code_change":
                self._insert_code_change(data, record_id, batch)

            self.db.commit()

        except IntegrityError:
            # Unique constraint violation on idempotent re-run — safe to skip
            self.db.rollback()
        except Exception as e:
            self.db.rollback()
            logger.warning(f"Error mapping record in table {table}: {e}")

    # ---- per-entity helpers ----

    def _upsert_issue(self, data: dict, record_id: int) -> None:
        issue_id = data.get("issue_id")
        existing = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
        if not existing:
            issue = Issue(
                source_issue_id=issue_id,
                issue_type=data.get("type"),
                summary=data.get("summary"),
                description=data.get("description"),
                priority=data.get("priority"),
                status=data.get("status"),
                resolution=data.get("resolution"),
                assignee=data.get("assignee"),
                reporter=data.get("reporter"),
                created_at=self._parse_date(data.get("created_date_zoned") or data.get("created_date")),
                updated_at=self._parse_date(data.get("updated_date_zoned") or data.get("updated_date")),
                resolved_at=self._parse_date(data.get("resolved_date_zoned") or data.get("resolved_date")),
                source_record_id=record_id,
            )
            self.db.add(issue)
        else:
            # Update mutable fields on a changed source record
            existing.issue_type = data.get("type")
            existing.summary = data.get("summary")
            existing.description = data.get("description")
            existing.priority = data.get("priority")
            existing.status = data.get("status")
            existing.resolution = data.get("resolution")
            existing.assignee = data.get("assignee")
            existing.reporter = data.get("reporter")
            existing.created_at = self._parse_date(
                data.get("created_date_zoned") or data.get("created_date")
            )
            existing.updated_at = self._parse_date(
                data.get("updated_date_zoned") or data.get("updated_date")
            )
            existing.resolved_at = self._parse_date(
                data.get("resolved_date_zoned") or data.get("resolved_date")
            )

    def _upsert_commit(self, data: dict, record_id: int) -> None:
        commit_hash = data.get("commit_hash")
        existing = self.db.query(Commit).filter_by(commit_hash=commit_hash).first()
        if not existing:
            commit = Commit(
                commit_hash=commit_hash,
                message=data.get("message"),
                author=data.get("author"),
                author_email=data.get("author_email"),
                committed_at=self._parse_date(
                    data.get("committed_date_zoned") or data.get("committed_date")
                ),
                is_merge=bool(data.get("is_merge", 0)),
                source_record_id=record_id,
            )
            self.db.add(commit)
        else:
            existing.message = data.get("message")
            existing.author = data.get("author")
            existing.author_email = data.get("author_email")
            existing.committed_at = self._parse_date(
                data.get("committed_date_zoned") or data.get("committed_date")
            )
            existing.is_merge = bool(data.get("is_merge", 0))

    def _insert_issue_comment(self, data: dict, record_id: int, batch) -> None:
        issue_id = data.get("issue_id")
        issue_exists = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
        if not issue_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved issue_comment → issue: {issue_id}")
            return

        # Check for existing via source_record_id to avoid duplicates on re-run
        existing = (
            self.db.query(IssueComment)
            .filter_by(source_record_id=record_id)
            .first()
        )
        if not existing:
            comment = IssueComment(
                source_issue_id=issue_id,
                username=data.get("username"),
                display_name=data.get("display_name"),
                message=data.get("message"),
                created_at=self._parse_date(
                    data.get("created_date_zoned") or data.get("created_date")
                ),
                source_record_id=record_id,
            )
            self.db.add(comment)

    def _insert_issue_component(self, data: dict, record_id: int, batch) -> None:
        issue_id = data.get("issue_id")
        component = data.get("component")
        issue_exists = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
        if not issue_exists:
            batch.unresolved_relationships += 1
            return
        # UniqueConstraint will deduplicate; check first to avoid rollback overhead
        existing = (
            self.db.query(IssueComponent)
            .filter_by(source_issue_id=issue_id, component=component)
            .first()
        )
        if not existing:
            self.db.add(IssueComponent(
                source_issue_id=issue_id,
                component=component,
                source_record_id=record_id,
            ))

    def _insert_issue_fix_version(self, data: dict, record_id: int, batch) -> None:
        issue_id = data.get("issue_id")
        fix_version = data.get("fix_version")
        issue_exists = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
        if not issue_exists:
            batch.unresolved_relationships += 1
            return
        existing = (
            self.db.query(IssueFixVersion)
            .filter_by(source_issue_id=issue_id, fix_version=fix_version)
            .first()
        )
        if not existing:
            self.db.add(IssueFixVersion(
                source_issue_id=issue_id,
                fix_version=fix_version,
                source_record_id=record_id,
            ))

    def _insert_issue_link(self, data: dict, record_id: int, batch) -> None:
        source_id = data.get("source_issue_id")
        target_id = data.get("target_issue_id")
        link_name = data.get("name")
        src_exists = self.db.query(Issue).filter_by(source_issue_id=source_id).first()
        tgt_exists = self.db.query(Issue).filter_by(source_issue_id=target_id).first()
        if not src_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved issue_link source: {source_id}")
            return
        if not tgt_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved issue_link target: {target_id}")
            return
        existing = (
            self.db.query(IssueLink)
            .filter_by(
                source_issue_id=source_id,
                target_issue_id=target_id,
                link_name=link_name,
            )
            .first()
        )
        if not existing:
            self.db.add(IssueLink(
                source_issue_id=source_id,
                target_issue_id=target_id,
                link_name=link_name,
                outward_label=data.get("outward_label"),
                is_containment=bool(data.get("is_containment", 0)),
                source_record_id=record_id,
            ))

    def _insert_commit_issue_link(self, data: dict, record_id: int, batch) -> None:
        commit_hash = data.get("commit_hash")
        issue_id = data.get("issue_id")
        commit_exists = self.db.query(Commit).filter_by(commit_hash=commit_hash).first()
        issue_exists = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
        if not commit_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved change_set_link commit: {commit_hash}")
            return
        if not issue_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved change_set_link issue: {issue_id}")
            return
        existing = (
            self.db.query(CommitIssueLink)
            .filter_by(commit_hash=commit_hash, source_issue_id=issue_id)
            .first()
        )
        if not existing:
            self.db.add(CommitIssueLink(
                commit_hash=commit_hash,
                source_issue_id=issue_id,
                source_record_id=record_id,
            ))

    def _insert_code_change(self, data: dict, record_id: int, batch) -> None:
        commit_hash = data.get("commit_hash")
        commit_exists = self.db.query(Commit).filter_by(commit_hash=commit_hash).first()
        if not commit_exists:
            batch.unresolved_relationships += 1
            logger.debug(f"Unresolved code_change commit: {commit_hash}")
            return
        # Deduplicate by source_record_id
        existing = (
            self.db.query(CodeChange)
            .filter_by(source_record_id=record_id)
            .first()
        )
        if not existing:
            self.db.add(CodeChange(
                commit_hash=commit_hash,
                file_path=data.get("file_path"),
                old_file_path=data.get("old_file_path"),
                change_type=data.get("change_type"),
                is_deleted=bool(data.get("is_deleted", 0)),
                lines_added=data.get("sum_added_lines"),
                lines_removed=data.get("sum_removed_lines"),
                source_record_id=record_id,
            ))

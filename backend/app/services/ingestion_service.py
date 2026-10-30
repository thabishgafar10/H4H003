import sqlite3
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink
from app.models.ingestion_batch import IngestionBatch
from app.models.unresolved_relationship import UnresolvedRelationship
from app.core.logging_config import logger


class IngestionService:
    def __init__(self, db: Session, source_db_path: str, dataset_name: str = "KafkaSQLite"):
        self.db = db
        self.source_db_path = source_db_path
        self.dataset_name = dataset_name
        self.known_issue_ids: Set[str] = set()
        self.known_commit_hashes: Set[str] = set()
        self.unresolved_set: Set[Tuple[str, str, str, str]] = set()

    def _init_caches(self):
        self.known_issue_ids = {
            r[0] for r in self.db.query(Issue.source_issue_id).all() if r[0]
        }
        self.known_commit_hashes = {
            r[0] for r in self.db.query(Commit.commit_hash).all() if r[0]
        }
        self.unresolved_set = {
            (r[0], r[1], r[2], r[3])
            for r in self.db.query(
                UnresolvedRelationship.source_table,
                UnresolvedRelationship.source_id,
                UnresolvedRelationship.target_type,
                UnresolvedRelationship.target_id,
            )
            .filter_by(dataset_name=self.dataset_name)
            .all()
        }

    def run_ingestion(self):
        batch = IngestionBatch(dataset_name=self.dataset_name, status="RUNNING")
        self.db.add(batch)
        self.db.commit()

        self._init_caches()

        try:
            conn = sqlite3.connect(f"file:{self.source_db_path}?mode=ro", uri=True)
            try:
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
            finally:
                conn.close()

            batch.status = "COMPLETED"
            batch.completed_at = datetime.now(timezone.utc)
            # Dataset-level unresolved relationship count persisted to batch
            batch.unresolved_relationships = (
                self.db.query(UnresolvedRelationship)
                .filter_by(dataset_name=self.dataset_name)
                .count()
            )
            self.db.commit()
            return batch
        except Exception as e:
            self.db.rollback()
            batch.status = "FAILED"
            batch.error_message = str(e)
            batch.completed_at = datetime.now(timezone.utc)
            batch.unresolved_relationships = (
                self.db.query(UnresolvedRelationship)
                .filter_by(dataset_name=self.dataset_name)
                .count()
            )
            self.db.commit()
            raise e

    def get_unresolved_relationships(
        self, dataset_name: Optional[str] = None, source_table: Optional[str] = None
    ) -> List[UnresolvedRelationship]:
        target_dataset = dataset_name or self.dataset_name
        query = self.db.query(UnresolvedRelationship).filter_by(dataset_name=target_dataset)
        if source_table:
            query = query.filter_by(source_table=source_table)
        return query.order_by(UnresolvedRelationship.id).all()

    def _process_table(self, cursor, table: str, batch: IngestionBatch):
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]

        # Load existing source records for this table to enable O(1) checks
        existing_sr = {
            sr.source_record_id: (sr.id, sr.source_record_hash)
            for sr in self.db.query(SourceRecord.source_record_id, SourceRecord.id, SourceRecord.source_record_hash)
            .filter_by(dataset_name=self.dataset_name, source_table=table)
            .all()
        }

        # Cache existing relationships to avoid redundant queries / duplicates
        existing_components = set()
        existing_fix_versions = set()
        existing_links = set()
        existing_commit_links = set()

        if table == "issue_component":
            existing_components = {
                (c.source_issue_id, c.component)
                for c in self.db.query(IssueComponent.source_issue_id, IssueComponent.component).all()
            }
        elif table == "issue_fix_version":
            existing_fix_versions = {
                (f.source_issue_id, f.fix_version)
                for f in self.db.query(IssueFixVersion.source_issue_id, IssueFixVersion.fix_version).all()
            }
        elif table == "issue_link":
            existing_links = {
                (l.source_issue_id, l.target_issue_id, l.link_name)
                for l in self.db.query(IssueLink.source_issue_id, IssueLink.target_issue_id, IssueLink.link_name).all()
            }
        elif table == "change_set_link":
            existing_commit_links = {
                (cl.commit_hash, cl.source_issue_id)
                for cl in self.db.query(CommitIssueLink.commit_hash, CommitIssueLink.source_issue_id).all()
            }

        uncommitted_count = 0

        for row in rows:
            data = dict(zip(column_names, row))
            record_id, status = self._ingest_record_cached(table, data, batch, existing_sr)
            if status != "skipped":
                self._map_to_application_models_cached(
                    table, data, record_id, batch, status,
                    existing_components, existing_fix_versions, existing_links, existing_commit_links
                )
            uncommitted_count += 1
            if uncommitted_count >= 1000:
                self.db.commit()
                uncommitted_count = 0

        if uncommitted_count > 0:
            self.db.commit()

    def _make_source_id(self, table: str, data: dict) -> str:
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
            # Composite of issue_id + username + created_date + message hash to prevent collapsing
            # distinct comments made within the same millisecond by the same bot/user
            msg = str(data.get("message") or "")
            msg_hash = hashlib.sha256(msg.encode("utf-8")).hexdigest()[:12]
            return f"{data.get('issue_id')}::{data.get('username')}::{data.get('created_date')}::{msg_hash}"
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
            row_json = json.dumps(data, sort_keys=True, default=str)
            return hashlib.sha256(row_json.encode()).hexdigest()[:32]

    def _ingest_record_cached(
        self, table: str, data: dict, batch: IngestionBatch, existing_sr: Dict[str, Tuple[int, str]]
    ) -> Tuple[int, str]:
        record_json = json.dumps(data, sort_keys=True, default=str)
        record_hash = hashlib.sha256(record_json.encode()).hexdigest()
        source_id = self._make_source_id(table, data)

        if source_id in existing_sr:
            sr_id, old_hash = existing_sr[source_id]
            if old_hash == record_hash:
                batch.records_skipped += 1
                batch.records_processed += 1
                return sr_id, "skipped"
            else:
                existing = self.db.query(SourceRecord).filter_by(id=sr_id).first()
                if existing:
                    existing.source_record_hash = record_hash
                    existing.raw_data = data
                existing_sr[source_id] = (sr_id, record_hash)
                batch.records_updated += 1
                batch.records_processed += 1
                return sr_id, "updated"

        sr = SourceRecord(
            dataset_name=batch.dataset_name or self.dataset_name,
            source_table=table,
            source_record_id=source_id,
            source_record_hash=record_hash,
            raw_data=data,
            ingestion_batch_id=batch.id,
            source_created_at=self._parse_date(
                data.get("created_date_zoned") or data.get("created_date") or
                data.get("committed_date_zoned") or data.get("committed_date")
            ),
            source_updated_at=self._parse_date(
                data.get("updated_date_zoned") or data.get("updated_date")
            ),
        )
        self.db.add(sr)
        self.db.flush()
        existing_sr[source_id] = (sr.id, record_hash)
        batch.records_inserted += 1
        batch.records_processed += 1
        return sr.id, "inserted"

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        normalised = str(date_str).strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalised)
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        logger.warning(f"Could not parse date string: {date_str!r}")
        return None

    def _record_unresolved_relationship(
        self,
        table: str,
        source_id: str,
        record_id: int,
        target_type: str,
        target_id: str,
        reason: str,
        batch: IngestionBatch,
    ) -> None:
        target_dataset = batch.dataset_name or self.dataset_name
        key = (table, source_id, target_type, target_id)
        if key in self.unresolved_set:
            return
        self.unresolved_set.add(key)
        batch.unresolved_relationships = (batch.unresolved_relationships or 0) + 1
        unres = UnresolvedRelationship(
            dataset_name=target_dataset,
            source_table=table,
            source_record_id=record_id,
            source_id=source_id,
            target_type=target_type,
            target_id=target_id,
            reason=reason,
            batch_id=batch.id,
        )
        self.db.add(unres)

    # ------------------------------------------------------------------
    # Application-model mapping (Cached)
    # ------------------------------------------------------------------

    def _map_to_application_models_cached(
        self, table: str, data: dict, record_id: int, batch: IngestionBatch, status: str,
        existing_components: Set, existing_fix_versions: Set, existing_links: Set, existing_commit_links: Set
    ) -> None:
        try:
            if table == "issue":
                self._upsert_issue(data, record_id, status)
                self.known_issue_ids.add(data.get("issue_id"))

            elif table == "change_set":
                self._upsert_commit(data, record_id, status)
                self.known_commit_hashes.add(data.get("commit_hash"))

            elif table == "issue_comment":
                self._insert_issue_comment_fast(data, record_id, batch, status)

            elif table == "issue_component":
                self._insert_issue_component_fast(data, record_id, batch, status, existing_components)

            elif table == "issue_fix_version":
                self._insert_issue_fix_version_fast(data, record_id, batch, status, existing_fix_versions)

            elif table == "issue_link":
                self._insert_issue_link_fast(data, record_id, batch, status, existing_links)

            elif table == "change_set_link":
                self._insert_commit_issue_link_fast(data, record_id, batch, status, existing_commit_links)

            elif table == "code_change":
                self._insert_code_change_fast(data, record_id, batch, status)

        except IntegrityError:
            self.db.rollback()
        except Exception as e:
            self.db.rollback()
            logger.warning(f"Error mapping record in table {table}: {e}")

    # Compatibility methods for tests calling private methods directly
    def _ingest_record(self, table: str, data: dict, batch: IngestionBatch) -> int:
        record_id, _ = self._ingest_record_cached(table, data, batch, {})
        return record_id

    def _map_to_application_models(self, table: str, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._map_to_application_models_cached(table, data, record_id, batch, "inserted", set(), set(), set(), set())

    # ---- per-entity helpers ----

    def _upsert_issue(self, data: dict, record_id: int, status: str) -> None:
        issue_id = data.get("issue_id")
        if status == "inserted":
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
            existing = self.db.query(Issue).filter_by(source_issue_id=issue_id).first()
            if existing:
                existing.issue_type = data.get("type")
                existing.summary = data.get("summary")
                existing.description = data.get("description")
                existing.priority = data.get("priority")
                existing.status = data.get("status")
                existing.resolution = data.get("resolution")
                existing.assignee = data.get("assignee")
                existing.reporter = data.get("reporter")
                existing.created_at = self._parse_date(data.get("created_date_zoned") or data.get("created_date"))
                existing.updated_at = self._parse_date(data.get("updated_date_zoned") or data.get("updated_date"))
                existing.resolved_at = self._parse_date(data.get("resolved_date_zoned") or data.get("resolved_date"))

    def _upsert_commit(self, data: dict, record_id: int, status: str) -> None:
        commit_hash = data.get("commit_hash")
        if status == "inserted":
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
            existing = self.db.query(Commit).filter_by(commit_hash=commit_hash).first()
            if existing:
                existing.message = data.get("message")
                existing.author = data.get("author")
                existing.author_email = data.get("author_email")
                existing.committed_at = self._parse_date(
                    data.get("committed_date_zoned") or data.get("committed_date")
                )
                existing.is_merge = bool(data.get("is_merge", 0))

    def _insert_issue_comment(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_issue_comment_fast(data, record_id, batch, "inserted")

    def _insert_issue_comment_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str) -> None:
        issue_id = data.get("issue_id")
        if issue_id not in self.known_issue_ids:
            if not self.db.query(Issue).filter_by(source_issue_id=issue_id).first():
                source_id = self._make_source_id("issue_comment", data)
                self._record_unresolved_relationship(
                    "issue_comment", source_id, record_id, "issue", issue_id,
                    f"Referenced issue '{issue_id}' not found", batch
                )
                logger.debug(f"Unresolved issue_comment -> issue: {issue_id}")
                return
            self.known_issue_ids.add(issue_id)

        if status == "inserted":
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

    def _insert_issue_component(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_issue_component_fast(data, record_id, batch, "inserted", set())

    def _insert_issue_component_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str, existing_set: Set) -> None:
        issue_id = data.get("issue_id")
        component = data.get("component")
        if issue_id not in self.known_issue_ids:
            if not self.db.query(Issue).filter_by(source_issue_id=issue_id).first():
                source_id = f"{issue_id}::{component}"
                self._record_unresolved_relationship(
                    "issue_component", source_id, record_id, "issue", issue_id,
                    f"Referenced issue '{issue_id}' not found", batch
                )
                return
            self.known_issue_ids.add(issue_id)

        key = (issue_id, component)
        if key not in existing_set:
            self.db.add(IssueComponent(
                source_issue_id=issue_id,
                component=component,
                source_record_id=record_id,
            ))
            existing_set.add(key)

    def _insert_issue_fix_version(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_issue_fix_version_fast(data, record_id, batch, "inserted", set())

    def _insert_issue_fix_version_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str, existing_set: Set) -> None:
        issue_id = data.get("issue_id")
        fix_version = data.get("fix_version")
        if issue_id not in self.known_issue_ids:
            if not self.db.query(Issue).filter_by(source_issue_id=issue_id).first():
                source_id = f"{issue_id}::{fix_version}"
                self._record_unresolved_relationship(
                    "issue_fix_version", source_id, record_id, "issue", issue_id,
                    f"Referenced issue '{issue_id}' not found", batch
                )
                return
            self.known_issue_ids.add(issue_id)

        key = (issue_id, fix_version)
        if key not in existing_set:
            self.db.add(IssueFixVersion(
                source_issue_id=issue_id,
                fix_version=fix_version,
                source_record_id=record_id,
            ))
            existing_set.add(key)

    def _insert_issue_link(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_issue_link_fast(data, record_id, batch, "inserted", set())

    def _insert_issue_link_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str, existing_set: Set) -> None:
        source_id = data.get("source_issue_id")
        target_id = data.get("target_issue_id")
        link_name = data.get("name")

        src_exists = (source_id in self.known_issue_ids) or bool(self.db.query(Issue).filter_by(source_issue_id=source_id).first())
        tgt_exists = (target_id in self.known_issue_ids) or bool(self.db.query(Issue).filter_by(source_issue_id=target_id).first())

        source_id_ref = f"{source_id}::{target_id}::{link_name}"

        if not src_exists or not tgt_exists:
            if not src_exists:
                self._record_unresolved_relationship(
                    "issue_link", source_id_ref, record_id, "issue", source_id,
                    f"Referenced source issue '{source_id}' not found in issues table", batch
                )
            if not tgt_exists:
                self._record_unresolved_relationship(
                    "issue_link", source_id_ref, record_id, "issue", target_id,
                    f"Referenced target issue '{target_id}' not found in issues table", batch
                )
            return

        key = (source_id, target_id, link_name)
        if key not in existing_set:
            self.db.add(IssueLink(
                source_issue_id=source_id,
                target_issue_id=target_id,
                link_name=link_name,
                outward_label=data.get("outward_label"),
                is_containment=bool(data.get("is_containment", 0)),
                source_record_id=record_id,
            ))
            existing_set.add(key)

    def _insert_commit_issue_link(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_commit_issue_link_fast(data, record_id, batch, "inserted", set())

    def _insert_commit_issue_link_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str, existing_set: Set) -> None:
        commit_hash = data.get("commit_hash")
        issue_id = data.get("issue_id")

        commit_exists = (commit_hash in self.known_commit_hashes) or bool(self.db.query(Commit).filter_by(commit_hash=commit_hash).first())
        issue_exists = (issue_id in self.known_issue_ids) or bool(self.db.query(Issue).filter_by(source_issue_id=issue_id).first())

        source_id_ref = f"{commit_hash}::{issue_id}"

        if not commit_exists or not issue_exists:
            if not commit_exists:
                self._record_unresolved_relationship(
                    "change_set_link", source_id_ref, record_id, "commit", commit_hash,
                    f"Referenced commit '{commit_hash}' not found", batch
                )
            if not issue_exists:
                self._record_unresolved_relationship(
                    "change_set_link", source_id_ref, record_id, "issue", issue_id,
                    f"Referenced issue '{issue_id}' not found", batch
                )
            return

        key = (commit_hash, issue_id)
        if key not in existing_set:
            self.db.add(CommitIssueLink(
                commit_hash=commit_hash,
                source_issue_id=issue_id,
                source_record_id=record_id,
            ))
            existing_set.add(key)

    def _insert_code_change(self, data: dict, record_id: int, batch: IngestionBatch) -> None:
        self._insert_code_change_fast(data, record_id, batch, "inserted")

    def _insert_code_change_fast(self, data: dict, record_id: int, batch: IngestionBatch, status: str) -> None:
        commit_hash = data.get("commit_hash")
        if commit_hash not in self.known_commit_hashes:
            if not self.db.query(Commit).filter_by(commit_hash=commit_hash).first():
                source_id = f"{commit_hash}::{data.get('file_path')}::{data.get('change_type')}"
                self._record_unresolved_relationship(
                    "code_change", source_id, record_id, "commit", commit_hash,
                    f"Referenced commit '{commit_hash}' not found", batch
                )
                return
            self.known_commit_hashes.add(commit_hash)

        if status == "inserted":
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

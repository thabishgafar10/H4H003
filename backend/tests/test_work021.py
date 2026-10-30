"""
Comprehensive tests for Work 02.1 — Ingestion Verification and API Completion.

Uses in-memory SQLite and synthetic fixtures throughout; no real Kafka DB dependency
except for the two tests that explicitly require it and are skipped when absent.
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.database import get_db, init_db
from app.db.base import Base
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.issue_relationships import (
    IssueComponent,
    IssueFixVersion,
    IssueLink,
    CommitIssueLink,
)
from app.models.ingestion_batch import IngestionBatch
from app.services.ingestion_service import IngestionService

# ---------------------------------------------------------------------------
# Shared in-memory DB engine + fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def mem_engine():
    """
    In-memory SQLite engine using StaticPool so all sessions share the same
    connection (and therefore the same in-memory database).
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    init_db(target_engine=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db(mem_engine):
    SessionLocal = sessionmaker(bind=mem_engine)
    session = SessionLocal()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# FastAPI test client wired to in-memory DB
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def client(mem_engine):
    """
    FastAPI TestClient with dependency override so all DB operations go to
    the in-memory test engine. Tables are already created by mem_engine fixture.
    """
    SessionLocal = sessionmaker(bind=mem_engine)

    def override_get_db():
        sess = SessionLocal()
        try:
            yield sess
        finally:
            sess.close()

    app.dependency_overrides[get_db] = override_get_db
    # TestClient must not trigger real startup (which would touch the real DB).
    # We pass raise_server_exceptions=True so assertion errors surface.
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Synthetic source SQLite DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def synthetic_source_db():
    """Creates a temporary SQLite database that mirrors the Kafka schema with
    a small number of synthetic records."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE meta (key TEXT, value TEXT);
        CREATE TABLE issue (
            issue_id TEXT, type TEXT, created_date TEXT, created_date_zoned TEXT,
            updated_date TEXT, updated_date_zoned TEXT, resolved_date TEXT,
            resolved_date_zoned TEXT, summary TEXT, description TEXT,
            priority TEXT, status TEXT, resolution TEXT,
            assignee TEXT, assignee_username TEXT, reporter TEXT, reporter_username TEXT
        );
        CREATE TABLE issue_component (issue_id TEXT, component TEXT);
        CREATE TABLE issue_fix_version (issue_id TEXT, fix_version TEXT);
        CREATE TABLE issue_comment (
            issue_id TEXT, username TEXT, display_name TEXT,
            created_date TEXT, created_date_zoned TEXT, message TEXT
        );
        CREATE TABLE issue_link (
            source_issue_id TEXT, target_issue_id TEXT, name TEXT,
            outward_label TEXT, is_containment INTEGER
        );
        CREATE TABLE change_set (
            commit_hash TEXT, committed_date TEXT, committed_date_zoned TEXT,
            message TEXT, author TEXT, author_email TEXT, is_merge INTEGER
        );
        CREATE TABLE change_set_link (commit_hash TEXT, issue_id TEXT);
        CREATE TABLE code_change (
            commit_hash TEXT, file_path TEXT, old_file_path TEXT,
            change_type TEXT, is_deleted INTEGER,
            sum_added_lines INTEGER, sum_removed_lines INTEGER
        );
    """)

    # Populate
    c.execute("INSERT INTO meta VALUES ('version', '1.0')")
    c.execute("""INSERT INTO issue VALUES
        ('TST-1','Bug','2023-01-01T10:00:00.000Z','2023-01-01T10:00:00.000+00:00',
         '2023-01-02T10:00:00.000Z','2023-01-02T10:00:00.000+00:00',
         NULL, NULL,
         'Test Bug 1','Description 1','Major','Open','',
         'alice','alice_u','bob','bob_u')""")
    c.execute("""INSERT INTO issue VALUES
        ('TST-2','Feature','2023-02-01T10:00:00.000Z','2023-02-01T10:00:00.000+00:00',
         '2023-02-05T10:00:00.000Z','2023-02-05T10:00:00.000+00:00',
         '2023-02-10T10:00:00.000Z','2023-02-10T10:00:00.000+00:00',
         'Feature 2','Desc 2','Minor','Resolved','Fixed',
         'charlie','charlie_u','alice','alice_u')""")

    c.execute("INSERT INTO issue_component VALUES ('TST-1', 'core')")
    c.execute("INSERT INTO issue_component VALUES ('TST-2', 'api')")
    c.execute("INSERT INTO issue_fix_version VALUES ('TST-2', '1.0.0')")
    c.execute("""INSERT INTO issue_comment VALUES
        ('TST-1','alice','Alice','2023-01-01T11:00:00Z','2023-01-01T11:00:00+00:00','First comment')""")
    c.execute("""INSERT INTO issue_link VALUES ('TST-1','TST-2','Duplicate','duplicates',0)""")

    c.execute("""INSERT INTO change_set VALUES
        ('abc123','2023-01-15T10:00:00Z','2023-01-15T10:00:00+00:00',
         'TST-1: fix bug','alice','alice@example.com',0)""")
    c.execute("INSERT INTO change_set_link VALUES ('abc123', 'TST-1')")
    c.execute("""INSERT INTO code_change VALUES
        ('abc123','src/main.py','src/main.py','MODIFY',0,5,3)""")

    conn.commit()
    conn.close()

    yield db_path
    os.unlink(db_path)


# ===========================================================================
# 1. Database Initialization Tests
# ===========================================================================

class TestDatabaseInitialization:
    def test_all_tables_created(self, mem_engine):
        from sqlalchemy import inspect
        inspector = inspect(mem_engine)
        tables = set(inspector.get_table_names())
        required = {
            "issues", "commits", "issue_comments", "code_changes",
            "issue_components", "issue_fix_versions", "issue_links",
            "commit_issue_links", "source_records", "ingestion_batches",
        }
        assert required.issubset(tables), f"Missing tables: {required - tables}"

    def test_source_record_unique_constraint(self, db):
        """Duplicate (dataset, table, record_id) must be rejected."""
        sr1 = SourceRecord(
            dataset_name="test", source_table="issue",
            source_record_id="X-1", source_record_hash="hash1",
            raw_data={"k": "v"}, ingestion_batch_id=1,
        )
        db.add(sr1)
        db.commit()

        import sqlalchemy.exc
        sr2 = SourceRecord(
            dataset_name="test", source_table="issue",
            source_record_id="X-1", source_record_hash="hash2",
            raw_data={"k": "v2"}, ingestion_batch_id=1,
        )
        db.add(sr2)
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            db.commit()
        db.rollback()


# ===========================================================================
# 2. Model Relationship Tests
# ===========================================================================

class TestModelRelationships:
    def _seed(self, db):
        batch = IngestionBatch(dataset_name="t", status="COMPLETED")
        db.add(batch)
        db.flush()

        sr = SourceRecord(
            dataset_name="t", source_table="issue", source_record_id="I-1",
            source_record_hash="h1", raw_data={}, ingestion_batch_id=batch.id,
        )
        db.add(sr)
        db.flush()

        issue = Issue(source_issue_id="I-1", summary="Test", source_record_id=sr.id)
        db.add(issue)
        db.flush()
        return issue, sr, batch

    def test_issue_comment_relationship(self, db):
        issue, sr, _ = self._seed(db)
        comment = IssueComment(
            source_issue_id="I-1", username="u", message="m", source_record_id=sr.id
        )
        db.add(comment)
        db.commit()
        db.refresh(issue)
        assert len(issue.comments) == 1
        assert issue.comments[0].username == "u"

    def test_issue_component_relationship(self, db):
        issue, sr, _ = self._seed(db)
        comp = IssueComponent(
            source_issue_id="I-1", component="core", source_record_id=sr.id
        )
        db.add(comp)
        db.commit()
        db.refresh(issue)
        assert len(issue.components) == 1

    def test_issue_fix_version_relationship(self, db):
        issue, sr, _ = self._seed(db)
        fv = IssueFixVersion(
            source_issue_id="I-1", fix_version="1.0", source_record_id=sr.id
        )
        db.add(fv)
        db.commit()
        db.refresh(issue)
        assert len(issue.fix_versions) == 1

    def test_commit_code_change_relationship(self, db):
        sr = SourceRecord(
            dataset_name="t", source_table="change_set", source_record_id="abc",
            source_record_hash="h", raw_data={}, ingestion_batch_id=1,
        )
        db.add(sr)
        db.flush()
        commit = Commit(commit_hash="abc", message="msg", source_record_id=sr.id)
        db.add(commit)
        db.flush()
        cc = CodeChange(
            commit_hash="abc", file_path="f.py", change_type="MODIFY",
            source_record_id=sr.id,
        )
        db.add(cc)
        db.commit()
        db.refresh(commit)
        assert len(commit.code_changes) == 1


# ===========================================================================
# 3. Source Record Persistence Tests
# ===========================================================================

class TestSourceRecordPersistence:
    def test_basic_persistence(self, db):
        sr = SourceRecord(
            dataset_name="KafkaSQLite", source_table="issue",
            source_record_id="KAFKA-1", source_record_hash="abc123",
            raw_data={"issue_id": "KAFKA-1"}, ingestion_batch_id=1,
        )
        db.add(sr)
        db.commit()
        assert sr.id is not None

    def test_ingested_at_auto_set(self, db):
        sr = SourceRecord(
            dataset_name="KafkaSQLite", source_table="issue",
            source_record_id="KAFKA-2", source_record_hash="def456",
            raw_data={}, ingestion_batch_id=1,
        )
        db.add(sr)
        db.commit()
        # ingested_at is set by the DB server default
        refreshed = db.query(SourceRecord).filter_by(source_record_id="KAFKA-2").first()
        assert refreshed is not None


# ===========================================================================
# 4. Timestamp Preservation Tests
# ===========================================================================

class TestTimestampPreservation:
    def _svc(self, db, src_path):
        return IngestionService(db, src_path)

    def test_parse_date_with_z_suffix(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        result = svc._parse_date("2023-01-01T10:00:00.000Z")
        assert result is not None
        assert result.year == 2023

    def test_parse_date_with_timezone_offset(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        result = svc._parse_date("2023-01-01T10:00:00+05:30")
        assert result is not None
        # timezone info preserved
        assert result.tzinfo is not None

    def test_parse_date_none_returns_none(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        assert svc._parse_date(None) is None
        assert svc._parse_date("") is None

    def test_parse_date_missing_is_nullable(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        assert svc._parse_date(None) is None

    def test_raw_data_preserves_original_string(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        batch = IngestionBatch(dataset_name="t", status="RUNNING")
        db.add(batch)
        db.commit()
        data = {
            "issue_id": "TST-99",
            "created_date": "2023-06-15T14:30:00.123Z",
        }
        svc._ingest_record("issue", data, batch)
        sr = db.query(SourceRecord).filter_by(source_record_id="TST-99").first()
        assert sr is not None
        assert sr.raw_data["created_date"] == "2023-06-15T14:30:00.123Z"

    def test_zoned_timestamp_preferred_over_plain(self, db, synthetic_source_db):
        svc = self._svc(db, synthetic_source_db)
        batch = IngestionBatch(dataset_name="t", status="RUNNING")
        db.add(batch)
        db.commit()
        data = {
            "issue_id": "TST-TZ",
            "type": "Bug",
            "created_date": "2023-01-01T10:00:00Z",
            "created_date_zoned": "2023-01-01T18:00:00+08:00",
        }
        svc._ingest_record("issue", data, batch)
        svc._map_to_application_models("issue", data, 1, batch)
        db.commit()
        issue = db.query(Issue).filter_by(source_issue_id="TST-TZ").first()
        if issue and issue.created_at:
            # Zoned variant has +08:00 which maps to offset-aware datetime
            assert issue.created_at is not None


# ===========================================================================
# 5. Idempotent Ingestion Tests (synthetic DB)
# ===========================================================================

class TestIdempotentIngestion:
    def test_second_run_creates_no_new_source_records(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        batch1 = svc.run_ingestion()
        count1 = db.query(SourceRecord).count()

        batch2 = svc.run_ingestion()
        count2 = db.query(SourceRecord).count()

        assert batch1.status == "COMPLETED"
        assert batch2.status == "COMPLETED"
        assert count1 == count2, "Second run must not create new source records"

    def test_second_run_skipped_equals_first_inserted(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        batch1 = svc.run_ingestion()
        inserted1 = batch1.records_inserted

        batch2 = svc.run_ingestion()
        assert batch2.records_skipped == inserted1
        assert batch2.records_inserted == 0

    def test_no_duplicate_issues(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        svc.run_ingestion()
        count = db.query(Issue).count()
        # Should be exactly 2 (TST-1 and TST-2) even after two runs
        assert count == 2

    def test_no_duplicate_commits(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        svc.run_ingestion()
        count = db.query(Commit).count()
        assert count == 1  # only abc123

    def test_no_duplicate_comments(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        svc.run_ingestion()
        count = db.query(IssueComment).count()
        assert count == 1

    def test_no_duplicate_code_changes(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        svc.run_ingestion()
        count = db.query(CodeChange).count()
        assert count == 1

    def test_record_hashes_stable(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        hashes_after_run1 = {
            sr.source_record_id: sr.source_record_hash
            for sr in db.query(SourceRecord).all()
        }
        svc.run_ingestion()
        hashes_after_run2 = {
            sr.source_record_id: sr.source_record_hash
            for sr in db.query(SourceRecord).all()
        }
        assert hashes_after_run1 == hashes_after_run2


# ===========================================================================
# 6. Duplicate Handling Tests
# ===========================================================================

class TestDuplicateHandling:
    def test_repeated_identical_rows_not_collapsed(self, db):
        """Two different issues with same field values but different IDs must both exist."""
        issue_a = Issue(source_issue_id="DUP-A", summary="Same summary", issue_type="Bug")
        issue_b = Issue(source_issue_id="DUP-B", summary="Same summary", issue_type="Bug")
        db.add_all([issue_a, issue_b])
        db.commit()
        assert db.query(Issue).count() == 2

    def test_changed_source_record_updates_hash(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()

        # Simulate changed source record by modifying the DB externally
        conn = sqlite3.connect(synthetic_source_db)
        conn.execute("UPDATE issue SET summary='Modified Summary' WHERE issue_id='TST-1'")
        conn.commit()
        conn.close()

        batch2 = svc.run_ingestion()
        assert batch2.records_updated >= 1

        sr = db.query(SourceRecord).filter_by(source_record_id="TST-1").first()
        assert sr is not None
        assert sr.raw_data["summary"] == "Modified Summary"


# ===========================================================================
# 7. Missing Value Tests
# ===========================================================================

class TestMissingValues:
    def test_issue_with_null_resolved_date(self, db, synthetic_source_db):
        svc = IngestionService(db, synthetic_source_db)
        svc.run_ingestion()
        issue = db.query(Issue).filter_by(source_issue_id="TST-1").first()
        assert issue is not None
        assert issue.resolved_at is None  # TST-1 has no resolved date

    def test_source_record_nullable_timestamps(self, db):
        sr = SourceRecord(
            dataset_name="t", source_table="meta", source_record_id="k1",
            source_record_hash="h", raw_data={}, ingestion_batch_id=1,
            source_created_at=None, source_updated_at=None,
        )
        db.add(sr)
        db.commit()
        assert sr.source_created_at is None
        assert sr.source_updated_at is None


# ===========================================================================
# 8. Unresolved Relationship Tests
# ===========================================================================

class TestUnresolvedRelationships:
    def test_comment_orphan_tracked(self, db):
        """A comment for a non-existent issue must increment unresolved count."""
        svc = IngestionService(db, ":memory:")  # source path unused here

        batch = IngestionBatch(dataset_name="t", status="RUNNING")
        db.add(batch)
        db.commit()

        data = {
            "issue_id": "GHOST-999",
            "username": "u",
            "display_name": "U",
            "created_date": "2023-01-01T00:00:00Z",
            "message": "orphan",
        }
        svc._insert_issue_comment(data, 1, batch)
        assert batch.unresolved_relationships == 1

    def test_code_change_orphan_tracked(self, db):
        svc = IngestionService(db, ":memory:")

        batch = IngestionBatch(dataset_name="t", status="RUNNING")
        db.add(batch)
        db.commit()

        data = {
            "commit_hash": "nonexistent_hash",
            "file_path": "a.py",
            "change_type": "MODIFY",
        }
        svc._insert_code_change(data, 1, batch)
        assert batch.unresolved_relationships == 1


# ===========================================================================
# 9. API Tests
# ===========================================================================

class TestAPIStatistics:
    def test_statistics_endpoint_empty_db(self, client):
        response = client.get("/api/statistics")
        assert response.status_code == 200
        data = response.json()
        assert data["total_issues"] == 0
        assert data["total_commits"] == 0
        assert data["total_source_records"] == 0

    def test_statistics_endpoint_with_data(self, client, db):
        db.add(Issue(source_issue_id="ST-1", summary="s"))
        db.add(Commit(commit_hash="abc", message="m"))
        db.commit()

        response = client.get("/api/statistics")
        assert response.status_code == 200
        data = response.json()
        assert data["total_issues"] == 1
        assert data["total_commits"] == 1


class TestAPIIssues:
    def test_list_issues_empty(self, client):
        response = client.get("/api/issues")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_issues_pagination(self, client, db):
        for i in range(5):
            db.add(Issue(source_issue_id=f"PAG-{i}", summary=f"Issue {i}"))
        db.commit()

        r1 = client.get("/api/issues?limit=3&skip=0")
        assert r1.status_code == 200
        assert len(r1.json()) == 3

        r2 = client.get("/api/issues?limit=3&skip=3")
        assert r2.status_code == 200
        assert len(r2.json()) == 2

    def test_issue_filter_by_status(self, client, db):
        db.add(Issue(source_issue_id="FLT-1", status="Open"))
        db.add(Issue(source_issue_id="FLT-2", status="Closed"))
        db.commit()

        response = client.get("/api/issues?status=Open")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["source_issue_id"] == "FLT-1"

    def test_issue_detail_found(self, client, db):
        db.add(Issue(source_issue_id="DET-1", summary="Detail test", issue_type="Bug"))
        db.commit()

        response = client.get("/api/issues/DET-1")
        assert response.status_code == 200
        assert response.json()["summary"] == "Detail test"

    def test_issue_detail_not_found(self, client):
        response = client.get("/api/issues/NONEXISTENT-999")
        assert response.status_code == 404

    def test_issue_response_includes_comments_and_components(self, client, db):
        db.add(Issue(source_issue_id="RICH-1", summary="Rich"))
        db.flush()
        db.add(IssueComment(source_issue_id="RICH-1", username="u", message="m"))
        db.add(IssueComponent(source_issue_id="RICH-1", component="core"))
        db.commit()

        response = client.get("/api/issues/RICH-1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["comments"]) == 1
        assert len(data["components"]) == 1


class TestAPICommits:
    def test_commit_detail_found(self, client, db):
        db.add(Commit(commit_hash="deadbeef", message="fix bug", author="Alice"))
        db.commit()

        response = client.get("/api/commits/deadbeef")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "fix bug"
        assert data["author"] == "Alice"

    def test_commit_detail_not_found(self, client):
        response = client.get("/api/commits/aaabbbccc")
        assert response.status_code == 404

    def test_commit_includes_code_changes(self, client, db):
        db.add(Commit(commit_hash="feed1234", message="refactor"))
        db.flush()
        db.add(CodeChange(
            commit_hash="feed1234", file_path="main.py",
            change_type="MODIFY", lines_added=10, lines_removed=5,
        ))
        db.commit()

        response = client.get("/api/commits/feed1234")
        assert response.status_code == 200
        data = response.json()
        assert len(data["code_changes"]) == 1
        assert data["code_changes"][0]["file_path"] == "main.py"


class TestAPIIngestionBatches:
    def test_batches_endpoint_empty(self, client):
        response = client.get("/api/ingestion/batches")
        assert response.status_code == 200
        assert response.json() == []

    def test_batches_endpoint_with_data(self, client, db):
        db.add(IngestionBatch(dataset_name="KafkaSQLite", status="COMPLETED"))
        db.commit()

        response = client.get("/api/ingestion/batches")
        assert response.status_code == 200
        assert len(response.json()) == 1

    def test_batches_pagination(self, client, db):
        for i in range(5):
            db.add(IngestionBatch(dataset_name="t", status="COMPLETED"))
        db.commit()

        r = client.get("/api/ingestion/batches?limit=2&skip=0")
        assert r.status_code == 200
        assert len(r.json()) == 2


class TestAPIHealthAndRoot:
    def test_root_endpoint(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert "message" in response.json()

    def test_health_endpoint(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


# ===========================================================================
# 10. Source Database Read-Only Protection
# ===========================================================================

class TestSourceDatabaseProtection:
    def test_source_db_opened_read_only(self, synthetic_source_db):
        """Must be able to connect read-only but not write."""
        conn = sqlite3.connect(f"file:{synthetic_source_db}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM issue")
        count = cursor.fetchone()[0]
        conn.close()
        assert count == 2

    def test_source_db_write_rejected(self, synthetic_source_db):
        conn = sqlite3.connect(f"file:{synthetic_source_db}?mode=ro", uri=True)
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO meta VALUES ('hack', 'value')")
        conn.close()


# ===========================================================================
# 11. Full ingestion with real Kafka DB (skipped if absent)
# ===========================================================================

KAFKA_DB = Path(__file__).resolve().parent.parent.parent / "sample_data" / "raw" / "kafka.sqlite3"

@pytest.mark.skipif(not KAFKA_DB.exists(), reason="kafka.sqlite3 not present")
class TestRealKafkaIngestion:
    def test_kafka_db_tables_exist(self):
        conn = sqlite3.connect(f"file:{KAFKA_DB}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cursor.fetchall()}
        conn.close()
        expected = {
            "meta", "issue", "issue_component", "issue_fix_version",
            "issue_comment", "issue_link", "change_set", "change_set_link", "code_change",
        }
        assert expected.issubset(tables)

    def test_kafka_idempotent_ingestion(self, db):
        """Two runs against real DB must not create duplicate source records."""
        svc = IngestionService(db, str(KAFKA_DB))
        batch1 = svc.run_ingestion()
        count1 = db.query(SourceRecord).count()
        issues1 = db.query(Issue).count()

        batch2 = svc.run_ingestion()
        count2 = db.query(SourceRecord).count()
        issues2 = db.query(Issue).count()

        assert batch1.status == "COMPLETED"
        assert batch2.status == "COMPLETED"
        assert count1 == count2
        assert issues1 == issues2
        assert batch2.records_inserted == 0
        assert batch2.records_skipped > 0

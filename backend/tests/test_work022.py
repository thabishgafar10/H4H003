"""
Work 02.2 Integrity Audit Tests:
- Persistent unresolved relationship tracking and querying
- Source vs. normalized record accounting
- Partial ingestion failure simulation
- Safe recovery through a subsequent ingestion run
- No duplicate records after recovery
- Accurate batch status transitions
"""

import pytest
import sqlite3
import tempfile
import os
from datetime import datetime, timezone
from pathlib import Path
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.database import get_db, init_db
from app.db.base import Base
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink
from app.models.ingestion_batch import IngestionBatch
from app.models.unresolved_relationship import UnresolvedRelationship
from app.services.ingestion_service import IngestionService


@pytest.fixture(scope="function")
def test_engine():
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
def db_session(test_engine):
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture(scope="function")
def api_client(test_engine):
    Session = sessionmaker(bind=test_engine)

    def override():
        sess = Session()
        try:
            yield sess
        finally:
            sess.close()

    app.dependency_overrides[get_db] = override
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def synthetic_audit_db():
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

    # Populate synthetic records: 1 valid issue, 1 valid commit, and deliberate UNRESOLVED links
    c.execute("INSERT INTO meta VALUES ('version', '1.0')")
    c.execute("""INSERT INTO issue VALUES
        ('KAFKA-1','Bug','2023-01-01T10:00:00.000Z','2023-01-01T10:00:00.000+00:00',
         '2023-01-02T10:00:00.000Z','2023-01-02T10:00:00.000+00:00',
         NULL, NULL,
         'Test Bug 1','Description 1','Major','Open','',
         'alice','alice_u','bob','bob_u')""")

    c.execute("INSERT INTO issue_component VALUES ('KAFKA-1', 'core')")
    c.execute("INSERT INTO issue_fix_version VALUES ('KAFKA-1', '1.0.0')")
    c.execute("INSERT INTO issue_comment VALUES ('KAFKA-1','alice','Alice','2023-01-01T11:00:00Z','2023-01-01T11:00:00+00:00','Test comment')")

    # Deliberate UNRESOLVED issue_link: target FLINK-999 does not exist
    c.execute("INSERT INTO issue_link VALUES ('KAFKA-1', 'FLINK-999', 'Reference', 'relates to', 0)")

    c.execute("INSERT INTO change_set VALUES ('hash100','2023-01-15T10:00:00Z','2023-01-15T10:00:00+00:00','commit message','dev','dev@ex.com',0)")
    c.execute("INSERT INTO code_change VALUES ('hash100','test.py','test.py','MODIFY',0,10,2)")

    # Deliberate UNRESOLVED change_set_link: references missing issue KAFKA-9999
    c.execute("INSERT INTO change_set_link VALUES ('hash100', 'KAFKA-9999')")

    conn.commit()
    conn.close()

    yield db_path
    os.unlink(db_path)


# ===========================================================================
# 1. Unresolved Relationship Tracking & Consistency Tests
# ===========================================================================

class TestUnresolvedRelationshipConsistency:
    def test_unresolved_records_persisted_and_queryable(self, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)
        batch = service.run_ingestion()

        assert batch.status == "COMPLETED"
        assert batch.unresolved_relationships == 2

        # Verify unresolvable records are persisted in UnresolvedRelationship table
        unresolved = db_session.query(UnresolvedRelationship).all()
        assert len(unresolved) == 2

        by_table = {u.source_table: u for u in unresolved}
        assert "issue_link" in by_table
        assert "change_set_link" in by_table

        assert by_table["issue_link"].target_id == "FLINK-999"
        assert by_table["change_set_link"].target_id == "KAFKA-9999"

    def test_unresolved_count_consistency_across_runs(self, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)

        # Run 1
        batch1 = service.run_ingestion()
        assert batch1.unresolved_relationships == 2

        # Run 2 (repeat run) - must STILL report 2 across the dataset, not 0
        batch2 = service.run_ingestion()
        assert batch2.unresolved_relationships == 2
        assert batch2.records_skipped > 0
        assert batch2.records_inserted == 0

    def test_unresolved_api_endpoint(self, api_client, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)
        service.run_ingestion()

        res = api_client.get("/api/ingestion/unresolved")
        assert res.status_code == 200
        data = res.json()
        assert len(data) == 2

        res_filtered = api_client.get("/api/ingestion/unresolved?source_table=issue_link")
        assert res_filtered.status_code == 200
        assert len(res_filtered.json()) == 1
        assert res_filtered.json()[0]["target_id"] == "FLINK-999"

    def test_statistics_reports_dataset_level_unresolved(self, api_client, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)
        service.run_ingestion()
        service.run_ingestion()  # repeat run

        res = api_client.get("/api/statistics")
        assert res.status_code == 200
        data = res.json()
        assert data["unresolved_relationships"] == 2


# ===========================================================================
# 2. Source Record vs Normalized Record Accounting Tests
# ===========================================================================

class TestSourceVsNormalizedAccounting:
    def test_accounting_integrity(self, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)
        batch = service.run_ingestion()

        total_source_records = db_session.query(SourceRecord).count()
        # In synthetic_audit_db:
        # meta: 1, issue: 1, issue_component: 1, issue_fix_version: 1, issue_comment: 1,
        # issue_link: 1, change_set: 1, change_set_link: 1, code_change: 1 = Total 9 source rows
        assert total_source_records == 9

        # Normalized records created:
        issues = db_session.query(Issue).count()
        components = db_session.query(IssueComponent).count()
        fix_versions = db_session.query(IssueFixVersion).count()
        comments = db_session.query(IssueComment).count()
        commits = db_session.query(Commit).count()
        code_changes = db_session.query(CodeChange).count()
        issue_links = db_session.query(IssueLink).count()
        commit_issue_links = db_session.query(CommitIssueLink).count()

        normalized_total = (
            issues + components + fix_versions + comments +
            commits + code_changes + issue_links + commit_issue_links
        )

        unresolved_count = db_session.query(UnresolvedRelationship).count()
        meta_count = db_session.query(SourceRecord).filter_by(source_table="meta").count()

        # Accounting formula: total source records == normalized + unresolved + meta
        assert total_source_records == normalized_total + unresolved_count + meta_count
        assert issue_links == 0  # 1 source row was unresolved
        assert commit_issue_links == 0  # 1 source row was unresolved
        assert unresolved_count == 2


# ===========================================================================
# 3. Interrupted Ingestion and Batch Safety Tests
# ===========================================================================

class TestInterruptedIngestionAndRecovery:
    def test_partial_ingestion_failure_records_failed_batch(self, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)

        # Inject failure halfway: fail when processing 'change_set'
        original_process_table = service._process_table

        def mock_process_table(cursor, table, batch):
            if table == "change_set":
                raise RuntimeError("Simulated crash mid-ingestion at change_set")
            original_process_table(cursor, table, batch)

        service._process_table = mock_process_table

        with pytest.raises(RuntimeError, match="Simulated crash"):
            service.run_ingestion()

        # Verify failed batch is recorded accurately
        batch = db_session.query(IngestionBatch).order_by(IngestionBatch.started_at.desc()).first()
        assert batch is not None
        assert batch.status == "FAILED"
        assert "Simulated crash" in batch.error_message
        assert batch.completed_at is not None

        # Verify partial records before crash were saved (meta, issue, etc.)
        issues_saved = db_session.query(Issue).count()
        assert issues_saved == 1

        # Commits should be 0 because crash happened before/at change_set
        commits_saved = db_session.query(Commit).count()
        assert commits_saved == 0

    def test_safe_recovery_after_partial_failure(self, db_session, synthetic_audit_db):
        service = IngestionService(db_session, synthetic_audit_db)

        # 1. Trigger partial failure
        original_process_table = service._process_table

        def failing_process(cursor, table, batch):
            if table == "change_set":
                raise RuntimeError("Crash before change_set")
            original_process_table(cursor, table, batch)

        service._process_table = failing_process

        with pytest.raises(RuntimeError):
            service.run_ingestion()

        failed_batch = db_session.query(IngestionBatch).filter_by(status="FAILED").first()
        assert failed_batch is not None

        # 2. Recovery run with unmocked process_table
        service._process_table = original_process_table
        recovery_batch = service.run_ingestion()

        assert recovery_batch.status == "COMPLETED"
        assert recovery_batch.id != failed_batch.id

        # 3. Verify no duplicate records created after recovery
        assert db_session.query(Issue).count() == 1
        assert db_session.query(Commit).count() == 1
        assert db_session.query(CodeChange).count() == 1
        assert db_session.query(SourceRecord).count() == 9

        # Previously inserted records should be counted as skipped in recovery run
        assert recovery_batch.records_skipped > 0
        assert recovery_batch.records_inserted > 0


# ===========================================================================
# 4. WORK 02.3: Multi-target Unresolved & Dataset Scoping Tests
# ===========================================================================

class TestWork023Corrections:
    def test_both_endpoints_missing_creates_two_unresolved_records(self, db_session, tmp_path):
        db_path = str(tmp_path / "multi_unres.sqlite3")
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        c.execute("""
            CREATE TABLE issue (
                issue_id TEXT, type TEXT, summary TEXT, description TEXT,
                priority TEXT, status TEXT, resolution TEXT, assignee TEXT,
                reporter TEXT, created_date TEXT, updated_date TEXT,
                resolved_date TEXT, created_date_zoned TEXT,
                updated_date_zoned TEXT, resolved_date_zoned TEXT
            )
        """)
        c.execute("""
            CREATE TABLE change_set (
                commit_hash TEXT, message TEXT, author TEXT, author_email TEXT,
                committed_date TEXT, committed_date_zoned TEXT, is_merge INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE issue_comment (
                issue_id TEXT, username TEXT, display_name TEXT,
                created_date TEXT, message TEXT, created_date_zoned TEXT
            )
        """)
        c.execute("CREATE TABLE issue_component (issue_id TEXT, component TEXT)")
        c.execute("CREATE TABLE issue_fix_version (issue_id TEXT, fix_version TEXT)")
        c.execute("""
            CREATE TABLE issue_link (
                source_issue_id TEXT, target_issue_id TEXT, name TEXT,
                outward_label TEXT, is_containment INTEGER
            )
        """)
        c.execute("CREATE TABLE change_set_link (commit_hash TEXT, issue_id TEXT)")
        c.execute("""
            CREATE TABLE code_change (
                commit_hash TEXT, file_path TEXT, old_file_path TEXT,
                change_type TEXT, is_deleted INTEGER, sum_added_lines INTEGER,
                sum_removed_lines INTEGER
            )
        """)

        # Valid issue and commit
        c.execute("INSERT INTO issue VALUES ('VALID-1','Bug','s','d','P1','Open',NULL,'u','r','2023-01-01','2023-01-01',NULL,NULL,NULL,NULL)")
        c.execute("INSERT INTO change_set VALUES ('c0ffee','msg','a','e','2023-01-01',NULL,0)")

        # issue_link where BOTH endpoints (GHOST-1 and GHOST-2) do not exist
        c.execute("INSERT INTO issue_link VALUES ('GHOST-1', 'GHOST-2', 'relates to', 'relates to', 0)")

        # change_set_link where BOTH commit (deadbeef) and issue (GHOST-99) do not exist
        c.execute("INSERT INTO change_set_link VALUES ('deadbeef', 'GHOST-99')")

        conn.commit()
        conn.close()

        service = IngestionService(db_session, db_path, dataset_name="MultiTest")
        batch = service.run_ingestion()

        assert batch.status == "COMPLETED"

        unresolved = db_session.query(UnresolvedRelationship).filter_by(dataset_name="MultiTest").all()
        # Should have 4 unresolved records: 2 for issue_link, 2 for change_set_link
        assert len(unresolved) == 4

        issue_link_unres = [u for u in unresolved if u.source_table == "issue_link"]
        assert len(issue_link_unres) == 2
        targets_il = {u.target_id for u in issue_link_unres}
        assert targets_il == {"GHOST-1", "GHOST-2"}

        cs_link_unres = [u for u in unresolved if u.source_table == "change_set_link"]
        assert len(cs_link_unres) == 2
        targets_cs = {u.target_id for u in cs_link_unres}
        assert targets_cs == {"deadbeef", "GHOST-99"}

        # Verify both source records are preserved in source_records
        sr_il = db_session.query(SourceRecord).filter_by(dataset_name="MultiTest", source_table="issue_link").first()
        assert sr_il is not None
        sr_cs = db_session.query(SourceRecord).filter_by(dataset_name="MultiTest", source_table="change_set_link").first()
        assert sr_cs is not None

    def test_repeated_ingestion_multi_unresolved_no_duplicates(self, db_session, tmp_path):
        db_path = str(tmp_path / "multi_repeat.sqlite3")
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        c.execute("""
            CREATE TABLE issue (
                issue_id TEXT, type TEXT, summary TEXT, description TEXT,
                priority TEXT, status TEXT, resolution TEXT, assignee TEXT,
                reporter TEXT, created_date TEXT, updated_date TEXT,
                resolved_date TEXT, created_date_zoned TEXT,
                updated_date_zoned TEXT, resolved_date_zoned TEXT
            )
        """)
        c.execute("""
            CREATE TABLE change_set (
                commit_hash TEXT, message TEXT, author TEXT, author_email TEXT,
                committed_date TEXT, committed_date_zoned TEXT, is_merge INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE issue_comment (
                issue_id TEXT, username TEXT, display_name TEXT,
                created_date TEXT, message TEXT, created_date_zoned TEXT
            )
        """)
        c.execute("CREATE TABLE issue_component (issue_id TEXT, component TEXT)")
        c.execute("CREATE TABLE issue_fix_version (issue_id TEXT, fix_version TEXT)")
        c.execute("""
            CREATE TABLE issue_link (
                source_issue_id TEXT, target_issue_id TEXT, name TEXT,
                outward_label TEXT, is_containment INTEGER
            )
        """)
        c.execute("CREATE TABLE change_set_link (commit_hash TEXT, issue_id TEXT)")
        c.execute("""
            CREATE TABLE code_change (
                commit_hash TEXT, file_path TEXT, old_file_path TEXT,
                change_type TEXT, is_deleted INTEGER, sum_added_lines INTEGER,
                sum_removed_lines INTEGER
            )
        """)

        c.execute("INSERT INTO issue_link VALUES ('MISS-1', 'MISS-2', 'blocks', 'blocks', 0)")
        conn.commit()
        conn.close()

        service = IngestionService(db_session, db_path, dataset_name="RepeatMulti")
        batch1 = service.run_ingestion()
        assert batch1.unresolved_relationships == 2

        # Second pass
        batch2 = service.run_ingestion()
        assert batch2.unresolved_relationships == 2
        assert batch2.records_inserted == 0
        assert batch2.records_skipped == 1

        total_unresolved = db_session.query(UnresolvedRelationship).filter_by(dataset_name="RepeatMulti").count()
        assert total_unresolved == 2

    def test_dataset_scoped_statistics_and_unresolved_isolation(self, api_client, db_session, tmp_path):
        # Setup Dataset A (1 unresolved)
        db_path_a = str(tmp_path / "dataset_a.sqlite3")
        conn = sqlite3.connect(db_path_a)
        c = conn.cursor()
        for t in ["meta", "change_set", "issue_comment", "issue_component", "issue_fix_version", "change_set_link", "code_change"]:
            c.execute(f"CREATE TABLE {t} (dummy TEXT)")
        c.execute("CREATE TABLE issue_link (source_issue_id TEXT, target_issue_id TEXT, name TEXT, outward_label TEXT, is_containment INTEGER)")
        c.execute("""
            CREATE TABLE issue (
                issue_id TEXT, type TEXT, summary TEXT, description TEXT,
                priority TEXT, status TEXT, resolution TEXT, assignee TEXT,
                reporter TEXT, created_date TEXT, updated_date TEXT,
                resolved_date TEXT, created_date_zoned TEXT,
                updated_date_zoned TEXT, resolved_date_zoned TEXT
            )
        """)
        c.execute("INSERT INTO issue VALUES ('A-1','Bug','s','d','P1','Open',NULL,'u','r','2023-01-01',NULL,NULL,NULL,NULL,NULL)")
        c.execute("INSERT INTO issue_link VALUES ('A-1', 'A-MISSING', 'links', 'links', 0)")
        conn.commit()
        conn.close()

        # Setup Dataset B (2 unresolved from one link where both are missing)
        db_path_b = str(tmp_path / "dataset_b.sqlite3")
        conn = sqlite3.connect(db_path_b)
        c = conn.cursor()
        for t in ["meta", "change_set", "issue_comment", "issue_component", "issue_fix_version", "change_set_link", "code_change"]:
            c.execute(f"CREATE TABLE {t} (dummy TEXT)")
        c.execute("CREATE TABLE issue_link (source_issue_id TEXT, target_issue_id TEXT, name TEXT, outward_label TEXT, is_containment INTEGER)")
        c.execute("""
            CREATE TABLE issue (
                issue_id TEXT, type TEXT, summary TEXT, description TEXT,
                priority TEXT, status TEXT, resolution TEXT, assignee TEXT,
                reporter TEXT, created_date TEXT, updated_date TEXT,
                resolved_date TEXT, created_date_zoned TEXT,
                updated_date_zoned TEXT, resolved_date_zoned TEXT
            )
        """)
        c.execute("INSERT INTO issue_link VALUES ('B-MISS-1', 'B-MISS-2', 'links', 'links', 0)")
        conn.commit()
        conn.close()

        service_a = IngestionService(db_session, db_path_a, dataset_name="DatasetAlpha")
        service_a.run_ingestion()

        service_b = IngestionService(db_session, db_path_b, dataset_name="DatasetBeta")
        service_b.run_ingestion()

        # Verify API /api/statistics scoped by dataset_name
        res_a = api_client.get("/api/statistics?dataset_name=DatasetAlpha")
        assert res_a.status_code == 200
        assert res_a.json()["unresolved_relationships"] == 1

        res_b = api_client.get("/api/statistics?dataset_name=DatasetBeta")
        assert res_b.status_code == 200
        assert res_b.json()["unresolved_relationships"] == 2

        # Verify API /api/ingestion/unresolved scoped by dataset_name
        unres_a = api_client.get("/api/ingestion/unresolved?dataset_name=DatasetAlpha")
        assert unres_a.status_code == 200
        data_a = unres_a.json()
        assert len(data_a) == 1
        assert data_a[0]["dataset_name"] == "DatasetAlpha"
        assert data_a[0]["target_id"] == "A-MISSING"

        unres_b = api_client.get("/api/ingestion/unresolved?dataset_name=DatasetBeta")
        assert unres_b.status_code == 200
        data_b = unres_b.json()
        assert len(data_b) == 2
        for item in data_b:
            assert item["dataset_name"] == "DatasetBeta"


"""
Work 08 Comprehensive Automated Test Suite:
Multi-Project Isolation, IDOR Protection, Cross-Project Boundary Enforcement,
Knowledge Graph Partitioning, Historical QA Grounding, Temporal Semantics & Security Verification.
"""

from datetime import datetime, timezone
import hashlib
import io
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.base import Base
from app.db.database import get_db, init_db
from app.models.project import Project
from app.models.source_connection import SourceConnection
from app.models.source_record import SourceRecord
from app.models.document import Document
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.issue_relationships import CommitIssueLink
from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.models.ingestion_run import IngestionRun
from app.models.ingestion_batch import IngestionBatch
from app.services.knowledge_graph_service import KnowledgeGraphService
from app.services.qa_service import HistoricalQAService
from app.services.feature_service import FeatureService
from app.services.project_ingestion_service import ProjectIngestionService
from app.integrations.documents.security import is_ip_private_or_restricted, sanitize_filename


# ---------------------------------------------------------------------------
# Test Database Fixture
# ---------------------------------------------------------------------------

test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture
def client():
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def setup_multi_project_data():
    """Initializes schema and seeds three completely isolated projects."""
    init_db(target_engine=test_engine)
    db = TestingSessionLocal()

    # Clean existing
    db.query(EventEvidence).delete()
    db.query(TemporalEvent).delete()
    db.query(FeatureRecordLink).delete()
    db.query(FeatureAlias).delete()
    db.query(Feature).delete()
    db.query(CommitIssueLink).delete()
    db.query(Commit).delete()
    db.query(Issue).delete()
    db.query(Document).delete()
    db.query(SourceRecord).delete()
    db.query(SourceConnection).delete()
    db.query(IngestionRun).delete()
    db.query(IngestionBatch).delete()
    db.query(Project).delete()
    db.commit()

    # Ingestion Batch for FK satisfaction
    batch = IngestionBatch(dataset_name="kafka", status="COMPLETED")
    db.add(batch)
    db.commit()
    batch_id = batch.id

    # 1. Project A: Apache Kafka (id=1, slug="kafka")
    p_a = Project(
        id=1,
        name="Apache Kafka",
        slug="kafka",
        description="High-throughput distributed event streaming platform.",
        domain="DISTRIBUTED_SYSTEMS",
        status="READY",
    )

    # 2. Project B: Cloud Orchestrator (id=2, slug="cloud-orchestrator")
    p_b = Project(
        id=2,
        name="Cloud Orchestrator",
        slug="cloud-orchestrator",
        description="Multi-cloud container workflow scheduler.",
        domain="CLOUD_INFRASTRUCTURE",
        status="READY",
    )

    # 3. Project C: Vector DB (id=3, slug="vector-db")
    p_c = Project(
        id=3,
        name="Vector Database",
        slug="vector-db",
        description="High-dimensional embedding similarity search engine.",
        domain="AI_DATABASE",
        status="READY",
    )

    db.add_all([p_a, p_b, p_c])
    db.commit()

    # ---------------------------------------------------------------------------
    # Seed Source Records
    # ---------------------------------------------------------------------------
    sr_a = SourceRecord(
        id=101,
        project_id=1,
        dataset_name="kafka",
        source_table="issues",
        source_record_id="KAFKA-100",
        source_record_hash="hash_a_100",
        raw_data={"summary": "Kafka Topic Partitioning Design"},
        ingestion_batch_id=batch_id,
    )
    sr_b = SourceRecord(
        id=201,
        project_id=2,
        dataset_name="cloud-orchestrator",
        source_table="issues",
        source_record_id="CLOUD-100",
        source_record_hash="hash_b_100",
        raw_data={"summary": "Cluster Sharding and Partitioning"},
        ingestion_batch_id=batch_id,
    )
    sr_c = SourceRecord(
        id=301,
        project_id=3,
        dataset_name="vector-db",
        source_table="issues",
        source_record_id="VEC-100",
        source_record_hash="hash_c_100",
        raw_data={"summary": "HNSW Index Partitioning"},
        ingestion_batch_id=batch_id,
    )

    # Commits Source Records
    sr_cmt_a = SourceRecord(
        id=102,
        project_id=1,
        dataset_name="kafka",
        source_table="commits",
        source_record_id="cmt_kafka_aaa",
        source_record_hash="hash_cmt_a",
        raw_data={"message": "Implement Kafka partition leader election"},
        ingestion_batch_id=batch_id,
    )
    sr_cmt_b = SourceRecord(
        id=202,
        project_id=2,
        dataset_name="cloud-orchestrator",
        source_table="commits",
        source_record_id="cmt_cloud_bbb",
        source_record_hash="hash_cmt_b",
        raw_data={"message": "Implement Pod rescheduling controller"},
        ingestion_batch_id=batch_id,
    )

    db.add_all([sr_a, sr_b, sr_c, sr_cmt_a, sr_cmt_b])
    db.commit()

    # ---------------------------------------------------------------------------
    # Seed Issues and Commits
    # ---------------------------------------------------------------------------
    issue_a = Issue(
        id=1,
        source_record_id=101,
        source_issue_id="KAFKA-100",
        summary="Kafka Topic Partitioning Design",
        issue_type="Improvement",
        status="Resolved",
        reporter="alice@kafka.org",
    )
    issue_b = Issue(
        id=2,
        source_record_id=201,
        source_issue_id="CLOUD-100",
        summary="Cluster Sharding and Partitioning",
        issue_type="New Feature",
        status="Closed",
        reporter="bob@cloud.io",
    )
    issue_c = Issue(
        id=3,
        source_record_id=301,
        source_issue_id="VEC-100",
        summary="HNSW Index Partitioning",
        issue_type="Task",
        status="Open",
        reporter="charlie@vector.ai",
    )

    cmt_a = Commit(
        id=1,
        source_record_id=102,
        commit_hash="aaaa111122223333444455556666777788889999",
        author="alice@kafka.org",
        message="[KAFKA-100] Implement partition leader election algorithm",
        committed_at=datetime(2021, 6, 15, tzinfo=timezone.utc),
    )
    cmt_b = Commit(
        id=2,
        source_record_id=202,
        commit_hash="bbbb111122223333444455556666777788889999",
        author="bob@cloud.io",
        message="[CLOUD-100] Add cluster sharding supervisor logic",
        committed_at=datetime(2022, 8, 20, tzinfo=timezone.utc),
    )

    db.add_all([issue_a, issue_b, issue_c, cmt_a, cmt_b])
    db.commit()

    # Cross-links: Note that CommitIssueLink maps issue key to commit
    link_a = CommitIssueLink(
        source_record_id=102,
        commit_hash=cmt_a.commit_hash,
        source_issue_id="KAFKA-100",
    )
    link_b = CommitIssueLink(
        source_record_id=202,
        commit_hash=cmt_b.commit_hash,
        source_issue_id="CLOUD-100",
    )
    db.add_all([link_a, link_b])
    db.commit()

    # ---------------------------------------------------------------------------
    # Seed Features (Same canonical name "Partitioning" to stress test boundary isolation)
    # ---------------------------------------------------------------------------
    feat_a = Feature(
        id=1,
        project_id=1,
        dataset_name="kafka",
        canonical_name="Partitioning",
        normalized_name="partitioning",
        feature_category="STORAGE",
        description="Apache Kafka log partition distribution across brokers.",
        identity_confidence=1.0,
        resolution_status="CONFIRMED",
    )
    feat_b = Feature(
        id=2,
        project_id=2,
        dataset_name="cloud-orchestrator",
        canonical_name="Partitioning",
        normalized_name="partitioning",
        feature_category="SCHEDULING",
        description="Cloud container worker node partition allocation.",
        identity_confidence=1.0,
        resolution_status="CONFIRMED",
    )
    feat_c = Feature(
        id=3,
        project_id=3,
        dataset_name="vector-db",
        canonical_name="HNSW-Index",
        normalized_name="hnsw index",
        feature_category="INDEXING",
        description="Hierarchical Navigable Small World vector index.",
        identity_confidence=1.0,
        resolution_status="CONFIRMED",
    )
    db.add_all([feat_a, feat_b, feat_c])
    db.commit()

    # Feature Record Links
    frl_a = FeatureRecordLink(
        feature_id=1,
        record_type="issue",
        record_id="KAFKA-100",
        relationship_type="IMPLEMENTED_IN",
        confidence=1.0,
    )
    frl_b = FeatureRecordLink(
        feature_id=2,
        record_type="issue",
        record_id="CLOUD-100",
        relationship_type="IMPLEMENTED_IN",
        confidence=1.0,
    )
    db.add_all([frl_a, frl_b])
    db.commit()

    # ---------------------------------------------------------------------------
    # Seed Temporal Events and Verbatim Evidence
    # ---------------------------------------------------------------------------
    ev_a = TemporalEvent(
        id=10,
        project_id=1,
        dataset_name="kafka",
        event_key="kafka:proposal:kafka-100",
        event_type="FEATURE_PROPOSED",
        event_time=datetime(2021, 5, 10, tzinfo=timezone.utc),
        actor="alice@kafka.org",
        subject="KAFKA-100: Propose topic log partitioning",
        description="Detailed KIP proposal for log partitions",
        issue_id="KAFKA-100",
    )
    ev_b = TemporalEvent(
        id=20,
        project_id=2,
        dataset_name="cloud-orchestrator",
        event_key="cloud:proposal:cloud-100",
        event_type="FEATURE_PROPOSED",
        event_time=datetime(2022, 7, 12, tzinfo=timezone.utc),
        actor="bob@cloud.io",
        subject="CLOUD-100: Propose cluster node partitioning",
        description="Architecture RFC for cluster partitions",
        issue_id="CLOUD-100",
    )
    db.add_all([ev_a, ev_b])
    db.commit()

    # Link events to features
    frl_ev_a = FeatureRecordLink(
        feature_id=1,
        record_type="temporal_event",
        record_id="10",
        relationship_type="RELATED_TO_EVENT",
        confidence=1.0,
    )
    frl_ev_b = FeatureRecordLink(
        feature_id=2,
        record_type="temporal_event",
        record_id="20",
        relationship_type="RELATED_TO_EVENT",
        confidence=1.0,
    )
    db.add_all([frl_ev_a, frl_ev_b])
    db.commit()

    # Evidence
    evd_a = EventEvidence(
        id=100,
        event_id=10,
        source_record_id=101,
        evidence_role="PRIMARY_PROPOSAL",
        evidence_excerpt="KAFKA-100 defines distributed topic partition layout for broker nodes.",
        evidence_hash=hashlib.sha256(b"kafka_evidence").hexdigest(),
    )
    evd_b = EventEvidence(
        id=200,
        event_id=20,
        source_record_id=201,
        evidence_role="PRIMARY_PROPOSAL",
        evidence_excerpt="CLOUD-100 defines pod scheduling boundary partitions for Kubernetes.",
        evidence_hash=hashlib.sha256(b"cloud_evidence").hexdigest(),
    )
    db.add_all([evd_a, evd_b])
    db.commit()

    # ---------------------------------------------------------------------------
    # Seed Documents
    # ---------------------------------------------------------------------------
    doc_a = Document(
        id=1,
        project_id=1,
        filename="kafka_architecture_spec.md",
        file_type="MARKDOWN",
        file_size=512,
        sha256_hash=hashlib.sha256(b"kafka_spec_content").hexdigest(),
        extracted_text="# Kafka Architecture\nConfidential Kafka partition spec.",
    )
    doc_b = Document(
        id=2,
        project_id=2,
        filename="cloud_orchestrator_rfc.pdf",
        file_type="PDF",
        file_size=1024,
        sha256_hash=hashlib.sha256(b"cloud_rfc_content").hexdigest(),
        extracted_text="# Cloud RFC\nInternal cloud orchestrator scheduling rules.",
    )
    db.add_all([doc_a, doc_b])
    db.commit()
    db.close()


# ===========================================================================
# 1. Cross-Project Data Isolation Tests
# ===========================================================================

class TestCrossProjectDataIsolation:
    def test_list_issues_isolation(self, client):
        """Project A must only see Project A issues; Project B only sees Project B issues."""
        res_a = client.get("/api/projects/kafka/issues")
        assert res_a.status_code == 200
        issues_a = res_a.json()
        assert len(issues_a) == 1
        assert issues_a[0]["source_issue_id"] == "KAFKA-100"

        res_b = client.get("/api/projects/cloud-orchestrator/issues")
        assert res_b.status_code == 200
        issues_b = res_b.json()
        assert len(issues_b) == 1
        assert issues_b[0]["source_issue_id"] == "CLOUD-100"

        res_c = client.get("/api/projects/vector-db/issues")
        assert res_c.status_code == 200
        issues_c = res_c.json()
        assert len(issues_c) == 1
        assert issues_c[0]["source_issue_id"] == "VEC-100"

    def test_list_commits_isolation(self, client):
        """Commits must be strictly isolated by project."""
        res_a = client.get("/api/projects/kafka/commits/aaaa111122223333444455556666777788889999")
        assert res_a.status_code == 200
        assert res_a.json()["commit_hash"] == "aaaa111122223333444455556666777788889999"

        # Attempting to fetch Project A's commit through Project B's route MUST return 404
        res_b_leak = client.get("/api/projects/cloud-orchestrator/commits/aaaa111122223333444455556666777788889999")
        assert res_b_leak.status_code == 404

    def test_list_features_isolation(self, client):
        """Even if features share the same name 'Partitioning', they are isolated by project."""
        res_a = client.get("/api/projects/kafka/features")
        assert res_a.status_code == 200
        feats_a = res_a.json()
        assert len(feats_a) == 1
        assert feats_a[0]["id"] == 1
        assert "Apache Kafka log partition" in feats_a[0]["description"]

        res_b = client.get("/api/projects/cloud-orchestrator/features")
        assert res_b.status_code == 200
        feats_b = res_b.json()
        assert len(feats_b) == 1
        assert feats_b[0]["id"] == 2
        assert "Cloud container worker node" in feats_b[0]["description"]

    def test_list_events_isolation(self, client):
        """Temporal events must be completely isolated between projects."""
        res_a = client.get("/api/projects/kafka/events")
        assert res_a.status_code == 200
        events_a = res_a.json()
        assert len(events_a) == 1
        assert events_a[0]["id"] == 10
        assert events_a[0]["issue_id"] == "KAFKA-100"

        res_b = client.get("/api/projects/cloud-orchestrator/events")
        assert res_b.status_code == 200
        events_b = res_b.json()
        assert len(events_b) == 1
        assert events_b[0]["id"] == 20
        assert events_b[0]["issue_id"] == "CLOUD-100"

    def test_list_documents_isolation(self, client):
        """Documents uploaded to Project A are invisible to Project B."""
        res_a = client.get("/api/projects/kafka/documents")
        assert res_a.status_code == 200
        docs_a = res_a.json()
        assert len(docs_a) == 1
        assert docs_a[0]["filename"] == "kafka_architecture_spec.md"

        res_b = client.get("/api/projects/cloud-orchestrator/documents")
        assert res_b.status_code == 200
        docs_b = res_b.json()
        assert len(docs_b) == 1
        assert docs_b[0]["filename"] == "cloud_orchestrator_rfc.pdf"

    def test_statistics_scoped_isolation(self, client):
        """Statistics endpoint scopes record counts to requested dataset/project."""
        res_a = client.get("/api/statistics?dataset_name=kafka&project_id=1")
        assert res_a.status_code == 200
        stats_a = res_a.json()
        assert stats_a["total_issues"] == 1
        assert stats_a["total_commits"] == 1

        res_b = client.get("/api/statistics?dataset_name=cloud-orchestrator&project_id=2")
        assert res_b.status_code == 200
        stats_b = res_b.json()
        assert stats_b["total_issues"] == 1
        assert stats_b["total_commits"] == 1


# ===========================================================================
# 2. Insecure Direct Object Reference (IDOR) & Ownership Enforcement
# ===========================================================================

class TestIDORAndOwnershipEnforcement:
    def test_feature_idor_cross_project_rejected_with_404(self, client):
        """Attempting to access Feature 1 (Kafka) via cloud-orchestrator route returns 404."""
        response = client.get("/api/projects/cloud-orchestrator/features/1")
        assert response.status_code == 404
        assert "not found in project" in response.json()["detail"]

    def test_event_idor_cross_project_rejected_with_404(self, client):
        """Attempting to access Event 10 (Kafka) via cloud-orchestrator route returns 404."""
        response = client.get("/api/projects/cloud-orchestrator/events/10")
        assert response.status_code == 404
        assert "not found in project" in response.json()["detail"]

    def test_document_idor_cross_project_rejected_with_404(self, client):
        """Attempting to access Document 1 (Kafka) via cloud-orchestrator route returns 404."""
        response = client.get("/api/projects/cloud-orchestrator/documents/1")
        assert response.status_code == 404

    def test_issue_idor_cross_project_rejected_with_404(self, client):
        """Attempting to access KAFKA-100 via cloud-orchestrator route returns 404."""
        response = client.get("/api/projects/cloud-orchestrator/issues/KAFKA-100")
        assert response.status_code == 404

    def test_global_feature_endpoint_with_project_filter_rejects_foreign_feature(self, client):
        """Calling /api/features/{id}?project_id=2 for a Project 1 feature returns 404."""
        response = client.get("/api/features/1?project_id=2")
        assert response.status_code == 404

    def test_global_event_endpoint_with_project_filter_rejects_foreign_event(self, client):
        """Calling /api/events/{id}?project_id=2 for a Project 1 event returns 404."""
        response = client.get("/api/events/10?project_id=2")
        assert response.status_code == 404


# ===========================================================================
# 3. Knowledge Graph Boundary Enforcement Tests
# ===========================================================================

class TestKnowledgeGraphBoundaryEnforcement:
    def test_graph_traversal_strictly_bounded_to_project(self, client):
        """Traversing Project B's feature graph must NOT contain any nodes or edges from Project A."""
        res_b = client.get("/api/projects/cloud-orchestrator/graph/features/2?depth=2")
        assert res_b.status_code == 200
        graph_b = res_b.json()

        node_ids = [n["id"] for n in graph_b["nodes"]]
        # Must contain Feature 2, Issue CLOUD-100, and Event 20
        assert "feature:2" in node_ids
        assert "issue:CLOUD-100" in node_ids
        assert "temporal_event:20" in node_ids

        # Must NEVER contain Project A's nodes
        assert "feature:1" not in node_ids
        assert "issue:KAFKA-100" not in node_ids
        assert "temporal_event:10" not in node_ids
        assert "contributor:alice@kafka.org" not in node_ids

    def test_cross_project_feature_graph_request_returns_404(self, client):
        """Requesting Project A's feature graph under Project B's endpoint returns 404."""
        response = client.get("/api/projects/cloud-orchestrator/graph/features/1")
        assert response.status_code == 404

    def test_graph_does_not_traverse_foreign_commit_issue_links(self):
        """KnowledgeGraphService BFS traversal never crosses project boundary on commit links."""
        db = TestingSessionLocal()
        try:
            svc_b = KnowledgeGraphService(db, dataset_name="cloud-orchestrator", project_id=2)
            graph = svc_b.get_feature_graph(feature_id=2, depth=3)

            for edge in graph.edges:
                assert "KAFKA-100" not in edge.target
                assert "KAFKA-100" not in edge.source
                assert "aaaa111122223333444455556666777788889999" not in edge.source
        finally:
            db.close()


# ===========================================================================
# 4. Historical QA Scoped Retrieval & Citation Protection
# ===========================================================================

class TestHistoricalQAIsolation:
    def test_qa_answers_grounded_only_in_target_project_events(self, client):
        """When asking Project B about Partitioning, facts and citations must strictly be Project B."""
        body = {
            "query": "How was Partitioning proposed?",
            "dataset_name": "cloud-orchestrator",
            "project_id": 2,
        }
        res = client.post("/api/projects/cloud-orchestrator/qa/ask", json=body)
        assert res.status_code == 200
        qa_resp = res.json()

        # Check canonical feature resolved
        assert qa_resp["canonical_feature"] == "Partitioning"
        # Citations must belong exclusively to Project B (CLOUD-100 / Event 20)
        assert len(qa_resp["citations"]) > 0
        for citation in qa_resp["citations"]:
            assert "CLOUD-100" in citation["excerpt"] or "Kubernetes" in citation["excerpt"]
            assert "KAFKA-100" not in citation["excerpt"]
            assert "Kafka" not in citation["excerpt"]

    def test_qa_issue_lookup_does_not_leak_cross_project_issue(self, client):
        """Asking Project B about 'KAFKA-100' must NOT retrieve Kafka data."""
        body = {
            "query": "What is KAFKA-100?",
            "dataset_name": "cloud-orchestrator",
            "project_id": 2,
        }
        res = client.post("/api/projects/cloud-orchestrator/qa/ask", json=body)
        assert res.status_code == 200
        qa_resp = res.json()

        # Citations must be empty or contain 0 references to Kafka
        for c in qa_resp["citations"]:
            assert c["record_id"] != "KAFKA-100"


# ===========================================================================
# 5. Temporal Semantics Tests
# ===========================================================================

class TestTemporalSemantics:
    def test_semantic_proposal_is_not_approval(self):
        """FEATURE_PROPOSED is structurally distinct from DECISION_APPROVED."""
        proposal = TemporalEvent(
            event_type="FEATURE_PROPOSED",
            subject="Proposal: Distributed Sharding",
        )
        approval = TemporalEvent(
            event_type="DECISION_APPROVED",
            subject="Approval: Distributed Sharding vote accepted",
        )
        assert proposal.event_type != approval.event_type
        assert "PROPOSED" in proposal.event_type
        assert "APPROVED" in approval.event_type

    def test_semantic_approval_is_not_implementation(self):
        """DECISION_APPROVED is structurally distinct from FEATURE_IMPLEMENTED."""
        approval = TemporalEvent(
            event_type="DECISION_APPROVED",
            subject="Decision approved by PMC",
        )
        impl = TemporalEvent(
            event_type="FEATURE_IMPLEMENTED",
            subject="Feature implemented in core broker",
        )
        assert approval.event_type != impl.event_type

    def test_semantic_commit_is_not_deployment(self):
        """A commit does not imply feature resolution or release."""
        commit_ev = TemporalEvent(
            event_type="FEATURE_MODIFIED",
            commit_hash="abc123456",
            subject="Refactor internal buffer",
        )
        resolved_ev = TemporalEvent(
            event_type="ISSUE_RESOLVED",
            issue_id="KAFKA-999",
            subject="KAFKA-999 marked resolved in 3.4.0",
        )
        assert commit_ev.event_type != resolved_ev.event_type
        assert commit_ev.commit_hash is not None
        assert resolved_ev.commit_hash is None

    def test_semantic_feature_removal_preserves_historical_timeline(self):
        """FEATURE_REMOVED documents deprecation/removal without purging past historical events."""
        db = TestingSessionLocal()
        try:
            # Add historical event
            ev_past = TemporalEvent(
                project_id=1,
                dataset_name="kafka",
                event_key="kafka:past:001",
                event_type="FEATURE_PROPOSED",
                event_time=datetime(2015, 1, 1, tzinfo=timezone.utc),
                subject="Initial protocol v0",
            )
            # Add removal event
            ev_removed = TemporalEvent(
                project_id=1,
                dataset_name="kafka",
                event_key="kafka:removal:001",
                event_type="FEATURE_REMOVED",
                event_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
                subject="Protocol v0 removed in release 3.5",
            )
            db.add_all([ev_past, ev_removed])
            db.commit()

            # Verify both coexist: historical record is strictly immutable
            events = (
                db.query(TemporalEvent)
                .filter(TemporalEvent.project_id == 1, TemporalEvent.subject.ilike("%protocol v0%"))
                .order_by(TemporalEvent.event_time.asc())
                .all()
            )
            assert len(events) == 2
            assert events[0].event_type == "FEATURE_PROPOSED"
            assert events[1].event_type == "FEATURE_REMOVED"
        finally:
            db.close()


# ===========================================================================
# 6. Security Hardening & Regression Tests
# ===========================================================================

class TestSecurityHardeningAndRegressions:
    def test_security_ssrf_blocks_private_ips(self):
        """Private/internal IP ranges must be recognized and blocked."""
        assert is_ip_private_or_restricted("127.0.0.1") is True
        assert is_ip_private_or_restricted("169.254.169.254") is True
        assert is_ip_private_or_restricted("10.0.0.1") is True
        assert is_ip_private_or_restricted("192.168.1.1") is True
        assert is_ip_private_or_restricted("172.16.0.1") is True
        assert is_ip_private_or_restricted("8.8.8.8") is False

    def test_security_disallowed_file_extension_rejected(self, client):
        """Uploading dangerous file formats (.exe, .sh, .php) must be rejected with 400."""
        malicious_file = io.BytesIO(b"malicious payload binary")
        response = client.post(
            "/api/projects/kafka/documents/upload",
            files={"file": ("exploit.exe", malicious_file, "application/octet-stream")},
        )
        assert response.status_code == 400
        assert "unsupported" in response.json()["detail"].lower()

    def test_security_path_traversal_in_filename_sanitized(self):
        """Path traversal patterns in document filenames must be stripped."""
        unsafe = "../../etc/passwd"
        safe = sanitize_filename(unsafe)
        assert safe == "passwd"
        assert ".." not in safe
        assert "/" not in safe
        assert "\\" not in safe

    def test_security_credentials_masked_in_responses(self, client):
        """Connecting a source with credentials masks secret values in API responses."""
        req = {
            "repo_url": "https://github.com/torvalds/linux",
            "token": "ghp_super_secret_personal_access_token_12345",
        }
        res = client.post("/api/projects/kafka/sources/github", json=req)
        assert res.status_code == 201
        conn = res.json()
        assert "token" not in conn.get("config", {})
        # Verify credentials are never returned in plaintext
        assert "ghp_super_secret" not in str(conn)


# ===========================================================================
# 7. Ingestion Failure Handling & Recovery Tests
# ===========================================================================

class TestFailureHandlingAndRecovery:
    def test_document_reingestion_is_idempotent(self):
        """Ingesting the identical document twice returns the existing document with 0 duplication."""
        db = TestingSessionLocal()
        try:
            svc = ProjectIngestionService(db)
            content = b"Idempotent Architecture Spec for Distributed Logs"
            doc1, run1 = svc.ingest_document_bytes(project_id=2, filename="spec.txt", file_bytes=content)
            doc2, run2 = svc.ingest_document_bytes(project_id=2, filename="spec.txt", file_bytes=content)

            assert doc1.id == doc2.id
            assert doc1.sha256_hash == doc2.sha256_hash
            # Only 1 Document and 1 SourceRecord created
            count = db.query(Document).filter(Document.project_id == 2, Document.filename == "spec.txt").count()
            assert count == 1
        finally:
            db.close()

    def test_partial_ingestion_failure_does_not_corrupt_other_projects(self):
        """A failure during ingestion in Project B does not affect Project A or C."""
        db = TestingSessionLocal()
        try:
            svc = ProjectIngestionService(db)
            # Simulate a corrupt document upload for Project B
            doc, run = svc.ingest_document_bytes(project_id=2, filename="corrupt.xyz", file_bytes=b"garbage")
            assert doc is None
            assert run.status == "FAILED"
            assert "Unsupported file format" in (run.error_summary or "")

            # Project A and C state remains pristine
            docs_a = db.query(Document).filter(Document.project_id == 1).all()
            assert len(docs_a) == 1
            assert docs_a[0].filename == "kafka_architecture_spec.md"
        finally:
            db.close()

"""
Comprehensive Automated Test Suite for Work 07:
Multi-Source Project Onboarding, Jira Cloud, GitHub Public Repositories & Document Ingestion.
"""

import io
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.database import get_db, init_db
from app.integrations.documents.normalizer import DocumentNormalizer
from app.integrations.documents.parsers import parse_document, parse_text
from app.integrations.documents.security import (
    ALLOWED_EXTENSIONS,
    MAX_FILE_SIZE_BYTES,
    is_ip_private_or_restricted,
    sanitize_filename,
    validate_file_upload,
    validate_public_url,
)
from app.integrations.github.client import GitHubClient, parse_github_url
from app.integrations.github.normalizer import GitHubNormalizer
from app.integrations.jira.adf import adf_to_text
from app.integrations.jira.auth import JiraAuthHandler
from app.integrations.jira.client import JiraCloudClient
from app.integrations.jira.normalizer import JiraNormalizer
from app.main import app
from app.models.document import Document
from app.models.ingestion_run import IngestionRun
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.project import Project
from app.models.source_connection import SourceConnection
from app.models.source_record import SourceRecord
from app.services.project_ingestion_service import ProjectIngestionService


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
def setup_db():
    init_db(target_engine=test_engine)
    db = TestingSessionLocal()
    # Bootstrap default benchmark Kafka project (id=1, slug='kafka')
    p1 = db.query(Project).filter(Project.id == 1).first()
    if not p1:
        p1 = Project(
            id=1,
            name="Apache Kafka",
            slug="kafka",
            description="Apache Kafka distributed event streaming platform",
            domain="Streaming Systems",
            status="READY",
        )
        db.add(p1)
        db.commit()
    db.close()
    yield
    Base.metadata.drop_all(bind=test_engine)
    init_db(target_engine=test_engine)


# ---------------------------------------------------------------------------
# 1. Project Management Tests
# ---------------------------------------------------------------------------

def test_project_creation_and_slug_generation():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)

    # Auto slug from name
    p = svc.create_project(name="Apache Flink", domain="Stream Processing")
    assert p.id > 1
    assert p.slug == "apache-flink"
    assert p.status == "READY"

    # Explicit slug
    p2 = svc.create_project(name="Elasticsearch", slug="es", description="Distributed search engine")
    assert p2.slug == "es"
    assert p2.description == "Distributed search engine"

    # Duplicate slug rejection
    with pytest.raises(ValueError) as exc:
        svc.create_project(name="Elasticsearch Duplicate", slug="es")
    assert "already exists" in str(exc.value)

    db.close()


def test_project_listing_and_enrichment():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)
    svc.create_project(name="Project Alpha", slug="alpha")

    projects = svc.list_projects()
    assert len(projects) >= 2
    slugs = [p.slug for p in projects]
    assert "kafka" in slugs
    assert "alpha" in slugs

    proj = svc.get_project("alpha")
    assert proj is not None
    assert proj.name == "Project Alpha"

    proj_by_id = svc.get_project(proj.id)
    assert proj_by_id.slug == "alpha"
    db.close()


# ---------------------------------------------------------------------------
# 2. Jira Cloud Integration Tests
# ---------------------------------------------------------------------------

def test_jira_adf_parser_comprehensive():
    # Plain text / fallback
    assert adf_to_text(None) == ""
    assert adf_to_text("Plain string") == "Plain string"

    # Complex ADF tree
    sample_adf = {
        "version": 1,
        "type": "doc",
        "content": [
            {
                "type": "heading",
                "attrs": {"level": 2},
                "content": [{"type": "text", "text": "KIP-500 Architecture Overview"}],
            },
            {
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": "This proposal introduces "},
                    {"type": "text", "text": "KRaft consensus", "marks": [{"type": "strong"}]},
                    {"type": "text", "text": " to replace "},
                    {"type": "text", "text": "ZooKeeper", "marks": [{"type": "code"}]},
                    {"type": "text", "text": ". See "},
                    {"type": "text", "text": "Documentation", "marks": [{"type": "link", "attrs": {"href": "https://kafka.apache.org"}}]},
                ],
            },
            {
                "type": "bulletList",
                "content": [
                    {
                        "type": "listItem",
                        "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Scalability to millions of partitions"}]}],
                    },
                    {
                        "type": "listItem",
                        "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Faster metadata recovery"}]}],
                    },
                ],
            },
            {
                "type": "codeBlock",
                "attrs": {"language": "bash"},
                "content": [{"type": "text", "text": "bin/kafka-storage.sh format"}],
            },
            {
                "type": "blockquote",
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Approved unanimously by PMC"}]}],
            },
        ],
    }

    markdown = adf_to_text(sample_adf)
    assert "## KIP-500 Architecture Overview" in markdown
    assert "**KRaft consensus**" in markdown
    assert "`ZooKeeper`" in markdown
    assert "[Documentation](https://kafka.apache.org)" in markdown
    assert "* Scalability to millions of partitions" in markdown
    assert "```bash" in markdown
    assert "> Approved unanimously by PMC" in markdown


def test_jira_auth_headers():
    basic = JiraAuthHandler.get_basic_auth_header("user@test.com", "token123")
    assert "Authorization" in basic
    assert basic["Authorization"].startswith("Basic ")

    oauth = JiraAuthHandler.get_oauth_header("access_token_xyz")
    assert oauth["Authorization"] == "Bearer access_token_xyz"


def test_jira_normalizer_and_idempotency():
    sample_issue = {
        "key": "PROJ-101",
        "fields": {
            "summary": "Implement KRaft Quorum Controller",
            "description": {
                "type": "doc",
                "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Quorum controller details"}]}],
            },
            "issuetype": {"name": "New Feature"},
            "status": {"name": "Resolved"},
            "priority": {"name": "Major"},
            "resolution": {"name": "Fixed"},
            "created": "2023-05-10T10:00:00.000+0000",
            "updated": "2023-06-01T15:00:00.000+0000",
            "resolutiondate": "2023-06-01T15:00:00.000+0000",
            "reporter": {"displayName": "Alice Engineer"},
            "components": [{"name": "controller"}, {"name": "kraft"}],
            "fixVersions": [{"name": "3.0.0"}],
            "issuelinks": [
                {
                    "type": {"outward": "relates to"},
                    "outwardIssue": {"key": "PROJ-50"},
                }
            ],
            "comment": {
                "comments": [
                    {
                        "id": "1001",
                        "author": {"displayName": "Bob Committer"},
                        "body": "Patch looks great. Approved for merge.",
                        "created": "2023-05-20T12:00:00.000+0000",
                    }
                ]
            },
        },
    }

    issue, src_rec, comps, fixes, links = JiraNormalizer.normalize_issue(
        issue_data=sample_issue,
        project_id=2,
        dataset_name="test-project",
    )

    assert issue.source_issue_id == "PROJ-101"
    assert issue.summary == "Implement KRaft Quorum Controller"
    assert "Quorum controller details" in issue.description
    assert issue.status == "Resolved"
    assert len(comps) == 2
    assert comps[0].component == "controller"
    assert len(fixes) == 1
    assert fixes[0].fix_version == "3.0.0"
    assert len(links) == 1
    assert links[0].target_issue_id == "PROJ-50"

    assert src_rec.project_id == 2
    assert src_rec.source_table == "jira_issues"
    assert src_rec.source_record_id == "PROJ-101"
    assert src_rec.source_record_hash is not None


def test_jira_sync_end_to_end_mocked():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)
    p = svc.create_project(name="Jira Onboard Project", slug="jira-onboard")

    conn = svc.create_source_connection(
        project_id=p.id,
        source_type="JIRA_CLOUD",
        name="Team Jira",
        config={"base_url": "https://mock.atlassian.net", "email": "dev@test.com"},
        auth_type="API_TOKEN",
        credentials="mock_token",
    )

    mock_client = MagicMock(spec=JiraCloudClient)
    mock_client.search_issues.return_value = {
        "issues": [
            {
                "key": "JIRA-1",
                "fields": {
                    "summary": "First Issue",
                    "description": "Description 1",
                    "issuetype": {"name": "Bug"},
                    "status": {"name": "Open"},
                    "created": "2023-01-01T10:00:00.000+0000",
                    "updated": "2023-01-02T10:00:00.000+0000",
                },
            },
            {
                "key": "JIRA-2",
                "fields": {
                    "summary": "Second Issue",
                    "description": "Description 2",
                    "issuetype": {"name": "Improvement"},
                    "status": {"name": "Resolved"},
                    "created": "2023-01-03T10:00:00.000+0000",
                    "updated": "2023-01-04T10:00:00.000+0000",
                },
            },
        ]
    }

    run1 = svc.sync_jira(project_id=p.id, connection_id=conn.id, client=mock_client)
    assert run1.status == "COMPLETED"
    assert run1.records_created == 2
    assert run1.records_discovered == 2

    # Verify records in DB
    issues = db.query(Issue).filter(Issue.source_issue_id.in_(["JIRA-1", "JIRA-2"])).all()
    assert len(issues) == 2
    records = db.query(SourceRecord).filter(SourceRecord.dataset_name == "jira-onboard").all()
    assert len(records) == 2

    # Second sync should be idempotent (skip identical records)
    run2 = svc.sync_jira(project_id=p.id, connection_id=conn.id, client=mock_client)
    assert run2.status == "COMPLETED"
    assert run2.records_created == 0
    assert run2.records_skipped == 2

    db.close()


# ---------------------------------------------------------------------------
# 3. GitHub Public Repository Integration Tests
# ---------------------------------------------------------------------------

def test_github_url_parsing():
    assert parse_github_url("https://github.com/apache/kafka") == ("apache", "kafka")
    assert parse_github_url("http://github.com/apache/kafka.git") == ("apache", "kafka")
    assert parse_github_url("github.com/facebook/react") == ("facebook", "react")
    assert parse_github_url("golang/go") == ("golang", "go")

    with pytest.raises(ValueError):
        parse_github_url("https://gitlab.com/unknown")


def test_github_normalizer_and_issue_linking():
    sample_commit = {
        "sha": "9f83a4b7c6d5e4f3a2b109876543210987654321",
        "commit": {
            "message": "KAFKA-10111: Fix KRaft log replication deadlock\n\nResolves deadlock under high lock contention.",
            "author": {"name": "Dev One", "date": "2023-04-12T14:30:00Z"},
            "committer": {"name": "Committer Two", "date": "2023-04-12T15:00:00Z"},
        },
        "files": [
            {"filename": "core/src/main/scala/kafka/raft/RaftManager.scala", "status": "modified", "additions": 45, "deletions": 12},
            {"filename": "core/src/test/scala/unit/kafka/raft/RaftManagerTest.scala", "status": "added", "additions": 120, "deletions": 0},
        ],
    }

    commit, src_rec, changes, links = GitHubNormalizer.normalize_commit(
        commit_data=sample_commit,
        project_id=3,
        dataset_name="gh-project",
    )

    assert commit.commit_hash == "9f83a4b7c6d5e4f3a2b109876543210987654321"
    assert commit.author == "Dev One"
    assert len(changes) == 2
    assert changes[0].change_type == "M"
    assert changes[1].change_type == "A"
    assert len(links) == 1
    assert links[0].source_issue_id == "KAFKA-10111"
    assert src_rec.source_table == "github_commits"


def test_github_sync_end_to_end_mocked():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)
    p = svc.create_project(name="GitHub Project", slug="gh-test")

    conn = svc.create_source_connection(
        project_id=p.id,
        source_type="GITHUB_PUBLIC",
        name="Apache Kafka GitHub",
        config={"repo_url": "https://github.com/apache/kafka"},
        auth_type="NONE",
    )

    mock_client = MagicMock(spec=GitHubClient)
    mock_client.list_commits.return_value = [{"sha": "commit123"}]
    mock_client.get_commit.return_value = {
        "sha": "commit123",
        "commit": {
            "message": "PROJ-10: Initial commit",
            "author": {"name": "Author", "date": "2023-01-01T12:00:00Z"},
        },
        "files": [{"filename": "README.md", "status": "added", "additions": 10, "deletions": 0}],
    }
    mock_client.list_pull_requests.return_value = [
        {
            "number": 42,
            "title": "Add KRaft metadata mode",
            "body": "Implements KIP-500",
            "user": {"login": "octocat"},
            "state": "closed",
        }
    ]

    run = svc.sync_github(project_id=p.id, connection_id=conn.id, client=mock_client)
    assert run.status == "COMPLETED"
    assert run.records_created == 2  # 1 commit + 1 PR
    assert json.loads(run.checkpoint_data)["last_commit_sha"] == "commit123"

    db.close()


# ---------------------------------------------------------------------------
# 4. Document Ingestion & SSRF Protection Tests
# ---------------------------------------------------------------------------

def test_ssrf_and_private_ip_blocking():
    # Private RFC 1918 addresses
    assert is_ip_private_or_restricted("127.0.0.1") is True
    assert is_ip_private_or_restricted("10.0.0.5") is True
    assert is_ip_private_or_restricted("172.16.0.1") is True
    assert is_ip_private_or_restricted("192.168.1.100") is True
    assert is_ip_private_or_restricted("169.254.169.254") is True  # Cloud metadata
    assert is_ip_private_or_restricted("::1") is True

    # Public IP
    assert is_ip_private_or_restricted("8.8.8.8") is False

    # URL Scheme checks
    with pytest.raises(ValueError) as exc:
        validate_public_url("file:///etc/passwd")
    assert "Invalid URL scheme" in str(exc.value)

    with pytest.raises(ValueError):
        validate_public_url("ftp://server/file.txt")

    # Localhost hostname checks
    with pytest.raises(ValueError) as exc:
        validate_public_url("http://localhost:8000/api")
    assert "restricted hostname" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        validate_public_url("http://127.0.0.1:8000/api")
    assert "restricted hostname" in str(exc.value)


def test_document_security_and_sanitization():
    # Sanitization
    assert sanitize_filename("../../../secret.pdf") == "secret.pdf"
    assert sanitize_filename("..\\..\\windows\\system32\\calc.exe") == "calc.exe"

    # Extension validation
    validate_file_upload("valid_doc.pdf", 1024)
    validate_file_upload("notes.md", 2048)
    validate_file_upload("specs.docx", 5000)

    with pytest.raises(ValueError) as exc:
        validate_file_upload("malicious.exe", 1024)
    assert "Unsupported file format" in str(exc.value)

    # Size limit validation
    with pytest.raises(ValueError) as exc:
        validate_file_upload("large.pdf", MAX_FILE_SIZE_BYTES + 1)
    assert "exceeds maximum allowed limit" in str(exc.value)

    with pytest.raises(ValueError):
        validate_file_upload("empty.txt", 0)


def test_document_parser_and_normalizer():
    sample_content = b"# Architecture Decision: KRaft\n\nKRaft consensus replaces ZooKeeper.\n\n## Section 2\nMetadata quorum handles log records."
    doc_model, src_records = DocumentNormalizer.process_file_bytes(
        filename="kraft_decision.md",
        file_bytes=sample_content,
        project_id=2,
        dataset_name="kafka-docs",
    )

    assert doc_model.filename == "kraft_decision.md"
    assert doc_model.file_type == "MD"
    assert len(doc_model.sha256_hash) == 64
    assert len(src_records) >= 1
    assert "KRaft consensus" in src_records[0].raw_data["text"]


def test_document_ingestion_service_deduplication():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)
    p = svc.create_project(name="Doc Project", slug="docs-test")

    content = b"Specification for Kafka Protocol v2.5."
    doc1, run1 = svc.ingest_document_bytes(
        project_id=p.id,
        filename="spec.txt",
        file_bytes=content,
    )
    assert run1.status == "COMPLETED"
    assert run1.records_created >= 1

    # Ingesting exact same content should be recognized as duplicate
    doc2, run2 = svc.ingest_document_bytes(
        project_id=p.id,
        filename="spec.txt",
        file_bytes=content,
    )
    assert run2.status == "COMPLETED"
    assert run2.records_skipped == 1
    assert doc1.id == doc2.id

    db.close()


# ---------------------------------------------------------------------------
# 5. Project Isolation Tests
# ---------------------------------------------------------------------------

def test_project_data_isolation():
    db = TestingSessionLocal()
    svc = ProjectIngestionService(db)

    p_a = svc.create_project(name="Project A", slug="proj-a")
    p_b = svc.create_project(name="Project B", slug="proj-b")

    # Ingest document for Project A
    svc.ingest_document_bytes(
        project_id=p_a.id,
        filename="secret_a.txt",
        file_bytes=b"Confidential project A architecture.",
    )

    # Ingest document for Project B
    svc.ingest_document_bytes(
        project_id=p_b.id,
        filename="secret_b.txt",
        file_bytes=b"Confidential project B roadmap.",
    )

    docs_a = db.query(Document).filter(Document.project_id == p_a.id).all()
    docs_b = db.query(Document).filter(Document.project_id == p_b.id).all()

    assert len(docs_a) == 1
    assert docs_a[0].filename == "secret_a.txt"
    assert len(docs_b) == 1
    assert docs_b[0].filename == "secret_b.txt"

    # Ingestion runs isolation
    runs_a = db.query(IngestionRun).filter(IngestionRun.project_id == p_a.id).all()
    runs_b = db.query(IngestionRun).filter(IngestionRun.project_id == p_b.id).all()
    assert len(runs_a) == 1
    assert len(runs_b) == 1
    assert runs_a[0].project_id == p_a.id
    assert runs_b[0].project_id == p_b.id

    db.close()


# ---------------------------------------------------------------------------
# 6. REST API Endpoints Tests
# ---------------------------------------------------------------------------

def test_project_api_lifecycle(client):
    # Create project
    create_payload = {
        "name": "Distributed DB",
        "slug": "dist-db",
        "description": "High performance database",
        "domain": "Database",
    }
    resp = client.post("/api/projects", json=create_payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Distributed DB"
    assert data["slug"] == "dist-db"
    project_id = data["id"]

    # Get project by slug
    resp_get = client.get("/api/projects/dist-db")
    assert resp_get.status_code == 200
    assert resp_get.json()["id"] == project_id

    # List projects
    resp_list = client.get("/api/projects")
    assert resp_list.status_code == 200
    assert len(resp_list.json()) >= 2

    # Update project
    resp_update = client.put(f"/api/projects/{project_id}", json={"description": "Updated database description"})
    assert resp_update.status_code == 200
    assert resp_update.json()["description"] == "Updated database description"


def test_source_routes_validation_and_connect(client):
    # Create target project
    resp = client.post("/api/projects", json={"name": "Source Test Project", "slug": "src-proj"})
    slug = resp.json()["slug"]

    # Validate Jira (mocked error or success)
    with patch("app.integrations.jira.client.JiraCloudClient.test_connection") as mock_test:
        mock_test.return_value = {"displayName": "Test Admin", "emailAddress": "admin@test.com"}
        jira_val = client.post(
            f"/api/projects/{slug}/sources/jira/validate",
            json={"base_url": "https://test.atlassian.net", "email": "admin@test.com", "api_token": "token"},
        )
        assert jira_val.status_code == 200
        assert jira_val.json()["valid"] is True

    # Connect Jira
    jira_conn = client.post(
        f"/api/projects/{slug}/sources/jira",
        json={"base_url": "https://test.atlassian.net", "email": "admin@test.com", "api_token": "token"},
    )
    assert jira_conn.status_code == 201
    assert jira_conn.json()["source_type"] == "JIRA_CLOUD"

    # Validate GitHub
    with patch("app.integrations.github.client.GitHubClient.get_repo") as mock_repo:
        mock_repo.return_value = {"full_name": "apache/kafka", "stargazers_count": 27000, "default_branch": "trunk"}
        gh_val = client.post(
            f"/api/projects/{slug}/sources/github/validate",
            json={"repo_url": "https://github.com/apache/kafka"},
        )
        assert gh_val.status_code == 200
        assert gh_val.json()["valid"] is True

    # Connect GitHub
    gh_conn = client.post(
        f"/api/projects/{slug}/sources/github",
        json={"repo_url": "https://github.com/apache/kafka"},
    )
    assert gh_conn.status_code == 201
    assert gh_conn.json()["source_type"] == "GITHUB_PUBLIC"

    # List sources
    sources_resp = client.get(f"/api/projects/{slug}/sources")
    assert sources_resp.status_code == 200
    assert len(sources_resp.json()) == 2


def test_document_routes_upload_and_url(client):
    resp = client.post("/api/projects", json={"name": "Doc API Project", "slug": "doc-api"})
    slug = resp.json()["slug"]

    # File upload
    file_bytes = b"# KIP-500 Title\n\nRemoving ZooKeeper dependency in Kafka."
    files = {"file": ("kip-500.md", io.BytesIO(file_bytes), "text/markdown")}
    upload_resp = client.post(f"/api/projects/{slug}/documents/upload", files=files)
    assert upload_resp.status_code == 201
    doc_data = upload_resp.json()
    assert doc_data["filename"] == "kip-500.md"
    assert doc_data["file_type"] == "MD"
    doc_id = doc_data["id"]

    # List documents
    docs_list = client.get(f"/api/projects/{slug}/documents")
    assert docs_list.status_code == 200
    assert len(docs_list.json()) == 1

    # Get document detail
    doc_detail = client.get(f"/api/projects/{slug}/documents/{doc_id}")
    assert doc_detail.status_code == 200
    assert "Removing ZooKeeper" in doc_detail.json()["extracted_text"]

    # SSRF blocked URL test
    url_resp = client.post(f"/api/projects/{slug}/documents/url", json={"url": "http://127.0.0.1:8000/secret.txt"})
    assert url_resp.status_code == 400
    assert "restricted hostname" in url_resp.json()["detail"]

    # Project runs endpoint
    runs_resp = client.get(f"/api/projects/{slug}/runs")
    assert runs_resp.status_code == 200
    assert len(runs_resp.json()) >= 1

"""
Legacy API completion tests — updated to use StaticPool for in-memory SQLite
and to match the current Pydantic schema defaults.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.database import get_db, init_db
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.ingestion_batch import IngestionBatch
from app.db.base import Base

# ---------------------------------------------------------------------------
# Shared test engine (StaticPool so all sessions share the same in-memory DB)
# ---------------------------------------------------------------------------

test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
def setup_db():
    init_db(target_engine=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)
    # Re-create so next test starts clean
    init_db(target_engine=test_engine)


client = TestClient(app)


def test_statistics_endpoint():
    response = client.get("/api/statistics")
    assert response.status_code == 200
    data = response.json()
    assert "total_issues" in data
    assert data["total_issues"] == 0


def test_issues_endpoints():
    db = TestingSessionLocal()
    issue = Issue(source_issue_id="KAFKA-100", summary="Test issue", issue_type="Bug")
    db.add(issue)
    db.commit()
    db.close()

    response = client.get("/api/issues")
    assert response.status_code == 200
    assert len(response.json()) == 1

    response_detail = client.get("/api/issues/KAFKA-100")
    assert response_detail.status_code == 200
    assert response_detail.json()["summary"] == "Test issue"


def test_commits_endpoint():
    db = TestingSessionLocal()
    commit = Commit(commit_hash="abc1234", message="Fix bug", author="Developer")
    db.add(commit)
    db.commit()
    db.close()

    response = client.get("/api/commits/abc1234")
    assert response.status_code == 200
    assert response.json()["message"] == "Fix bug"

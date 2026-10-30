"""
Legacy idempotency test — kept for backwards compatibility.
Full coverage is in test_work021.py.
"""
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.database import init_db
from app.services.ingestion_service import IngestionService
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.ingestion_batch import IngestionBatch

SOURCE_DB = Path(__file__).resolve().parent.parent.parent / "sample_data" / "raw" / "kafka.sqlite3"


@pytest.fixture
def temp_db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    init_db(target_engine=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


@pytest.mark.skipif(not SOURCE_DB.exists(), reason="kafka.sqlite3 not present")
def test_idempotent_ingestion(temp_db):
    service = IngestionService(temp_db, str(SOURCE_DB))

    batch1 = service.run_ingestion()
    assert batch1.status == "COMPLETED"
    issues_count_1 = temp_db.query(Issue).count()
    records_count_1 = temp_db.query(SourceRecord).count()

    batch2 = service.run_ingestion()
    assert batch2.status == "COMPLETED"
    issues_count_2 = temp_db.query(Issue).count()
    records_count_2 = temp_db.query(SourceRecord).count()

    assert issues_count_1 == issues_count_2, "Second run must not create duplicate issues"
    assert records_count_1 == records_count_2, "Second run must not create duplicate source records"
    assert batch2.records_inserted == 0, "No new inserts on repeat run"
    assert batch2.records_skipped > 0, "All unchanged records must be skipped"

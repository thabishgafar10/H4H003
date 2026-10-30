"""
Database model unit tests.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.database import init_db
from app.models.source_record import SourceRecord

SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db_session():
    init_db(target_engine=engine)
    session = TestingSessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def test_source_record_persistence(db_session):
    sr = SourceRecord(
        dataset_name="test",
        source_table="issue",
        source_record_id="1",
        source_record_hash="abc",
        raw_data={"test": "data"},
        ingestion_batch_id=1,
    )
    db_session.add(sr)
    db_session.commit()
    assert sr.id is not None

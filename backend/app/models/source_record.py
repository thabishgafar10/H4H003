from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from app.db.base import Base

class SourceRecord(Base):
    __tablename__ = "source_records"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    dataset_name = Column(String, nullable=False)
    source_table = Column(String, nullable=False)
    source_record_id = Column(String, nullable=False)
    source_record_hash = Column(String, nullable=False)
    raw_data = Column(JSON, nullable=False)
    source_created_at = Column(DateTime, nullable=True)
    source_updated_at = Column(DateTime, nullable=True)
    ingested_at = Column(DateTime, server_default=func.now())
    ingestion_batch_id = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint('dataset_name', 'source_table', 'source_record_id', name='_source_record_uc'),
    )

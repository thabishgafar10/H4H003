from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from app.db.base import Base

class IngestionBatch(Base):
    __tablename__ = "ingestion_batches"

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False)
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="RUNNING")
    records_processed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    unresolved_relationships = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)

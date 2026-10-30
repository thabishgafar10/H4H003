from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from app.db.base import Base

class UnresolvedRelationship(Base):
    __tablename__ = "unresolved_relationships"

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False, default="KafkaSQLite")
    source_table = Column(String, nullable=False, index=True)
    source_record_id = Column(Integer, ForeignKey("source_records.id"), nullable=True, index=True)
    source_id = Column(String, nullable=False, index=True)
    target_type = Column(String, nullable=False)  # 'issue' or 'commit'
    target_id = Column(String, nullable=False)    # missing target key
    reason = Column(Text, nullable=False)
    batch_id = Column(Integer, ForeignKey("ingestion_batches.id"), nullable=True, index=True)
    detected_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint('dataset_name', 'source_table', 'source_id', 'target_type', 'target_id', name='_unresolved_rel_uc'),
    )


"""
Ingestion Run Model for Work 07.
Tracks execution metrics, checkpoints, and incremental synchronization state across project sources.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    source_connection_id = Column(Integer, ForeignKey("source_connections.id", ondelete="SET NULL"), nullable=True)
    source_system = Column(String(50), nullable=False)  # JIRA_CLOUD, GITHUB_PUBLIC, DOCUMENT_UPLOAD, DOCUMENT_URL, LEGACY_DATASET
    status = Column(String(50), nullable=False, default="PENDING")  # PENDING, RUNNING, COMPLETED, PARTIALLY_COMPLETED, FAILED, CANCELLED
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime, nullable=True)
    records_discovered = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_summary = Column(Text, nullable=True)
    checkpoint_data = Column(Text, nullable=True)  # JSON checkpoint string (e.g. last_commit_sha, last_updated_time)

    # Relationships
    project = relationship("Project", back_populates="ingestion_runs")
    source_connection = relationship("SourceConnection", back_populates="ingestion_runs")

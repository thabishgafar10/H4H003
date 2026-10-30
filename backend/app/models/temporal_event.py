from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class TemporalEvent(Base):
    __tablename__ = "temporal_events"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    dataset_name = Column(String, nullable=False, default="KafkaSQLite", index=True)
    event_key = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)
    event_time = Column(DateTime, nullable=True, index=True)
    subject = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    actor = Column(String, nullable=True)
    issue_id = Column(String, nullable=True, index=True)
    commit_hash = Column(String, nullable=True, index=True)
    extraction_run_id = Column(Integer, ForeignKey("extraction_runs.id"), nullable=True, index=True)
    created_at = Column(DateTime, server_default=func.now())

    extraction_run = relationship("ExtractionRun", back_populates="events")
    evidence = relationship("EventEvidence", back_populates="event", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('dataset_name', 'event_key', name='_temporal_event_key_uc'),
    )

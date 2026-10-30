from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class ExtractionRun(Base):
    __tablename__ = "extraction_runs"

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False, default="KafkaSQLite", index=True)
    status = Column(String, nullable=False, default="RUNNING", index=True)
    extraction_mode = Column(String, nullable=False, default="DETERMINISTIC")  # DETERMINISTIC or HYBRID
    extractor_version = Column(String, nullable=False, default="1.0.0")
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime, nullable=True)
    records_scanned = Column(Integer, default=0)
    candidates_created = Column(Integer, default=0)
    events_accepted = Column(Integer, default=0)
    events_rejected = Column(Integer, default=0)
    events_needing_review = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)

    candidates = relationship("EventCandidate", back_populates="extraction_run", cascade="all, delete-orphan")
    events = relationship("TemporalEvent", back_populates="extraction_run")

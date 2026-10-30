from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class EventCandidate(Base):
    __tablename__ = "event_candidates"

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False, default="KafkaSQLite", index=True)
    event_type = Column(String, nullable=False, index=True)
    event_time = Column(DateTime, nullable=True, index=True)
    subject = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    actor = Column(String, nullable=True)
    extractor_type = Column(String, nullable=False)  # DETERMINISTIC or LLM
    extractor_version = Column(String, nullable=False, default="1.0.0")
    confidence_band = Column(String, nullable=False)  # HIGH, MEDIUM, LOW
    validation_status = Column(String, nullable=False, default="PENDING", index=True)  # PENDING, ACCEPTED, REJECTED, NEEDS_REVIEW
    rejection_reason = Column(Text, nullable=True)
    candidate_fingerprint = Column(String, nullable=False, index=True)
    extraction_run_id = Column(Integer, ForeignKey("extraction_runs.id"), nullable=True, index=True)
    created_at = Column(DateTime, server_default=func.now())

    extraction_run = relationship("ExtractionRun", back_populates="candidates")
    evidence_records = relationship("EventEvidence", back_populates="candidate", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('dataset_name', 'candidate_fingerprint', name='_candidate_fingerprint_uc'),
    )

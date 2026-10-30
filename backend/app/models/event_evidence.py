from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class EventEvidence(Base):
    __tablename__ = "event_evidence"

    id = Column(Integer, primary_key=True)
    candidate_id = Column(Integer, ForeignKey("event_candidates.id"), nullable=True, index=True)
    event_id = Column(Integer, ForeignKey("temporal_events.id"), nullable=True, index=True)
    source_record_id = Column(Integer, ForeignKey("source_records.id"), nullable=True, index=True)
    evidence_role = Column(String, nullable=False, default="PRIMARY")  # PRIMARY, SUPPORTING, CONTRADICTORY
    evidence_excerpt = Column(Text, nullable=False)
    evidence_hash = Column(String, nullable=False, index=True)  # SHA-256 of excerpt
    created_at = Column(DateTime, server_default=func.now())

    candidate = relationship("EventCandidate", back_populates="evidence_records")
    event = relationship("TemporalEvent", back_populates="evidence")
    source_record = relationship("SourceRecord")

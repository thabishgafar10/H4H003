"""
Pydantic schemas for Temporal Events, Candidates, Evidence, and Issue Timelines.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict


class EventEvidenceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    candidate_id: Optional[int] = None
    event_id: Optional[int] = None
    source_record_id: Optional[int] = None
    evidence_role: str
    evidence_excerpt: str
    evidence_hash: str
    created_at: Optional[datetime] = None


class EventCandidateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_name: str
    event_type: str
    event_time: Optional[datetime] = None
    subject: str
    description: Optional[str] = None
    actor: Optional[str] = None
    extractor_type: str
    extractor_version: str
    confidence_band: str
    validation_status: str
    rejection_reason: Optional[str] = None
    candidate_fingerprint: str
    extraction_run_id: Optional[int] = None
    created_at: Optional[datetime] = None
    evidence_records: List[EventEvidenceResponse] = []


class TemporalEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_name: str
    event_key: str
    event_type: str
    event_time: Optional[datetime] = None
    subject: str
    description: Optional[str] = None
    actor: Optional[str] = None
    issue_id: Optional[str] = None
    commit_hash: Optional[str] = None
    extraction_run_id: Optional[int] = None
    created_at: Optional[datetime] = None
    evidence: List[EventEvidenceResponse] = []


class IssueTimelineResponse(BaseModel):
    issue_id: str
    summary: Optional[str] = None
    status: Optional[str] = None
    total_events: int
    timeline: List[TemporalEventResponse]

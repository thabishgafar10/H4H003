"""
Pydantic Schemas for Work 06: Historical Natural-Language Question Answering.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class QACitation(BaseModel):
    source_record_id: Optional[int] = None
    record_type: str = Field(..., description="issue, commit, comment, etc.")
    record_id: str = Field(..., description="Issue key, commit hash, or comment id")
    excerpt: str = Field(..., description="Verbatim quote from source record")
    evidence_hash: Optional[str] = Field(None, description="SHA-256 hash of verbatim excerpt")
    timestamp: Optional[str] = None


class QATimelineEvent(BaseModel):
    event_key: str
    event_type: str
    date: Optional[str] = None
    actor: Optional[str] = None
    summary: str
    issue_id: Optional[str] = None
    commit_hash: Optional[str] = None


class QARequest(BaseModel):
    query: str = Field(..., description="Natural language question about product evolution")
    dataset_name: str = Field("KafkaSQLite", description="Dataset scope")
    include_graph: bool = Field(False, description="Optionally include neighborhood graph")


class QAResponse(BaseModel):
    query: str
    canonical_feature: Optional[str] = None
    feature_category: Optional[str] = None
    confidence: float
    answer: str
    temporal_timeline: List[QATimelineEvent] = Field(default_factory=list)
    citations: List[QACitation] = Field(default_factory=list)
    synthesis_mode: str = Field("DETERMINISTIC_GROUNDED", description="DETERMINISTIC_GROUNDED or LLM_GROUNDED")
    missing_evidence_notes: Optional[str] = None
    graph: Optional[Dict[str, Any]] = None


class SampleQuestion(BaseModel):
    category: str
    question: str
    description: str


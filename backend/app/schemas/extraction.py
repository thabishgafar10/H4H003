"""
Pydantic schemas for Temporal Event Extraction Runs and Statistics.
"""

from datetime import datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class ExtractionRunRequest(BaseModel):
    dataset_name: str = "KafkaSQLite"
    extraction_mode: str = "DETERMINISTIC"  # DETERMINISTIC or HYBRID
    chunk_size: int = 500
    max_issues: Optional[int] = None
    max_comments: Optional[int] = None
    max_commits: Optional[int] = None


class ExtractionRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_name: str
    status: str
    extraction_mode: str
    extractor_version: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    records_scanned: int = 0
    candidates_created: int = 0
    events_accepted: int = 0
    events_rejected: int = 0
    events_needing_review: int = 0
    error_message: Optional[str] = None


class ExtractionStatsResponse(BaseModel):
    dataset_name: str
    total_runs: int
    latest_run: Optional[Dict] = None
    total_candidates: int
    candidates_by_status: Dict[str, int]
    total_temporal_events: int
    events_by_type: Dict[str, int]
    total_evidence_links: int

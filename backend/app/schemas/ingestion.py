from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any, List
from datetime import datetime

class IngestionRunResponse(BaseModel):
    batch_id: int
    status: str
    started_at: datetime
    message: str

class IngestionBatchResponse(BaseModel):
    id: int
    dataset_name: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    unresolved_relationships: int = 0
    error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

class SystemStatisticsResponse(BaseModel):
    total_issues: int = 0
    total_comments: int = 0
    total_commits: int = 0
    total_code_changes: int = 0
    total_issue_components: int = 0
    total_fix_version_mappings: int = 0
    total_issue_links: int = 0
    total_commit_issue_links: int = 0
    total_source_records: int = 0
    unresolved_relationships: int = 0
    latest_ingestion_status: Optional[str] = None
    latest_ingestion_completed_at: Optional[datetime] = None

class UnresolvedRelationshipResponse(BaseModel):
    id: int
    dataset_name: str
    source_table: str
    source_record_id: Optional[int] = None
    source_id: str
    target_type: str
    target_id: str
    reason: str
    batch_id: Optional[int] = None
    detected_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


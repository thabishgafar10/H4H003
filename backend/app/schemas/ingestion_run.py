from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class IngestionRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    project_id: int
    source_connection_id: Optional[int] = None
    source_system: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    records_discovered: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    error_summary: Optional[str] = None
    checkpoint_data: Optional[str] = None

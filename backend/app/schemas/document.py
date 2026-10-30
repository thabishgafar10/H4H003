from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class DocumentUrlRequest(BaseModel):
    url: str
    name: Optional[str] = None


class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    project_id: int
    source_connection_id: Optional[int] = None
    filename: str
    file_type: str
    source_url: Optional[str] = None
    file_size: int
    sha256_hash: str
    page_count: Optional[int] = None
    extracted_text: Optional[str] = None
    metadata_json: Optional[str] = None
    created_at: Optional[datetime] = None

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict


class JiraConnectRequest(BaseModel):
    base_url: str
    email: Optional[str] = None
    api_token: Optional[str] = None
    oauth_token: Optional[str] = None
    name: Optional[str] = None
    project_key: Optional[str] = None


class GitHubConnectRequest(BaseModel):
    repo_url: str
    token: Optional[str] = None
    name: Optional[str] = None


class SourceConnectionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    project_id: int
    source_type: str
    name: str
    config: Optional[str] = None
    auth_type: str
    status: str
    last_sync_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class ConnectionValidationResponse(BaseModel):
    valid: bool
    message: str
    details: Optional[Dict[str, Any]] = None

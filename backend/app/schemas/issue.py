from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from datetime import datetime

class CommentResponse(BaseModel):
    id: int
    source_issue_id: str
    username: Optional[str] = None
    display_name: Optional[str] = None
    message: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class ComponentResponse(BaseModel):
    id: int
    component: str

    model_config = ConfigDict(from_attributes=True)

class FixVersionResponse(BaseModel):
    id: int
    fix_version: str

    model_config = ConfigDict(from_attributes=True)

class IssueResponse(BaseModel):
    id: int
    source_issue_id: str
    issue_type: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    resolution: Optional[str] = None
    assignee: Optional[str] = None
    reporter: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    comments: List[CommentResponse] = []
    components: List[ComponentResponse] = []
    fix_versions: List[FixVersionResponse] = []

    model_config = ConfigDict(from_attributes=True)

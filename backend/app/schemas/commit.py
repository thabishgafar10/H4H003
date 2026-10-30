from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from datetime import datetime

class CodeChangeResponse(BaseModel):
    id: int
    file_path: Optional[str] = None
    old_file_path: Optional[str] = None
    change_type: Optional[str] = None
    is_deleted: Optional[bool] = None
    lines_added: Optional[int] = None
    lines_removed: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)

class CommitResponse(BaseModel):
    id: int
    commit_hash: str
    message: Optional[str] = None
    author: Optional[str] = None
    author_email: Optional[str] = None
    committed_at: Optional[datetime] = None
    is_merge: Optional[bool] = None
    code_changes: List[CodeChangeResponse] = []

    model_config = ConfigDict(from_attributes=True)

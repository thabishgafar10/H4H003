from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.ingestion_batch import IngestionBatch
from app.models.source_record import SourceRecord
from app.models.issue_comment import IssueComment
from app.models.code_change import CodeChange
from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink
from app.schemas.issue import IssueResponse
from app.schemas.commit import CommitResponse
from app.schemas.ingestion import IngestionBatchResponse, SystemStatisticsResponse
from typing import List, Optional
from sqlalchemy import func

router = APIRouter()


@router.get("/statistics", response_model=SystemStatisticsResponse)
def get_statistics(db: Session = Depends(get_db)):
    latest = db.query(IngestionBatch).order_by(IngestionBatch.started_at.desc()).first()
    total_unresolved = 0
    if latest:
        total_unresolved = latest.unresolved_relationships or 0

    stats = SystemStatisticsResponse(
        total_issues=db.query(Issue).count(),
        total_comments=db.query(IssueComment).count(),
        total_commits=db.query(Commit).count(),
        total_code_changes=db.query(CodeChange).count(),
        total_issue_components=db.query(IssueComponent).count(),
        total_fix_version_mappings=db.query(IssueFixVersion).count(),
        total_issue_links=db.query(IssueLink).count(),
        total_commit_issue_links=db.query(CommitIssueLink).count(),
        total_source_records=db.query(SourceRecord).count(),
        unresolved_relationships=total_unresolved,
        latest_ingestion_status=latest.status if latest else None,
        latest_ingestion_completed_at=latest.completed_at if latest else None,
    )
    return stats


@router.get("/issues", response_model=List[IssueResponse])
def list_issues(
    skip: int = Query(0, ge=0, description="Records to skip for pagination"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    status: Optional[str] = Query(None, description="Filter by issue status"),
    issue_type: Optional[str] = Query(None, description="Filter by issue type"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    db: Session = Depends(get_db),
):
    query = db.query(Issue)
    if status:
        query = query.filter(Issue.status == status)
    if issue_type:
        query = query.filter(Issue.issue_type == issue_type)
    if priority:
        query = query.filter(Issue.priority == priority)
    return query.order_by(Issue.id).offset(skip).limit(limit).all()


@router.get("/issues/{issue_id}", response_model=IssueResponse)
def get_issue(issue_id: str, db: Session = Depends(get_db)):
    issue = db.query(Issue).filter(Issue.source_issue_id == issue_id).first()
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")
    return issue


@router.get("/commits/{commit_hash}", response_model=CommitResponse)
def get_commit(commit_hash: str, db: Session = Depends(get_db)):
    commit = db.query(Commit).filter(Commit.commit_hash == commit_hash).first()
    if not commit:
        raise HTTPException(status_code=404, detail="Commit not found")
    return commit

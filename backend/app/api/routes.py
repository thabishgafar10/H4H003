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
from app.models.unresolved_relationship import UnresolvedRelationship
from app.schemas.issue import IssueResponse
from app.schemas.commit import CommitResponse
from app.schemas.ingestion import IngestionBatchResponse, SystemStatisticsResponse
from typing import List, Optional
from sqlalchemy import func

router = APIRouter()


@router.get("/statistics", response_model=SystemStatisticsResponse)
def get_statistics(
    dataset_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    batch_query = db.query(IngestionBatch)
    if dataset_name:
        batch_query = batch_query.filter(IngestionBatch.dataset_name == dataset_name)
    latest = batch_query.order_by(IngestionBatch.started_at.desc()).first()

    target_dataset = dataset_name if dataset_name else (latest.dataset_name if latest else None)

    unres_query = db.query(UnresolvedRelationship)
    if target_dataset:
        unres_query = unres_query.filter(UnresolvedRelationship.dataset_name == target_dataset)
    unresolved_count = unres_query.count()

    if unresolved_count == 0 and latest and (target_dataset is None or latest.dataset_name == target_dataset):
        unresolved_count = latest.unresolved_relationships or 0

    sr_query = db.query(SourceRecord)
    if target_dataset or project_id:
        if target_dataset and project_id:
            sr_query = sr_query.filter((SourceRecord.dataset_name == target_dataset) | (SourceRecord.project_id == project_id))
        elif target_dataset:
            sr_query = sr_query.filter(SourceRecord.dataset_name == target_dataset)
        elif project_id:
            sr_query = sr_query.filter(SourceRecord.project_id == project_id)

    if dataset_name or project_id:
        filter_cond = (
            (SourceRecord.dataset_name == target_dataset) | (SourceRecord.project_id == project_id)
            if target_dataset and project_id
            else (SourceRecord.dataset_name == target_dataset if target_dataset else SourceRecord.project_id == project_id)
        )
        total_issues = db.query(Issue).join(SourceRecord, Issue.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_comments = db.query(IssueComment).join(SourceRecord, IssueComment.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_commits = db.query(Commit).join(SourceRecord, Commit.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_code_changes = db.query(CodeChange).join(SourceRecord, CodeChange.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_issue_components = db.query(IssueComponent).join(SourceRecord, IssueComponent.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_fix_version_mappings = db.query(IssueFixVersion).join(SourceRecord, IssueFixVersion.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_issue_links = db.query(IssueLink).join(SourceRecord, IssueLink.source_record_id == SourceRecord.id).filter(filter_cond).count()
        total_commit_issue_links = db.query(CommitIssueLink).join(SourceRecord, CommitIssueLink.source_record_id == SourceRecord.id).filter(filter_cond).count()
    else:
        total_issues = db.query(Issue).count()
        total_comments = db.query(IssueComment).count()
        total_commits = db.query(Commit).count()
        total_code_changes = db.query(CodeChange).count()
        total_issue_components = db.query(IssueComponent).count()
        total_fix_version_mappings = db.query(IssueFixVersion).count()
        total_issue_links = db.query(IssueLink).count()
        total_commit_issue_links = db.query(CommitIssueLink).count()

    stats = SystemStatisticsResponse(
        total_issues=total_issues,
        total_comments=total_comments,
        total_commits=total_commits,
        total_code_changes=total_code_changes,
        total_issue_components=total_issue_components,
        total_fix_version_mappings=total_fix_version_mappings,
        total_issue_links=total_issue_links,
        total_commit_issue_links=total_commit_issue_links,
        total_source_records=sr_query.count(),
        unresolved_relationships=unresolved_count,
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
    dataset_name: Optional[str] = Query(None, description="Filter by dataset scope"),
    project_id: Optional[int] = Query(None, description="Filter by project ID"),
    db: Session = Depends(get_db),
):
    query = db.query(Issue)
    if dataset_name or project_id:
        query = query.join(SourceRecord, Issue.source_record_id == SourceRecord.id)
        if dataset_name and project_id:
            query = query.filter((SourceRecord.dataset_name == dataset_name) | (SourceRecord.project_id == project_id))
        elif dataset_name:
            query = query.filter(SourceRecord.dataset_name == dataset_name)
        elif project_id:
            query = query.filter(SourceRecord.project_id == project_id)
    if status:
        query = query.filter(Issue.status == status)
    if issue_type:
        query = query.filter(Issue.issue_type == issue_type)
    if priority:
        query = query.filter(Issue.priority == priority)
    return query.order_by(Issue.id).offset(skip).limit(limit).all()


@router.get("/issues/{issue_id}", response_model=IssueResponse)
def get_issue(
    issue_id: str,
    dataset_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(Issue).filter(Issue.source_issue_id == issue_id)
    if dataset_name or project_id:
        query = query.join(SourceRecord, Issue.source_record_id == SourceRecord.id)
        if dataset_name and project_id:
            query = query.filter((SourceRecord.dataset_name == dataset_name) | (SourceRecord.project_id == project_id))
        elif dataset_name:
            query = query.filter(SourceRecord.dataset_name == dataset_name)
        elif project_id:
            query = query.filter(SourceRecord.project_id == project_id)
    issue = query.first()
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")
    return issue


@router.get("/commits/{commit_hash}", response_model=CommitResponse)
def get_commit(
    commit_hash: str,
    dataset_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(Commit).filter(Commit.commit_hash == commit_hash)
    if dataset_name or project_id:
        query = query.join(SourceRecord, Commit.source_record_id == SourceRecord.id)
        if dataset_name and project_id:
            query = query.filter((SourceRecord.dataset_name == dataset_name) | (SourceRecord.project_id == project_id))
        elif dataset_name:
            query = query.filter(SourceRecord.dataset_name == dataset_name)
        elif project_id:
            query = query.filter(SourceRecord.project_id == project_id)
    commit = query.first()
    if not commit:
        raise HTTPException(status_code=404, detail="Commit not found")
    return commit

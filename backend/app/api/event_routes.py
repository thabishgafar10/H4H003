"""
API routes for querying Temporal Events, Evidence, and Issue Timelines.
"""

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.db.database import get_db
from app.models.temporal_event import TemporalEvent
from app.models.issue import Issue
from app.models.source_record import SourceRecord
from app.services.extractors.base import ALL_EVENT_TYPES
from app.schemas.temporal_event import (
    TemporalEventResponse,
    IssueTimelineResponse,
)

router = APIRouter()

TAXONOMY_DESCRIPTIONS = {
    "ISSUE_CREATED": "Issue created by reporter",
    "ISSUE_RESOLVED": "Issue status marked resolved or closed",
    "FEATURE_PROPOSED": "New feature, proposal, or KIP initiated",
    "DECISION_APPROVED": "Explicit decision approval, LGTM, or vote passing",
    "FEATURE_IMPLEMENTED": "Feature or support implementation committed to codebase",
    "FEATURE_MODIFIED": "Existing feature updated, enhanced, or refactored",
    "FEATURE_REMOVED": "Deprecated feature, protocol, or legacy module removed",
    "BUG_REPORTED": "Defect or bug issue reported",
    "CONCERN_RAISED": "Objection, blocker, compatibility concern, or -1 vote raised",
    "DECISION_REVERSED": "Prior decision reverted, proposal withdrawn, or issue reopened",
}


@router.get("/taxonomy")
def get_event_taxonomy():
    """Returns the canonical 10-type temporal event taxonomy."""
    return [
        {"event_type": et, "description": TAXONOMY_DESCRIPTIONS.get(et, "")}
        for et in sorted(ALL_EVENT_TYPES)
    ]


@router.get("", response_model=List[TemporalEventResponse])
def get_temporal_events(
    response: Response,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    dataset_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    event_type: Optional[str] = Query(None),
    issue_id: Optional[str] = Query(None),
    commit_hash: Optional[str] = Query(None),
    actor: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(TemporalEvent).options(joinedload(TemporalEvent.evidence))

    if project_id and dataset_name:
        query = query.filter((TemporalEvent.project_id == project_id) | (TemporalEvent.dataset_name == dataset_name))
    elif project_id:
        query = query.filter(TemporalEvent.project_id == project_id)
    elif dataset_name:
        query = query.filter(TemporalEvent.dataset_name == dataset_name)

    if event_type and event_type.strip():
        cleaned_type = event_type.strip().upper()
        query = query.filter(func.upper(TemporalEvent.event_type) == cleaned_type)

    if issue_id and issue_id.strip():
        cleaned_issue = issue_id.strip().upper()
        query = query.filter(func.upper(TemporalEvent.issue_id) == cleaned_issue)

    if commit_hash and commit_hash.strip():
        cleaned_hash = commit_hash.strip().lower()
        query = query.filter(func.lower(TemporalEvent.commit_hash).like(f"{cleaned_hash}%"))

    if actor and actor.strip():
        cleaned_actor = actor.strip()
        query = query.filter(TemporalEvent.actor.ilike(f"%{cleaned_actor}%"))

    if search and search.strip():
        term = f"%{search.strip()}%"
        query = query.filter((TemporalEvent.subject.ilike(term)) | (TemporalEvent.description.ilike(term)))

    if start_date:
        query = query.filter(TemporalEvent.event_time >= start_date)
    if end_date:
        query = query.filter(TemporalEvent.event_time <= end_date)

    # Compute total count for client-side pagination & filter feedback
    total_count = query.count()
    response.headers["X-Total-Count"] = str(total_count)

    return query.order_by(TemporalEvent.event_time.asc().nullslast(), TemporalEvent.id.asc()).offset(skip).limit(limit).all()


@router.get("/issues/{issue_id}/timeline", response_model=IssueTimelineResponse)
def get_issue_timeline(
    issue_id: str,
    project_id: Optional[int] = Query(None),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Returns the complete chronological event timeline for a given issue.
    """
    cleaned_id = (issue_id or "").strip()
    issue_q = db.query(Issue).filter(func.upper(Issue.source_issue_id) == cleaned_id.upper())
    ev_q = (
        db.query(TemporalEvent)
        .options(joinedload(TemporalEvent.evidence))
        .filter(func.upper(TemporalEvent.issue_id) == cleaned_id.upper())
    )

    if project_id and dataset_name:
        issue_q = issue_q.join(SourceRecord, Issue.source_record_id == SourceRecord.id).filter(
            (SourceRecord.project_id == project_id) | (SourceRecord.dataset_name == dataset_name)
        )
        ev_q = ev_q.filter(
            (TemporalEvent.project_id == project_id) | (TemporalEvent.dataset_name == dataset_name)
        )
    elif project_id:
        issue_q = issue_q.join(SourceRecord, Issue.source_record_id == SourceRecord.id).filter(
            SourceRecord.project_id == project_id
        )
        ev_q = ev_q.filter(TemporalEvent.project_id == project_id)
    elif dataset_name:
        issue_q = issue_q.join(SourceRecord, Issue.source_record_id == SourceRecord.id).filter(
            SourceRecord.dataset_name == dataset_name
        )
        ev_q = ev_q.filter(TemporalEvent.dataset_name == dataset_name)

    issue = issue_q.first()
    if not issue:
        # Fallback for synthetic issues in tests
        fallback = db.query(Issue).filter(func.upper(Issue.source_issue_id) == cleaned_id.upper()).first()
        if fallback and (not fallback.source_record_id or not project_id):
            issue = fallback

    events = ev_q.order_by(TemporalEvent.event_time.asc().nullslast(), TemporalEvent.id.asc()).all()
    canonical_id = issue.source_issue_id if issue else cleaned_id.upper()

    return IssueTimelineResponse(
        issue_id=canonical_id,
        summary=issue.summary if issue else None,
        status=issue.status if issue else None,
        total_events=len(events),
        timeline=events,
    )


@router.get("/{event_id}", response_model=TemporalEventResponse)
def get_event_by_id(
    event_id: int,
    project_id: Optional[int] = Query(None),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = (
        db.query(TemporalEvent)
        .options(joinedload(TemporalEvent.evidence))
        .filter(TemporalEvent.id == event_id)
    )
    if project_id and dataset_name:
        query = query.filter((TemporalEvent.project_id == project_id) | (TemporalEvent.dataset_name == dataset_name))
    elif project_id:
        query = query.filter(TemporalEvent.project_id == project_id)
    elif dataset_name:
        query = query.filter(TemporalEvent.dataset_name == dataset_name)

    event = query.first()
    if not event:
        raise HTTPException(status_code=404, detail=f"Temporal event {event_id} not found")
    return event

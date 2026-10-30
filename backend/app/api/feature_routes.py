"""
API routes for Feature Identity and Entity Resolution (Work 04).
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

from app.db.database import get_db
from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.temporal_event import TemporalEvent
from app.services.feature_service import FeatureService
from app.schemas.feature import (
    FeatureResponse,
    FeatureResolveRequest,
    FeatureResolveResponse,
    FeatureResolveCandidateResponse,
)
from app.schemas.temporal_event import TemporalEventResponse

router = APIRouter()


@router.get("", response_model=List[FeatureResponse])
def get_features(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    dataset_name: Optional[str] = Query(None),
    project_id: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = (
        db.query(Feature)
        .options(
            joinedload(Feature.aliases),
            joinedload(Feature.record_links),
        )
    )
    if project_id and dataset_name:
        query = query.filter((Feature.project_id == project_id) | (Feature.dataset_name == dataset_name))
    elif project_id:
        query = query.filter(Feature.project_id == project_id)
    elif dataset_name:
        query = query.filter(Feature.dataset_name == dataset_name)
    if category:
        query = query.filter(Feature.feature_category == category.upper())
    if status:
        query = query.filter(Feature.resolution_status == status.upper())
    if q:
        search = f"%{q}%"
        query = query.filter(
            (Feature.canonical_name.ilike(search)) |
            (Feature.description.ilike(search))
        )

    return query.order_by(Feature.canonical_name.asc()).offset(skip).limit(limit).all()


@router.get("/{feature_id}", response_model=FeatureResponse)
def get_feature_by_id(
    feature_id: int,
    project_id: Optional[int] = Query(None),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = (
        db.query(Feature)
        .options(
            joinedload(Feature.aliases),
            joinedload(Feature.record_links),
        )
        .filter(Feature.id == feature_id)
    )
    if project_id and dataset_name:
        query = query.filter((Feature.project_id == project_id) | (Feature.dataset_name == dataset_name))
    elif project_id:
        query = query.filter(Feature.project_id == project_id)
    elif dataset_name:
        query = query.filter(Feature.dataset_name == dataset_name)
    feature = query.first()
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature with id {feature_id} not found")
    return feature


@router.get("/{feature_id}/history", response_model=List[TemporalEventResponse])
def get_feature_history(
    feature_id: int,
    project_id: Optional[int] = Query(None),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Returns the chronological timeline of events linked to this feature.
    """
    f_query = db.query(Feature).filter(Feature.id == feature_id)
    if project_id and dataset_name:
        f_query = f_query.filter((Feature.project_id == project_id) | (Feature.dataset_name == dataset_name))
    elif project_id:
        f_query = f_query.filter(Feature.project_id == project_id)
    elif dataset_name:
        f_query = f_query.filter(Feature.dataset_name == dataset_name)
    feature = f_query.first()
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature with id {feature_id} not found")

    # Fetch event IDs linked to this feature
    event_links = (
        db.query(FeatureRecordLink)
        .filter(
            FeatureRecordLink.feature_id == feature_id,
            FeatureRecordLink.record_type == "temporal_event",
        )
        .all()
    )
    event_ids = [int(l.record_id) for l in event_links if l.record_id.isdigit()]

    # Also fetch events matching linked issues
    issue_links = (
        db.query(FeatureRecordLink)
        .filter(
            FeatureRecordLink.feature_id == feature_id,
            FeatureRecordLink.record_type == "issue",
        )
        .all()
    )
    issue_ids = [l.record_id for l in issue_links]

    ev_query = (
        db.query(TemporalEvent)
        .options(joinedload(TemporalEvent.evidence))
        .filter(
            (TemporalEvent.id.in_(event_ids)) |
            (TemporalEvent.issue_id.in_(issue_ids))
        )
    )
    if feature.project_id:
        ev_query = ev_query.filter(
            (TemporalEvent.project_id == feature.project_id) | (TemporalEvent.dataset_name == feature.dataset_name)
        )
    elif feature.dataset_name:
        ev_query = ev_query.filter(TemporalEvent.dataset_name == feature.dataset_name)

    events = ev_query.order_by(TemporalEvent.event_time.asc().nullslast(), TemporalEvent.id.asc()).all()
    return events


@router.post("/resolve", response_model=FeatureResolveResponse)
def resolve_feature_identity(
    body: Optional[FeatureResolveRequest] = None,
    query: Optional[str] = Query(None, description="Feature name, KIP, alias, or description text"),
    dataset_name: Optional[str] = Query("KafkaSQLite"),
    project_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    q_str = (body.query if body else None) or query
    if not q_str:
        raise HTTPException(status_code=422, detail="Query parameter or JSON body required")
    ds = (body.dataset_name if body and body.dataset_name else None) or dataset_name or "KafkaSQLite"
    service = FeatureService(db, dataset_name=ds, project_id=project_id)
    matches = service.resolve_feature(q_str)

    candidate_responses = [
        FeatureResolveCandidateResponse(
            feature_id=m.feature.id,
            canonical_name=m.feature.canonical_name,
            matched_alias=m.matched_alias.alias_name if m.matched_alias else None,
            match_type=m.match_type,
            confidence=m.confidence,
            resolution_status=m.resolution_status,
        )
        for m in matches
    ]

    # Flag ambiguity if multiple high-confidence matches are close
    is_ambiguous = len(candidate_responses) > 1 and (
        candidate_responses[0].confidence - candidate_responses[1].confidence < 0.10
    )

    return FeatureResolveResponse(
        query=q_str,
        total_matches=len(candidate_responses),
        is_ambiguous=is_ambiguous,
        candidates=candidate_responses,
    )


@router.post("/seed")
def seed_core_features(
    dataset_name: Optional[str] = Query("KafkaSQLite"),
    db: Session = Depends(get_db)
):
    service = FeatureService(db, dataset_name=dataset_name or "KafkaSQLite")
    seeded_count = service.seed_kafka_core_features()
    links_count = service.link_features_to_events_and_issues()
    return {
        "status": "success",
        "dataset_name": dataset_name,
        "features_seeded": seeded_count,
        "links_established": links_count,
    }


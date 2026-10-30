from typing import List, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

from app.db.database import get_db
from app.models.project import Project
from app.models.source_connection import SourceConnection
from app.models.source_record import SourceRecord
from app.models.temporal_event import TemporalEvent
from app.models.feature import Feature, FeatureRecordLink
from app.models.ingestion_run import IngestionRun
from app.models.issue import Issue
from app.models.commit import Commit
from app.schemas.project import ProjectCreate, ProjectUpdate, ProjectResponse
from app.schemas.ingestion_run import IngestionRunResponse
from app.schemas.issue import IssueResponse
from app.schemas.commit import CommitResponse
from app.schemas.feature import FeatureResponse
from app.schemas.temporal_event import TemporalEventResponse
from app.schemas.graph import GraphResponse
from app.schemas.qa import QARequest, QAResponse
from app.services.project_ingestion_service import ProjectIngestionService
from app.services.feature_service import FeatureService
from app.services.knowledge_graph_service import KnowledgeGraphService
from app.services.qa_service import HistoricalQAService

router = APIRouter(prefix="/api/projects", tags=["Projects"])


def _enrich_project_response(db: Session, project: Project) -> ProjectResponse:
    sources_cnt = db.query(func.count(SourceConnection.id)).filter(SourceConnection.project_id == project.id).scalar() or 0
    records_cnt = db.query(func.count(SourceRecord.id)).filter(
        (SourceRecord.project_id == project.id) | (SourceRecord.dataset_name == project.slug)
    ).scalar() or 0
    events_cnt = db.query(func.count(TemporalEvent.id)).filter(
        (TemporalEvent.project_id == project.id) | (TemporalEvent.dataset_name == project.slug)
    ).scalar() or 0
    features_cnt = db.query(func.count(Feature.id)).filter(
        (Feature.project_id == project.id) | (Feature.dataset_name == project.slug)
    ).scalar() or 0

    return ProjectResponse(
        id=project.id,
        name=project.name,
        slug=project.slug,
        description=project.description,
        domain=project.domain,
        status=project.status,
        created_at=project.created_at,
        updated_at=project.updated_at,
        sources_count=sources_cnt,
        records_count=records_cnt,
        events_count=events_cnt,
        features_count=features_cnt,
    )


@router.post("", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
def create_project(req: ProjectCreate, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    try:
        project = svc.create_project(
            name=req.name,
            slug=req.slug,
            description=req.description,
            domain=req.domain,
        )
        return _enrich_project_response(db, project)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=List[ProjectResponse])
def list_projects(db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    projects = svc.list_projects()
    return [_enrich_project_response(db, p) for p in projects]


@router.get("/{project_id_or_slug}", response_model=ProjectResponse)
def get_project(project_id_or_slug: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")
    return _enrich_project_response(db, project)


@router.put("/{project_id_or_slug}", response_model=ProjectResponse)
def update_project(project_id_or_slug: str, req: ProjectUpdate, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    if req.name is not None:
        project.name = req.name.strip()
    if req.description is not None:
        project.description = req.description.strip()
    if req.domain is not None:
        project.domain = req.domain.strip()
    if req.status is not None:
        project.status = req.status.strip()

    db.commit()
    db.refresh(project)
    return _enrich_project_response(db, project)


@router.get("/{project_id_or_slug}/runs", response_model=List[IngestionRunResponse])
def list_project_runs(project_id_or_slug: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    runs = (
        db.query(IngestionRun)
        .filter(IngestionRun.project_id == project.id)
        .order_by(IngestionRun.id.desc())
        .all()
    )
    return runs


@router.get("/{project_id_or_slug}/issues", response_model=List[IssueResponse])
def list_project_issues(
    project_id_or_slug: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    issues = (
        db.query(Issue)
        .join(SourceRecord, Issue.source_record_id == SourceRecord.id)
        .filter((SourceRecord.project_id == project.id) | (SourceRecord.dataset_name == project.slug))
        .order_by(Issue.id.asc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return issues


@router.get("/{project_id_or_slug}/issues/{issue_id}", response_model=IssueResponse)
def get_project_issue(project_id_or_slug: str, issue_id: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    issue = (
        db.query(Issue)
        .join(SourceRecord, Issue.source_record_id == SourceRecord.id)
        .filter(
            Issue.source_issue_id == issue_id,
            (SourceRecord.project_id == project.id) | (SourceRecord.dataset_name == project.slug),
        )
        .first()
    )
    if not issue:
        raise HTTPException(status_code=404, detail=f"Issue '{issue_id}' not found in project '{project_id_or_slug}'.")
    return issue


@router.get("/{project_id_or_slug}/commits/{commit_hash}", response_model=CommitResponse)
def get_project_commit(project_id_or_slug: str, commit_hash: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    commit = (
        db.query(Commit)
        .join(SourceRecord, Commit.source_record_id == SourceRecord.id)
        .filter(
            Commit.commit_hash == commit_hash,
            (SourceRecord.project_id == project.id) | (SourceRecord.dataset_name == project.slug),
        )
        .first()
    )
    if not commit:
        raise HTTPException(status_code=404, detail=f"Commit '{commit_hash}' not found in project '{project_id_or_slug}'.")
    return commit


@router.get("/{project_id_or_slug}/features", response_model=List[FeatureResponse])
def list_project_features(
    project_id_or_slug: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    features = (
        db.query(Feature)
        .options(joinedload(Feature.aliases), joinedload(Feature.record_links))
        .filter((Feature.project_id == project.id) | (Feature.dataset_name == project.slug))
        .order_by(Feature.canonical_name.asc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return features


@router.get("/{project_id_or_slug}/features/{feature_id}", response_model=FeatureResponse)
def get_project_feature(project_id_or_slug: str, feature_id: int, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    feature = (
        db.query(Feature)
        .options(joinedload(Feature.aliases), joinedload(Feature.record_links))
        .filter(
            Feature.id == feature_id,
            (Feature.project_id == project.id) | (Feature.dataset_name == project.slug),
        )
        .first()
    )
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature {feature_id} not found in project '{project_id_or_slug}'.")
    return feature


@router.get("/{project_id_or_slug}/events", response_model=List[TemporalEventResponse])
def list_project_events(
    project_id_or_slug: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    events = (
        db.query(TemporalEvent)
        .options(joinedload(TemporalEvent.evidence))
        .filter((TemporalEvent.project_id == project.id) | (TemporalEvent.dataset_name == project.slug))
        .order_by(TemporalEvent.event_time.asc().nullslast(), TemporalEvent.id.asc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return events


@router.get("/{project_id_or_slug}/events/{event_id}", response_model=TemporalEventResponse)
def get_project_event(project_id_or_slug: str, event_id: int, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    event = (
        db.query(TemporalEvent)
        .options(joinedload(TemporalEvent.evidence))
        .filter(
            TemporalEvent.id == event_id,
            (TemporalEvent.project_id == project.id) | (TemporalEvent.dataset_name == project.slug),
        )
        .first()
    )
    if not event:
        raise HTTPException(status_code=404, detail=f"Temporal event {event_id} not found in project '{project_id_or_slug}'.")
    return event


@router.get("/{project_id_or_slug}/graph/features/{feature_id}", response_model=GraphResponse)
def get_project_feature_graph(
    project_id_or_slug: str,
    feature_id: int,
    depth: int = Query(2, ge=1, le=4),
    max_nodes: int = Query(80, ge=5, le=300),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    graph_svc = KnowledgeGraphService(db, dataset_name=project.slug, project_id=project.id)
    graph = graph_svc.get_feature_graph(
        feature_id=feature_id,
        depth=depth,
        max_nodes=max_nodes,
    )
    if graph.node_count == 0:
        raise HTTPException(status_code=404, detail=f"Feature {feature_id} not found in project graph.")
    return graph


@router.post("/{project_id_or_slug}/qa/ask", response_model=QAResponse)
def ask_project_question(
    project_id_or_slug: str,
    request: QARequest,
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    qa_svc = HistoricalQAService(db, dataset_name=project.slug, project_id=project.id)
    request.project_id = project.id
    request.dataset_name = project.slug
    return qa_svc.answer_question(request)

from typing import List, Union
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db.database import get_db
from app.models.project import Project
from app.models.source_connection import SourceConnection
from app.models.source_record import SourceRecord
from app.models.temporal_event import TemporalEvent
from app.models.feature import Feature
from app.models.ingestion_run import IngestionRun
from app.schemas.project import ProjectCreate, ProjectUpdate, ProjectResponse
from app.schemas.ingestion_run import IngestionRunResponse
from app.services.project_ingestion_service import ProjectIngestionService

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

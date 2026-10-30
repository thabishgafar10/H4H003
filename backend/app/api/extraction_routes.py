"""
API routes for Temporal Event Extraction Runs, Status, and Statistics.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.extraction_run import ExtractionRun
from app.models.event_candidate import EventCandidate
from app.services.extraction_orchestrator import ExtractionOrchestrator
from app.schemas.extraction import (
    ExtractionRunRequest,
    ExtractionRunResponse,
    ExtractionStatsResponse,
)
from app.schemas.temporal_event import EventCandidateResponse

router = APIRouter()


@router.post("/run", response_model=ExtractionRunResponse)
def trigger_extraction_run(
    req: ExtractionRunRequest,
    db: Session = Depends(get_db)
):
    orchestrator = ExtractionOrchestrator(db, dataset_name=req.dataset_name)
    try:
        run = orchestrator.start_extraction_run(
            extraction_mode=req.extraction_mode,
            chunk_size=req.chunk_size,
            max_issues=req.max_issues,
            max_comments=req.max_comments,
            max_commits=req.max_commits,
        )
        return run
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")


@router.get("/status", response_model=ExtractionRunResponse)
def get_latest_extraction_status(
    dataset_name: Optional[str] = Query("KafkaSQLite"),
    db: Session = Depends(get_db)
):
    query = db.query(ExtractionRun)
    if dataset_name:
        query = query.filter(ExtractionRun.dataset_name == dataset_name)
    run = query.order_by(ExtractionRun.id.desc()).first()
    if not run:
        raise HTTPException(status_code=404, detail="No extraction runs found")
    return run


@router.get("/runs", response_model=List[ExtractionRunResponse])
def get_extraction_runs(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(ExtractionRun)
    if dataset_name:
        query = query.filter(ExtractionRun.dataset_name == dataset_name)
    return query.order_by(ExtractionRun.id.desc()).offset(skip).limit(limit).all()


@router.get("/runs/{run_id}", response_model=ExtractionRunResponse)
def get_extraction_run_by_id(
    run_id: int,
    db: Session = Depends(get_db)
):
    run = db.query(ExtractionRun).filter(ExtractionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Extraction run {run_id} not found")
    return run


@router.get("/statistics", response_model=ExtractionStatsResponse)
def get_extraction_statistics(
    dataset_name: Optional[str] = Query("KafkaSQLite"),
    db: Session = Depends(get_db)
):
    orchestrator = ExtractionOrchestrator(db, dataset_name=dataset_name or "KafkaSQLite")
    return orchestrator.get_statistics()


@router.get("/candidates", response_model=List[EventCandidateResponse])
def get_event_candidates(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    dataset_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    confidence_band: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(EventCandidate)
    if dataset_name:
        query = query.filter(EventCandidate.dataset_name == dataset_name)
    if status:
        query = query.filter(EventCandidate.validation_status == status)
    if event_type:
        query = query.filter(EventCandidate.event_type == event_type)
    if confidence_band:
        query = query.filter(EventCandidate.confidence_band == confidence_band)
    return query.order_by(EventCandidate.id.desc()).offset(skip).limit(limit).all()

"""
FastAPI router for Work 06: Historical Natural-Language Question Answering.
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.qa import QARequest, QAResponse, SampleQuestion
from app.services.qa_service import HistoricalQAService

router = APIRouter(prefix="/api/qa", tags=["Historical QA"])


@router.post("/ask", response_model=QAResponse)
def ask_question(
    request: QARequest,
    db: Session = Depends(get_db),
):
    """Answers a natural-language historical question grounded in extracted temporal events and evidence."""
    service = HistoricalQAService(db, dataset_name=request.dataset_name)
    return service.answer_question(request)


@router.get("/sample-questions", response_model=List[SampleQuestion])
def get_sample_questions(
    db: Session = Depends(get_db),
):
    """Returns curated benchmark questions demonstrating temporal reasoning across software evolution."""
    service = HistoricalQAService(db)
    return service.get_sample_questions()


@router.get("/health")
def qa_health(
    db: Session = Depends(get_db),
):
    """Health check for QA engine components."""
    service = HistoricalQAService(db)
    samples = service.get_sample_questions()
    return {
        "status": "healthy",
        "sample_questions_count": len(samples),
        "engine": "HistoricalQAService",
    }


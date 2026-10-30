from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.services.ingestion_service import IngestionService
from app.core.config import settings
from app.schemas.ingestion import IngestionRunResponse, IngestionBatchResponse
from app.models.ingestion_batch import IngestionBatch
from typing import List

router = APIRouter()

@router.post("/run", response_model=IngestionRunResponse)
def run_ingestion(db: Session = Depends(get_db)):
    service = IngestionService(db, str(settings.SOURCE_DB_PATH))
    batch = service.run_ingestion()
    return {
        "batch_id": batch.id,
        "status": batch.status,
        "started_at": batch.started_at,
        "message": "Ingestion completed successfully"
    }

@router.get("/status", response_model=IngestionBatchResponse)
def get_ingestion_status(db: Session = Depends(get_db)):
    batch = db.query(IngestionBatch).order_by(IngestionBatch.started_at.desc()).first()
    if not batch:
        raise HTTPException(status_code=404, detail="No ingestion batches found")
    return batch

@router.get("/batches", response_model=List[IngestionBatchResponse])
def get_ingestion_batches(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return db.query(IngestionBatch).order_by(IngestionBatch.started_at.desc()).offset(skip).limit(limit).all()

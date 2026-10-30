from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.services.ingestion_service import IngestionService
from app.core.config import settings
from app.schemas.ingestion import IngestionRunResponse, IngestionBatchResponse, UnresolvedRelationshipResponse
from app.models.ingestion_batch import IngestionBatch
from app.models.unresolved_relationship import UnresolvedRelationship
from typing import List, Optional
from fastapi import Query

router = APIRouter()

@router.post("/run", response_model=IngestionRunResponse)
def run_ingestion(dataset_name: Optional[str] = Query(None), db: Session = Depends(get_db)):
    service = IngestionService(db, str(settings.SOURCE_DB_PATH), dataset_name=dataset_name or "KafkaSQLite")
    batch = service.run_ingestion()
    return {
        "batch_id": batch.id,
        "status": batch.status,
        "started_at": batch.started_at,
        "message": "Ingestion completed successfully"
    }

@router.get("/status", response_model=IngestionBatchResponse)
def get_ingestion_status(dataset_name: Optional[str] = Query(None), db: Session = Depends(get_db)):
    query = db.query(IngestionBatch)
    if dataset_name:
        query = query.filter(IngestionBatch.dataset_name == dataset_name)
    batch = query.order_by(IngestionBatch.started_at.desc()).first()
    if not batch:
        raise HTTPException(status_code=404, detail="No ingestion batches found")
    return batch

@router.get("/batches", response_model=List[IngestionBatchResponse])
def get_ingestion_batches(
    skip: int = 0,
    limit: int = 10,
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(IngestionBatch)
    if dataset_name:
        query = query.filter(IngestionBatch.dataset_name == dataset_name)
    return query.order_by(IngestionBatch.started_at.desc()).offset(skip).limit(limit).all()

@router.get("/unresolved", response_model=List[UnresolvedRelationshipResponse])
def get_unresolved_relationships(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    source_table: Optional[str] = Query(None),
    dataset_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(UnresolvedRelationship)
    if dataset_name:
        query = query.filter(UnresolvedRelationship.dataset_name == dataset_name)
    if source_table:
        query = query.filter(UnresolvedRelationship.source_table == source_table)
    return query.order_by(UnresolvedRelationship.id).offset(skip).limit(limit).all()

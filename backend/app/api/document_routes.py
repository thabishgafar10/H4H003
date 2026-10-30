from typing import List
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.document import Document
from app.schemas.document import DocumentResponse, DocumentUrlRequest
from app.services.project_ingestion_service import ProjectIngestionService

router = APIRouter(prefix="/api/projects/{project_id_or_slug}/documents", tags=["Documents"])


@router.post("/upload", response_model=DocumentResponse, status_code=status.HTTP_201_CREATED)
async def upload_document(
    project_id_or_slug: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    try:
        content = await file.read()
        doc, run = svc.ingest_document_bytes(
            project_id=project.id,
            filename=file.filename or "document.txt",
            file_bytes=content,
        )
        if not doc or run.status == "FAILED":
            raise HTTPException(status_code=400, detail=run.error_summary or "Document processing failed.")
        return doc
    except ValueError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Document upload failed: {str(e)}")


@router.post("/url", response_model=DocumentResponse, status_code=status.HTTP_201_CREATED)
def ingest_document_from_url(
    project_id_or_slug: str,
    req: DocumentUrlRequest,
    db: Session = Depends(get_db),
):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    try:
        doc, run = svc.ingest_document_url(
            project_id=project.id,
            url=req.url,
        )
        if not doc or run.status == "FAILED":
            raise HTTPException(status_code=400, detail=run.error_summary or "Document URL ingestion failed.")
        return doc
    except ValueError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Document URL ingestion failed: {str(e)}")


@router.get("", response_model=List[DocumentResponse])
def list_project_documents(project_id_or_slug: str, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    docs = (
        db.query(Document)
        .filter(Document.project_id == project.id)
        .order_by(Document.id.desc())
        .all()
    )
    return docs


@router.get("/{document_id}", response_model=DocumentResponse)
def get_document_details(project_id_or_slug: str, document_id: int, db: Session = Depends(get_db)):
    svc = ProjectIngestionService(db)
    project = svc.get_project(project_id_or_slug)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id_or_slug}' not found.")

    doc = (
        db.query(Document)
        .filter(
            Document.project_id == project.id,
            Document.id == document_id,
        )
        .first()
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document {document_id} not found in project.")
    return doc

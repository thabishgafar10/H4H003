"""
Document Normalizer.
Converts parsed document content into Document model rows and SourceRecord chunks
with SHA-256 verification and provenance tracking.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.integrations.documents.parsers import parse_document
from app.integrations.documents.security import sanitize_filename, validate_file_upload
from app.models.document import Document
from app.models.source_record import SourceRecord


def calculate_bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 150) -> List[str]:
    """Split text into overlapping chunks if it exceeds chunk_size."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            break_pt = text.rfind("\n", start, end)
            if break_pt == -1 or break_pt < start + (chunk_size // 2):
                break_pt = text.rfind(". ", start, end)
            if break_pt != -1 and break_pt > start + (chunk_size // 2):
                end = break_pt + 1
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap if end < len(text) else len(text)
    return chunks


class DocumentNormalizer:
    @classmethod
    def process_file_bytes(
        cls,
        filename: str,
        file_bytes: bytes,
        project_id: int,
        dataset_name: str,
        source_connection_id: Optional[int] = None,
        source_url: Optional[str] = None,
    ) -> Tuple[Document, List[SourceRecord]]:
        """
        Validate, parse, and normalize document file bytes into a Document model and SourceRecord chunks.
        """
        clean_filename = sanitize_filename(filename)
        validate_file_upload(clean_filename, len(file_bytes))

        checksum = calculate_bytes_sha256(file_bytes)
        ext = os.path.splitext(clean_filename.lower())[1].lstrip(".").upper()

        full_text, sections = parse_document(clean_filename, file_bytes)

        # 1. Document Model
        doc_model = Document(
            project_id=project_id,
            source_connection_id=source_connection_id,
            filename=clean_filename,
            file_type=ext,
            file_size=len(file_bytes),
            sha256_hash=checksum,
            source_url=source_url,
            extracted_text=full_text,
            page_count=len(sections),
            metadata_json=json.dumps({"page_count": len(sections), "chunk_count": len(sections)}),
            created_at=datetime.now(timezone.utc),
        )

        # 2. SourceRecord chunks for searchable provenance and knowledge extraction
        source_records: List[SourceRecord] = []
        chunk_counter = 0

        for sec in sections:
            page_num = sec.get("page_number", 1)
            sec_title = sec.get("section_title", "")
            sec_text = sec.get("text", "")

            text_chunks = chunk_text(sec_text)
            for chunk_str in text_chunks:
                chunk_counter += 1
                meta = {
                    "filename": clean_filename,
                    "sha256_hash": checksum,
                    "page_number": page_num,
                    "section_title": sec_title,
                    "chunk_index": chunk_counter,
                    "source_url": source_url,
                    "text": chunk_str,
                }
                chunk_hash = hashlib.sha256(chunk_str.encode("utf-8")).hexdigest()

                record = SourceRecord(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    source_table="documents",
                    source_record_id=f"doc-{checksum[:12]}-c{chunk_counter}",
                    source_record_hash=chunk_hash,
                    raw_data=meta,
                    source_created_at=datetime.now(timezone.utc),
                    source_updated_at=datetime.now(timezone.utc),
                    ingestion_batch_id=0,
                )
                source_records.append(record)

        return doc_model, source_records

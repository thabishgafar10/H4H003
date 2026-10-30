"""
Document Model for Work 07.
Tracks uploaded documents and document URLs with parsed content, page markers, and SHA-256 hashes.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    source_connection_id = Column(Integer, ForeignKey("source_connections.id", ondelete="SET NULL"), nullable=True)
    filename = Column(String(255), nullable=False)
    file_type = Column(String(50), nullable=False)  # PDF, DOCX, TXT, MARKDOWN
    source_url = Column(String(1000), nullable=True)
    file_size = Column(Integer, nullable=False)
    sha256_hash = Column(String(64), nullable=False, index=True)
    page_count = Column(Integer, nullable=True)
    extracted_text = Column(Text, nullable=False)
    metadata_json = Column(Text, nullable=True)  # JSON metadata: title, author, dates, headers
    created_at = Column(DateTime, server_default=func.now())

    # Relationships
    project = relationship("Project", back_populates="documents")
    source_connection = relationship("SourceConnection", back_populates="documents")

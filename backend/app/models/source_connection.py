"""
Source Connection Model for Work 07.
Tracks connected data sources for a project (Jira Cloud, GitHub Public, Document Uploads, Document URLs).
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class SourceConnection(Base):
    __tablename__ = "source_connections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    source_type = Column(String(50), nullable=False)  # JIRA_CLOUD, GITHUB_PUBLIC, DOCUMENT_UPLOAD, DOCUMENT_URL, LEGACY_DATASET
    name = Column(String(255), nullable=False)
    config = Column(Text, nullable=False)  # JSON configuration string (URLs, keys, repos)
    auth_type = Column(String(50), nullable=False, default="NONE")  # NONE, API_TOKEN, OAUTH2
    credentials_encrypted = Column(Text, nullable=True)  # Securely stored credentials
    status = Column(String(50), nullable=False, default="CONNECTED")  # CONNECTED, DISCONNECTED, ERROR
    last_sync_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships
    project = relationship("Project", back_populates="source_connections")
    documents = relationship("Document", back_populates="source_connection")
    ingestion_runs = relationship("IngestionRun", back_populates="source_connection")

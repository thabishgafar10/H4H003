"""
Project Model for Work 07.
Represents a software product workspace / product intelligence project.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(100), nullable=False, unique=True, index=True)
    description = Column(Text, nullable=True)
    domain = Column(String(100), nullable=True)
    status = Column(String(50), nullable=False, default="READY")  # CREATED, CONFIGURING, INGESTING, READY, FAILED, ARCHIVED
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships
    source_connections = relationship("SourceConnection", back_populates="project", cascade="all, delete-orphan")
    documents = relationship("Document", back_populates="project", cascade="all, delete-orphan")
    ingestion_runs = relationship("IngestionRun", back_populates="project", cascade="all, delete-orphan")

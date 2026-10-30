from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base import Base

class Issue(Base):
    __tablename__ = "issues"

    id = Column(Integer, primary_key=True)
    source_issue_id = Column(String, unique=True, nullable=False, index=True)
    issue_type = Column(String)
    summary = Column(String)
    description = Column(String)
    priority = Column(String)
    status = Column(String)
    resolution = Column(String)
    assignee = Column(String)
    reporter = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    resolved_at = Column(DateTime)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    source_record = relationship("SourceRecord")
    comments = relationship("IssueComment", back_populates="issue")
    components = relationship("IssueComponent", back_populates="issue")
    fix_versions = relationship("IssueFixVersion", back_populates="issue")

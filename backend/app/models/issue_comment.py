from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from app.db.base import Base

class IssueComment(Base):
    __tablename__ = "issue_comments"

    id = Column(Integer, primary_key=True)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))
    source_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    username = Column(String)
    display_name = Column(String)
    message = Column(Text)
    created_at = Column(DateTime)

    issue = relationship("Issue", back_populates="comments")
    source_record = relationship("SourceRecord")


from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.orm import relationship
from app.db.base import Base

class Commit(Base):
    __tablename__ = "commits"

    id = Column(Integer, primary_key=True)
    commit_hash = Column(String, unique=True, nullable=False, index=True)
    message = Column(Text)
    author = Column(String)
    author_email = Column(String)
    committed_at = Column(DateTime)
    is_merge = Column(Boolean)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    source_record = relationship("SourceRecord")
    code_changes = relationship("CodeChange", back_populates="commit")

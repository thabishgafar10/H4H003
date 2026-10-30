from sqlalchemy import Column, Integer, String, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from app.db.base import Base

class CodeChange(Base):
    __tablename__ = "code_changes"

    id = Column(Integer, primary_key=True)
    commit_hash = Column(String, ForeignKey("commits.commit_hash"), nullable=False, index=True)
    file_path = Column(String)
    old_file_path = Column(String)
    change_type = Column(String)
    is_deleted = Column(Boolean)
    lines_added = Column(Integer)
    lines_removed = Column(Integer)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    commit = relationship("Commit", back_populates="code_changes")
    source_record = relationship("SourceRecord")

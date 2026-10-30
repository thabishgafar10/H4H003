from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from app.db.base import Base

class IssueComponent(Base):
    __tablename__ = "issue_components"

    id = Column(Integer, primary_key=True)
    source_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    component = Column(String, nullable=False)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    issue = relationship("Issue", back_populates="components")
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint('source_issue_id', 'component', name='_issue_component_uc'),
    )

class IssueFixVersion(Base):
    __tablename__ = "issue_fix_versions"

    id = Column(Integer, primary_key=True)
    source_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    fix_version = Column(String, nullable=False)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    issue = relationship("Issue", back_populates="fix_versions")
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint('source_issue_id', 'fix_version', name='_issue_fix_version_uc'),
    )

class IssueLink(Base):
    __tablename__ = "issue_links"

    id = Column(Integer, primary_key=True)
    source_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    target_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    link_name = Column(String)
    outward_label = Column(String)
    is_containment = Column(Boolean)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    source_issue = relationship("Issue", foreign_keys=[source_issue_id])
    target_issue = relationship("Issue", foreign_keys=[target_issue_id])
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint('source_issue_id', 'target_issue_id', 'link_name', name='_issue_link_uc'),
    )

class CommitIssueLink(Base):
    __tablename__ = "commit_issue_links"

    id = Column(Integer, primary_key=True)
    commit_hash = Column(String, ForeignKey("commits.commit_hash"), nullable=False, index=True)
    source_issue_id = Column(String, ForeignKey("issues.source_issue_id"), nullable=False, index=True)
    source_record_id = Column(Integer, ForeignKey("source_records.id"))

    commit = relationship("Commit")
    issue = relationship("Issue")
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint('commit_hash', 'source_issue_id', name='_commit_issue_link_uc'),
    )

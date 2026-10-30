"""
Feature Identity and Entity Resolution Models for Work 04.
Supports canonical features, multi-alias resolution, feature renames, and traceable record links.
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Float, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.db.base import Base


class Feature(Base):
    __tablename__ = "features"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    dataset_name = Column(String, nullable=False, default="KafkaSQLite", index=True)
    canonical_name = Column(String, nullable=False, index=True)
    normalized_name = Column(String, nullable=False, index=True)
    description = Column(Text, nullable=True)
    feature_category = Column(String, nullable=False, default="CORE", index=True)
    first_observed_at = Column(DateTime, nullable=True, index=True)
    last_observed_at = Column(DateTime, nullable=True, index=True)
    identity_confidence = Column(Float, nullable=False, default=1.0)
    resolution_status = Column(String, nullable=False, default="CONFIRMED", index=True)  # CONFIRMED, NEEDS_REVIEW, REJECTED
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    aliases = relationship("FeatureAlias", back_populates="feature", cascade="all, delete-orphan")
    record_links = relationship("FeatureRecordLink", back_populates="feature", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("dataset_name", "normalized_name", name="_feature_canonical_uc"),
    )


class FeatureAlias(Base):
    __tablename__ = "feature_aliases"

    id = Column(Integer, primary_key=True)
    feature_id = Column(Integer, ForeignKey("features.id"), nullable=False, index=True)
    alias_name = Column(String, nullable=False)
    normalized_alias = Column(String, nullable=False, index=True)
    alias_type = Column(String, nullable=False, default="ALTERNATIVE_NAME", index=True)
    # ACRONYM, KIP, JIRA_KEY, HISTORICAL_NAME, RENAMED_FROM, COMPONENT, ALTERNATIVE_NAME
    source_record_id = Column(Integer, ForeignKey("source_records.id"), nullable=True, index=True)
    valid_from = Column(DateTime, nullable=True)
    valid_until = Column(DateTime, nullable=True)  # Populated when feature is renamed/superseded
    confidence = Column(Float, nullable=False, default=1.0)
    verification_status = Column(String, nullable=False, default="CONFIRMED", index=True)  # CONFIRMED, NEEDS_REVIEW, REJECTED
    created_at = Column(DateTime, server_default=func.now())

    feature = relationship("Feature", back_populates="aliases")
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint("feature_id", "normalized_alias", name="_feature_alias_uc"),
    )


class FeatureRecordLink(Base):
    __tablename__ = "feature_record_links"

    id = Column(Integer, primary_key=True)
    feature_id = Column(Integer, ForeignKey("features.id"), nullable=False, index=True)
    record_type = Column(String, nullable=False, index=True)  # issue, commit, comment, temporal_event, code_change
    record_id = Column(String, nullable=False, index=True)    # source_issue_id, commit_hash, or event_id
    relationship_type = Column(String, nullable=False, index=True)
    # INTRODUCED_BY, IMPLEMENTED_BY, MODIFIED_BY, DISCUSSED_IN, REMOVED_BY, RELATED_TO
    confidence = Column(Float, nullable=False, default=1.0)
    source_evidence = Column(Text, nullable=True)
    evidence_hash = Column(String, nullable=True, index=True)
    source_record_id = Column(Integer, ForeignKey("source_records.id"), nullable=True, index=True)
    verification_status = Column(String, nullable=False, default="CONFIRMED", index=True)
    created_at = Column(DateTime, server_default=func.now())

    feature = relationship("Feature", back_populates="record_links")
    source_record = relationship("SourceRecord")

    __table_args__ = (
        UniqueConstraint("feature_id", "record_type", "record_id", "relationship_type", name="_feature_record_link_uc"),
    )

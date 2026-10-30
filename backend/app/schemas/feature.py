"""
Pydantic schemas for Feature Identity, Aliases, Record Links, and Entity Resolution.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field


class FeatureAliasResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    feature_id: int
    alias_name: str
    normalized_alias: str
    alias_type: str
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    confidence: float
    verification_status: str
    created_at: Optional[datetime] = None


class FeatureRecordLinkResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    feature_id: int
    record_type: str
    record_id: str
    relationship_type: str
    confidence: float
    source_evidence: Optional[str] = None
    evidence_hash: Optional[str] = None
    verification_status: str
    created_at: Optional[datetime] = None


class FeatureResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_name: str
    canonical_name: str
    normalized_name: str
    description: Optional[str] = None
    feature_category: str
    first_observed_at: Optional[datetime] = None
    last_observed_at: Optional[datetime] = None
    identity_confidence: float
    resolution_status: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    aliases: List[FeatureAliasResponse] = []
    record_links: List[FeatureRecordLinkResponse] = []


class FeatureResolveCandidateResponse(BaseModel):
    feature_id: int
    canonical_name: str
    matched_alias: Optional[str] = None
    match_type: str
    confidence: float
    resolution_status: str


class FeatureResolveRequest(BaseModel):
    query: str
    dataset_name: Optional[str] = "KafkaSQLite"


class FeatureResolveResponse(BaseModel):
    query: str
    total_matches: int
    is_ambiguous: bool = False
    candidates: List[FeatureResolveCandidateResponse]


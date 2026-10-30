"""
Base definitions, constants, and helper functions for temporal event extractors.
"""

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
from typing import List, Optional, Set


# Canonical 10-type event taxonomy
EVENT_ISSUE_CREATED = "ISSUE_CREATED"
EVENT_ISSUE_RESOLVED = "ISSUE_RESOLVED"
EVENT_FEATURE_PROPOSED = "FEATURE_PROPOSED"
EVENT_DECISION_APPROVED = "DECISION_APPROVED"
EVENT_FEATURE_IMPLEMENTED = "FEATURE_IMPLEMENTED"
EVENT_FEATURE_MODIFIED = "FEATURE_MODIFIED"
EVENT_FEATURE_REMOVED = "FEATURE_REMOVED"
EVENT_BUG_REPORTED = "BUG_REPORTED"
EVENT_CONCERN_RAISED = "CONCERN_RAISED"
EVENT_DECISION_REVERSED = "DECISION_REVERSED"

ALL_EVENT_TYPES: Set[str] = {
    EVENT_ISSUE_CREATED,
    EVENT_ISSUE_RESOLVED,
    EVENT_FEATURE_PROPOSED,
    EVENT_DECISION_APPROVED,
    EVENT_FEATURE_IMPLEMENTED,
    EVENT_FEATURE_MODIFIED,
    EVENT_FEATURE_REMOVED,
    EVENT_BUG_REPORTED,
    EVENT_CONCERN_RAISED,
    EVENT_DECISION_REVERSED,
}


@dataclass
class EvidencePayload:
    source_record_id: Optional[int]
    evidence_excerpt: str
    evidence_role: str = "PRIMARY"  # PRIMARY, SUPPORTING, CONTRADICTORY
    evidence_hash: str = ""

    def __post_init__(self):
        if not self.evidence_hash and self.evidence_excerpt:
            self.evidence_hash = hashlib.sha256(self.evidence_excerpt.encode("utf-8")).hexdigest()


@dataclass
class CandidatePayload:
    dataset_name: str
    event_type: str
    event_time: Optional[datetime]
    subject: str
    description: Optional[str]
    actor: Optional[str]
    extractor_type: str  # DETERMINISTIC or LLM
    extractor_version: str = "1.0.0"
    confidence_band: str = "HIGH"  # HIGH, MEDIUM, LOW
    validation_status: str = "PENDING"  # PENDING, ACCEPTED, REJECTED, NEEDS_REVIEW
    rejection_reason: Optional[str] = None
    candidate_fingerprint: str = ""
    issue_id: Optional[str] = None
    commit_hash: Optional[str] = None
    evidence_items: List[EvidencePayload] = field(default_factory=list)


def compute_candidate_fingerprint(
    dataset_name: str,
    source_table: str,
    source_id: str,
    event_type: str,
    disambiguator: str = ""
) -> str:
    raw = f"{dataset_name}:{source_table}:{source_id}:{event_type}:{disambiguator}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_event_key(
    dataset_name: str,
    event_type: str,
    primary_entity: str,
    event_time: Optional[datetime],
    disambiguator: str = ""
) -> str:
    time_str = event_time.isoformat() if event_time else "NO_TIME"
    raw = f"{dataset_name}:{event_type}:{primary_entity}:{time_str}:{disambiguator}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

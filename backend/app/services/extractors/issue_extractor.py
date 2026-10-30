"""
Deterministic extractor for Issue records.
Extracts: ISSUE_CREATED, ISSUE_RESOLVED, BUG_REPORTED, FEATURE_PROPOSED.
"""

from typing import List, Optional
from app.models.issue import Issue
from app.services.extractors.base import (
    CandidatePayload,
    EvidencePayload,
    EVENT_ISSUE_CREATED,
    EVENT_ISSUE_RESOLVED,
    EVENT_BUG_REPORTED,
    EVENT_FEATURE_PROPOSED,
    compute_candidate_fingerprint,
)


class IssueExtractor:
    VERSION = "1.0.0"

    def __init__(self, dataset_name: str = "KafkaSQLite"):
        self.dataset_name = dataset_name

    def extract(self, issue: Issue) -> List[CandidatePayload]:
        candidates: List[CandidatePayload] = []
        issue_id = issue.source_issue_id
        src_rec_id = issue.source_record_id

        # 1. ISSUE_CREATED
        summary_text = issue.summary or issue_id
        created_evidence = EvidencePayload(
            source_record_id=src_rec_id,
            evidence_excerpt=summary_text,
            evidence_role="PRIMARY",
        )
        candidates.append(
            CandidatePayload(
                dataset_name=self.dataset_name,
                event_type=EVENT_ISSUE_CREATED,
                event_time=issue.created_at,
                subject=f"Issue created: [{issue_id}] {issue.summary or 'No summary'}",
                description=issue.description,
                actor=issue.reporter,
                extractor_type="DETERMINISTIC",
                extractor_version=self.VERSION,
                confidence_band="HIGH",
                validation_status="ACCEPTED",
                candidate_fingerprint=compute_candidate_fingerprint(
                    self.dataset_name, "issues", issue_id, EVENT_ISSUE_CREATED
                ),
                issue_id=issue_id,
                evidence_items=[created_evidence],
            )
        )

        # 2. BUG_REPORTED
        issue_type_lower = (issue.issue_type or "").lower()
        if "bug" in issue_type_lower or "defect" in issue_type_lower:
            bug_excerpt = issue.issue_type if issue.issue_type else "Bug"
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_BUG_REPORTED,
                    event_time=issue.created_at,
                    subject=f"Bug reported: [{issue_id}] {issue.summary or 'No summary'}",
                    description=issue.description,
                    actor=issue.reporter,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issues", issue_id, EVENT_BUG_REPORTED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=bug_excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 3. FEATURE_PROPOSED (from Issue type)
        feature_keywords = ["feature", "improvement", "proposal", "kip", "new feature"]
        if any(kw in issue_type_lower for kw in feature_keywords):
            feat_excerpt = issue.issue_type if issue.issue_type else "Improvement"
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_FEATURE_PROPOSED,
                    event_time=issue.created_at,
                    subject=f"Feature proposed: [{issue_id}] {issue.summary or 'No summary'}",
                    description=issue.description,
                    actor=issue.reporter,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issues", issue_id, EVENT_FEATURE_PROPOSED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=feat_excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 4. ISSUE_RESOLVED
        status_norm = (issue.status or "").strip().lower()
        if status_norm in {"resolved", "closed"} or issue.resolved_at is not None:
            resolved_excerpt = issue.status or "Resolved"
            if issue.resolution:
                resolved_excerpt = issue.resolution
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_ISSUE_RESOLVED,
                    event_time=issue.resolved_at,  # If None, preserved as None (no artificial current_time)
                    subject=f"Issue resolved: [{issue_id}] Status: {issue.status}, Resolution: {issue.resolution or 'Done'}",
                    description=f"Issue {issue_id} marked {issue.status} with resolution {issue.resolution or 'Done'}",
                    actor=issue.assignee or "system",
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issues", issue_id, EVENT_ISSUE_RESOLVED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=resolved_excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        return candidates

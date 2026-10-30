"""
Deterministic extractor for Commit and CodeChange records.
Extracts: FEATURE_REMOVED, FEATURE_IMPLEMENTED, FEATURE_MODIFIED.
"""

import re
from typing import List, Optional
from app.models.commit import Commit
from app.services.extractors.base import (
    CandidatePayload,
    EvidencePayload,
    EVENT_FEATURE_REMOVED,
    EVENT_FEATURE_IMPLEMENTED,
    EVENT_FEATURE_MODIFIED,
    compute_candidate_fingerprint,
)

RE_ISSUE_KEY = re.compile(r"\b(KAFKA-\d+)\b", re.IGNORECASE)

RE_FEATURE_REMOVED = re.compile(
    r"\b(deprecat(?:ed?|ing)\s+and\s+remov(?:ed?|ing)|remov(?:ed?|ing)\s+deprecated|"
    r"delet(?:ed?|ing)\s+legacy|drop(?:ping|ped)?\s+support\s+for|remov(?:ed?|ing)\s+support\s+for)\b",
    re.IGNORECASE,
)

RE_FEATURE_IMPLEMENTED = re.compile(
    r"\b(implement(?:ed?|ing|s)?\s+(?:KIP-\d+|feature|support)|add(?:ed?|ing)?\s+support\s+for|"
    r"introduce(?:d?|s)?\s+new\s+(?:API|feature|protocol)|initial\s+implementation)\b",
    re.IGNORECASE,
)

RE_FEATURE_MODIFIED = re.compile(
    r"\b(update(?:d?|ing)?\s+(?:KIP-\d+|feature)|refactor(?:ed?|ing)?(?:\s+the)?|"
    r"modify(?:ing|ied)?\s+(?:behavior|feature)|enhanc(?:ed?|ing)(?:\s+the)?)\b",
    re.IGNORECASE,
)


class CommitExtractor:
    VERSION = "1.0.0"

    def __init__(self, dataset_name: str = "KafkaSQLite"):
        self.dataset_name = dataset_name

    def extract(self, commit: Commit) -> List[CandidatePayload]:
        candidates: List[CandidatePayload] = []
        msg = commit.message or ""
        if not msg.strip():
            return candidates

        # Check for issue key in commit message
        issue_match = RE_ISSUE_KEY.search(msg)
        issue_id = issue_match.group(1).upper() if issue_match else None

        commit_hash = commit.commit_hash
        src_rec_id = commit.source_record_id
        actor = commit.author or "unknown"
        first_line = msg.splitlines()[0].strip() if msg.splitlines() else msg[:200]

        # 1. FEATURE_REMOVED
        removed_match = RE_FEATURE_REMOVED.search(msg)
        if removed_match:
            excerpt = first_line
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_FEATURE_REMOVED,
                    event_time=commit.committed_at,
                    subject=f"Feature removed: {first_line[:120]}",
                    description=msg[:500],
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "commits", commit_hash, EVENT_FEATURE_REMOVED
                    ),
                    issue_id=issue_id,
                    commit_hash=commit_hash,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 2. FEATURE_IMPLEMENTED
        impl_match = RE_FEATURE_IMPLEMENTED.search(msg)
        if impl_match and not removed_match:
            excerpt = first_line
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_FEATURE_IMPLEMENTED,
                    event_time=commit.committed_at,
                    subject=f"Feature implemented: {first_line[:120]}",
                    description=msg[:500],
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "commits", commit_hash, EVENT_FEATURE_IMPLEMENTED
                    ),
                    issue_id=issue_id,
                    commit_hash=commit_hash,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 3. FEATURE_MODIFIED
        mod_match = RE_FEATURE_MODIFIED.search(msg)
        if mod_match and not removed_match and not impl_match:
            excerpt = first_line
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_FEATURE_MODIFIED,
                    event_time=commit.committed_at,
                    subject=f"Feature modified: {first_line[:120]}",
                    description=msg[:500],
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="MEDIUM",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "commits", commit_hash, EVENT_FEATURE_MODIFIED
                    ),
                    issue_id=issue_id,
                    commit_hash=commit_hash,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        return candidates

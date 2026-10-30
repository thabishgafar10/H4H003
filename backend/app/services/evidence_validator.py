"""
Evidence Validation and Promotion Engine.
Enforces:
1. Strict verbatim verification against source_records.
2. Event taxonomy validation.
3. Candidate persistence with confidence bands and review statuses.
4. Promotion of ACCEPTED candidates to deduplicated TemporalEvents.
"""

import json
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from app.models.event_candidate import EventCandidate
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.models.source_record import SourceRecord
from app.services.extractors.base import (
    CandidatePayload,
    ALL_EVENT_TYPES,
    compute_event_key,
)
from app.core.logging_config import logger


class EvidenceValidator:
    def __init__(self, db: Session, dataset_name: str = "KafkaSQLite"):
        self.db = db
        self.dataset_name = dataset_name
        self._source_record_cache: Dict[int, str] = {}

    def _get_source_record_text(self, source_record_id: int) -> Optional[str]:
        if source_record_id in self._source_record_cache:
            return self._source_record_cache[source_record_id]

        rec = self.db.query(SourceRecord).filter(SourceRecord.id == source_record_id).first()
        if not rec:
            return None

        # Convert raw_data JSON to normalized string for substring lookup
        if isinstance(rec.raw_data, (dict, list)):
            text_repr = json.dumps(rec.raw_data, ensure_ascii=False)
        else:
            text_repr = str(rec.raw_data)

        self._source_record_cache[source_record_id] = text_repr
        return text_repr

    def validate_candidate(self, payload: CandidatePayload) -> CandidatePayload:
        """
        Validates candidate against taxonomy, checks for evidence,
        and verifies excerpts verbatim against source records.
        """
        # 1. Taxonomy check
        if payload.event_type not in ALL_EVENT_TYPES:
            payload.validation_status = "REJECTED"
            payload.rejection_reason = f"Unknown event type: {payload.event_type}"
            return payload

        # 2. Must have subject
        if not payload.subject or not payload.subject.strip():
            payload.validation_status = "REJECTED"
            payload.rejection_reason = "Candidate has empty subject"
            return payload

        # 3. Must have evidence
        if not payload.evidence_items:
            payload.validation_status = "REJECTED"
            payload.rejection_reason = "No evidence items provided for candidate"
            return payload

        # 4. If status is already NEEDS_REVIEW, keep it (e.g. conditional statements)
        if payload.validation_status == "NEEDS_REVIEW":
            return payload

        # 5. Verbatim evidence verification
        for ev in payload.evidence_items:
            if not ev.evidence_excerpt or not ev.evidence_excerpt.strip():
                payload.validation_status = "REJECTED"
                payload.rejection_reason = "Evidence excerpt is empty"
                return payload

            if ev.source_record_id is not None:
                source_text = self._get_source_record_text(ev.source_record_id)
                if source_text is None:
                    payload.validation_status = "REJECTED"
                    payload.rejection_reason = f"Source record ID {ev.source_record_id} does not exist"
                    return payload

                # Check if verbatim excerpt is contained in source_record raw data
                # Allow minor whitespace flexibility
                excerpt_clean = ev.evidence_excerpt.strip()
                if excerpt_clean not in source_text:
                    # Check JSON escaped version in case of special characters
                    json_escaped = json.dumps(excerpt_clean)[1:-1]
                    if json_escaped not in source_text:
                        payload.validation_status = "REJECTED"
                        payload.rejection_reason = (
                            f"Evidence excerpt '{excerpt_clean[:50]}...' not found verbatim in source record"
                        )
                        return payload

        return payload

    def process_and_persist_candidate(
        self,
        payload: CandidatePayload,
        run_id: Optional[int] = None
    ) -> Tuple[EventCandidate, Optional[TemporalEvent]]:
        """
        Validates, persists EventCandidate and EventEvidence.
        If ACCEPTED, promotes to or updates TemporalEvent.
        """
        validated = self.validate_candidate(payload)

        # 1. Check if candidate already persisted
        existing_candidate = (
            self.db.query(EventCandidate)
            .filter(
                EventCandidate.dataset_name == validated.dataset_name,
                EventCandidate.candidate_fingerprint == validated.candidate_fingerprint,
            )
            .first()
        )

        if existing_candidate:
            candidate_obj = existing_candidate
        else:
            candidate_obj = EventCandidate(
                dataset_name=validated.dataset_name,
                event_type=validated.event_type,
                event_time=validated.event_time,
                subject=validated.subject,
                description=validated.description,
                actor=validated.actor,
                extractor_type=validated.extractor_type,
                extractor_version=validated.extractor_version,
                confidence_band=validated.confidence_band,
                validation_status=validated.validation_status,
                rejection_reason=validated.rejection_reason,
                candidate_fingerprint=validated.candidate_fingerprint,
                extraction_run_id=run_id,
            )
            self.db.add(candidate_obj)
            self.db.flush()

        temporal_event: Optional[TemporalEvent] = None

        # 2. If ACCEPTED, promote to TemporalEvent
        if validated.validation_status == "ACCEPTED":
            primary_entity = validated.issue_id or validated.commit_hash or validated.subject
            event_key = compute_event_key(
                dataset_name=validated.dataset_name,
                event_type=validated.event_type,
                primary_entity=primary_entity,
                event_time=validated.event_time,
            )

            # Check if event already exists
            existing_event = (
                self.db.query(TemporalEvent)
                .filter(
                    TemporalEvent.dataset_name == validated.dataset_name,
                    TemporalEvent.event_key == event_key,
                )
                .first()
            )

            if existing_event:
                temporal_event = existing_event
            else:
                temporal_event = TemporalEvent(
                    dataset_name=validated.dataset_name,
                    event_key=event_key,
                    event_type=validated.event_type,
                    event_time=validated.event_time,
                    subject=validated.subject,
                    description=validated.description,
                    actor=validated.actor,
                    issue_id=validated.issue_id,
                    commit_hash=validated.commit_hash,
                    extraction_run_id=run_id,
                )
                self.db.add(temporal_event)
                self.db.flush()

        # 3. Persist Evidence Records
        for ev in validated.evidence_items:
            # Check if exact evidence already linked to this candidate
            ev_query = self.db.query(EventEvidence).filter(
                EventEvidence.candidate_id == candidate_obj.id,
                EventEvidence.evidence_hash == ev.evidence_hash,
            )
            if not ev_query.first():
                evidence_obj = EventEvidence(
                    candidate_id=candidate_obj.id,
                    event_id=temporal_event.id if temporal_event else None,
                    source_record_id=ev.source_record_id,
                    evidence_role=ev.evidence_role,
                    evidence_excerpt=ev.evidence_excerpt,
                    evidence_hash=ev.evidence_hash,
                )
                self.db.add(evidence_obj)

        return candidate_obj, temporal_event

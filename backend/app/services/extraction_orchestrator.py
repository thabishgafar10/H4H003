"""
Extraction Orchestrator.
Manages end-to-end event extraction runs, chunking, deduplication, and transaction management.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.extraction_run import ExtractionRun
from app.models.event_candidate import EventCandidate
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.services.extractors.issue_extractor import IssueExtractor
from app.services.extractors.comment_extractor import CommentExtractor
from app.services.extractors.commit_extractor import CommitExtractor
from app.services.llm_extractor import LLMExtractor
from app.services.evidence_validator import EvidenceValidator
from app.core.logging_config import logger


class ExtractionOrchestrator:
    VERSION = "1.0.0"

    def __init__(self, db: Session, dataset_name: str = "KafkaSQLite", project_id: Optional[int] = None):
        self.db = db
        self.dataset_name = dataset_name
        self.project_id = project_id
        self.issue_extractor = IssueExtractor(dataset_name=dataset_name)
        self.comment_extractor = CommentExtractor(dataset_name=dataset_name)
        self.commit_extractor = CommitExtractor(dataset_name=dataset_name)
        self.llm_extractor = LLMExtractor(dataset_name=dataset_name)
        self.validator = EvidenceValidator(db=db, dataset_name=dataset_name, project_id=project_id)

    def start_extraction_run(
        self,
        extraction_mode: str = "DETERMINISTIC",
        chunk_size: int = 500,
        max_issues: Optional[int] = None,
        max_comments: Optional[int] = None,
        max_commits: Optional[int] = None,
    ) -> ExtractionRun:
        """
        Executes a complete or chunked extraction run across issues, comments, and commits.
        """
        run = ExtractionRun(
            dataset_name=self.dataset_name,
            project_id=self.project_id,
            status="RUNNING",
            extraction_mode=extraction_mode,
            extractor_version=self.VERSION,
            records_scanned=0,
            candidates_created=0,
            events_accepted=0,
            events_rejected=0,
            events_needing_review=0,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)

        try:
            # 1. Process Issues
            self._extract_issues(run, chunk_size=chunk_size, limit=max_issues)

            # 2. Process Comments
            self._extract_comments(run, chunk_size=chunk_size, limit=max_comments, extraction_mode=extraction_mode)

            # 3. Process Commits
            self._extract_commits(run, chunk_size=chunk_size, limit=max_commits)

            # Mark Run Completed
            run.status = "COMPLETED"
            run.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(run)
            logger.info(
                f"Extraction run {run.id} completed. Candidates: {run.candidates_created}, "
                f"Accepted: {run.events_accepted}, Needs Review: {run.events_needing_review}, "
                f"Rejected: {run.events_rejected}"
            )
            return run

        except Exception as e:
            self.db.rollback()
            logger.error(f"Extraction run {run.id} failed: {e}", exc_info=True)
            run.status = "FAILED"
            run.error_message = str(e)
            run.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(run)
            raise e

    def _extract_issues(self, run: ExtractionRun, chunk_size: int, limit: Optional[int]):
        query = self.db.query(Issue)
        if limit:
            query = query.limit(limit)

        offset = 0
        while True:
            batch = query.offset(offset).limit(chunk_size).all()
            if not batch:
                break

            for issue in batch:
                run.records_scanned += 1
                candidates = self.issue_extractor.extract(issue)
                for cand in candidates:
                    self._persist_candidate_payload(cand, run)

            self.db.commit()
            offset += len(batch)
            if limit and offset >= limit:
                break

    def _extract_comments(
        self,
        run: ExtractionRun,
        chunk_size: int,
        limit: Optional[int],
        extraction_mode: str,
    ):
        query = self.db.query(IssueComment)
        if limit:
            query = query.limit(limit)

        offset = 0
        while True:
            batch = query.offset(offset).limit(chunk_size).all()
            if not batch:
                break

            for comment in batch:
                run.records_scanned += 1
                # Deterministic extraction
                candidates = self.comment_extractor.extract(comment)

                # Optional LLM-assisted extraction
                if extraction_mode in {"HYBRID", "LLM"} and self.llm_extractor.is_available():
                    llm_candidates = self.llm_extractor.extract_from_comment(comment)
                    candidates.extend(llm_candidates)

                for cand in candidates:
                    self._persist_candidate_payload(cand, run)

            self.db.commit()
            offset += len(batch)
            if limit and offset >= limit:
                break

    def _extract_commits(self, run: ExtractionRun, chunk_size: int, limit: Optional[int]):
        query = self.db.query(Commit)
        if limit:
            query = query.limit(limit)

        offset = 0
        while True:
            batch = query.offset(offset).limit(chunk_size).all()
            if not batch:
                break

            for commit in batch:
                run.records_scanned += 1
                candidates = self.commit_extractor.extract(commit)
                for cand in candidates:
                    self._persist_candidate_payload(cand, run)

            self.db.commit()
            offset += len(batch)
            if limit and offset >= limit:
                break

    def _persist_candidate_payload(self, cand_payload, run: ExtractionRun):
        # Check if already present before validating
        existing = (
            self.db.query(EventCandidate)
            .filter(
                EventCandidate.dataset_name == cand_payload.dataset_name,
                EventCandidate.candidate_fingerprint == cand_payload.candidate_fingerprint,
            )
            .first()
        )
        if existing:
            return

        candidate_obj, temporal_event = self.validator.process_and_persist_candidate(
            cand_payload, run_id=run.id
        )
        run.candidates_created += 1

        if candidate_obj.validation_status == "ACCEPTED":
            run.events_accepted += 1
        elif candidate_obj.validation_status == "NEEDS_REVIEW":
            run.events_needing_review += 1
        elif candidate_obj.validation_status == "REJECTED":
            run.events_rejected += 1

    def get_statistics(self) -> Dict:
        """Returns comprehensive statistics for temporal events, candidates, and runs."""
        total_runs = self.db.query(func.count(ExtractionRun.id)).filter(
            ExtractionRun.dataset_name == self.dataset_name
        ).scalar() or 0

        latest_run = (
            self.db.query(ExtractionRun)
            .filter(ExtractionRun.dataset_name == self.dataset_name)
            .order_by(ExtractionRun.id.desc())
            .first()
        )

        total_candidates = self.db.query(func.count(EventCandidate.id)).filter(
            EventCandidate.dataset_name == self.dataset_name
        ).scalar() or 0

        accepted_candidates = self.db.query(func.count(EventCandidate.id)).filter(
            EventCandidate.dataset_name == self.dataset_name,
            EventCandidate.validation_status == "ACCEPTED",
        ).scalar() or 0

        review_candidates = self.db.query(func.count(EventCandidate.id)).filter(
            EventCandidate.dataset_name == self.dataset_name,
            EventCandidate.validation_status == "NEEDS_REVIEW",
        ).scalar() or 0

        rejected_candidates = self.db.query(func.count(EventCandidate.id)).filter(
            EventCandidate.dataset_name == self.dataset_name,
            EventCandidate.validation_status == "REJECTED",
        ).scalar() or 0

        total_events = self.db.query(func.count(TemporalEvent.id)).filter(
            TemporalEvent.dataset_name == self.dataset_name
        ).scalar() or 0

        total_evidence = self.db.query(func.count(EventEvidence.id)).scalar() or 0

        # Event counts by taxonomy type
        event_type_counts = dict(
            self.db.query(TemporalEvent.event_type, func.count(TemporalEvent.id))
            .filter(TemporalEvent.dataset_name == self.dataset_name)
            .group_by(TemporalEvent.event_type)
            .all()
        )

        return {
            "dataset_name": self.dataset_name,
            "total_runs": total_runs,
            "latest_run": {
                "id": latest_run.id,
                "status": latest_run.status,
                "records_scanned": latest_run.records_scanned,
                "candidates_created": latest_run.candidates_created,
                "events_accepted": latest_run.events_accepted,
                "completed_at": latest_run.completed_at.isoformat() if latest_run.completed_at else None,
            } if latest_run else None,
            "total_candidates": total_candidates,
            "candidates_by_status": {
                "accepted": accepted_candidates,
                "needs_review": review_candidates,
                "rejected": rejected_candidates,
            },
            "total_temporal_events": total_events,
            "events_by_type": event_type_counts,
            "total_evidence_links": total_evidence,
        }

"""
Comprehensive automated tests for Work 03: Temporal Event Extraction.
Tests deterministic extractors, conservative guardrails (negation, conditionals, bots, quotes),
evidence validator verbatim matching, LLM adapter fallback/mocking, orchestrator idempotency, and APIs.
"""

from datetime import datetime, timezone
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models.source_record import SourceRecord
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.extraction_run import ExtractionRun
from app.models.event_candidate import EventCandidate
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence

from app.services.extractors.base import (
    EVENT_ISSUE_CREATED,
    EVENT_ISSUE_RESOLVED,
    EVENT_BUG_REPORTED,
    EVENT_FEATURE_PROPOSED,
    EVENT_DECISION_APPROVED,
    EVENT_CONCERN_RAISED,
    EVENT_DECISION_REVERSED,
    EVENT_FEATURE_IMPLEMENTED,
    EVENT_FEATURE_MODIFIED,
    EVENT_FEATURE_REMOVED,
)
from app.services.extractors.issue_extractor import IssueExtractor
from app.services.extractors.comment_extractor import CommentExtractor
from app.services.extractors.commit_extractor import CommitExtractor
from app.services.evidence_validator import EvidenceValidator
from app.services.llm_extractor import LLMExtractor, LLMExtractedEvent
from app.services.extraction_orchestrator import ExtractionOrchestrator
from starlette.testclient import TestClient
from app.main import app


@pytest.fixture
def test_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    TestingSession = sessionmaker(bind=engine)
    session = TestingSession()
    try:
        yield session
    finally:
        session.close()


def create_sample_source_record(db, table: str, rec_id: str, data: dict) -> SourceRecord:
    sr = SourceRecord(
        dataset_name="TestDataset",
        source_table=table,
        source_record_id=rec_id,
        source_record_hash="hash_" + rec_id,
        raw_data=data,
        ingestion_batch_id=1,
    )
    db.add(sr)
    db.flush()
    return sr


# -------------------------------------------------------------------------
# 1. IssueExtractor Tests
# -------------------------------------------------------------------------

def test_issue_extractor_types(test_db):
    sr = create_sample_source_record(
        test_db, "issues", "KAFKA-101",
        {"issue_id": "KAFKA-101", "summary": "Fix race condition in ReplicaManager", "type": "Bug"}
    )
    created_dt = datetime(2023, 1, 15, 10, 0, tzinfo=timezone.utc)
    resolved_dt = datetime(2023, 1, 18, 14, 30, tzinfo=timezone.utc)

    issue = Issue(
        source_issue_id="KAFKA-101",
        issue_type="Bug",
        summary="Fix race condition in ReplicaManager",
        status="Resolved",
        resolution="Fixed",
        reporter="alice",
        assignee="bob",
        created_at=created_dt,
        resolved_at=resolved_dt,
        source_record_id=sr.id,
    )
    test_db.add(issue)
    test_db.flush()

    extractor = IssueExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(issue)

    types = {c.event_type for c in candidates}
    assert EVENT_ISSUE_CREATED in types
    assert EVENT_BUG_REPORTED in types
    assert EVENT_ISSUE_RESOLVED in types

    created_cand = next(c for c in candidates if c.event_type == EVENT_ISSUE_CREATED)
    assert created_cand.event_time == created_dt
    assert created_cand.actor == "alice"
    assert created_cand.confidence_band == "HIGH"
    assert created_cand.evidence_items[0].evidence_excerpt == "Fix race condition in ReplicaManager"

    resolved_cand = next(c for c in candidates if c.event_type == EVENT_ISSUE_RESOLVED)
    assert resolved_cand.event_time == resolved_dt
    assert resolved_cand.actor == "bob"


def test_issue_extractor_feature_proposed(test_db):
    sr = create_sample_source_record(
        test_db, "issues", "KAFKA-202",
        {"issue_id": "KAFKA-202", "summary": "KIP-500: Replace ZooKeeper with Raft Metadata mode", "type": "New Feature"}
    )
    issue = Issue(
        source_issue_id="KAFKA-202",
        issue_type="New Feature",
        summary="KIP-500: Replace ZooKeeper with Raft Metadata mode",
        status="Open",
        reporter="colin",
        created_at=datetime(2023, 2, 1, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(issue)
    test_db.flush()

    extractor = IssueExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(issue)

    types = {c.event_type for c in candidates}
    assert EVENT_FEATURE_PROPOSED in types
    assert EVENT_ISSUE_CREATED in types
    assert EVENT_ISSUE_RESOLVED not in types


# -------------------------------------------------------------------------
# 2. CommentExtractor & Guardrail Tests
# -------------------------------------------------------------------------

def test_comment_extractor_approval(test_db):
    sr = create_sample_source_record(
        test_db, "issue_comments", "1",
        {"issue_id": "KAFKA-101", "message": "Thanks for the patch. +1 (binding) from me.", "username": "junrao"}
    )
    comment = IssueComment(
        id=1,
        source_issue_id="KAFKA-101",
        username="junrao",
        display_name="Jun Rao",
        message="Thanks for the patch. +1 (binding) from me.",
        created_at=datetime(2023, 1, 16, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    extractor = CommentExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(comment)

    assert len(candidates) >= 1
    approval = candidates[0]
    assert approval.event_type == EVENT_DECISION_APPROVED
    assert approval.confidence_band == "HIGH"
    assert approval.validation_status == "ACCEPTED"
    assert "+1 (binding)" in approval.evidence_items[0].evidence_excerpt


def test_comment_extractor_negation_guard(test_db):
    """Verifies that 'do not approve' or 'cannot approve' is NOT extracted as DECISION_APPROVED."""
    sr = create_sample_source_record(
        test_db, "issue_comments", "2",
        {"issue_id": "KAFKA-101", "message": "I cannot approve this patch because it lacks test coverage.", "username": "neha"}
    )
    comment = IssueComment(
        id=2,
        source_issue_id="KAFKA-101",
        username="neha",
        display_name="Neha Narkhede",
        message="I cannot approve this patch because it lacks test coverage.",
        created_at=datetime(2023, 1, 16, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    extractor = CommentExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(comment)

    # Must NOT have DECISION_APPROVED
    types = {c.event_type for c in candidates}
    assert EVENT_DECISION_APPROVED not in types
    # Should be flagged as CONCERN_RAISED
    assert EVENT_CONCERN_RAISED in types


def test_comment_extractor_conditional_guard(test_db):
    """Verifies that conditional approvals are flagged as NEEDS_REVIEW, not accepted."""
    sr = create_sample_source_record(
        test_db, "issue_comments", "3",
        {"issue_id": "KAFKA-101", "message": "I would approve if you fix the memory leak.", "username": "jay"}
    )
    comment = IssueComment(
        id=3,
        source_issue_id="KAFKA-101",
        username="jay",
        display_name="Jay Kreps",
        message="I would approve if you fix the memory leak.",
        created_at=datetime(2023, 1, 16, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    extractor = CommentExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(comment)

    if candidates:
        for c in candidates:
            if c.event_type == EVENT_DECISION_APPROVED:
                assert c.validation_status == "NEEDS_REVIEW"
                assert c.confidence_band == "LOW"


def test_comment_extractor_bot_filtering(test_db):
    """Verifies comments from CI bots are ignored."""
    sr = create_sample_source_record(
        test_db, "issue_comments", "4",
        {"issue_id": "KAFKA-101", "message": "Build passed. LGTM.", "username": "asfbot"}
    )
    comment = IssueComment(
        id=4,
        source_issue_id="KAFKA-101",
        username="asfbot",
        display_name="ASF GitHub Bot",
        message="Build passed. LGTM.",
        created_at=datetime(2023, 1, 16, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    extractor = CommentExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(comment)
    assert len(candidates) == 0


def test_comment_extractor_quote_omission(test_db):
    """Verifies that quoted text containing LGTM is not falsely attributed to replying author."""
    raw = "> LGTM, looks good to me\nI disagree with the above approach; this is a blocker."
    sr = create_sample_source_record(
        test_db, "issue_comments", "5",
        {"issue_id": "KAFKA-101", "message": raw, "username": "reviewer"}
    )
    comment = IssueComment(
        id=5,
        source_issue_id="KAFKA-101",
        username="reviewer",
        message=raw,
        created_at=datetime(2023, 1, 17, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    extractor = CommentExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(comment)

    types = {c.event_type for c in candidates}
    assert EVENT_DECISION_APPROVED not in types
    assert EVENT_CONCERN_RAISED in types


# -------------------------------------------------------------------------
# 3. CommitExtractor Tests
# -------------------------------------------------------------------------

def test_commit_extractor_features(test_db):
    sr = create_sample_source_record(
        test_db, "change_set", "a1b2c3d4",
        {"commit_hash": "a1b2c3d4", "message": "KAFKA-101: implement KIP-500 Raft controller protocol", "author": "colin"}
    )
    commit = Commit(
        commit_hash="a1b2c3d4",
        message="KAFKA-101: implement KIP-500 Raft controller protocol\nDetailed commit body here.",
        author="Colin McCabe",
        committed_at=datetime(2023, 3, 1, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(commit)
    test_db.flush()

    extractor = CommitExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(commit)

    assert len(candidates) == 1
    cand = candidates[0]
    assert cand.event_type == EVENT_FEATURE_IMPLEMENTED
    assert cand.issue_id == "KAFKA-101"
    assert cand.commit_hash == "a1b2c3d4"
    assert cand.confidence_band == "HIGH"


def test_commit_extractor_feature_removed(test_db):
    sr = create_sample_source_record(
        test_db, "change_set", "e5f6g7h8",
        {"commit_hash": "e5f6g7h8", "message": "KAFKA-303: deprecate and remove legacy Scala producer", "author": "junrao"}
    )
    commit = Commit(
        commit_hash="e5f6g7h8",
        message="KAFKA-303: deprecate and remove legacy Scala producer",
        author="Jun Rao",
        committed_at=datetime(2023, 4, 1, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(commit)
    test_db.flush()

    extractor = CommitExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(commit)

    assert len(candidates) == 1
    assert candidates[0].event_type == EVENT_FEATURE_REMOVED


# -------------------------------------------------------------------------
# 4. EvidenceValidator & Promotion Tests
# -------------------------------------------------------------------------

def test_evidence_validator_verbatim_rejection(test_db):
    """Verifies that a fabricated excerpt not present in source record is REJECTED."""
    sr = create_sample_source_record(
        test_db, "issues", "KAFKA-999",
        {"issue_id": "KAFKA-999", "summary": "Genuine summary text here"}
    )
    issue = Issue(
        source_issue_id="KAFKA-999",
        summary="Genuine summary text here",
        source_record_id=sr.id,
    )
    test_db.add(issue)
    test_db.flush()

    extractor = IssueExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(issue)
    created_cand = candidates[0]

    # Tamper with evidence excerpt to make it fabricated
    created_cand.evidence_items[0].evidence_excerpt = "Fabricated text that does not exist"

    validator = EvidenceValidator(test_db, dataset_name="TestDataset")
    candidate_obj, temporal_event = validator.process_and_persist_candidate(created_cand)

    # Must be REJECTED and NO TemporalEvent created
    assert candidate_obj.validation_status == "REJECTED"
    assert "not found verbatim" in candidate_obj.rejection_reason
    assert temporal_event is None


def test_evidence_validator_valid_promotion(test_db):
    sr = create_sample_source_record(
        test_db, "issues", "KAFKA-555",
        {"issue_id": "KAFKA-555", "summary": "Fix connection timeout in NetworkClient"}
    )
    issue = Issue(
        source_issue_id="KAFKA-555",
        summary="Fix connection timeout in NetworkClient",
        created_at=datetime(2023, 5, 1, tzinfo=timezone.utc),
        source_record_id=sr.id,
    )
    test_db.add(issue)
    test_db.flush()

    extractor = IssueExtractor(dataset_name="TestDataset")
    candidates = extractor.extract(issue)
    created_cand = candidates[0]

    validator = EvidenceValidator(test_db, dataset_name="TestDataset")
    candidate_obj, temporal_event = validator.process_and_persist_candidate(created_cand)

    assert candidate_obj.validation_status == "ACCEPTED"
    assert temporal_event is not None
    assert temporal_event.subject == created_cand.subject
    assert len(candidate_obj.evidence_records) == 1
    assert candidate_obj.evidence_records[0].evidence_excerpt == "Fix connection timeout in NetworkClient"


# -------------------------------------------------------------------------
# 5. LLM Adapter Tests (Graceful fallback & mocked proposal)
# -------------------------------------------------------------------------

def test_llm_extractor_graceful_fallback(test_db):
    """When no LLM API key is present, returns empty list without error."""
    adapter = LLMExtractor(api_key=None)
    assert adapter.is_available() is False

    comment = IssueComment(
        id=99,
        source_issue_id="KAFKA-1",
        message="A complex proposal about protocol changes",
    )
    candidates = adapter.extract_from_comment(comment)
    assert candidates == []


def test_llm_extractor_mocked_proposal(test_db, monkeypatch):
    """Tests that LLM extraction strictly produces candidates that pass through validation."""
    sr = create_sample_source_record(
        test_db, "issue_comments", "100",
        {"issue_id": "KAFKA-1", "message": "After review, the team agreed to proceed with design B."}
    )
    comment = IssueComment(
        id=100,
        source_issue_id="KAFKA-1",
        username="architect",
        message="After review, the team agreed to proceed with design B.",
        source_record_id=sr.id,
    )
    test_db.add(comment)
    test_db.flush()

    adapter = LLMExtractor(api_key="mock-key")
    assert adapter.is_available() is True

    # Mock the LLM provider response
    mock_events = [
        LLMExtractedEvent(
            event_type=EVENT_DECISION_APPROVED,
            subject="Agreed to proceed with design B",
            confidence_band="HIGH",
            evidence_excerpt="the team agreed to proceed with design B",
            reasoning="Consensus reached on design choice",
        )
    ]
    monkeypatch.setattr(adapter, "_call_llm_provider", lambda text, issue_id: mock_events)

    candidates = adapter.extract_from_comment(comment)
    assert len(candidates) == 1
    cand = candidates[0]
    assert cand.extractor_type == "LLM"
    assert cand.evidence_items[0].evidence_excerpt == "the team agreed to proceed with design B"

    # Verify candidate can be validated and promoted
    validator = EvidenceValidator(test_db, dataset_name="TestDataset")
    cand_obj, event_obj = validator.process_and_persist_candidate(cand)
    assert cand_obj.validation_status == "ACCEPTED"
    assert event_obj is not None


# -------------------------------------------------------------------------
# 6. Extraction Orchestrator & Idempotency Tests
# -------------------------------------------------------------------------

def test_orchestrator_complete_flow_and_idempotency(test_db):
    # Seed records
    sr_issue = create_sample_source_record(
        test_db, "issues", "KAFKA-10",
        {"issue_id": "KAFKA-10", "summary": "Fix broker shutdown deadlocks", "type": "Bug"}
    )
    issue = Issue(
        source_issue_id="KAFKA-10",
        issue_type="Bug",
        summary="Fix broker shutdown deadlocks",
        status="Closed",
        resolution="Fixed",
        created_at=datetime(2023, 1, 1, tzinfo=timezone.utc),
        resolved_at=datetime(2023, 1, 2, tzinfo=timezone.utc),
        source_record_id=sr_issue.id,
    )
    test_db.add(issue)

    sr_comm = create_sample_source_record(
        test_db, "issue_comments", "20",
        {"issue_id": "KAFKA-10", "message": "Looks good to me. +1 (binding)", "username": "committer"}
    )
    comment = IssueComment(
        id=20,
        source_issue_id="KAFKA-10",
        username="committer",
        message="Looks good to me. +1 (binding)",
        created_at=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
        source_record_id=sr_comm.id,
    )
    test_db.add(comment)

    sr_commit = create_sample_source_record(
        test_db, "change_set", "commit1",
        {"commit_hash": "commit1", "message": "KAFKA-10: implement graceful shutdown for KafkaServer"}
    )
    commit = Commit(
        commit_hash="commit1",
        message="KAFKA-10: implement graceful shutdown for KafkaServer",
        committed_at=datetime(2023, 1, 2, 10, 0, tzinfo=timezone.utc),
        source_record_id=sr_commit.id,
    )
    test_db.add(commit)
    test_db.commit()

    # Pass 1
    orch = ExtractionOrchestrator(test_db, dataset_name="TestDataset")
    run1 = orch.start_extraction_run(extraction_mode="DETERMINISTIC")

    assert run1.status == "COMPLETED"
    assert run1.candidates_created > 0
    assert run1.events_accepted > 0

    stats1 = orch.get_statistics()
    initial_events = stats1["total_temporal_events"]
    initial_candidates = stats1["total_candidates"]
    assert initial_events > 0

    # Pass 2: Repeated extraction should create 0 new candidates and 0 new events
    run2 = orch.start_extraction_run(extraction_mode="DETERMINISTIC")
    assert run2.status == "COMPLETED"
    assert run2.candidates_created == 0

    stats2 = orch.get_statistics()
    assert stats2["total_temporal_events"] == initial_events
    assert stats2["total_candidates"] == initial_candidates


# -------------------------------------------------------------------------
# 7. API Routes Verification Tests
# -------------------------------------------------------------------------

def test_api_endpoints_work03():
    client = TestClient(app)

    # 1. Taxonomy endpoint
    tax_res = client.get("/api/events/taxonomy")
    assert tax_res.status_code == 200
    tax_types = {item["event_type"] for item in tax_res.json()}
    assert len(tax_types) == 10
    assert "DECISION_APPROVED" in tax_types
    assert "FEATURE_IMPLEMENTED" in tax_types

    # 2. Statistics endpoint
    stats_res = client.get("/api/extraction/statistics?dataset_name=KafkaSQLite")
    assert stats_res.status_code == 200
    data = stats_res.json()
    assert "total_temporal_events" in data
    assert "candidates_by_status" in data

    # 3. Events list endpoint
    events_res = client.get("/api/events?limit=10&dataset_name=KafkaSQLite")
    assert events_res.status_code == 200
    assert isinstance(events_res.json(), list)

    # 4. Web Dashboard UI endpoint
    dash_res = client.get("/dashboard")
    assert dash_res.status_code == 200
    assert "Temporal Product Knowledge" in dash_res.text

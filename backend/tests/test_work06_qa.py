"""
Automated tests for Work 06: Historical Natural-Language Question Answering.
Tests intent parsing, feature resolution, chronological timeline generation,
evidence citations, attribution queries, fallbacks, and REST APIs.
"""
from datetime import datetime, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

from app.db.base import Base
from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.issue_relationships import CommitIssueLink
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.services.qa_service import HistoricalQAService
from app.schemas.qa import QARequest
from app.main import app


@pytest.fixture
def qa_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Seed test knowledge base
        feat = Feature(
            dataset_name="TestQA",
            canonical_name="KRaft Mode",
            normalized_name="kraft mode",
            feature_category="CORE",
            resolution_status="CONFIRMED",
            description="Kafka Raft metadata mode replacing ZooKeeper.",
            first_observed_at=datetime(2020, 4, 1, tzinfo=timezone.utc),
        )
        session.add(feat)
        session.commit()
        session.refresh(feat)

        alias_kip = FeatureAlias(
            feature_id=feat.id,
            alias_name="KIP-500",
            normalized_alias="kip 500",
            alias_type="KIP",
            confidence=1.0,
        )
        alias_zk = FeatureAlias(
            feature_id=feat.id,
            alias_name="ZooKeeper",
            normalized_alias="zookeeper",
            alias_type="RENAMED_FROM",
            confidence=0.95,
        )
        session.add_all([alias_kip, alias_zk])

        issue = Issue(
            source_issue_id="KAFKA-9000",
            summary="Replace ZooKeeper with Kafka Raft Metadata quorum",
            issue_type="New Feature",
            status="Resolved",
            reporter="colinmccabe",
            assignee="colinmccabe",
            created_at=datetime(2020, 4, 10, tzinfo=timezone.utc),
            resolved_at=datetime(2021, 5, 20, tzinfo=timezone.utc),
        )
        session.add(issue)

        commit = Commit(
            commit_hash="kraftabc1234567890",
            author="colinmccabe",
            message="KAFKA-9000: Implement Raft controller metadata quorum",
            committed_at=datetime(2020, 8, 15, tzinfo=timezone.utc),
        )
        session.add(commit)

        ev1 = TemporalEvent(
            dataset_name="TestQA",
            event_key="EV_KRAFT_PROP",
            event_type="FEATURE_PROPOSED",
            actor="colinmccabe",
            subject="KIP-500 proposed to eliminate ZooKeeper dependency",
            event_time=datetime(2020, 4, 1, 10, 0, tzinfo=timezone.utc),
            issue_id="KAFKA-9000",
        )
        ev2 = TemporalEvent(
            dataset_name="TestQA",
            event_key="EV_KRAFT_IMPL",
            event_type="FEATURE_IMPLEMENTED",
            actor="colinmccabe",
            subject="Merged Kafka Raft controller into master",
            event_time=datetime(2020, 8, 15, 14, 0, tzinfo=timezone.utc),
            issue_id="KAFKA-9000",
            commit_hash="kraftabc1234567890",
        )
        session.add_all([ev1, ev2])
        session.commit()
        session.refresh(ev1)
        session.refresh(ev2)

        evd1 = EventEvidence(
            event_id=ev1.id,
            source_record_id=5001,
            evidence_role="PRIMARY",
            evidence_excerpt="KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum",
            evidence_hash="hash5001",
        )
        evd2 = EventEvidence(
            event_id=ev2.id,
            source_record_id=5002,
            evidence_role="PRIMARY",
            evidence_excerpt="Commit kraftabc1234567890: Initial merge of KRaft controller quorum",
            evidence_hash="hash5002",
        )
        session.add_all([evd1, evd2])

        # Feature links
        fl1 = FeatureRecordLink(
            feature_id=feat.id,
            record_type="temporal_event",
            record_id=str(ev1.id),
            relationship_type="INTRODUCED_BY",
            confidence=1.0,
        )
        fl2 = FeatureRecordLink(
            feature_id=feat.id,
            record_type="temporal_event",
            record_id=str(ev2.id),
            relationship_type="IMPLEMENTED_BY",
            confidence=1.0,
        )
        session.add_all([fl1, fl2])
        session.commit()

        yield session
    finally:
        session.close()


def test_qa_feature_and_kip_resolution(qa_db):
    svc = HistoricalQAService(qa_db, dataset_name="TestQA")

    # Ask using KIP number
    req = QARequest(query="How was KIP-500 proposed and implemented?", dataset_name="TestQA")
    resp = svc.answer_question(req)

    assert resp.canonical_feature == "KRaft Mode"
    assert resp.feature_category == "CORE"
    assert resp.confidence >= 0.85
    assert len(resp.temporal_timeline) >= 2
    assert "colinmccabe" in resp.answer
    assert len(resp.citations) >= 2


def test_qa_attribution_who_intent(qa_db):
    svc = HistoricalQAService(qa_db, dataset_name="TestQA")

    req = QARequest(query="Who implemented KRaft Mode?", dataset_name="TestQA")
    resp = svc.answer_question(req)

    assert resp.canonical_feature == "KRaft Mode"
    assert "Key Contributors & Attribution" in resp.answer
    assert "colinmccabe" in resp.answer


def test_qa_verbatim_evidence_citations(qa_db):
    svc = HistoricalQAService(qa_db, dataset_name="TestQA")

    req = QARequest(query="What is the history of KRaft Mode?", dataset_name="TestQA")
    resp = svc.answer_question(req)

    assert len(resp.citations) >= 1
    first_cit = resp.citations[0]
    assert first_cit.source_record_id is not None
    assert first_cit.evidence_hash is not None
    assert len(first_cit.excerpt) > 10


def test_qa_unknown_concept_fallback(qa_db):
    svc = HistoricalQAService(qa_db, dataset_name="TestQA")

    req = QARequest(query="What is quantum machine learning teleportation?", dataset_name="TestQA")
    resp = svc.answer_question(req)

    assert resp.canonical_feature is None
    assert "No verified historical records" in resp.answer
    assert resp.missing_evidence_notes is not None


def test_qa_sample_questions_list(qa_db):
    svc = HistoricalQAService(qa_db, dataset_name="TestQA")
    samples = svc.get_sample_questions()

    assert len(samples) >= 5
    categories = {s.category for s in samples}
    assert "Attribution & Contributors" in categories
    assert "Feature Lifecycle & Timeline" in categories


def test_qa_api_endpoints():
    client = TestClient(app)

    # 1. Health check
    resp_h = client.get("/api/qa/health")
    assert resp_h.status_code == 200
    assert resp_h.json()["status"] == "healthy"

    # 2. Sample questions
    resp_s = client.get("/api/qa/sample-questions")
    assert resp_s.status_code == 200
    samples = resp_s.json()
    assert len(samples) >= 5

    # 3. Ask question
    payload = {"query": "When was Exactly-Once Semantics introduced?", "dataset_name": "KafkaSQLite"}
    resp_ask = client.post("/api/qa/ask", json=payload)
    assert resp_ask.status_code == 200
    data = resp_ask.json()
    assert "answer" in data
    assert "temporal_timeline" in data
    assert "citations" in data


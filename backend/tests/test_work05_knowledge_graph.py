"""
Automated tests for Work 05: Temporal Knowledge Graph.
Tests node and edge extraction, multi-hop traversal, depth bounds, time slicing,
and Knowledge Graph REST APIs.
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
from app.services.knowledge_graph_service import KnowledgeGraphService
from app.schemas.graph import GraphQueryRequest
from app.main import app


@pytest.fixture
def graph_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Seed test graph entities
        feat = Feature(
            dataset_name="TestGraph",
            canonical_name="Log Compaction",
            normalized_name="log compaction",
            feature_category="STORAGE",
            resolution_status="CONFIRMED",
            first_observed_at=datetime(2014, 1, 1, tzinfo=timezone.utc),
        )
        session.add(feat)
        session.commit()
        session.refresh(feat)

        alias = FeatureAlias(
            feature_id=feat.id,
            alias_name="Topic Compaction",
            normalized_alias="topic compaction",
            alias_type="ALTERNATIVE_NAME",
            confidence=0.95,
        )
        session.add(alias)

        issue = Issue(
            source_issue_id="KAFKA-555",
            summary="Add log cleaner and topic compaction",
            issue_type="New Feature",
            status="Resolved",
            reporter="jaykreps",
            assignee="junrao",
            created_at=datetime(2014, 2, 1, tzinfo=timezone.utc),
            resolved_at=datetime(2014, 4, 1, tzinfo=timezone.utc),
        )
        session.add(issue)

        commit = Commit(
            commit_hash="c0ffee1234567890abcdef",
            author="junrao",
            message="KAFKA-555: Implement log cleaner thread for topic compaction",
            committed_at=datetime(2014, 3, 15, tzinfo=timezone.utc),
        )
        session.add(commit)

        link_ci = CommitIssueLink(
            commit_hash=commit.commit_hash,
            source_issue_id=issue.source_issue_id,
        )
        session.add(link_ci)

        ev = TemporalEvent(
            dataset_name="TestGraph",
            event_key="EV_TEST_001",
            event_type="FEATURE_IMPLEMENTED",
            actor="junrao",
            subject="Log cleaner merged into trunk",
            event_time=datetime(2014, 3, 15, 12, 0, tzinfo=timezone.utc),
            issue_id=issue.source_issue_id,
            commit_hash=commit.commit_hash,
        )
        session.add(ev)
        session.commit()
        session.refresh(ev)

        evd = EventEvidence(
            event_id=ev.id,
            source_record_id=101,
            evidence_role="PRIMARY",
            evidence_excerpt="Log compaction implementation committed by junrao for KAFKA-555",
            evidence_hash="abcdef123456",
        )
        session.add(evd)

        # Feature links
        fl1 = FeatureRecordLink(
            feature_id=feat.id,
            record_type="issue",
            record_id=issue.source_issue_id,
            relationship_type="INTRODUCED_BY",
            confidence=1.0,
        )
        fl2 = FeatureRecordLink(
            feature_id=feat.id,
            record_type="temporal_event",
            record_id=str(ev.id),
            relationship_type="IMPLEMENTED_BY",
            confidence=0.98,
        )
        session.add_all([fl1, fl2])
        session.commit()

        yield session
    finally:
        session.close()


def test_feature_subgraph_traversal(graph_db):
    svc = KnowledgeGraphService(graph_db, dataset_name="TestGraph")
    feat = graph_db.query(Feature).filter(Feature.canonical_name == "Log Compaction").first()

    graph = svc.get_feature_graph(feature_id=feat.id, depth=2, max_nodes=50)

    assert graph.root_node_id == f"feature:{feat.id}"
    assert graph.node_count >= 5  # Feature, Alias, Issue, TemporalEvent, Contributor/Commit
    assert graph.edge_count >= 4

    node_types = {n.type for n in graph.nodes}
    assert "FEATURE" in node_types
    assert "ALIAS" in node_types
    assert "ISSUE" in node_types
    assert "TEMPORAL_EVENT" in node_types

    edge_types = {e.type for e in graph.edges}
    assert "FEATURE_HAS_ALIAS" in edge_types
    assert "FEATURE_INTRODUCED_BY" in edge_types or "FEATURE_RELATED_TO_EVENT" in edge_types


def test_depth_bounding(graph_db):
    svc = KnowledgeGraphService(graph_db, dataset_name="TestGraph")
    feat = graph_db.query(Feature).filter(Feature.canonical_name == "Log Compaction").first()

    # Depth 1: Only immediate neighbors (Feature, Alias, Issue, TemporalEvent)
    graph_d1 = svc.get_feature_graph(feature_id=feat.id, depth=1)
    # Depth 2: Multi-hop (traverses from Issue to Commit and Contributor, from Event to Evidence)
    graph_d2 = svc.get_feature_graph(feature_id=feat.id, depth=2)

    assert graph_d2.node_count >= graph_d1.node_count
    assert graph_d2.edge_count >= graph_d1.edge_count


def test_max_nodes_limit(graph_db):
    svc = KnowledgeGraphService(graph_db, dataset_name="TestGraph")
    feat = graph_db.query(Feature).filter(Feature.canonical_name == "Log Compaction").first()

    # Strict limit = 3
    graph = svc.get_feature_graph(feature_id=feat.id, depth=2, max_nodes=3)
    assert graph.node_count <= 3


def test_issue_subgraph_traversal(graph_db):
    svc = KnowledgeGraphService(graph_db, dataset_name="TestGraph")
    req = GraphQueryRequest(
        start_node_type="ISSUE",
        start_node_id="KAFKA-555",
        depth=2,
        dataset_name="TestGraph",
    )
    graph = svc.query_graph(req)

    assert graph.root_node_id == "issue:KAFKA-555"
    assert graph.node_count >= 2
    # Verify commit and contributor are discovered
    types = {n.type for n in graph.nodes}
    assert "ISSUE" in types
    assert "COMMIT" in types or "CONTRIBUTOR" in types


def test_time_window_filtering(graph_db):
    svc = KnowledgeGraphService(graph_db, dataset_name="TestGraph")
    feat = graph_db.query(Feature).filter(Feature.canonical_name == "Log Compaction").first()

    # Filter for future window where no events/commits existed
    future_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    future_end = datetime(2025, 12, 31, tzinfo=timezone.utc)

    graph = svc.get_feature_graph(
        feature_id=feat.id,
        depth=2,
        start_time=future_start,
        end_time=future_end,
    )
    # Timed nodes like events and commits should be filtered out
    event_nodes = [n for n in graph.nodes if n.type in {"TEMPORAL_EVENT", "COMMIT"}]
    assert len(event_nodes) == 0


def test_graph_api_endpoints():
    client = TestClient(app)

    # 1. Stats
    resp_stats = client.get("/api/graph/stats")
    assert resp_stats.status_code == 200
    data = resp_stats.json()
    assert "canonical_features" in data
    assert "temporal_events" in data

    # 2. Features list to get an ID
    resp_feat = client.get("/api/features")
    assert resp_feat.status_code == 200
    features = resp_feat.json()
    if features:
        feat_id = features[0]["id"]
        resp_graph = client.get(f"/api/graph/features/{feat_id}?depth=2&max_nodes=30")
        assert resp_graph.status_code == 200
        g_data = resp_graph.json()
        assert "nodes" in g_data
        assert "edges" in g_data
        assert g_data["node_count"] >= 1

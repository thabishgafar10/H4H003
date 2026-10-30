"""
Automated test suite for Work 04: Feature Identity and Entity Resolution.
Tests canonical identity creation, aliases, deterministic & fuzzy matching,
rename detection, record linking, idempotency, and REST APIs.
"""

from datetime import datetime, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.temporal_event import TemporalEvent
from app.models.issue import Issue
from app.services.feature_service import FeatureService, normalize_text, compute_string_similarity
from starlette.testclient import TestClient
from app.main import app


@pytest.fixture
def feature_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    TestingSession = sessionmaker(bind=engine)
    session = TestingSession()
    try:
        yield session
    finally:
        session.close()


def test_normalize_text():
    assert normalize_text("Consumer-Group_v2.0 (Core)") == "consumer group v2 0 core"
    assert normalize_text("  KIP-500: Raft Mode!  ") == "kip 500 raft mode"
    assert normalize_text("") == ""


def test_create_canonical_feature_and_aliases(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    feat = svc.get_or_create_feature(
        canonical_name="Kafka Streams",
        feature_category="STREAMS",
        description="Stream processing library for Apache Kafka",
    )
    assert feat.id is not None
    assert feat.normalized_name == "kafka streams"
    assert feat.resolution_status == "CONFIRMED"

    # Add multiple aliases
    al1 = svc.add_alias(feat.id, "KStreams", alias_type="ACRONYM", confidence=1.0)
    al2 = svc.add_alias(feat.id, "Streams API", alias_type="ALTERNATIVE_NAME", confidence=0.95)
    al3 = svc.add_alias(feat.id, "KIP-28", alias_type="KIP", confidence=1.0)

    assert al1.id is not None
    assert al2.id is not None
    assert al3.id is not None
    assert len(feat.aliases) == 3

    # Idempotent re-creation
    feat_again = svc.get_or_create_feature("Kafka Streams")
    assert feat_again.id == feat.id
    al_again = svc.add_alias(feat.id, "KStreams")
    assert al_again.id == al1.id


def test_exact_and_alias_resolution(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    feat = svc.get_or_create_feature("Log Compaction", feature_category="STORAGE")
    svc.add_alias(feat.id, "Topic Compaction", alias_type="ALTERNATIVE_NAME", confidence=0.95)
    svc.add_alias(feat.id, "LogCleaner", alias_type="COMPONENT", confidence=0.90)

    # 1. Exact canonical
    m1 = svc.resolve_feature("Log Compaction")
    assert len(m1) >= 1
    assert m1[0].feature.id == feat.id
    assert m1[0].match_type == "EXACT_CANONICAL"
    assert m1[0].confidence == 1.0

    # 2. Exact alias
    m2 = svc.resolve_feature("Topic Compaction")
    assert len(m2) >= 1
    assert m2[0].feature.id == feat.id
    assert m2[0].match_type == "EXACT_ALIAS"
    assert m2[0].confidence == 0.95


def test_kip_pattern_resolution(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    feat = svc.get_or_create_feature("KRaft Mode", feature_category="CORE")
    svc.add_alias(feat.id, "KIP-500", alias_type="KIP", confidence=1.0)

    matches = svc.resolve_feature("Discussion on KIP-500 implementation")
    assert len(matches) >= 1
    assert matches[0].feature.canonical_name == "KRaft Mode"
    assert matches[0].match_type == "KIP_REFERENCE"
    assert matches[0].confidence >= 0.95


def test_fuzzy_similarity_and_thresholds(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    feat = svc.get_or_create_feature("Consumer Group Rebalance", feature_category="CLIENT")

    # High similarity -> CONFIRMED
    matches_high = svc.resolve_feature("consumer group rebalancing")
    assert len(matches_high) >= 1
    assert matches_high[0].feature.id == feat.id
    assert matches_high[0].confidence >= 0.85
    assert matches_high[0].resolution_status == "CONFIRMED"

    # Distant unrelated query -> None or < 0.60
    matches_none = svc.resolve_feature("unrelated quantum database transaction")
    assert len(matches_none) == 0


def test_feature_rename_preservation(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    event_time = datetime(2021, 5, 1, 12, 0, tzinfo=timezone.utc)
    text = "The component was renamed from legacy mirror maker to MirrorMaker 2.0 as part of KIP-382."

    svc.detect_and_record_renames(text, event_time=event_time)

    # Verify new canonical feature exists
    feat = feature_db.query(Feature).filter(Feature.canonical_name == "MirrorMaker 2.0").first()
    assert feat is not None

    # Verify old name preserved as RENAMED_FROM alias with valid_until
    alias = (
        feature_db.query(FeatureAlias)
        .filter(FeatureAlias.feature_id == feat.id, FeatureAlias.alias_name == "legacy mirror maker")
        .first()
    )
    assert alias is not None
    assert alias.alias_type == "RENAMED_FROM"
    assert alias.valid_until.replace(tzinfo=timezone.utc) == event_time


def test_feature_record_linking(feature_db):
    svc = FeatureService(feature_db, dataset_name="TestDataset")
    feat = svc.get_or_create_feature("Exactly-Once Semantics", feature_category="CORE")

    link1 = svc.link_record(
        feature_id=feat.id,
        record_type="issue",
        record_id="KAFKA-98",
        relationship_type="INTRODUCED_BY",
        confidence=1.0,
        source_evidence="KAFKA-98: Exactly-Once Delivery and Transactional Messaging",
    )
    assert link1.id is not None
    assert link1.evidence_hash is not None

    # Link commit
    link2 = svc.link_record(
        feature_id=feat.id,
        record_type="commit",
        record_id="commit123abc",
        relationship_type="IMPLEMENTED_BY",
        confidence=1.0,
        source_evidence="implement idempotent and transactional producer for EOS",
    )
    assert link2.id is not None

    # Idempotent repeated linking
    link1_repeat = svc.link_record(
        feature_id=feat.id,
        record_type="issue",
        record_id="KAFKA-98",
        relationship_type="INTRODUCED_BY",
    )
    assert link1_repeat.id == link1.id

    assert len(feat.record_links) == 2


def test_feature_api_endpoints():
    client = TestClient(app)

    # 1. Seed features
    seed_res = client.post("/api/features/seed?dataset_name=KafkaSQLite")
    assert seed_res.status_code == 200
    assert seed_res.json()["features_seeded"] >= 10

    # 2. List features
    list_res = client.get("/api/features?limit=10&dataset_name=KafkaSQLite")
    assert list_res.status_code == 200
    features = list_res.json()
    assert len(features) >= 10

    # 3. Get feature detail
    feat_id = features[0]["id"]
    detail_res = client.get(f"/api/features/{feat_id}")
    assert detail_res.status_code == 200
    assert "aliases" in detail_res.json()
    assert "record_links" in detail_res.json()

    # 4. Resolve endpoint
    resolve_res = client.post("/api/features/resolve?query=KIP-500&dataset_name=KafkaSQLite")
    assert resolve_res.status_code == 200
    res_data = resolve_res.json()
    assert res_data["total_matches"] >= 1
    assert res_data["candidates"][0]["canonical_name"] == "KRaft Mode"

    # 5. Feature history endpoint
    hist_res = client.get(f"/api/features/{feat_id}/history")
    assert hist_res.status_code == 200
    assert isinstance(hist_res.json(), list)


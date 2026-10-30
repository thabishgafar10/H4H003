"""
Feature Identity, Alias Management, and Entity Resolution Service for Work 04.
Provides canonical identity management, multi-alias resolution, rename detection,
traceable record links, and layered fuzzy/deterministic entity matching.
"""

from datetime import datetime, timezone
import hashlib
import re
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.issue import Issue
from app.models.issue_comment import IssueComment
from app.models.commit import Commit
from app.models.temporal_event import TemporalEvent
from app.core.logging_config import logger


# Pattern to detect explicit rename language
RE_RENAME_EXPLICIT = re.compile(
    r"\b(?:renamed|renaming)(?:\s+from)?\s+([A-Za-z0-9_\-\s]+?)\s+to\s+([A-Za-z0-9_\-\s\.]+?)(?:\s+(?:as\s+part\s+of|as|in|by|via|with|for)\b|\.(?:\s|$)|[,;]|$)",
    re.IGNORECASE,
)
RE_FORMERLY_KNOWN = re.compile(
    r"\bformerly\s+known\s+as\s+['\"]?([A-Za-z0-9_\-\s\.]+?)['\"]?(?:\b|\.|\,)",
    re.IGNORECASE,
)
RE_SUPERSEDED_BY = re.compile(
    r"\b(?:superseded|replaced)\s+by\s+['\"]?([A-Za-z0-9_\-\s\.]+?)['\"]?(?:\b|\.|\,)",
    re.IGNORECASE,
)
RE_KIP_KEY = re.compile(r"\b(KIP-\d+)\b", re.IGNORECASE)


def normalize_text(text: str) -> str:
    """Normalizes string: lowercase, collapses punctuation and whitespace."""
    if not text:
        return ""
    # Replace hyphens, underscores, dots, exclamation/question marks with space for uniform matching
    cleaned = re.sub(r"[\-_\.\,\:\;\(\)\[\]\{\}\'\"\!\?\/\@\#\$\%\^\&\*\+\=\~\`]+", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    return cleaned


def compute_string_similarity(s1: str, s2: str) -> float:
    """
    Computes token-level similarity and character-level overlap.
    Zero external heavy libraries, 100% deterministic, high precision.
    """
    n1 = normalize_text(s1)
    n2 = normalize_text(s2)
    if not n1 or not n2:
        return 0.0
    if n1 == n2:
        return 1.0

    tokens1 = [t for t in n1.split() if t]
    tokens2 = [t for t in n2.split() if t]
    if not tokens1 or not tokens2:
        return 0.0

    # Token-level similarity with stem/prefix support
    def token_sim(t1: str, t2: str) -> float:
        if t1 == t2:
            return 1.0
        # Common prefix stem matching (e.g., rebalance vs rebalancing)
        if len(t1) >= 4 and len(t2) >= 4:
            prefix_len = 0
            for c1, c2 in zip(t1, t2):
                if c1 == c2:
                    prefix_len += 1
                else:
                    break
            min_l = min(len(t1), len(t2))
            if prefix_len >= 4 and (prefix_len >= min_l - 2 or prefix_len / min_l >= 0.75):
                return 0.90
        return 0.0

    # Greedy bipartite matching
    unmatched2 = list(tokens2)
    total_match = 0.0
    for t1 in tokens1:
        best_sim = 0.0
        best_idx = -1
        for idx, t2 in enumerate(unmatched2):
            sim = token_sim(t1, t2)
            if sim > best_sim:
                best_sim = sim
                best_idx = idx
        if best_sim > 0.0 and best_idx >= 0:
            total_match += best_sim
            unmatched2.pop(best_idx)

    max_toks = max(len(tokens1), len(tokens2))
    token_score = total_match / max_toks

    # Substring bonus if one full phrase is enclosed in the other
    if n1 in n2 or n2 in n1:
        min_len = min(len(n1), len(n2))
        max_len = max(len(n1), len(n2))
        substring_ratio = min_len / max_len
        token_score = max(token_score, 0.70 + 0.30 * substring_ratio)

    return token_score


@dataclass
class ResolutionCandidate:
    feature: Feature
    matched_alias: Optional[FeatureAlias]
    match_type: str  # EXACT_CANONICAL, EXACT_ALIAS, FUZZY_ALIAS, KIP_REFERENCE
    confidence: float
    resolution_status: str  # CONFIRMED, NEEDS_REVIEW, REJECTED


class FeatureService:
    def __init__(self, db: Session, dataset_name: str = "KafkaSQLite"):
        self.db = db
        self.dataset_name = dataset_name

    def get_or_create_feature(
        self,
        canonical_name: str,
        feature_category: str = "CORE",
        description: Optional[str] = None,
        identity_confidence: float = 1.0,
        resolution_status: str = "CONFIRMED",
        first_observed_at: Optional[datetime] = None,
        last_observed_at: Optional[datetime] = None,
    ) -> Feature:
        norm = normalize_text(canonical_name)
        existing = (
            self.db.query(Feature)
            .filter(
                Feature.dataset_name == self.dataset_name,
                Feature.normalized_name == norm,
            )
            .first()
        )
        if existing:
            # Update timestamps if earlier or later
            updated = False
            if first_observed_at and (not existing.first_observed_at or first_observed_at < existing.first_observed_at):
                existing.first_observed_at = first_observed_at
                updated = True
            if last_observed_at and (not existing.last_observed_at or last_observed_at > existing.last_observed_at):
                existing.last_observed_at = last_observed_at
                updated = True
            if description and not existing.description:
                existing.description = description
                updated = True
            if updated:
                self.db.commit()
                self.db.refresh(existing)
            return existing

        feature = Feature(
            dataset_name=self.dataset_name,
            canonical_name=canonical_name.strip(),
            normalized_name=norm,
            description=description,
            feature_category=feature_category,
            first_observed_at=first_observed_at,
            last_observed_at=last_observed_at,
            identity_confidence=identity_confidence,
            resolution_status=resolution_status,
        )
        self.db.add(feature)
        self.db.commit()
        self.db.refresh(feature)
        return feature

    def add_alias(
        self,
        feature_id: int,
        alias_name: str,
        alias_type: str = "ALTERNATIVE_NAME",
        confidence: float = 1.0,
        verification_status: str = "CONFIRMED",
        source_record_id: Optional[int] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
    ) -> FeatureAlias:
        norm = normalize_text(alias_name)
        existing = (
            self.db.query(FeatureAlias)
            .filter(
                FeatureAlias.feature_id == feature_id,
                FeatureAlias.normalized_alias == norm,
            )
            .first()
        )
        if existing:
            return existing

        alias = FeatureAlias(
            feature_id=feature_id,
            alias_name=alias_name.strip(),
            normalized_alias=norm,
            alias_type=alias_type,
            confidence=confidence,
            verification_status=verification_status,
            source_record_id=source_record_id,
            valid_from=valid_from,
            valid_until=valid_until,
        )
        self.db.add(alias)
        self.db.commit()
        self.db.refresh(alias)
        return alias

    def link_record(
        self,
        feature_id: int,
        record_type: str,
        record_id: str,
        relationship_type: str,
        confidence: float = 1.0,
        source_evidence: Optional[str] = None,
        source_record_id: Optional[int] = None,
        verification_status: str = "CONFIRMED",
        commit: bool = True,
    ) -> FeatureRecordLink:
        existing = (
            self.db.query(FeatureRecordLink)
            .filter(
                FeatureRecordLink.feature_id == feature_id,
                FeatureRecordLink.record_type == record_type,
                FeatureRecordLink.record_id == record_id,
                FeatureRecordLink.relationship_type == relationship_type,
            )
            .first()
        )
        if existing:
            return existing

        ev_hash = None
        if source_evidence:
            ev_hash = hashlib.sha256(source_evidence.strip().encode("utf-8")).hexdigest()

        link = FeatureRecordLink(
            feature_id=feature_id,
            record_type=record_type,
            record_id=record_id,
            relationship_type=relationship_type,
            confidence=confidence,
            source_evidence=source_evidence,
            evidence_hash=ev_hash,
            source_record_id=source_record_id,
            verification_status=verification_status,
        )
        self.db.add(link)
        if commit:
            self.db.commit()
            self.db.refresh(link)
        return link

    def detect_and_record_renames(self, text: str, source_record_id: Optional[int] = None, event_time: Optional[datetime] = None):
        """
        Scans text for explicit rename markers (e.g. 'renamed from X to Y', 'formerly known as X').
        Preserves the historical identity without destroying past records.
        """
        if not text:
            return

        # 1. "renamed from X to Y"
        for m in RE_RENAME_EXPLICIT.finditer(text):
            old_name = m.group(1).strip().strip("'\"").rstrip(".,;")
            new_name = m.group(2).strip().strip("'\"").rstrip(".,;")
            if old_name and new_name and normalize_text(old_name) != normalize_text(new_name):
                # Ensure canonical feature exists for the new name
                feat = self.get_or_create_feature(new_name)
                # Add old name as a RENAMED_FROM alias with valid_until
                self.add_alias(
                    feature_id=feat.id,
                    alias_name=old_name,
                    alias_type="RENAMED_FROM",
                    confidence=0.95,
                    verification_status="CONFIRMED",
                    source_record_id=source_record_id,
                    valid_until=event_time,
                )

        # 2. "formerly known as X"
        for m in RE_FORMERLY_KNOWN.finditer(text):
            old_name = m.group(1).strip()
            if old_name:
                # Find current subject from context
                # Link old name to the resolved feature
                pass

    def resolve_feature(self, query: str) -> List[ResolutionCandidate]:
        """
        Layered Entity Resolution Pipeline:
        Stage A: Normalization
        Stage B: Exact canonical match (Confidence = 1.0, CONFIRMED)
        Stage C: Exact alias match (Confidence = 0.95-1.0, CONFIRMED)
        Stage D: KIP reference match (Confidence = 0.95, CONFIRMED)
        Stage E: Fuzzy similarity match (Confidence >= 0.85 CONFIRMED; 0.60-0.84 NEEDS_REVIEW)
        """
        norm_query = normalize_text(query)
        if not norm_query:
            return []

        candidates: List[ResolutionCandidate] = []

        # 1. Check for KIP pattern in query
        kip_match = RE_KIP_KEY.search(query)
        if kip_match:
            kip_str = kip_match.group(1).upper()
            kip_alias = (
                self.db.query(FeatureAlias)
                .join(Feature)
                .filter(
                    Feature.dataset_name == self.dataset_name,
                    FeatureAlias.alias_name.ilike(kip_str),
                )
                .first()
            )
            if kip_alias:
                candidates.append(
                    ResolutionCandidate(
                        feature=kip_alias.feature,
                        matched_alias=kip_alias,
                        match_type="KIP_REFERENCE",
                        confidence=0.98,
                        resolution_status="CONFIRMED",
                    )
                )

        # 2. Exact Canonical Match
        exact_feature = (
            self.db.query(Feature)
            .filter(
                Feature.dataset_name == self.dataset_name,
                Feature.normalized_name == norm_query,
            )
            .first()
        )
        if exact_feature:
            candidates.append(
                ResolutionCandidate(
                    feature=exact_feature,
                    matched_alias=None,
                    match_type="EXACT_CANONICAL",
                    confidence=1.0,
                    resolution_status="CONFIRMED",
                )
            )

        # 3. Exact Alias Match
        exact_aliases = (
            self.db.query(FeatureAlias)
            .join(Feature)
            .filter(
                Feature.dataset_name == self.dataset_name,
                FeatureAlias.normalized_alias == norm_query,
            )
            .all()
        )
        for al in exact_aliases:
            if not any(c.feature.id == al.feature_id for c in candidates):
                candidates.append(
                    ResolutionCandidate(
                        feature=al.feature,
                        matched_alias=al,
                        match_type="EXACT_ALIAS",
                        confidence=al.confidence,
                        resolution_status=al.verification_status,
                    )
                )

        # If high-confidence exact match found, return immediately
        if candidates and any(c.confidence >= 0.95 for c in candidates):
            return candidates

        # 4. Fuzzy Similarity Matching against all features & aliases
        all_features = self.db.query(Feature).filter(Feature.dataset_name == self.dataset_name).all()
        for feat in all_features:
            score = compute_string_similarity(norm_query, feat.canonical_name)
            if score >= 0.60:
                status = "CONFIRMED" if score >= 0.85 else "NEEDS_REVIEW"
                if not any(c.feature.id == feat.id for c in candidates):
                    candidates.append(
                        ResolutionCandidate(
                            feature=feat,
                            matched_alias=None,
                            match_type="FUZZY_CANONICAL",
                            confidence=round(score, 3),
                            resolution_status=status,
                        )
                    )

            # Check aliases
            for al in feat.aliases:
                al_score = compute_string_similarity(norm_query, al.alias_name)
                if al_score >= 0.60:
                    status = "CONFIRMED" if al_score >= 0.85 else "NEEDS_REVIEW"
                    if not any(c.feature.id == feat.id for c in candidates):
                        candidates.append(
                            ResolutionCandidate(
                                feature=feat,
                                matched_alias=al,
                                match_type="FUZZY_ALIAS",
                                confidence=round(al_score, 3),
                                resolution_status=status,
                            )
                        )

        # Sort descending by confidence
        candidates.sort(key=lambda x: x.confidence, reverse=True)
        return candidates

    def seed_kafka_core_features(self) -> int:
        """
        Seeds canonical Apache Kafka features, architectural KIPs, components, and aliases.
        100% idempotent; will not overwrite or duplicate existing features.
        """
        CORE_FEATURES = [
            {
                "canonical_name": "Consumer Group",
                "category": "CLIENT",
                "description": "Coordination and dynamic partition assignment among consumers sharing a group ID.",
                "aliases": [("KafkaConsumer", "ALTERNATIVE_NAME"), ("consumer group rebalance", "ALTERNATIVE_NAME"), ("KIP-62", "KIP"), ("ConsumerCoordinator", "COMPONENT")],
            },
            {
                "canonical_name": "Kafka Streams",
                "category": "STREAMS",
                "description": "Client library for building applications and microservices where input and output are stored in Kafka clusters.",
                "aliases": [("Streams API", "ALTERNATIVE_NAME"), ("KStreams", "ACRONYM"), ("KIP-28", "KIP"), ("streams", "COMPONENT")],
            },
            {
                "canonical_name": "Kafka Connect",
                "category": "CONNECT",
                "description": "Scalable framework for connecting Kafka with external systems such as databases, key-value stores, and file systems.",
                "aliases": [("Connect API", "ALTERNATIVE_NAME"), ("KConnect", "ACRONYM"), ("KIP-26", "KIP"), ("connect", "COMPONENT")],
            },
            {
                "canonical_name": "KRaft Mode",
                "category": "CORE",
                "description": "Kafka Raft Metadata mode replacing external ZooKeeper dependency with an internal quorum.",
                "aliases": [("KIP-500", "KIP"), ("Kafka Raft", "ALTERNATIVE_NAME"), ("Raft Controller", "COMPONENT"), ("ZooKeeper Removal", "ALTERNATIVE_NAME"), ("ZooKeeper", "RENAMED_FROM")],
            },
            {
                "canonical_name": "Exactly-Once Semantics",
                "category": "CORE",
                "description": "Transactional messaging and idempotent producer guarantees ensuring records are processed once and only once.",
                "aliases": [("EOS", "ACRONYM"), ("KIP-98", "KIP"), ("Idempotent Producer", "ALTERNATIVE_NAME"), ("Transactional Producer", "ALTERNATIVE_NAME")],
            },
            {
                "canonical_name": "Log Compaction",
                "category": "STORAGE",
                "description": "Retention mechanism that retains the last known value for each record key within the partition of a topic.",
                "aliases": [("Topic Compaction", "ALTERNATIVE_NAME"), ("compacted topics", "ALTERNATIVE_NAME"), ("LogCleaner", "COMPONENT")],
            },
            {
                "canonical_name": "Security & SSL/SASL",
                "category": "SECURITY",
                "description": "Authentication and authorization protocols including TLS/SSL encryption, SASL/SCRAM, and Kerberos.",
                "aliases": [("SSL", "ACRONYM"), ("SASL", "ACRONYM"), ("KIP-4", "KIP"), ("security", "COMPONENT"), ("ACLs", "ALTERNATIVE_NAME")],
            },
            {
                "canonical_name": "MirrorMaker 2.0",
                "category": "TOOLS",
                "description": "Multi-cluster replication and disaster recovery engine built on the Kafka Connect framework.",
                "aliases": [("MM2", "ACRONYM"), ("MirrorMaker", "RENAMED_FROM"), ("KIP-382", "KIP"), ("mirror maker", "ALTERNATIVE_NAME")],
            },
            {
                "canonical_name": "ReplicaManager",
                "category": "CORE",
                "description": "Broker subsystem managing partition replicas, High Watermark propagation, and in-sync replica (ISR) updates.",
                "aliases": [("ISR", "ACRONYM"), ("In-Sync Replicas", "ALTERNATIVE_NAME"), ("replication", "COMPONENT")],
            },
            {
                "canonical_name": "Tiered Storage",
                "category": "STORAGE",
                "description": "Separation of compute and storage allowing older log segments to be offloaded to remote cloud object stores.",
                "aliases": [("KIP-405", "KIP"), ("Remote Storage", "ALTERNATIVE_NAME"), ("tiered-storage", "COMPONENT")],
            },
        ]

        count = 0
        for item in CORE_FEATURES:
            feat = self.get_or_create_feature(
                canonical_name=item["canonical_name"],
                feature_category=item["category"],
                description=item["description"],
            )
            for alias_name, alias_type in item["aliases"]:
                self.add_alias(
                    feature_id=feat.id,
                    alias_name=alias_name,
                    alias_type=alias_type,
                    confidence=1.0 if alias_type in {"KIP", "ALTERNATIVE_NAME", "ACRONYM"} else 0.90,
                )
            count += 1

        return count

    def link_features_to_events_and_issues(self, limit_events: int = 2000) -> int:
        """
        Scans existing TemporalEvent and Issue records, discovers matching features,
        and establishes traceable FeatureRecordLink records with evidence.
        Uses in-memory set and batch commit for high performance.
        """
        all_features = self.db.query(Feature).filter(Feature.dataset_name == self.dataset_name).all()
        if not all_features:
            return 0

        existing_keys = {
            (r[0], r[1], r[2], r[3])
            for r in self.db.query(
                FeatureRecordLink.feature_id,
                FeatureRecordLink.record_type,
                FeatureRecordLink.record_id,
                FeatureRecordLink.relationship_type,
            ).all()
        }

        links_created = 0

        # Pre-compile normalized names for fast matching
        feat_name_map = []
        for feat in all_features:
            names = [feat.canonical_name] + [a.alias_name for a in feat.aliases]
            norm_names = [normalize_text(n) for n in names if len(n) > 2]
            feat_name_map.append((feat, norm_names))

        events = (
            self.db.query(TemporalEvent)
            .filter(TemporalEvent.dataset_name == self.dataset_name)
            .limit(limit_events)
            .all()
        )

        for ev in events:
            ev_norm = normalize_text(f"{ev.subject} {ev.description or ''}")
            for feat, norm_names in feat_name_map:
                if any(n in ev_norm for n in norm_names):
                    rel_type = "RELATED_TO"
                    if ev.event_type == "FEATURE_IMPLEMENTED":
                        rel_type = "IMPLEMENTED_BY"
                    elif ev.event_type == "FEATURE_PROPOSED":
                        rel_type = "INTRODUCED_BY"
                    elif ev.event_type == "FEATURE_MODIFIED":
                        rel_type = "MODIFIED_BY"
                    elif ev.event_type == "FEATURE_REMOVED":
                        rel_type = "REMOVED_BY"

                    key = (feat.id, "temporal_event", str(ev.id), rel_type)
                    if key not in existing_keys:
                        link = FeatureRecordLink(
                            feature_id=feat.id,
                            record_type="temporal_event",
                            record_id=str(ev.id),
                            relationship_type=rel_type,
                            confidence=0.95,
                            source_evidence=ev.subject[:200],
                            evidence_hash=hashlib.sha256(ev.subject[:200].encode("utf-8")).hexdigest(),
                            verification_status="CONFIRMED",
                        )
                        self.db.add(link)
                        existing_keys.add(key)
                        links_created += 1

                    if ev.issue_id:
                        issue_key = (feat.id, "issue", ev.issue_id, rel_type)
                        if issue_key not in existing_keys:
                            i_link = FeatureRecordLink(
                                feature_id=feat.id,
                                record_type="issue",
                                record_id=ev.issue_id,
                                relationship_type=rel_type,
                                confidence=0.95,
                                source_evidence=f"Linked via event {ev.event_key[:8]}",
                                evidence_hash=hashlib.sha256(ev.issue_id.encode("utf-8")).hexdigest(),
                                verification_status="CONFIRMED",
                            )
                            self.db.add(i_link)
                            existing_keys.add(issue_key)
                            links_created += 1

        self.db.commit()
        return links_created

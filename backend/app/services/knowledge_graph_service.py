"""
Knowledge Graph Engine and Traversal Service for Work 05.
Supports multi-hop traversal, time-bounded slicing, entity resolution integration,
and bounded-graph extraction across Features, Issues, Commits, Events, Contributors, and Evidence.
"""

from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.issue_relationships import CommitIssueLink
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.models.source_record import SourceRecord
from app.schemas.graph import GraphNode, GraphEdge, GraphResponse, GraphQueryRequest
from app.core.logging_config import logger


class KnowledgeGraphService:
    def __init__(self, db: Session, dataset_name: str = "KafkaSQLite", project_id: Optional[int] = None):
        self.db = db
        self.dataset_name = dataset_name
        self.project_id = project_id

    def _make_node_id(self, node_type: str, raw_id: Any) -> str:
        return f"{node_type.lower()}:{raw_id}"

    def _make_edge_id(self, source_id: str, target_id: str, edge_type: str) -> str:
        return f"e:{source_id}->{target_id}:{edge_type}"

    def get_feature_graph(
        self,
        feature_id: int,
        depth: int = 2,
        max_nodes: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> GraphResponse:
        """Retrieves an interactive knowledge subgraph centered on a canonical Feature."""
        req = GraphQueryRequest(
            start_node_type="FEATURE",
            start_node_id=str(feature_id),
            depth=depth,
            max_nodes=max_nodes,
            start_time=start_time,
            end_time=end_time,
            dataset_name=self.dataset_name,
        )
        return self.query_graph(req)

    def query_graph(self, req: GraphQueryRequest) -> GraphResponse:
        """General multi-hop graph traversal engine with cycle prevention and node bounds."""
        nodes_dict: Dict[str, GraphNode] = {}
        edges_dict: Dict[str, GraphEdge] = {}
        visited_nodes: Set[str] = set()

        raw_type = (req.start_node_type or "").strip().upper()
        raw_id = (req.start_node_id or "").strip()
        root_id = self._make_node_id(raw_type, raw_id)

        # 1. Fetch and create root node
        if raw_type == "FEATURE":
            try:
                feat_id = int(raw_id)
            except ValueError:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            feat_q = self.db.query(Feature).filter(Feature.id == feat_id)
            if self.project_id:
                feat_q = feat_q.filter(Feature.project_id == self.project_id)
            elif req.dataset_name:
                feat_q = feat_q.filter(Feature.dataset_name == req.dataset_name)
            feat = feat_q.first()
            if not feat:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            root_id = self._make_node_id("feature", feat.id)
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="FEATURE",
                label=feat.canonical_name,
                properties={
                    "category": feat.feature_category,
                    "status": feat.resolution_status,
                    "confidence": feat.identity_confidence,
                    "first_observed": feat.first_observed_at.isoformat() if feat.first_observed_at else None,
                    "last_observed": feat.last_observed_at.isoformat() if feat.last_observed_at else None,
                    "description": feat.description,
                },
            )
        elif raw_type == "ISSUE":
            issue_q = (
                self.db.query(Issue)
                .join(SourceRecord, Issue.source_record_id == SourceRecord.id)
                .filter(func.upper(Issue.source_issue_id) == raw_id.upper())
            )
            if self.project_id:
                issue_q = issue_q.filter(SourceRecord.project_id == self.project_id)
            elif req.dataset_name:
                issue_q = issue_q.filter(SourceRecord.dataset_name == req.dataset_name)
            issue = issue_q.first()
            if not issue:
                if not self.project_id:
                    fallback = self.db.query(Issue).filter(func.upper(Issue.source_issue_id) == raw_id.upper()).first()
                    if fallback and (not fallback.source_record_id or not req.dataset_name or fallback.dataset_name == req.dataset_name):
                        issue = fallback
                if not issue:
                    return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            root_id = self._make_node_id("issue", issue.source_issue_id)
            lbl = f"{issue.source_issue_id}: {issue.summary[:35]}" if issue.summary else issue.source_issue_id
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="ISSUE",
                label=lbl,
                properties={
                    "source_issue_id": issue.source_issue_id,
                    "summary": issue.summary,
                    "issue_type": issue.issue_type,
                    "status": issue.status,
                    "created_at": issue.created_at.isoformat() if issue.created_at else None,
                },
            )
        elif raw_type == "COMMIT":
            # 1. Exact or prefix match on Commit hash
            cmt_q = (
                self.db.query(Commit)
                .join(SourceRecord, Commit.source_record_id == SourceRecord.id)
                .filter(Commit.commit_hash.ilike(f"{raw_id}%"))
            )
            if self.project_id:
                cmt_q = cmt_q.filter(SourceRecord.project_id == self.project_id)
            elif req.dataset_name:
                cmt_q = cmt_q.filter(SourceRecord.dataset_name == req.dataset_name)
            cmt = cmt_q.first()

            if not cmt and not self.project_id:
                cmt = self.db.query(Commit).filter(Commit.commit_hash.ilike(f"{raw_id}%")).first()

            # 2. Check if raw_id is an issue key linked to a commit
            if not cmt:
                cil_q = (
                    self.db.query(CommitIssueLink)
                    .join(Commit, CommitIssueLink.commit_hash == Commit.commit_hash)
                    .join(SourceRecord, Commit.source_record_id == SourceRecord.id)
                    .filter(func.upper(CommitIssueLink.source_issue_id) == raw_id.upper())
                )
                if self.project_id:
                    cil_q = cil_q.filter(SourceRecord.project_id == self.project_id)
                elif req.dataset_name:
                    cil_q = cil_q.filter(SourceRecord.dataset_name == req.dataset_name)
                cil = cil_q.first()
                if not cil and not self.project_id:
                    cil = self.db.query(CommitIssueLink).filter(func.upper(CommitIssueLink.source_issue_id) == raw_id.upper()).first()
                if cil:
                    cmt = self.db.query(Commit).filter(Commit.commit_hash == cil.commit_hash).first()

            # 3. Check if raw_id is an issue in the DB (even if without commits, e.g. KAFKA-10)
            if not cmt:
                issue_q = (
                    self.db.query(Issue)
                    .join(SourceRecord, Issue.source_record_id == SourceRecord.id)
                    .filter(func.upper(Issue.source_issue_id) == raw_id.upper())
                )
                if self.project_id:
                    issue_q = issue_q.filter(SourceRecord.project_id == self.project_id)
                elif req.dataset_name:
                    issue_q = issue_q.filter(SourceRecord.dataset_name == req.dataset_name)
                issue_fallback = issue_q.first()
                if not issue_fallback and not self.project_id:
                    issue_fallback = self.db.query(Issue).filter(func.upper(Issue.source_issue_id) == raw_id.upper()).first()

                if issue_fallback:
                    root_id = self._make_node_id("issue", issue_fallback.source_issue_id)
                    lbl = f"{issue_fallback.source_issue_id}: {issue_fallback.summary[:35]}" if issue_fallback.summary else issue_fallback.source_issue_id
                    nodes_dict[root_id] = GraphNode(
                        id=root_id,
                        type="ISSUE",
                        label=lbl,
                        properties={
                            "source_issue_id": issue_fallback.source_issue_id,
                            "summary": issue_fallback.summary,
                            "issue_type": issue_fallback.issue_type,
                            "status": issue_fallback.status,
                            "created_at": issue_fallback.created_at.isoformat() if issue_fallback.created_at else None,
                            "note": f"Seed COMMIT resolved to issue {issue_fallback.source_issue_id}",
                        },
                    )

            if cmt:
                root_id = self._make_node_id("commit", cmt.commit_hash)
                nodes_dict[root_id] = GraphNode(
                    id=root_id,
                    type="COMMIT",
                    label=cmt.commit_hash[:8],
                    properties={
                        "commit_hash": cmt.commit_hash,
                        "message": cmt.message[:120] if cmt.message else "",
                        "author": cmt.author,
                        "committed_at": cmt.committed_at.isoformat() if cmt.committed_at else None,
                    },
                )

            if not nodes_dict:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

        elif raw_type in ("TEMPORAL_EVENT", "EVENT"):
            try:
                ev_id = int(raw_id)
            except ValueError:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            ev_q = self.db.query(TemporalEvent).filter(TemporalEvent.id == ev_id)
            if self.project_id:
                ev_q = ev_q.filter(TemporalEvent.project_id == self.project_id)
            elif req.dataset_name:
                ev_q = ev_q.filter(TemporalEvent.dataset_name == req.dataset_name)
            ev = ev_q.first()
            if not ev:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            root_id = self._make_node_id("temporal_event", ev.id)
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="TEMPORAL_EVENT",
                label=f"{ev.event_type}: {ev.subject[:35]}",
                properties={
                    "event_type": ev.event_type,
                    "timestamp": ev.event_time.isoformat() if ev.event_time else None,
                    "actor": ev.actor,
                },
            )

        elif raw_type == "CONTRIBUTOR":
            root_id = self._make_node_id("contributor", raw_id)
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="CONTRIBUTOR",
                label=raw_id,
                properties={"role": "contributor"},
            )
        elif raw_type == "EVIDENCE":
            try:
                evd_id = int(raw_id)
            except ValueError:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            evd = self.db.query(EventEvidence).filter(EventEvidence.id == evd_id).first()
            if not evd:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            root_id = self._make_node_id("evidence", evd.id)
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="EVIDENCE",
                label=f"Excerpt: {evd.evidence_excerpt[:35]}...",
                properties={
                    "excerpt": evd.evidence_excerpt[:150],
                    "source_record_id": evd.source_record_id,
                    "hash": evd.evidence_hash[:16] if evd.evidence_hash else None,
                    "role": evd.evidence_role,
                    "event_id": evd.event_id,
                },
            )
        elif raw_type == "ALIAS":
            try:
                al_id = int(raw_id)
            except ValueError:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            al = self.db.query(FeatureAlias).filter(FeatureAlias.id == al_id).first()
            if not al:
                return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

            root_id = self._make_node_id("alias", al.id)
            nodes_dict[root_id] = GraphNode(
                id=root_id,
                type="ALIAS",
                label=al.alias_name,
                properties={
                    "alias_type": al.alias_type,
                    "confidence": al.confidence,
                    "feature_id": al.feature_id,
                },
            )
        else:
            return GraphResponse(root_node_id=root_id, node_count=0, edge_count=0, nodes=[], edges=[])

        current_frontier = {root_id}

        # BFS Traversal up to requested depth
        for current_depth in range(1, req.depth + 1):
            if len(nodes_dict) >= req.max_nodes or not current_frontier:
                break

            next_frontier: Set[str] = set()

            for node_id in current_frontier:
                if node_id in visited_nodes:
                    continue
                visited_nodes.add(node_id)

                ntype, nid = node_id.split(":", 1)
                ntype = ntype.upper()

                if ntype == "FEATURE":
                    self._expand_feature_node(
                        int(nid), node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype == "ISSUE":
                    self._expand_issue_node(
                        nid, node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype == "COMMIT":
                    self._expand_commit_node(
                        nid, node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype in ("TEMPORAL_EVENT", "EVENT"):
                    self._expand_event_node(
                        int(nid), node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype == "CONTRIBUTOR":
                    self._expand_contributor_node(
                        nid, node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype == "EVIDENCE":
                    self._expand_evidence_node(
                        int(nid), node_id, nodes_dict, edges_dict, next_frontier, req
                    )
                elif ntype == "ALIAS":
                    self._expand_alias_node(
                        int(nid), node_id, nodes_dict, edges_dict, next_frontier, req
                    )

            current_frontier = next_frontier

        # Apply time window filters if requested
        filtered_nodes = list(nodes_dict.values())
        if req.start_time or req.end_time:
            filtered_nodes = [
                n for n in filtered_nodes
                if self._node_in_time_window(n, req.start_time, req.end_time)
            ]
            valid_node_ids = {n.id for n in filtered_nodes}
            filtered_edges = [
                e for e in edges_dict.values()
                if e.source in valid_node_ids and e.target in valid_node_ids
            ]
        else:
            filtered_edges = list(edges_dict.values())

        return GraphResponse(
            root_node_id=root_id,
            node_count=len(filtered_nodes),
            edge_count=len(filtered_edges),
            nodes=filtered_nodes,
            edges=filtered_edges,
            metadata={
                "depth": req.depth,
                "dataset_name": req.dataset_name,
                "max_nodes_hit": len(nodes_dict) >= req.max_nodes,
            },
        )

    def _node_in_time_window(self, node: GraphNode, start: Optional[datetime], end: Optional[datetime]) -> bool:
        from datetime import timezone
        ts_str = node.properties.get("timestamp") or node.properties.get("created_at") or node.properties.get("committed_at") or node.properties.get("first_observed")
        if not ts_str:
            return True  # Retain timeless/structural nodes like Features
        try:
            ts = datetime.fromisoformat(ts_str)
            if start and start.tzinfo is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            elif start and start.tzinfo is None and ts.tzinfo is not None:
                start = start.replace(tzinfo=timezone.utc)

            if end and end.tzinfo is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            elif end and end.tzinfo is None and ts.tzinfo is not None:
                end = end.replace(tzinfo=timezone.utc)

            if start and ts < start:
                return False
            if end and ts > end:
                return False
            return True
        except Exception:
            return True

    def _expand_feature_node(
        self,
        feature_id: int,
        feature_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        # 1. Aliases
        feat = self.db.query(Feature).filter(Feature.id == feature_id).first()
        if feat:
            for al in feat.aliases[:5]:
                if len(nodes) >= req.max_nodes:
                    return
                alias_id = self._make_node_id("alias", al.id)
                if alias_id not in nodes:
                    nodes[alias_id] = GraphNode(
                        id=alias_id,
                        type="ALIAS",
                        label=al.alias_name,
                        properties={"alias_type": al.alias_type, "confidence": al.confidence},
                    )
                edge_id = self._make_edge_id(feature_node_id, alias_id, "FEATURE_HAS_ALIAS")
                edges[edge_id] = GraphEdge(
                    id=edge_id,
                    source=feature_node_id,
                    target=alias_id,
                    type="FEATURE_HAS_ALIAS",
                    properties={"confidence": al.confidence},
                )

        # 2. Record links (issues, commits, events)
        links = (
            self.db.query(FeatureRecordLink)
            .filter(FeatureRecordLink.feature_id == feature_id)
            .limit(35)
            .all()
        )
        for link in links:
            if len(nodes) >= req.max_nodes:
                return

            if link.record_type == "issue":
                target_node_id = self._make_node_id("issue", link.record_id)
                if target_node_id not in nodes:
                    issue_q = (
                        self.db.query(Issue)
                        .join(SourceRecord, Issue.source_record_id == SourceRecord.id)
                        .filter(Issue.source_issue_id == link.record_id)
                    )
                    if self.project_id and req.dataset_name:
                        issue_q = issue_q.filter((SourceRecord.project_id == self.project_id) | (SourceRecord.dataset_name == req.dataset_name))
                    elif self.project_id:
                        issue_q = issue_q.filter(SourceRecord.project_id == self.project_id)
                    elif req.dataset_name:
                        issue_q = issue_q.filter(SourceRecord.dataset_name == req.dataset_name)
                    issue = issue_q.first()
                    if not issue:
                        fallback = self.db.query(Issue).filter(Issue.source_issue_id == link.record_id).first()
                        if fallback and not fallback.source_record_id:
                            issue = fallback
                        else:
                            continue

                    lbl = issue.summary[:40] if issue and issue.summary else link.record_id
                    nodes[target_node_id] = GraphNode(
                        id=target_node_id,
                        type="ISSUE",
                        label=f"{link.record_id}: {lbl}",
                        properties={
                            "source_issue_id": link.record_id,
                            "issue_type": issue.issue_type if issue else "Unknown",
                            "created_at": issue.created_at.isoformat() if issue and issue.created_at else None,
                        },
                    )
                    next_frontier.add(target_node_id)

                rel_type = f"FEATURE_{link.relationship_type}"
                edge_id = self._make_edge_id(feature_node_id, target_node_id, rel_type)
                edges[edge_id] = GraphEdge(
                    id=edge_id,
                    source=feature_node_id,
                    target=target_node_id,
                    type=rel_type,
                    properties={"confidence": link.confidence},
                )

            elif link.record_type == "temporal_event":
                try:
                    ev_id = int(link.record_id)
                except ValueError:
                    continue
                target_node_id = self._make_node_id("temporal_event", ev_id)
                if target_node_id not in nodes:
                    ev_q = self.db.query(TemporalEvent).filter(TemporalEvent.id == ev_id)
                    if self.project_id and req.dataset_name:
                        ev_q = ev_q.filter((TemporalEvent.project_id == self.project_id) | (TemporalEvent.dataset_name == req.dataset_name))
                    elif self.project_id:
                        ev_q = ev_q.filter(TemporalEvent.project_id == self.project_id)
                    elif req.dataset_name:
                        ev_q = ev_q.filter(TemporalEvent.dataset_name == req.dataset_name)
                    ev = ev_q.first()
                    if not ev:
                        continue
                    nodes[target_node_id] = GraphNode(
                        id=target_node_id,
                        type="TEMPORAL_EVENT",
                        label=f"{ev.event_type}: {ev.subject[:35]}",
                        properties={
                            "event_type": ev.event_type,
                            "timestamp": ev.event_time.isoformat() if ev.event_time else None,
                            "actor": ev.actor,
                        },
                    )
                    next_frontier.add(target_node_id)

                rel_type = "FEATURE_RELATED_TO_EVENT"
                edge_id = self._make_edge_id(feature_node_id, target_node_id, rel_type)
                edges[edge_id] = GraphEdge(
                    id=edge_id,
                    source=feature_node_id,
                    target=target_node_id,
                    type=rel_type,
                    properties={"confidence": link.confidence},
                )

    def _expand_issue_node(
        self,
        issue_key: str,
        issue_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        issue = (
            self.db.query(Issue)
            .filter(func.upper(Issue.source_issue_id) == issue_key.upper())
            .first()
        )
        if not issue:
            return

        # 1. Contributor (Reporter / Assignee)
        if issue.reporter and len(nodes) < req.max_nodes:
            c_node_id = self._make_node_id("contributor", issue.reporter)
            if c_node_id not in nodes:
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="CONTRIBUTOR",
                    label=issue.reporter,
                    properties={"role": "reporter"},
                )
            e_id = self._make_edge_id(c_node_id, issue_node_id, "CONTRIBUTOR_REPORTED_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=c_node_id, target=issue_node_id, type="CONTRIBUTOR_REPORTED_ISSUE")

        if issue.assignee and len(nodes) < req.max_nodes:
            c_node_id = self._make_node_id("contributor", issue.assignee)
            if c_node_id not in nodes:
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="CONTRIBUTOR",
                    label=issue.assignee,
                    properties={"role": "assignee"},
                )
            e_id = self._make_edge_id(c_node_id, issue_node_id, "CONTRIBUTOR_ASSIGNED_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=c_node_id, target=issue_node_id, type="CONTRIBUTOR_ASSIGNED_ISSUE")

        # 2. Linked Commits
        commit_links_q = (
            self.db.query(CommitIssueLink)
            .join(Commit, CommitIssueLink.commit_hash == Commit.commit_hash)
            .join(SourceRecord, Commit.source_record_id == SourceRecord.id)
            .filter(func.upper(CommitIssueLink.source_issue_id) == issue_key.upper())
        )
        if self.project_id and req.dataset_name:
            commit_links_q = commit_links_q.filter((SourceRecord.project_id == self.project_id) | (SourceRecord.dataset_name == req.dataset_name))
        elif self.project_id:
            commit_links_q = commit_links_q.filter(SourceRecord.project_id == self.project_id)
        elif req.dataset_name:
            commit_links_q = commit_links_q.filter(SourceRecord.dataset_name == req.dataset_name)
        commit_links = commit_links_q.limit(10).all()
        if not commit_links and not self.project_id:
            fallback_links = (
                self.db.query(CommitIssueLink)
                .filter(func.upper(CommitIssueLink.source_issue_id) == issue_key.upper())
                .limit(10)
                .all()
            )
            commit_links = [
                cl for cl in fallback_links
                if (not self.db.query(Commit).filter(Commit.commit_hash == cl.commit_hash).first() or
                    not self.db.query(Commit).filter(Commit.commit_hash == cl.commit_hash).first().source_record_id)
            ]

        for cl in commit_links:
            if len(nodes) >= req.max_nodes:
                return
            c_node_id = self._make_node_id("commit", cl.commit_hash)
            if c_node_id not in nodes:
                cmt = (
                    self.db.query(Commit)
                    .filter(Commit.commit_hash == cl.commit_hash)
                    .first()
                )
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="COMMIT",
                    label=cl.commit_hash[:8],
                    properties={
                        "message": cmt.message[:80] if cmt and cmt.message else "",
                        "author": cmt.author if cmt else "Unknown",
                        "committed_at": cmt.committed_at.isoformat() if cmt and cmt.committed_at else None,
                    },
                )
                next_frontier.add(c_node_id)
            e_id = self._make_edge_id(c_node_id, issue_node_id, "COMMIT_REFERENCES_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=c_node_id, target=issue_node_id, type="COMMIT_REFERENCES_ISSUE")

        # 3. Linked Temporal Events
        ev_q = self.db.query(TemporalEvent).filter(func.upper(TemporalEvent.issue_id) == issue_key.upper())
        if self.project_id and req.dataset_name:
            ev_q = ev_q.filter((TemporalEvent.project_id == self.project_id) | (TemporalEvent.dataset_name == req.dataset_name))
        elif self.project_id:
            ev_q = ev_q.filter(TemporalEvent.project_id == self.project_id)
        elif req.dataset_name:
            ev_q = ev_q.filter(TemporalEvent.dataset_name == req.dataset_name)
        events = ev_q.limit(15).all()

        for ev in events:
            if len(nodes) >= req.max_nodes:
                return
            ev_node_id = self._make_node_id("temporal_event", ev.id)
            if ev_node_id not in nodes:
                nodes[ev_node_id] = GraphNode(
                    id=ev_node_id,
                    type="TEMPORAL_EVENT",
                    label=f"{ev.event_type}: {ev.subject[:35]}",
                    properties={
                        "event_type": ev.event_type,
                        "timestamp": ev.event_time.isoformat() if ev.event_time else None,
                        "actor": ev.actor,
                    },
                )
                next_frontier.add(ev_node_id)
            rel_type = "ISSUE_GENERATED_EVENT"
            e_id = self._make_edge_id(issue_node_id, ev_node_id, rel_type)
            edges[e_id] = GraphEdge(id=e_id, source=issue_node_id, target=ev_node_id, type=rel_type)

        # 4. Linked Features
        feat_links = (
            self.db.query(FeatureRecordLink)
            .filter(
                FeatureRecordLink.record_type == "issue",
                func.upper(FeatureRecordLink.record_id) == issue_key.upper()
            )
            .limit(5)
            .all()
        )
        for fl in feat_links:
            if len(nodes) >= req.max_nodes:
                return
            f_node_id = self._make_node_id("feature", fl.feature_id)
            if f_node_id not in nodes:
                feat = self.db.query(Feature).filter(Feature.id == fl.feature_id).first()
                if feat:
                    nodes[f_node_id] = GraphNode(
                        id=f_node_id,
                        type="FEATURE",
                        label=feat.canonical_name,
                        properties={
                            "category": feat.feature_category,
                            "status": feat.resolution_status,
                            "confidence": fl.confidence,
                        },
                    )
                    next_frontier.add(f_node_id)
            rel_type = f"FEATURE_{fl.relationship_type}"
            e_id = self._make_edge_id(f_node_id, issue_node_id, rel_type)
            edges[e_id] = GraphEdge(id=e_id, source=f_node_id, target=issue_node_id, type=rel_type, properties={"confidence": fl.confidence})

    def _expand_commit_node(
        self,
        commit_hash: str,
        commit_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        cmt = (
            self.db.query(Commit)
            .filter(Commit.commit_hash.ilike(f"{commit_hash}%"))
            .first()
        )
        if not cmt:
            return

        # 1. Contributor (Author)
        if cmt.author and len(nodes) < req.max_nodes:
            c_node_id = self._make_node_id("contributor", cmt.author)
            if c_node_id not in nodes:
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="CONTRIBUTOR",
                    label=cmt.author,
                    properties={"role": "author"},
                )
            e_id = self._make_edge_id(c_node_id, commit_node_id, "CONTRIBUTOR_AUTHORED_COMMIT")
            edges[e_id] = GraphEdge(id=e_id, source=c_node_id, target=commit_node_id, type="CONTRIBUTOR_AUTHORED_COMMIT")

        # 2. Linked Issues via CommitIssueLink
        issue_links = (
            self.db.query(CommitIssueLink)
            .filter(CommitIssueLink.commit_hash == cmt.commit_hash)
            .limit(10)
            .all()
        )
        for il in issue_links:
            if len(nodes) >= req.max_nodes:
                return
            i_node_id = self._make_node_id("issue", il.source_issue_id)
            if i_node_id not in nodes:
                issue = (
                    self.db.query(Issue)
                    .filter(func.upper(Issue.source_issue_id) == il.source_issue_id.upper())
                    .first()
                )
                lbl = f"{il.source_issue_id}: {issue.summary[:35]}" if issue and issue.summary else il.source_issue_id
                nodes[i_node_id] = GraphNode(
                    id=i_node_id,
                    type="ISSUE",
                    label=lbl,
                    properties={
                        "source_issue_id": il.source_issue_id,
                        "issue_type": issue.issue_type if issue else "Unknown",
                        "status": issue.status if issue else "Unknown",
                        "created_at": issue.created_at.isoformat() if issue and issue.created_at else None,
                    },
                )
                next_frontier.add(i_node_id)
            e_id = self._make_edge_id(commit_node_id, i_node_id, "COMMIT_REFERENCES_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=commit_node_id, target=i_node_id, type="COMMIT_REFERENCES_ISSUE")

        # 3. Linked Temporal Events
        ev_q = self.db.query(TemporalEvent).filter(TemporalEvent.commit_hash == cmt.commit_hash)
        if self.project_id and req.dataset_name:
            ev_q = ev_q.filter((TemporalEvent.project_id == self.project_id) | (TemporalEvent.dataset_name == req.dataset_name))
        elif self.project_id:
            ev_q = ev_q.filter(TemporalEvent.project_id == self.project_id)
        elif req.dataset_name:
            ev_q = ev_q.filter(TemporalEvent.dataset_name == req.dataset_name)
        evs = ev_q.limit(10).all()

        for ev in evs:
            if len(nodes) >= req.max_nodes:
                return
            ev_node_id = self._make_node_id("temporal_event", ev.id)
            if ev_node_id not in nodes:
                nodes[ev_node_id] = GraphNode(
                    id=ev_node_id,
                    type="TEMPORAL_EVENT",
                    label=f"{ev.event_type}: {ev.subject[:35]}",
                    properties={
                        "event_type": ev.event_type,
                        "timestamp": ev.event_time.isoformat() if ev.event_time else None,
                        "actor": ev.actor,
                    },
                )
                next_frontier.add(ev_node_id)
            rel_type = "COMMIT_GENERATED_EVENT"
            e_id = self._make_edge_id(commit_node_id, ev_node_id, rel_type)
            edges[e_id] = GraphEdge(id=e_id, source=commit_node_id, target=ev_node_id, type=rel_type)

        # 4. Linked Features
        feat_links = (
            self.db.query(FeatureRecordLink)
            .filter(
                FeatureRecordLink.record_type == "commit",
                FeatureRecordLink.record_id == cmt.commit_hash
            )
            .limit(5)
            .all()
        )
        for fl in feat_links:
            if len(nodes) >= req.max_nodes:
                return
            f_node_id = self._make_node_id("feature", fl.feature_id)
            if f_node_id not in nodes:
                feat = self.db.query(Feature).filter(Feature.id == fl.feature_id).first()
                if feat:
                    nodes[f_node_id] = GraphNode(
                        id=f_node_id,
                        type="FEATURE",
                        label=feat.canonical_name,
                        properties={
                            "category": feat.feature_category,
                            "status": feat.resolution_status,
                            "confidence": fl.confidence,
                        },
                    )
                    next_frontier.add(f_node_id)
            rel_type = f"COMMIT_{fl.relationship_type}"
            e_id = self._make_edge_id(commit_node_id, f_node_id, rel_type)
            edges[e_id] = GraphEdge(id=e_id, source=commit_node_id, target=f_node_id, type=rel_type, properties={"confidence": fl.confidence})

    def _expand_event_node(
        self,
        event_id: int,
        event_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        ev_q = self.db.query(TemporalEvent).filter(TemporalEvent.id == event_id)
        if self.project_id and req.dataset_name:
            ev_q = ev_q.filter((TemporalEvent.project_id == self.project_id) | (TemporalEvent.dataset_name == req.dataset_name))
        elif self.project_id:
            ev_q = ev_q.filter(TemporalEvent.project_id == self.project_id)
        elif req.dataset_name:
            ev_q = ev_q.filter(TemporalEvent.dataset_name == req.dataset_name)
        ev = ev_q.first()
        if not ev:
            return

        # 1. Evidence
        evidences = (
            self.db.query(EventEvidence)
            .filter(EventEvidence.event_id == event_id)
            .limit(3)
            .all()
        )
        for evd in evidences:
            if len(nodes) >= req.max_nodes:
                return
            evd_node_id = self._make_node_id("evidence", evd.id)
            if evd_node_id not in nodes:
                nodes[evd_node_id] = GraphNode(
                    id=evd_node_id,
                    type="EVIDENCE",
                    label=f"Excerpt: {evd.evidence_excerpt[:35]}...",
                    properties={
                        "excerpt": evd.evidence_excerpt[:150],
                        "source_record_id": evd.source_record_id,
                        "hash": evd.evidence_hash[:16] if evd.evidence_hash else None,
                        "role": evd.evidence_role,
                    },
                )
            e_id = self._make_edge_id(event_node_id, evd_node_id, "EVENT_SUPPORTED_BY_EVIDENCE")
            edges[e_id] = GraphEdge(
                id=e_id,
                source=event_node_id,
                target=evd_node_id,
                type="EVENT_SUPPORTED_BY_EVIDENCE",
                properties={"role": evd.evidence_role},
            )

        # 2. Contributor (Actor)
        if ev.actor and len(nodes) < req.max_nodes:
            c_node_id = self._make_node_id("contributor", ev.actor)
            if c_node_id not in nodes:
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="CONTRIBUTOR",
                    label=ev.actor,
                    properties={"role": "event_actor"},
                )
            e_id = self._make_edge_id(c_node_id, event_node_id, "CONTRIBUTOR_TRIGGERED_EVENT")
            edges[e_id] = GraphEdge(id=e_id, source=c_node_id, target=event_node_id, type="CONTRIBUTOR_TRIGGERED_EVENT")

        # 3. Associated Issue (if any)
        if ev.issue_id and len(nodes) < req.max_nodes:
            i_node_id = self._make_node_id("issue", ev.issue_id)
            if i_node_id not in nodes:
                issue = self.db.query(Issue).filter(func.upper(Issue.source_issue_id) == ev.issue_id.upper()).first()
                lbl = f"{ev.issue_id}: {issue.summary[:35]}" if issue and issue.summary else ev.issue_id
                nodes[i_node_id] = GraphNode(
                    id=i_node_id,
                    type="ISSUE",
                    label=lbl,
                    properties={
                        "source_issue_id": ev.issue_id,
                        "issue_type": issue.issue_type if issue else "Unknown",
                        "created_at": issue.created_at.isoformat() if issue and issue.created_at else None,
                    },
                )
                next_frontier.add(i_node_id)
            e_id = self._make_edge_id(event_node_id, i_node_id, "EVENT_ASSOCIATED_WITH_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=event_node_id, target=i_node_id, type="EVENT_ASSOCIATED_WITH_ISSUE")

        # 4. Associated Commit (if any)
        if ev.commit_hash and len(nodes) < req.max_nodes:
            c_node_id = self._make_node_id("commit", ev.commit_hash)
            if c_node_id not in nodes:
                cmt = self.db.query(Commit).filter(Commit.commit_hash == ev.commit_hash).first()
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="COMMIT",
                    label=cmt.commit_hash[:8] if cmt else ev.commit_hash[:8],
                    properties={
                        "message": cmt.message[:80] if cmt and cmt.message else "",
                        "author": cmt.author if cmt else "Unknown",
                        "committed_at": cmt.committed_at.isoformat() if cmt and cmt.committed_at else None,
                    },
                )
                next_frontier.add(c_node_id)
            e_id = self._make_edge_id(event_node_id, c_node_id, "EVENT_ASSOCIATED_WITH_COMMIT")
            edges[e_id] = GraphEdge(id=e_id, source=event_node_id, target=c_node_id, type="EVENT_ASSOCIATED_WITH_COMMIT")

    def _expand_contributor_node(
        self,
        actor_name: str,
        actor_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        # 1. Recent Commits authored
        cmts = (
            self.db.query(Commit)
            .filter(Commit.author.ilike(actor_name))
            .limit(5)
            .all()
        )
        for cmt in cmts:
            if len(nodes) >= req.max_nodes:
                return
            c_node_id = self._make_node_id("commit", cmt.commit_hash)
            if c_node_id not in nodes:
                nodes[c_node_id] = GraphNode(
                    id=c_node_id,
                    type="COMMIT",
                    label=cmt.commit_hash[:8],
                    properties={
                        "message": cmt.message[:80] if cmt.message else "",
                        "author": cmt.author,
                        "committed_at": cmt.committed_at.isoformat() if cmt.committed_at else None,
                    },
                )
                next_frontier.add(c_node_id)
            e_id = self._make_edge_id(actor_node_id, c_node_id, "CONTRIBUTOR_AUTHORED_COMMIT")
            edges[e_id] = GraphEdge(id=e_id, source=actor_node_id, target=c_node_id, type="CONTRIBUTOR_AUTHORED_COMMIT")

        # 2. Issues reported
        issues = (
            self.db.query(Issue)
            .filter((Issue.reporter.ilike(actor_name)) | (Issue.assignee.ilike(actor_name)))
            .limit(5)
            .all()
        )
        for iss in issues:
            if len(nodes) >= req.max_nodes:
                return
            i_node_id = self._make_node_id("issue", iss.source_issue_id)
            if i_node_id not in nodes:
                lbl = f"{iss.source_issue_id}: {iss.summary[:35]}" if iss.summary else iss.source_issue_id
                nodes[i_node_id] = GraphNode(
                    id=i_node_id,
                    type="ISSUE",
                    label=lbl,
                    properties={
                        "source_issue_id": iss.source_issue_id,
                        "issue_type": iss.issue_type,
                    },
                )
                next_frontier.add(i_node_id)
            e_id = self._make_edge_id(actor_node_id, i_node_id, "CONTRIBUTOR_REPORTED_ISSUE")
            edges[e_id] = GraphEdge(id=e_id, source=actor_node_id, target=i_node_id, type="CONTRIBUTOR_REPORTED_ISSUE")

    def _expand_evidence_node(
        self,
        evidence_id: int,
        evidence_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        evd = self.db.query(EventEvidence).filter(EventEvidence.id == evidence_id).first()
        if not evd:
            return
        if len(nodes) >= req.max_nodes:
            return
        ev = self.db.query(TemporalEvent).filter(TemporalEvent.id == evd.event_id).first()
        if ev:
            ev_node_id = self._make_node_id("temporal_event", ev.id)
            if ev_node_id not in nodes:
                nodes[ev_node_id] = GraphNode(
                    id=ev_node_id,
                    type="TEMPORAL_EVENT",
                    label=f"{ev.event_type}: {ev.subject[:35]}",
                    properties={
                        "event_type": ev.event_type,
                        "timestamp": ev.event_time.isoformat() if ev.event_time else None,
                        "actor": ev.actor,
                    },
                )
                next_frontier.add(ev_node_id)
            e_id = self._make_edge_id(ev_node_id, evidence_node_id, "EVENT_SUPPORTED_BY_EVIDENCE")
            edges[e_id] = GraphEdge(
                id=e_id,
                source=ev_node_id,
                target=evidence_node_id,
                type="EVENT_SUPPORTED_BY_EVIDENCE",
                properties={"role": evd.evidence_role},
            )

    def _expand_alias_node(
        self,
        alias_id: int,
        alias_node_id: str,
        nodes: Dict[str, GraphNode],
        edges: Dict[str, GraphEdge],
        next_frontier: Set[str],
        req: GraphQueryRequest,
    ):
        al = self.db.query(FeatureAlias).filter(FeatureAlias.id == alias_id).first()
        if not al:
            return
        if len(nodes) >= req.max_nodes:
            return
        feat = self.db.query(Feature).filter(Feature.id == al.feature_id).first()
        if feat:
            f_node_id = self._make_node_id("feature", feat.id)
            if f_node_id not in nodes:
                nodes[f_node_id] = GraphNode(
                    id=f_node_id,
                    type="FEATURE",
                    label=feat.canonical_name,
                    properties={
                        "category": feat.feature_category,
                        "status": feat.resolution_status,
                        "confidence": feat.identity_confidence,
                    },
                )
                next_frontier.add(f_node_id)
            e_id = self._make_edge_id(f_node_id, alias_node_id, "FEATURE_HAS_ALIAS")
            edges[e_id] = GraphEdge(
                id=e_id,
                source=f_node_id,
                target=alias_node_id,
                type="FEATURE_HAS_ALIAS",
                properties={"confidence": al.confidence},
            )

    def get_graph_statistics(self) -> Dict[str, Any]:
        """Returns overall graph metrics across the dataset or project."""
        if self.project_id:
            feat_count = self.db.query(Feature).filter(Feature.project_id == self.project_id).count()
            alias_count = self.db.query(FeatureAlias).join(Feature).filter(Feature.project_id == self.project_id).count()
            link_count = self.db.query(FeatureRecordLink).join(Feature).filter(Feature.project_id == self.project_id).count()
            event_count = self.db.query(TemporalEvent).filter(TemporalEvent.project_id == self.project_id).count()
            issue_count = self.db.query(Issue).join(SourceRecord, Issue.source_record_id == SourceRecord.id).filter(SourceRecord.project_id == self.project_id).count()
            commit_count = self.db.query(Commit).join(SourceRecord, Commit.source_record_id == SourceRecord.id).filter(SourceRecord.project_id == self.project_id).count()
        else:
            feat_count = self.db.query(Feature).filter(Feature.dataset_name == self.dataset_name).count()
            alias_count = self.db.query(FeatureAlias).join(Feature).filter(Feature.dataset_name == self.dataset_name).count()
            link_count = self.db.query(FeatureRecordLink).join(Feature).filter(Feature.dataset_name == self.dataset_name).count()
            event_count = self.db.query(TemporalEvent).filter(TemporalEvent.dataset_name == self.dataset_name).count()
            issue_count = self.db.query(Issue).count()
            commit_count = self.db.query(Commit).count()

        return {
            "canonical_features": feat_count,
            "feature_aliases": alias_count,
            "feature_record_links": link_count,
            "temporal_events": event_count,
            "issues": issue_count,
            "commits": commit_count,
            "total_graph_elements": feat_count + alias_count + link_count + event_count + issue_count + commit_count,
        }

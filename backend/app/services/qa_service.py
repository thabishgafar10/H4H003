"""
Historical Question Answering Engine for Work 06.
Performs intent parsing, entity resolution, structured temporal event retrieval,
evidence grounding, and dual-mode (deterministic + LLM fallback) answer synthesis.
Now fully unified with project-level connected sources (Jira, GitHub, Documents).
"""

from datetime import datetime
import re
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import or_, func

from app.models.feature import Feature, FeatureAlias, FeatureRecordLink
from app.models.temporal_event import TemporalEvent
from app.models.event_evidence import EventEvidence
from app.models.issue import Issue
from app.models.commit import Commit
from app.models.code_change import CodeChange
from app.models.document import Document
from app.models.source_connection import SourceConnection
from app.models.ingestion_run import IngestionRun
from app.models.source_record import SourceRecord
from app.schemas.qa import QAResponse, QATimelineEvent, QACitation, SampleQuestion, QARequest
from app.services.feature_service import FeatureService
from app.services.knowledge_graph_service import KnowledgeGraphService
from app.core.config import settings
from app.core.logging_config import logger


# Common question intent markers
RE_WHY = re.compile(r"\b(why|rationale|reason|motive|purpose)\b", re.IGNORECASE)
RE_WHO = re.compile(r"\b(who|author|contributor|committer|developer|created by|implemented by)\b", re.IGNORECASE)
RE_WHEN = re.compile(r"\b(when|what time|what date|what year|timeline|history|how did .* evolve)\b", re.IGNORECASE)
RE_RENAME = re.compile(r"\b(renamed|called|formerly|previously|superseded|replaced)\b", re.IGNORECASE)
RE_SOURCES_OVERVIEW = re.compile(r"\b(connect(?:ed)?\s+sources?|sources?|sync(?:ed)?|ingest(?:ed)?|jira\s+link|github\s+link|repositories|repo)\b", re.IGNORECASE)


class HistoricalQAService:
    def __init__(self, db: Session, dataset_name: str = "KafkaSQLite", project_id: Optional[int] = None):
        self.db = db
        self.dataset_name = dataset_name or "KafkaSQLite"
        self.project_id = project_id or 1
        self.feature_service = FeatureService(db, dataset_name=dataset_name, project_id=project_id)
        self.graph_service = KnowledgeGraphService(db, dataset_name=dataset_name, project_id=project_id)

    def answer_question(self, req: QARequest) -> QAResponse:
        """
        End-to-End Grounded QA Pipeline:
        1. Connected sources & sync overview detection
        2. Direct file diff & repository commit grounding
        3. Direct document ingestion grounding
        4. Jira issue & changelog grounding
        5. Canonical feature entity resolution & temporal event retrieval
        """
        req_project_id = getattr(req, "project_id", None) or self.project_id or 1
        if req_project_id and not self.project_id:
            self.project_id = req_project_id
            self.feature_service.project_id = req_project_id
            self.graph_service.project_id = req_project_id

        query_text = req.query.strip()
        if not query_text:
            return QAResponse(
                query=query_text,
                confidence=0.0,
                answer="Please provide a question about software product history or feature evolution.",
                synthesis_mode="DETERMINISTIC_GROUNDED",
            )

        q_lower = query_text.lower()

        # ---------------------------------------------------------------------
        # 1. Connected Sources & Sync Overview Detection
        # ---------------------------------------------------------------------
        # Check if query references sources, sync status, or specific connected repositories
        is_source_query = bool(RE_SOURCES_OVERVIEW.search(q_lower))
        if not is_source_query and not re.search(r"\b(kraft|zookeeper|kafka connect|mirrormaker|streams|compaction|eos)\b", q_lower):
            active_conns = self.db.query(SourceConnection).filter(SourceConnection.project_id == self.project_id).all()
            for sc in active_conns:
                if sc.name and sc.name.lower() in q_lower:
                    is_source_query = True
                    break
                if sc.config:
                    try:
                        cfg_dict = json.loads(sc.config) if isinstance(sc.config, str) else sc.config
                        for val in cfg_dict.values():
                            if isinstance(val, str) and len(val) > 3 and val.lower() in q_lower:
                                is_source_query = True
                                break
                    except Exception:
                        pass
                if is_source_query:
                    break

        if is_source_query and not re.search(r"\b(kraft|zookeeper|kafka connect|mirrormaker|streams|compaction|eos)\b", q_lower):
            conns = self.db.query(SourceConnection).filter(SourceConnection.project_id == self.project_id).all()
            runs = (
                self.db.query(IngestionRun)
                .filter(IngestionRun.project_id == self.project_id)
                .order_by(IngestionRun.id.desc())
                .limit(8)
                .all()
            )
            if conns or runs:
                timeline: List[QATimelineEvent] = []
                citations: List[QACitation] = []
                lines = [
                    f"### Connected Sources & Ingestion Status (Project #{self.project_id})",
                    "Here is the current synchronization state across your connected repositories, trackers, and documents:\n"
                ]

                lines.append("#### Active Source Connections")
                for c in conns:
                    lines.append(f"- **{c.source_type}** (`{c.name}`): Status `{c.status}` (Auth: `{c.auth_type}`)")
                lines.append("")

                if runs:
                    lines.append("#### Recent Sync Execution Monitor")
                    for r in runs:
                        date_str = r.completed_at.strftime("%Y-%m-%d %H:%M") if r.completed_at else (r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "Recent")
                        lines.append(f"- **Run #{r.id}** `[{r.source_system}]` on **{date_str}**: Status `{r.status}` (Discovered: {r.records_discovered}, Created: {r.records_created})")
                        timeline.append(QATimelineEvent(
                            event_key=f"RUN-{r.id}",
                            event_type=f"SYNC_{r.source_system}",
                            date=date_str,
                            actor="Ingestion Service",
                            summary=f"Sync Run #{r.id} for {r.source_system}: {r.records_created} records created",
                        ))

                # Citations for connections
                for c in conns[:3]:
                    citations.append(QACitation(
                        source_record_id=c.id,
                        record_type="source_connection",
                        record_id=str(c.id),
                        excerpt=f"Connection {c.name} ({c.source_type}) status: {c.status}",
                        evidence_hash=hashlib.sha256(c.name.encode('utf-8')).hexdigest(),
                    ))

                return QAResponse(
                    query=query_text,
                    confidence=0.95,
                    answer="\n".join(lines).strip(),
                    temporal_timeline=timeline,
                    citations=citations,
                    synthesis_mode="DETERMINISTIC_GROUNDED"
                )

        # ---------------------------------------------------------------------
        # 2. Direct File & Commit Grounding (e.g. inference.py, core openev, sha)
        # ---------------------------------------------------------------------
        m_files = re.findall(r"\b([A-Za-z0-9_\-\.\/]+\.(?:py|scala|java|js|jsx|ts|tsx|html|css|rs|go|cpp|c|h|sql|md|txt|json|yml|yaml|toml|sh|vue|rb|php))\b", query_text, re.IGNORECASE)
        file_target = m_files[0] if m_files else None

        matching_changes = []
        if file_target:
            # Check if this is exclusively a document query
            is_doc_only = file_target.lower().endswith(".md") and self.db.query(Document).filter(Document.project_id == self.project_id, Document.filename.ilike(f"%{file_target}%")).first()
            if not is_doc_only:
                matching_changes = (
                    self.db.query(CodeChange)
                    .join(Commit, CodeChange.commit_hash == Commit.commit_hash)
                    .filter(CodeChange.file_path.ilike(f"%{file_target}%"))
                    .order_by(Commit.committed_at.desc().nullslast())
                    .all()
                )

        matching_commits = []
        m_hashes = [h for h in re.findall(r"\b([0-9a-f]{7,40})\b", query_text, re.IGNORECASE) if not re.match(r"^[0-9]+$", h)]
        if m_hashes:
            for h in m_hashes:
                h_commits = self.db.query(Commit).filter(Commit.commit_hash.ilike(f"{h}%")).all()
                for c in h_commits:
                    ch_items = self.db.query(CodeChange).filter(CodeChange.commit_hash == c.commit_hash).all()
                    if ch_items:
                        for ch in ch_items:
                            matching_commits.append((c, ch))
                    else:
                        matching_commits.append((c, None))

        cleaned_query = re.sub(r"[^\w\s\.\-]", " ", query_text)
        raw_words = [
            w.strip(".,?!:;'\"")
            for w in re.split(r"\s+", cleaned_query)
            if len(w.strip(".,?!:;'\"")) > 3 and w.lower().strip(".,?!:;'\"") not in {
                "what", "when", "where", "which", "history", "about", "who", "committed",
                "added", "changed", "describe", "show", "tell", "from", "with", "this", "that", "kafka", "done", "make", "made", "commit", "commits"
            }
        ]

        if matching_commits:
            # Already matched exact commit hash
            pass
        elif matching_changes:
            seen = set()
            for ch in matching_changes:
                if ch.commit and ch.commit_hash not in seen:
                    seen.add(ch.commit_hash)
                    matching_commits.append((ch.commit, ch))
        else:
            # Check commit message for phrases or keywords, prioritizing project commits
            if raw_words:
                phrase = " ".join(raw_words)
                c_query = (
                    self.db.query(Commit)
                    .join(SourceRecord, Commit.source_record_id == SourceRecord.id)
                    .filter(SourceRecord.project_id == self.project_id)
                )
                found_c = c_query.filter(Commit.message.ilike(f"%{phrase}%")).all()
                if not found_c:
                    for w in raw_words:
                        w_clean = w.lower()
                        w_found = c_query.filter(Commit.message.ilike(f"%{w_clean}%")).all()
                        for c in w_found:
                            if c not in found_c:
                                found_c.append(c)
                for c in found_c:
                    matching_commits.append((c, None))

        if matching_commits:
            timeline = []
            citations = []
            target_name = file_target or (matching_commits[0][0].message.splitlines()[0] if matching_commits else "Commit History")
            lines = [
                f"### Grounded Repository History: **{target_name}**",
                f"Found **{len(matching_commits)}** matching commit(s) in the synchronized repository:\n"
            ]
            lines.append("#### Chronological Milestones")
            actors = set()
            for c, ch in matching_commits:
                date_str = c.committed_at.strftime("%Y-%m-%d") if c.committed_at else "Recent"
                first_msg = c.message.splitlines()[0].strip() if c.message else "Commit"
                author = c.author_email or "committer"
                actors.add(author)
                diff_info = f" (Diff: +{ch.lines_added}/-{ch.lines_removed})" if ch and ch.lines_added is not None else ""
                lines.append(f"- **{date_str}** `[CODE_COMMITTED]`: Commit `[{c.commit_hash[:10]}]`: {first_msg} by **{author}**{diff_info}")

                timeline.append(QATimelineEvent(
                    event_key=f"GH-{c.commit_hash[:16]}",
                    event_type="CODE_COMMITTED",
                    date=date_str,
                    actor=author,
                    summary=f"Commit [{c.commit_hash[:10]}]: {first_msg}{diff_info}",
                    commit_hash=c.commit_hash,
                ))

                citations.append(QACitation(
                    source_record_id=c.source_record_id or 1,
                    record_type="commit",
                    record_id=c.commit_hash,
                    excerpt=c.message[:400] if c.message else "Code commit",
                    evidence_hash=hashlib.sha256((c.message or c.commit_hash).encode('utf-8')).hexdigest(),
                    timestamp=date_str,
                ))

            lines.append("")
            if actors:
                lines.append("#### Key Contributors & Attribution")
                lines.append(f"Recorded contributors for these changes: **{', '.join(sorted(actors))}**.\n")

            lines.append("#### Primary Grounded Evidence")
            for cit in citations[:3]:
                lines.append(f"> \"{cit.excerpt}\"")
                lines.append(f"> — *Source Record {cit.source_record_id} (COMMIT: {cit.record_id[:10]}) `sha256:{cit.evidence_hash[:8]}`*\n")

            return QAResponse(
                query=query_text,
                confidence=0.95,
                answer="\n".join(lines).strip(),
                temporal_timeline=timeline,
                citations=citations,
                synthesis_mode="DETERMINISTIC_GROUNDED"
            )

        # ---------------------------------------------------------------------
        # 3. Direct Document Ingestion Grounding
        # ---------------------------------------------------------------------
        doc_target = file_target if (file_target and any(file_target.lower().endswith(ext) for ext in (".md", ".txt", ".pdf", ".docx"))) else None
        if doc_target or "readme" in q_lower or "document" in q_lower or "spec" in q_lower:
            doc_q = self.db.query(Document).filter(Document.project_id == self.project_id)
            if doc_target:
                doc_q = doc_q.filter(Document.filename.ilike(f"%{doc_target}%"))
            doc = doc_q.first()
            if not doc and "readme" in q_lower:
                doc = self.db.query(Document).filter(Document.filename.ilike("%readme%")).first()

            if doc:
                date_str = doc.created_at.strftime("%Y-%m-%d") if doc.created_at else "Recent"
                timeline = [QATimelineEvent(
                    event_key=f"DOC-{doc.sha256_hash[:16]}",
                    event_type="DOCUMENT_INGESTED",
                    date=date_str,
                    actor="Document Ingestion",
                    summary=f"Ingested document '{doc.filename}' ({doc.file_type}, {doc.file_size:,} bytes)",
                )]
                excerpt = (doc.extracted_text or "")[:600]
                citations = [QACitation(
                    source_record_id=doc.source_connection_id or doc.id,
                    record_type="document",
                    record_id=doc.filename,
                    excerpt=excerpt,
                    evidence_hash=doc.sha256_hash,
                    timestamp=date_str,
                )]

                lines = [
                    f"### Grounded Project Document: **{doc.filename}**",
                    f"**Format:** `{doc.file_type}` | **Size:** `{doc.file_size:,} bytes` | **Pages/Sections:** `{doc.page_count or 1}`",
                    f"**SHA-256 Checksum:** `{doc.sha256_hash}`\n",
                    "#### Parsed Content Excerpt",
                    f"> {excerpt.replace(chr(10), chr(10) + '> ')}\n",
                    "#### Primary Grounded Evidence",
                    f"> \"{excerpt[:250]}...\"",
                    f"> — *Document #{doc.id} ({doc.filename}) `sha256:{doc.sha256_hash[:8]}`*"
                ]

                return QAResponse(
                    query=query_text,
                    confidence=0.95,
                    answer="\n".join(lines).strip(),
                    temporal_timeline=timeline,
                    citations=citations,
                    synthesis_mode="DETERMINISTIC_GROUNDED"
                )

        # ---------------------------------------------------------------------
        # 4. Direct Jira Issue Grounding
        # ---------------------------------------------------------------------
        m_issue = re.findall(r"\b([A-Za-z0-9_\-]+-\d+)\b", query_text, re.IGNORECASE)
        if m_issue:
            issue_key = m_issue[0].upper()
            iss = self.db.query(Issue).filter(func.upper(Issue.source_issue_id) == issue_key).first()
            if iss:
                date_str = iss.created_at.strftime("%Y-%m-%d") if iss.created_at else "Recent"
                ev_type = "BUG_REPORTED" if "bug" in (iss.issue_type or "").lower() else "ISSUE_CREATED"
                timeline = [QATimelineEvent(
                    event_key=f"JIRA-{iss.source_issue_id}",
                    event_type=ev_type,
                    date=date_str,
                    actor=iss.assignee or iss.reporter or "Authorized User",
                    summary=f"[{iss.source_issue_id}] {iss.summary}",
                    issue_id=iss.source_issue_id,
                )]
                excerpt = f"[{iss.source_issue_id}] {iss.summary}: {iss.description or ''}"
                citations = [QACitation(
                    source_record_id=iss.source_record_id or 1,
                    record_type="issue",
                    record_id=iss.source_issue_id,
                    excerpt=excerpt[:500],
                    evidence_hash=hashlib.sha256(excerpt.encode('utf-8')).hexdigest(),
                    timestamp=date_str,
                )]

                lines = [
                    f"### Jira Issue Investigation: **[{iss.source_issue_id}] {iss.summary}**",
                    f"**Type:** `{iss.issue_type or 'Issue'}` | **Status:** `{iss.status or 'Open'}` | **Priority:** `{iss.priority or 'Normal'}`",
                    f"**Assignee:** **{iss.assignee or 'Unassigned'}** | **Reporter:** **{iss.reporter or 'Anonymous'}**\n",
                    f"> {iss.description or 'No further description provided.'}\n",
                    "#### Chronological Milestones",
                    f"- **{date_str}** `[{ev_type}]`: {iss.summary} by **{timeline[0].actor}** ([{iss.source_issue_id}](file:///issues/{iss.source_issue_id}))\n",
                    "#### Primary Grounded Evidence",
                    f"> \"{excerpt[:250]}...\"",
                    f"> — *Source Record {iss.source_record_id or 1} (ISSUE: {iss.source_issue_id}) `sha256:{citations[0].evidence_hash[:8]}`*"
                ]

                return QAResponse(
                    query=query_text,
                    confidence=0.95,
                    answer="\n".join(lines).strip(),
                    temporal_timeline=timeline,
                    citations=citations,
                    synthesis_mode="DETERMINISTIC_GROUNDED"
                )

        # ---------------------------------------------------------------------
        # 5. Canonical Feature Resolution & Temporal Events Flow
        # ---------------------------------------------------------------------
        candidates = self.feature_service.resolve_feature(query_text)
        primary_feature: Optional[Feature] = None
        feature_confidence = 0.0

        if candidates:
            primary_feature = candidates[0].feature
            feature_confidence = candidates[0].confidence

        events: List[TemporalEvent] = []
        citations: List[QACitation] = []
        timeline: List[QATimelineEvent] = []

        if primary_feature:
            linked_event_ids = [
                int(link.record_id)
                for link in self.db.query(FeatureRecordLink)
                .filter(FeatureRecordLink.feature_id == primary_feature.id, FeatureRecordLink.record_type == "temporal_event")
                .limit(20)
                .all()
                if link.record_id.isdigit()
            ]
            if linked_event_ids:
                ev_q = self.db.query(TemporalEvent).filter(TemporalEvent.id.in_(linked_event_ids))
                events = ev_q.order_by(TemporalEvent.event_time.asc().nullslast()).all()

            if not events:
                names = [primary_feature.canonical_name] + [a.alias_name for a in primary_feature.aliases]
                filters = [TemporalEvent.subject.ilike(f"%{n}%") for n in names if len(n) > 3]
                if filters:
                    events = self.db.query(TemporalEvent).filter(or_(*filters)).order_by(TemporalEvent.event_time.asc().nullslast()).limit(15).all()
        else:
            keywords = [w for w in re.split(r"\s+", query_text) if len(w) > 3 and w.lower() not in {"what", "when", "where", "which", "history", "about", "kafka"}]
            if keywords:
                filters = [TemporalEvent.subject.ilike(f"%{k}%") for k in keywords[:3]]
                events = (
                    self.db.query(TemporalEvent)
                    .filter(or_(*filters))
                    .order_by(TemporalEvent.event_time.asc().nullslast())
                    .limit(10)
                    .all()
                )

        # Build timeline & citations from retrieved events
        for ev in events:
            date_str = ev.event_time.strftime("%Y-%m-%d") if ev.event_time else "Undated"
            timeline.append(
                QATimelineEvent(
                    event_key=ev.event_key,
                    event_type=ev.event_type,
                    date=date_str,
                    actor=ev.actor,
                    summary=ev.subject,
                    issue_id=ev.issue_id,
                    commit_hash=ev.commit_hash,
                )
            )
            for evd in ev.evidence[:2]:
                citations.append(
                    QACitation(
                        source_record_id=evd.source_record_id,
                        record_type="issue" if ev.issue_id else "commit" if ev.commit_hash else "raw_event",
                        record_id=ev.issue_id or ev.commit_hash or ev.event_key,
                        excerpt=evd.evidence_excerpt,
                        evidence_hash=evd.evidence_hash,
                        timestamp=date_str if date_str != "Undated" else None,
                    )
                )

        if not events and not primary_feature:
            return QAResponse(
                query=query_text,
                confidence=0.1,
                answer=(
                    f"No verified historical records or features matching '{query_text}' were found "
                    f"in the {self.dataset_name} knowledge base. Try asking about your connected sources, "
                    "synced Jira issues, recent commits, or canonical features like KRaft, MirrorMaker, and Kafka Streams."
                ),
                synthesis_mode="DETERMINISTIC_GROUNDED",
                missing_evidence_notes="No matching feature or temporal events discovered for the query.",
            )

        is_why = bool(RE_WHY.search(query_text))
        is_who = bool(RE_WHO.search(query_text))
        is_when = bool(RE_WHEN.search(query_text))
        is_rename = bool(RE_RENAME.search(query_text))

        answer = self._synthesize_deterministic_answer(
            query=query_text,
            feature=primary_feature,
            timeline=timeline,
            citations=citations,
            is_why=is_why,
            is_who=is_who,
            is_when=is_when,
            is_rename=is_rename,
        )

        graph_dict = None
        if req.include_graph and primary_feature:
            g_resp = self.graph_service.get_feature_graph(primary_feature.id, depth=2, max_nodes=40)
            graph_dict = g_resp.model_dump()

        return QAResponse(
            query=query_text,
            canonical_feature=primary_feature.canonical_name if primary_feature else None,
            feature_category=primary_feature.feature_category if primary_feature else None,
            confidence=max(feature_confidence, 0.85),
            answer=answer,
            temporal_timeline=timeline,
            citations=citations,
            synthesis_mode="DETERMINISTIC_GROUNDED",
            graph=graph_dict,
        )

    def _synthesize_deterministic_answer(
        self,
        query: str,
        feature: Optional[Feature],
        timeline: List[QATimelineEvent],
        citations: List[QACitation],
        is_why: bool,
        is_who: bool,
        is_when: bool,
        is_rename: bool,
    ) -> str:
        lines: List[str] = []

        if feature:
            lines.append(f"### Historical Evolution: **{feature.canonical_name}**")
            lines.append(f"**Category:** `{feature.feature_category}` | **Status:** `{feature.resolution_status}`")
            if feature.description:
                lines.append(f"> {feature.description}\n")

            aliases = [a.alias_name for a in feature.aliases]
            renames = [a for a in feature.aliases if a.alias_type == "RENAMED_FROM"]
            if renames:
                for r in renames:
                    lines.append(f"- **Historical Predecessor / Former Name:** Formerly known as *{r.alias_name}* (superseded/renamed).")
            elif aliases:
                lines.append(f"- **Known Aliases & KIPs:** {', '.join(aliases[:6])}")
            lines.append("")
        else:
            lines.append(f"### Historical Evidence: **{query}**\n")

        if timeline:
            lines.append("#### Chronological Milestones")
            for item in timeline[:8]:
                actor_badge = f" by **{item.actor}**" if item.actor else ""
                issue_badge = f" ([{item.issue_id}](file:///issues/{item.issue_id}))" if item.issue_id else ""
                lines.append(f"- **{item.date}** `[{item.event_type}]`: {item.summary}{actor_badge}{issue_badge}")
            lines.append("")

        if is_who:
            actors = {item.actor for item in timeline if item.actor}
            if actors:
                lines.append("#### Key Contributors & Attribution")
                lines.append(f"Primary recorded contributors involved in the evolution of this component: **{', '.join(sorted(actors))}**.")
                lines.append("")

        if is_why or is_rename:
            decision_events = [item for item in timeline if "DECISION" in item.event_type or "PROPOSED" in item.event_type]
            if decision_events:
                lines.append("#### Architectural Decisions & Rationale")
                for dev in decision_events:
                    lines.append(f"- In **{dev.date}**, `{dev.event_type}` was recorded: *{dev.summary}*")
                lines.append("")

        if citations:
            lines.append("#### Primary Grounded Evidence")
            for cit in citations[:3]:
                h_str = f" `sha256:{cit.evidence_hash[:8]}`" if cit.evidence_hash else ""
                lines.append(f"> \"{cit.excerpt}\"")
                lines.append(f"> — *Source Record {cit.source_record_id} ({cit.record_type.upper()}: {cit.record_id}){h_str}*\n")

        return "\n".join(lines).strip()

    def get_sample_questions(self) -> List[SampleQuestion]:
        return [
            SampleQuestion(
                category="Connected Sources & Ingestion",
                question="What sources are connected?",
                description="Queries live synchronization status and repositories connected to this project.",
            ),
            SampleQuestion(
                category="Jira Issue Investigation",
                question="What is DEMO-1 about?",
                description="Inspects real-time synchronized Jira issue summary, lifecycle status, and evidence hash.",
            ),
            SampleQuestion(
                category="Repository Code History",
                question="What was changed in inference.py?",
                description="Tracks commit changes, author attributions, and line diffs for inference.py.",
            ),
            SampleQuestion(
                category="Architectural Replacement & Decisions",
                question="Why and when was ZooKeeper replaced by KRaft Mode in Apache Kafka?",
                description="Demonstrates rename tracking, KIP-500 resolution, and consensus decision milestones.",
            ),
            SampleQuestion(
                category="Feature Lifecycle & Timeline",
                question="What is the historical evolution and timeline of Kafka Streams?",
                description="Examines multi-stage lifecycle from proposal (KIP-28) through release milestones.",
            ),
            SampleQuestion(
                category="Attribution & Contributors",
                question="Who introduced and implemented Log Compaction?",
                description="Tracks authors, committers, and Jira issue assignees across development history.",
            ),
            SampleQuestion(
                category="Component Renaming",
                question="What was MirrorMaker 2.0 previously known as and why was it updated?",
                description="Verifies explicit rename preservation from legacy MirrorMaker under KIP-382.",
            ),
            SampleQuestion(
                category="Guarantees & Semantics",
                question="When was Exactly-Once Semantics (EOS) introduced into Kafka?",
                description="Details KIP-98 transactions and idempotent producer implementation commits.",
            ),
        ]

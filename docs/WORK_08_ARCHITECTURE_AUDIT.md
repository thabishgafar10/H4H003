# WORK 08: MULTI-PROJECT ISOLATION & DATA INTEGRITY ARCHITECTURE AUDIT

## 1. Executive Summary

This architecture audit evaluates the multi-project data isolation guarantees of the **Temporal Product Knowledge System**. Following the Work 07 delivery of multi-source project onboarding (Jira Cloud, public GitHub repositories, and unstructured document ingestion), this audit verifies whether Project A can be completely isolated from Project B and Project C across all tiers of the application:
1. Physical and Logical Database Storage.
2. Ingestion & Extraction Services.
3. Feature Identity & Entity Resolution Engines.
4. Temporal Knowledge Graph BFS Traversal.
5. Historical Question Answering (QA) & Evidence Retrieval.
6. REST API Endpoints & Authorization Bounds.

### Key Audit Findings
- **Data Model Level**: `Project`, `SourceConnection`, `Document`, `IngestionRun`, `ExtractionRun`, `EventCandidate`, `TemporalEvent`, `Feature`, `SourceRecord`, and `UnresolvedRelationship` now hold `project_id` and/or `dataset_name`. However, lower-level domain entities (`Issue`, `Commit`, `IssueComment`, `CodeChange`, `IssueComponent`, `IssueFixVersion`, `IssueLink`, `CommitIssueLink`) link to `SourceRecord` via `source_record_id` and do not store `project_id` directly. Unscoped SQL queries directly hitting these tables leak records across projects.
- **REST API Routes**: Legacy endpoints (`/api/issues`, `/api/commits/{hash}`, `/api/events`, `/api/features/{id}`, `/api/statistics`) historically defaulted to global database queries or hardcoded `dataset_name="KafkaSQLite"`. Without explicit project-level scoping or foreign-key ownership verification, requests querying identifiers (such as issue key `1` or event ID `42`) exhibit Insecure Direct Object Reference (IDOR) vulnerabilities.
- **Knowledge Graph**: In `KnowledgeGraphService`, BFS traversal queries neighbor edges using source keys (e.g., `CommitIssueLink.source_issue_id == issue_key`) without scoping to `project_id` or `dataset_name`. An identical issue key across two projects (e.g., Jira issue `PROJ-1` vs GitHub issue `#1`) would bridge knowledge graphs across independent projects.
- **Historical QA**: `HistoricalQAService` relies on `FeatureRecordLink` and regex token matching. If an issue key or feature name from another project is retrieved, the service previously lacked a strict project boundary filter on `TemporalEvent` and `EventEvidence` retrieval, allowing cross-project fact leakage in synthesized answers.

---

## 2. Comprehensive Audit Matrix

| Component / Subsystem | Entity / Target | Current Scoping Mechanism | Identified Vulnerability / Gap | Risk Level |
| :--- | :--- | :--- | :--- | :--- |
| **Data Models** | `Project` | Primary key `id`, unique `slug` | None. Clean boundary. | LOW |
| **Data Models** | `SourceConnection` | `project_id` foreign key | None. Scoped to project. | LOW |
| **Data Models** | `Document` | `project_id` foreign key | None. Scoped to project. | LOW |
| **Data Models** | `IngestionRun` | `project_id` foreign key | None. Scoped to project. | LOW |
| **Data Models** | `SourceRecord` | `project_id` & `dataset_name` | Nullable `project_id` for legacy records (migrated to Kafka project). | LOW |
| **Data Models** | `Issue` | `source_record_id` -> `SourceRecord` | No direct `project_id`. Direct queries to `Issue` return rows globally. | HIGH |
| **Data Models** | `Commit` | `source_record_id` -> `SourceRecord` | No direct `project_id`. Commits queried by hash without project context. | MEDIUM |
| **Data Models** | `IssueComment` | `source_record_id` -> `SourceRecord` | No direct `project_id`. Comments queried by issue ID cross-project. | HIGH |
| **Data Models** | `CodeChange` | `source_record_id` -> `SourceRecord` | No direct `project_id`. Code changes linked to commit id. | LOW |
| **Data Models** | `TemporalEvent` | `project_id` & `dataset_name` | Queries on `id` or `issue_id` without filtering by `project_id`. | HIGH |
| **Data Models** | `EventEvidence` | `event_id` -> `TemporalEvent` | Indirect scoping via event. If event is leaked, evidence is leaked. | MEDIUM |
| **Data Models** | `Feature` | `project_id` & `dataset_name` | Endpoints default to `KafkaSQLite` or query by `Feature.id` globally. | HIGH |
| **Data Models** | `FeatureAlias` | `feature_id` -> `Feature` | Scoped via Feature parent. | LOW |
| **Data Models** | `FeatureRecordLink` | `feature_id` -> `Feature` | Links contain `record_id` (string). Does not store `project_id`. | HIGH |
| **API Endpoints** | `GET /api/issues` | None | Returns issues across ALL projects. | CRITICAL |
| **API Endpoints** | `GET /api/issues/{id}` | Filter on `source_issue_id` | IDOR: returns issue belonging to any project. | HIGH |
| **API Endpoints** | `GET /api/commits/{hash}` | Filter on `commit_hash` | IDOR: returns commit from any project without verification. | MEDIUM |
| **API Endpoints** | `GET /api/statistics` | Optional `dataset_name` | Query counts `Issue`, `Commit`, etc. globally across DB. | HIGH |
| **API Endpoints** | `GET /api/events` | Optional `dataset_name` | No `project_id` parameter. Default query returns all events. | HIGH |
| **API Endpoints** | `GET /api/events/{id}` | Filter on `TemporalEvent.id` | IDOR: exposes private project events to unauthorized callers. | HIGH |
| **API Endpoints** | `GET /api/events/issues/{id}/timeline` | Filter on `issue_id` | Unscoped: aggregates events for same issue key across projects. | HIGH |
| **API Endpoints** | `GET /api/features` | Default `KafkaSQLite` | Hardcoded default dataset; misses multi-project tenancy. | MEDIUM |
| **API Endpoints** | `GET /api/features/{id}` | Filter on `Feature.id` | IDOR: feature from Project B can be viewed by Project A caller. | HIGH |
| **API Endpoints** | `GET /api/features/{id}/history` | Filter on `Feature.id` | IDOR: returns event history for foreign feature. | HIGH |
| **API Endpoints** | `POST /api/features/resolve` | Default `KafkaSQLite` | Resolves query against default dataset instead of project scope. | MEDIUM |
| **API Endpoints** | `GET /api/graph/features/{id}` | Default `KafkaSQLite` | Traversal checks `dataset_name`, but hops can traverse foreign nodes. | HIGH |
| **API Endpoints** | `GET /api/graph/nodes/{type}/{id}`| Parameter `dataset_name` | Does not verify start node ownership before BFS expansion. | HIGH |
| **API Endpoints** | `POST /api/graph/query` | Request `dataset_name` | BFS expansion queries links without project boundary checks. | HIGH |
| **API Endpoints** | `POST /api/qa/ask` | Request `dataset_name` | Fallback searches query `TemporalEvent` or `Issue` cross-project. | HIGH |
| **API Endpoints** | `GET /api/extraction/status` | Default `KafkaSQLite` | No `project_id` scoping. | MEDIUM |
| **API Endpoints** | `GET /api/extraction/candidates` | Optional `dataset_name` | No `project_id` parameter; candidates can leak. | HIGH |
| **Services** | `KnowledgeGraphService` | `dataset_name` attribute | BFS neighbor traversal queries `CommitIssueLink` unscoped. | HIGH |
| **Services** | `HistoricalQAService` | `dataset_name` attribute | Issue key lookup regex queries `Issue` table globally. | HIGH |
| **Services** | `FeatureService` | `dataset_name` attribute | Alias and record link lookups rely only on `Feature.id`. | MEDIUM |
| **Services** | `ExtractionOrchestrator`| `dataset_name` attribute | Unscoped candidate queries if dataset is not explicitly passed. | MEDIUM |

---

## 3. Direct Object Reference (IDOR) & Unscoped Primary Key Vulnerabilities

### Vulnerability 1: `/api/features/{feature_id}` and `/api/features/{feature_id}/history`
- **Location**: `backend/app/api/feature_routes.py#L58-L115`
- **Pattern**:
  ```python
  feature = db.query(Feature).filter(Feature.id == feature_id).first()
  ```
- **Risk**: An attacker or authenticated user from Project A can enumerate incremental integer IDs `1, 2, 3...` to view private feature architectures, design decisions, and internal timelines belonging to Project B or Project C.
- **Remediation**:
  1. Add project-scoped routes: `GET /api/projects/{project_id_or_slug}/features/{feature_id}`.
  2. For global route `GET /api/features/{feature_id}`, accept optional `project_id_or_slug` or `dataset_name` query parameter, and verify that the target feature belongs to the authorized project scope. If mismatch, return `404 Not Found`.

### Vulnerability 2: `/api/events/{event_id}`
- **Location**: `backend/app/api/event_routes.py#L104-L117`
- **Pattern**:
  ```python
  event = db.query(TemporalEvent).filter(TemporalEvent.id == event_id).first()
  ```
- **Risk**: Allows reading verbatim evidence, actor identities, code diff excerpts, and security concerns from any project.
- **Remediation**:
  Support `project_id` / `dataset_name` verification; return `404 Not Found` if the requested event belongs to another project.

### Vulnerability 3: `/api/issues/{issue_id}` and `/api/commits/{commit_hash}`
- **Location**: `backend/app/api/routes.py#L81-L94`
- **Pattern**:
  ```python
  issue = db.query(Issue).filter(Issue.source_issue_id == issue_id).first()
  commit = db.query(Commit).filter(Commit.commit_hash == commit_hash).first()
  ```
- **Risk**: Issue IDs (e.g. `1`, `PROJ-1`) and commit hashes are queried globally without verifying project association.
- **Remediation**:
  Join with `SourceRecord` and filter by `SourceRecord.project_id` or `SourceRecord.dataset_name`.

---

## 4. Default/Hardcoded Global Dataset Dependencies

1. **`FeatureService` Default**:
   - `def __init__(self, db: Session, dataset_name: str = "KafkaSQLite", project_id: Optional[int] = None)`
   - Defaults to benchmark dataset `"KafkaSQLite"`. If callers omit `dataset_name`, all operations mutate or inspect Kafka features.
2. **`ExtractionRoutes` Default**:
   - `dataset_name: Optional[str] = Query("KafkaSQLite")`
   - Hardcoded default assumes a single-tenant Kafka deployment.
3. **`routes.py` Statistics Default**:
   - Counts total records in `Issue`, `Commit`, `IssueComment`, `CodeChange` across the entire database rather than isolating to `target_dataset` or `project_id`.

---

## 5. Knowledge Graph Cross-Project Traversal Vulnerabilities

### Traversal Boundary Analysis
In `backend/app/services/knowledge_graph_service.py`:
1. **Frontier Expansion**:
   When expanding an `ISSUE` node (`_expand_issue_node`):
   ```python
   commit_links = (
       self.db.query(CommitIssueLink)
       .filter(CommitIssueLink.source_issue_id == issue_key)
       .limit(10)
       .all()
   )
   ```
   `CommitIssueLink` only maps `(source_issue_id, commit_hash)`. If Project A has Jira issue `101` and Project B has GitHub issue `#101`, the BFS algorithm links commits from Project A into Project B's knowledge graph!
2. **Commit Expansion**:
   `self.db.query(Commit).filter(Commit.commit_hash == commit_hash).first()` does not check if the commit record belongs to the requested `dataset_name` or `project_id`.
3. **Neighbor Node Ingestion**:
   In `_expand_feature_node`, `FeatureRecordLink` points to `record_id`. When querying `Issue` or `TemporalEvent`, it must strictly enforce `dataset_name == req.dataset_name`.

---

## 6. Historical QA Cross-Project Retrieval & Citation Leakage

### QA Pipeline Analysis
In `backend/app/services/qa_service.py`:
1. **Issue Key Extraction**:
   If a user asks "Why did we make changes in PROJ-10?", the service searches `Issue.source_issue_id == "PROJ-10"` without scoping to the user's project.
2. **Event Retrieval**:
   `self.db.query(TemporalEvent).filter(TemporalEvent.id.in_(linked_event_ids)).all()` retrieves events without enforcing `TemporalEvent.dataset_name == self.dataset_name`.
3. **Citation Assembly**:
   `EventEvidence` attached to those events is quoted verbatim into `citations`. This creates a vulnerability where Project B's question returns verbatim source code or issue discussion from Project A.

---

## 7. Ingestion & Document Pipeline Isolation Gaps

1. **Document Storage**:
   `Document` records store `project_id` correctly. Document chunking in `ProjectIngestionService.ingest_document_bytes` creates `SourceRecord` with `project_id=project.id`.
2. **Jira & GitHub Sync**:
   `ProjectIngestionService` creates `SourceRecord` with `project_id=project.id` and `dataset_name=project.slug`.
3. **Unresolved Relationships**:
   Relationships detected during multi-source ingestion must set `project_id` to prevent cross-project relationship resolution.

---

## 8. Evidence Chain & Provenance Integrity Across Projects

An evidence chain consists of:
`EventEvidence` $\to$ `TemporalEvent` $\to$ `SourceRecord` $\to$ `Project`
To enforce complete isolation:
- An event evidence item must NEVER point to a `SourceRecord` belonging to a different `project_id`.
- An event must NEVER reference an `issue_id` or `commit_hash` outside its project.
- Knowledge graph edges must NEVER connect nodes across different projects.

---

## 9. Endpoints Lacking Project-Level Ownership/Authorization Checks

| Endpoint | Missing Check | Target Fix |
| :--- | :--- | :--- |
| `GET /api/issues` | No `project_id` or `dataset_name` filter | Add `project_id` / `dataset_name` query params & join with `SourceRecord` |
| `GET /api/issues/{id}` | No project ownership verification | Verify issue belongs to requested project/dataset |
| `GET /api/commits/{hash}` | No project ownership verification | Verify commit belongs to requested project/dataset |
| `GET /api/events` | No `project_id` parameter | Accept `project_id`, enforce filter |
| `GET /api/events/{id}` | No project ownership check | Verify `event.project_id == project_id` |
| `GET /api/events/issues/{id}/timeline`| No project ownership check | Filter by `project_id` / `dataset_name` |
| `GET /api/features/{id}` | No project ownership check | Verify `feature.project_id == project_id` |
| `GET /api/features/{id}/history` | No project ownership check | Verify `feature.project_id == project_id` |
| `POST /api/features/resolve` | No `project_id` support | Add `project_id` parameter |
| `GET /api/graph/features/{id}` | No project ownership check | Return 404 if feature is outside project |
| `GET /api/graph/nodes/{type}/{id}`| No project ownership check | Validate node belongs to requested project |
| `POST /api/graph/query` | BFS hops unscoped | Enforce project boundary on all neighbor queries |
| `POST /api/qa/ask` | Event retrieval unscoped | Strictly filter all events, features & citations |

---

## 10. Remediation Roadmap for Work 08

1. **Phase 2: Isolation Design & Multi-Layer Enforcement**:
   - Deliver `docs/WORK_08_ISOLATION_DESIGN.md`.
   - Update `KnowledgeGraphService` to strictly check `project_id` / `dataset_name` on all BFS hops and neighbor links.
   - Update `HistoricalQAService` to strictly scope entity resolution, issue lookup, event retrieval, and citation building to the requested project.
   - Update `FeatureService` to scope queries, resolutions, and links by `project_id` / `dataset_name`.
   - Update API routes (`routes.py`, `event_routes.py`, `feature_routes.py`, `graph_routes.py`, `qa_routes.py`) with strict project filtering and ownership validation.
2. **Phase 3: Multi-Project Test Matrix & End-to-End Scenarios**:
   - Implement `backend/tests/test_work08_isolation.py` with multi-project test fixtures (Project A, Project B, Project C).
3. **Phase 4-7: Security Review, Verification & Documentation**:
   - Deliver `docs/WORK_08_SECURITY_REVIEW.md` and `docs/WORK_08_TEST_REPORT.md`.

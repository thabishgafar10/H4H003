# WORK 08: MULTI-PROJECT ISOLATION & ENFORCEMENT DESIGN SPECIFICATION

## 1. Architecture Overview

The Temporal Product Knowledge System implements a multi-tenant, multi-source knowledge reconstruction architecture. Multiple software projects (e.g. Apache Kafka, cloud microservices, vector databases) coexist within the same application instance.

To guarantee that **Project A can NEVER retrieve, mutate, expose, or indirectly infer information from Project B or Project C**, this specification defines a **Defense-in-Depth Isolation Model** operating across four distinct architectural layers:

```
[Layer 1: HTTP / REST API Gateways]
    ├── Project Context Extraction (slug or project_id in route/query)
    ├── Parameter Validation & IDOR Ownership Pre-Checks
    └── Rejection of Foreign Identifiers (HTTP 404 / 403)
            │
[Layer 2: Service & Business Logic Engines]
    ├── KnowledgeGraphService: Project-Bounded BFS Traversal
    ├── HistoricalQAService: Scoped Intent & Citation Retrieval
    ├── FeatureService: Project-Partitioned Entity Resolution
    └── ExtractionOrchestrator: Bounded Candidate Generation
            │
[Layer 3: Data Access & Query Construction]
    ├── Mandatory Project/Dataset Scoping on All Queries
    ├── Join-based Scoping for Domain Models (Issue, Commit via SourceRecord)
    └── Parameterized Boundary Guards on Neighbor Expansion
            │
[Layer 4: Relational Database Schema & Foreign Keys]
    ├── Projects Table as Root Anchor
    ├── Project-Scoped SourceRecords, Connections, Documents, Runs
    └── Referential Integrity and Compound Unique Constraints
```

---

## 2. Multi-Layer Enforcement Specification

### 2.1 Database & Schema Isolation Rules
1. **Root Tenancy Anchor**: Every top-level entity (`SourceConnection`, `Document`, `IngestionRun`, `ExtractionRun`, `EventCandidate`, `TemporalEvent`, `Feature`, `UnresolvedRelationship`) has a direct `project_id` foreign key referencing `projects.id`.
2. **Domain Entity Scoping**: Domain entities (`Issue`, `Commit`, `IssueComment`, `CodeChange`, `IssueComponent`, `IssueFixVersion`, `IssueLink`, `CommitIssueLink`) relate to `SourceRecord` via `source_record_id`.
   - All queries against domain tables must join `SourceRecord` and filter by `SourceRecord.project_id == project_id` (or `SourceRecord.dataset_name == dataset_name`).
3. **Compound Uniqueness**:
   - `EventCandidate`: Unique index on `(dataset_name, candidate_fingerprint)`
   - `TemporalEvent`: Unique index on `(dataset_name, event_key)`
   - `Feature`: Unique index on `(dataset_name, canonical_name)`
   - `SourceRecord`: Unique index on `(dataset_name, source_table, source_record_id)`

### 2.2 API Layer & IDOR Protection Rules
1. **Explicit Project Context**:
   - For project-scoped routes (`/api/projects/{slug}/...`), the project is looked up first. If not found, return `404 Not Found`.
   - All child resources (connections, documents, runs, features) must verify that `resource.project_id == project.id`.
2. **Global / Convenience Routes**:
   - Routes like `/api/features/{feature_id}`, `/api/events/{event_id}`, `/api/issues/{issue_id}` must accept optional `project_id` or `dataset_name`.
   - When provided, the endpoint verifies ownership. If the resource exists but belongs to a different project, the endpoint returns `404 Not Found` (to prevent information leakage by ID enumeration).
   - If not provided, routes maintain backward compatibility with benchmark datasets while preventing cross-talk.
3. **Statistics Isolation**:
   - `/api/statistics` must accept `dataset_name` or `project_id`. When filtered, it counts ONLY records belonging to that project's source records.

### 2.3 Knowledge Graph Traversal Boundaries
1. **Initial Node Scoping**:
   - Starting node lookups must verify ownership against `req.dataset_name` or `project_id`.
   - If the root node belongs to another project, return an empty graph response immediately (`node_count: 0`).
2. **BFS Expansion Scoping**:
   - **Feature $\to$ Issue / Event**: Only expand `FeatureRecordLink` where linked `Issue` or `TemporalEvent` belongs to the requesting dataset.
   - **Issue $\to$ Commit**: `CommitIssueLink` queries must join `SourceRecord` on `Commit` to ensure the commit belongs to the target project.
   - **Commit $\to$ Contributor**: Contributors are scoped to the commit within the project.
   - **Event $\to$ Evidence**: `EventEvidence` items must point to `TemporalEvent` and `SourceRecord` belonging to the target project.
3. **Neighbor Pruning**:
   - Any neighbor node or edge that does not match the project boundary is pruned from the frontier before adding to the returned graph.

### 2.4 Historical QA Pipeline Isolation
1. **Entity Resolution**:
   - `FeatureService.resolve_feature(query)` must only match features where `Feature.dataset_name == self.dataset_name` or `Feature.project_id == self.project_id`.
2. **Issue Key Lookup**:
   - When extracting issue keys from natural language (e.g. `PROJ-123`), the lookup query on `Issue` must join `SourceRecord` and match `SourceRecord.dataset_name == self.dataset_name`.
3. **Event Retrieval**:
   - `TemporalEvent` queries must include `TemporalEvent.dataset_name == self.dataset_name` (or `project_id`).
4. **Citation Isolation**:
   - All `QACitation` items in `QAResponse.citations` must originate strictly from the verified events of the target project. Cross-project evidence injection is strictly rejected.

---

## 3. Data Flow Diagram: Multi-Project Isolation

```
User Request (Project: "cloud-orchestrator", Feature: 15)
                    │
                    ▼
         [API Layer / Gateway]
   Is Feature 15 owned by "cloud-orchestrator"?
       ├── NO  ──> HTTP 404 "Feature 15 not found in project"
       └── YES ──> Delegate to Service Layer
                    │
                    ▼
          [Service Layer]
   KnowledgeGraphService(dataset_name="cloud-orchestrator")
       ├── Root Node: Verify Feature 15 dataset_name == "cloud-orchestrator"
       ├── BFS Hop 1: Issue Links (Filter SourceRecord.dataset_name == "cloud-orchestrator")
       ├── BFS Hop 2: Commits (Filter SourceRecord.dataset_name == "cloud-orchestrator")
       └── Return Graph: 100% nodes and edges belong to "cloud-orchestrator"
                    │
                    ▼
         [Client Response]
   Isolated Graph & Evidence (Zero foreign nodes or edges)
```

---

## 4. Backward Compatibility Guarantees

1. The primary benchmark project **Apache Kafka** retains full compatibility:
   - `slug: "kafka"`
   - `dataset_name: "KafkaSQLite"`
   - All existing benchmark tests (Work 02 through Work 07) continue to pass without regression.
2. In the absence of an explicit project header or query param on legacy endpoints, the system defaults to safe isolated single-tenant semantics rather than unbounded cross-tenant data amalgamation.

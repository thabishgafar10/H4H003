# Temporal Product Knowledge System
> **"GitHub tells us how code changed. Our system connects engineering records to explain how a product evolved, when decisions happened, who contributed, what changed, and what evidence supports each conclusion."**

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg)](https://fastapi.tiangolo.com)
[![Tests](https://img.shields.io/badge/Tests-130%20passing-success.svg)](backend/tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The **Temporal Product Knowledge System** is a multi-project product intelligence platform that reconstructs how software products evolve over time using historical development records. It transforms raw issue trackers, architecture discussions, votes, commit histories, and design documents into a structured, queryable knowledge base of temporal product events backed by cryptographic verbatim evidence provenance.

---

## Architecture & Subsystems

```
                                  +---------------------------------------+
                                  |     Work 07: Multi-Source Onboarding  |
                                  | - Multi-Project Workspaces (Projects) |
                                  | - Jira Cloud REST v3 (ADF & History)  |
                                  | - GitHub Public Repos (Commits & PRs) |
                                  | - Documents (PDF, DOCX, MD, SSRF URL) |
                                  | - Ingestion Runs & Sync Checkpoints   |
                                  +-------------------+-------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |    Work 02: Ingestion & Provenance    |
                                  | - Immutable SourceRecord Provenance   |
                                  | - Benchmark: 91,008 Kafka SQLite rows |
                                  | - SHA-256 Hashes & Idempotent Sync    |
                                  +-------------------+-------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |    Work 03: Temporal Event Extraction |
                                  | - 19,319 Events / 10 Taxonomy Types   |
                                  | - Verbatim Substring Evidence Hashes  |
                                  | - Strict Negation & Bot Filtering     |
                                  +-------------------+-------------------+
                                                      |
                   +----------------------------------+----------------------------------+
                   |                                  |                                  |
                   v                                  v                                  v
+----------------------------------+ +----------------------------------+ +----------------------------------+
|      Work 04: Feature Identity   | |   Work 05: Temporal Knowledge    | |     Work 06: Historical AI QA    |
|        & Entity Resolution       | |              Graph               | |               Engine             |
| - Canonical Features & Categories| | - Multi-Hop BFS Traversal Engine | | - Natural Language Intent Parser |
| - Dynamic Alias Resolution       | | - Graph Nodes (Feature, Issue,   | | - Entity Resolution Integration  |
| - Rename & Succession Tracking   | |   Commit, Event, Contributor)    | | - Evidence-Grounded Synthesis    |
| - Traceable Record Links         | | - Time Slicing & Neighborhoods   | | - Verifiable SHA-256 Citations   |
+----------------------------------+ +----------------------------------+ +----------------------------------+
                   |                                  |                                  |
                   +----------------------------------+----------------------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |         Unified Web Dashboard         |
                                  |     (/dashboard, /app, & /docs)       |
                                  | - Project Selector & Onboarding Modal |
                                  | - Sources Connect & Ingestion Monitor |
                                  +---------------------------------------+
```

---

## Key Capabilities

1. **Multi-Source Project Onboarding (Work 07)**:
   - **First-Class Project Isolation**: Projects workspace container (`projects`, `source_connections`, `documents`, `ingestion_runs`) with strict isolation ensuring Project A data is completely segregated from Project B. Apache Kafka remains the baseline benchmark (`project_id=1`, `slug='kafka'`).
   - **Jira Cloud REST API v3**: Read-only integration supporting issue search pagination, recursive Atlassian Document Format (ADF) parser to clean markdown, comments, and historical changelog transition extraction. Supports both API Token and OAuth 2.0 3LO code exchange flows with rate-limit (`Retry-After`) handling.
   - **GitHub Public Repositories**: Read-only GitHub integration supporting URL parsing (`owner/repo` or full URL), repository validation, commit pagination, file diff statistics (additions, deletions, status), pull request synchronization, and rate-limit safety.
   - **Document Direct Upload & Public URLs**: Ingestion for `.pdf`, `.docx`, `.txt`, and `.md` files with page markers and section headings. Includes **strict SSRF protection** resolving domains and blocking loopback (`127.0.0.1`), RFC 1918 private IP ranges, link-local metadata (`169.254.169.254`), and non-HTTP schemes.
   - **Ingestion Execution Monitor**: Tracks execution state (`PENDING` $\to$ `RUNNING` $\to$ `COMPLETED` / `FAILED`), record counts, and incremental synchronization checkpoints.

2. **Deterministic & Conservative Temporal Event Extraction (Work 03)**:
   - Identifies product events using 10 taxonomy types across issues, comments, and commits.
   - Guarded against false positives with strict **negation filtering** ("not approved", "do not merge"), **conditional handling** ("would approve if"), **bot comment filtering** (e.g. `githubbot`), and **quote omission**.
   - Preserves missing timestamps as `NULL` without fabricating timestamps.
   - Enforces strict verbatim substring verification against immutable `SourceRecord.raw_data` with SHA-256 hashes.

3. **Feature Identity & Entity Resolution (Work 04)**:
   - Canonical feature entities with category classification (`CORE`, `CLIENT`, `STREAMS`, `CONNECT`, `STORAGE`, `SECURITY`, `TOOLS`).
   - Layered fuzzy and deterministic matching resolving variations, acronyms, and Kafka Improvement Proposals (KIPs).
   - Historical rename and succession tracking (e.g., ZooKeeper $\to$ KRaft Mode, legacy MirrorMaker $\to$ MirrorMaker 2.0).
   - Traceable, bidirectional record links across Jira issues, Git commits, and temporal events.

4. **Temporal Knowledge Graph (Work 05)**:
   - Rich multi-entity graph schema: `FEATURE`, `ISSUE`, `COMMIT`, `TEMPORAL_EVENT`, `CONTRIBUTOR`, and `EVIDENCE`.
   - Multi-hop traversal engine with cycle detection, depth limits, and max node boundaries.
   - Temporal slicing: examine the topological state of product knowledge within specific historical time windows.

5. **Historical Natural-Language Question Answering (Work 06)**:
   - Answers complex architectural, attribution, and lifecycle questions with zero hallucinations.
   - Dual-mode synthesis: deterministic grounded synthesis with automatic fallback when LLM API keys are unconfigured.
   - Grounded citations linking directly to raw source record IDs and SHA-256 verification hashes.

6. **Interactive Web Dashboard UI**:
   - **Project Selector**: Global header selector allowing instant switching between software projects, with a modal to onboard new projects.
   - **Connect Sources Tab**: Interactive onboarding cards for Jira Cloud, GitHub, and Document ingestion with real-time connection validation, plus an Ingestion Execution Monitor table.
   - **Overview Tab**: Live metrics, taxonomy distribution, and graph topology overview.
   - **Features Tab**: Searchable catalog of canonical features with aliases and observed lifespans.
   - **Knowledge Graph Tab**: Interactive SVG visualization of multi-hop entity networks with node inspector.
   - **Historical QA Tab**: One-click benchmark questions and custom natural-language query input.
   - **Events Tab**: Filterable table of 19,319 verified temporal events with pagination.
   - **Timeline Tab**: Issue lifecycle reconstruction from inception to resolution.
   - **Extraction Tab**: Interactive controls to trigger runs and view historical run logs.

---

## 10-Type Event Taxonomy

| Event Type | Source Entity | Description |
| :--- | :--- | :--- |
| `ISSUE_CREATED` | Issue | Issue creation by reporter |
| `ISSUE_RESOLVED` | Issue | Issue status marked resolved or closed |
| `BUG_REPORTED` | Issue | Defect or bug issue reported |
| `FEATURE_PROPOSED` | Issue / Comment | New feature, proposal, or KIP initiated |
| `DECISION_APPROVED` | Comment | Explicit decision approval, LGTM, or vote passing |
| `CONCERN_RAISED` | Comment | Objection, blocker, compatibility concern, or -1 vote |
| `DECISION_REVERSED` | Comment | Prior decision reverted, proposal withdrawn, or issue reopened |
| `FEATURE_IMPLEMENTED`| Commit | Feature or support implementation committed |
| `FEATURE_MODIFIED` | Commit | Existing feature updated, enhanced, or refactored |
| `FEATURE_REMOVED` | Commit | Deprecated feature, protocol, or legacy module removed |

---

## Quickstart

### 1. Local Setup
```bash
# Clone the repository
git clone https://github.com/thabishgafar10/H4H003.git
cd Temporal-PK

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Run migrations (001, 002, 003, 004)
python backend/migrations/runner.py

# Start the application
uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port 8000 --reload
```

Open your browser at:
- **Interactive Web Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard)
- **Interactive API Documentation (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **System Healthcheck**: [http://localhost:8000/health](http://localhost:8000/health)

### 2. Docker Setup
```bash
# Build and run with Docker Compose
docker compose up -d --build
```

---

## API Reference

### Multi-Project & Onboarding Routes (Work 07)
- `POST /api/projects`: Create a new product workspace project.
- `GET /api/projects`: List all onboarded projects with resource counts.
- `GET /api/projects/{slug}`: Retrieve project details, status, and metadata.
- `PUT /api/projects/{slug}`: Update project information or status.
- `GET /api/projects/{slug}/runs`: List all ingestion execution runs and checkpoints.
- `POST /api/projects/{slug}/sources/jira/validate`: Validate credentials against Jira Cloud API.
- `POST /api/projects/{slug}/sources/jira`: Connect a Jira Cloud instance to a project.
- `POST /api/projects/{slug}/sources/jira/sync`: Trigger Jira incremental synchronization.
- `POST /api/projects/{slug}/sources/github/validate`: Validate public GitHub repository access.
- `POST /api/projects/{slug}/sources/github`: Connect a GitHub repository to a project.
- `POST /api/projects/{slug}/sources/github/sync`: Trigger GitHub incremental synchronization.
- `POST /api/projects/{slug}/documents/upload`: Direct upload of PDF, DOCX, TXT, or MD design documents.
- `POST /api/projects/{slug}/documents/url`: Ingest public document URL with SSRF protection.
- `GET /api/projects/{slug}/documents`: List parsed project documents and page counts.

### Feature Identity Routes (Work 04)
- `GET /api/features`: List canonical features with aliases and observed ranges.
- `GET /api/features/{id}`: Detailed view of a canonical feature and its record links.
- `GET /api/features/{id}/history`: Chronological event history for a feature.
- `POST /api/features/resolve`: Multi-layer entity resolution from natural-language text.
- `POST /api/features/seed`: Idempotent seeder for canonical features and links.

### Knowledge Graph Routes (Work 05)
- `GET /api/graph/features/{feature_id}`: Returns subgraph centered on a feature.
- `GET /api/graph/nodes/{node_type}/{node_id}`: Subgraph centered on any arbitrary node.
- `POST /api/graph/query`: Parameterized multi-hop traversal with time filtering.
- `GET /api/graph/stats`: High-level graph topology metrics and entity distribution.

### Historical Question Answering Routes (Work 06)
- `POST /api/qa/ask`: Natural-language question answering with timeline and citations.
- `GET /api/qa/sample-questions`: Curated benchmark questions showcasing temporal reasoning.
- `GET /api/qa/health`: QA engine status and benchmark catalog count.

### Temporal Events & Evidence Routes (Work 03)
- `GET /api/events`: Query temporal events with filtering by `event_type`, `issue_id`, `actor`, and date range.
- `GET /api/events/{id}`: Detailed view of a temporal event and attached evidence links.
- `GET /api/events/issues/{issue_id}/timeline`: Full chronological timeline of an issue.
- `GET /api/events/taxonomy`: List of all 10 event types with descriptions.

---

## Automated Test Suite

The test suite contains **130 passing automated tests**:
- **Work 07 (Multi-Project, Jira, GitHub & Documents)**: 17 tests (Project lifecycle, Jira ADF/Changelog, GitHub file diffs/PRs, SSRF protection, Document parsing, Project isolation, REST endpoints).
- **Work 02 (Data Ingestion & Integrity)**: 70 tests (SourceRecord provenance, SHA-256 hashes, zero data loss, migrations).
- **Work 03 (Temporal Event Extraction)**: 18 tests (10 taxonomy types, negative guards, evidence validator, LLM dispatch).
- **Work 04 (Feature Identity & Entity Resolution)**: 8 tests (normalization, canonical features, KIP matching, fuzzy similarity, renames).
- **Work 05 (Knowledge Graph Engine)**: 6 tests (multi-hop traversal, depth bounds, time slicing, node neighborhoods).
- **Work 06 (Historical Question Answering)**: 6 tests (intent parsing, attribution, chronological timelines, citations, fallbacks).
- **End-to-End Acceptance**: 5 tests (complete full-stack user lifecycle verification).

Run the full test suite:
```bash
python -m pytest backend/tests -v
```

---

## Deployment Documentation

Comprehensive deployment instructions for Docker, Docker Compose, Railway, Render, and Fly.io are located in [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

---

## License

This project is licensed under the MIT License.

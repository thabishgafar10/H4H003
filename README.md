# Temporal Product Knowledge System
> **"GitHub tells us how code changed. Our system connects engineering records to explain how a product evolved and what evidence supports that history."**

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg)](https://fastapi.tiangolo.com)
[![Tests](https://img.shields.io/badge/Tests-87%20passing-success.svg)](backend/tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The **Temporal Product Knowledge System** reconstructs how software products evolve over time using historical development records. It transforms raw issue trackers, architecture discussions, votes, and commit histories into a structured, queryable knowledge base of temporal product events backed by cryptographic verbatim evidence provenance.

---

## Key Capabilities

1. **Deterministic & Conservative Temporal Event Extraction**:
   - Identifies product events using 10 taxonomy types across issues, comments, and commits.
   - Guarded against false positives with strict **negation filtering** ("not approved", "do not merge"), **conditional handling** ("would approve if"), **bot comment filtering** (e.g. `githubbot`), and **quote omission**.
   - Preserves missing timestamps as `NULL` without fabricating timestamps.

2. **Verifiable Evidence Provenance**:
   - Every accepted event links directly to at least one `EventEvidence` record.
   - Enforces strict verbatim substring verification against immutable `SourceRecord.raw_data`.
   - Generates SHA-256 evidence integrity hashes.

3. **Hybrid & Pluggable Architecture**:
   - Clean abstraction for optional LLM-assisted candidate extraction (OpenAI, Anthropic, or local model).
   - Functions 100% on deterministic extractors alone when unconfigured.
   - Candidates must still pass through strict `EvidenceValidator` before promotion.

4. **Interactive Web Dashboard UI**:
   - **Overview Tab**: Live dataset statistics, candidates breakdown, taxonomy distribution, latest extraction run metrics.
   - **Event Explorer Tab**: Filterable table of temporal events with pagination.
   - **Evidence Inspector Modal**: One-click modal displaying verbatim source text, role, and SHA-256 hash.
   - **Issue Timeline Tab**: Chronological reconstruction of any issue from creation to resolution.
   - **Extraction Engine Tab**: Interactive controls to trigger runs and view historical run logs.

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
cd H4H003

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Run migrations
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

### Core & Extraction Routes
- `POST /api/extraction/run`: Trigger extraction across issues, comments, and commits.
- `GET /api/extraction/status`: Get status and metrics of the latest run.
- `GET /api/extraction/statistics`: Summary statistics of candidates, events, and evidence.
- `GET /api/extraction/runs`: List historical extraction runs with pagination.
- `GET /api/extraction/candidates`: Query proposed candidates by status and confidence.

### Temporal Events & Evidence Routes
- `GET /api/events`: Query temporal events with filtering by `event_type`, `issue_id`, `actor`, and date range.
- `GET /api/events/{id}`: Detailed view of a temporal event and all attached verbatim evidence links.
- `GET /api/events/issues/{issue_id}/timeline`: Full chronological timeline of an issue.
- `GET /api/events/taxonomy`: List of all 10 event types with descriptions.

---

## Automated Test Suite

The test suite contains **87 passing automated tests** covering:
- Database migrations & repeatable schema transitions (Migrations 001 and 002).
- SourceRecord provenance and 0-data-loss accounting.
- Deterministic extractors (Issues, Comments, Commits).
- Negative test cases: negation guards, conditionals, bot filtering, quote omission.
- Evidence validator verbatim substring verification and SHA-256 integrity.
- Idempotent repeated extraction runs.
- REST API endpoints and Web Dashboard routes.

Run the test suite:
```bash
python -m pytest backend/tests -v
```

---

## Deployment Documentation

Comprehensive deployment instructions for Docker, Docker Compose, Railway, Render, and Fly.io are located in [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

---

## License

This project is licensed under the MIT License.

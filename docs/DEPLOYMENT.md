# Deployment & Operational Guide
## Temporal Product Knowledge System

The **Temporal Product Knowledge System** extracts, reconstructs, and queries the evolution of software products by connecting historical engineering records (issues, comments, commits, code changes) with verifiable evidence provenance, canonical feature identities, multi-hop knowledge graphs, and historical natural-language question answering.

---

## 1. Architecture Overview

- **Backend**: FastAPI (Python 3.12)
- **Database**: SQLite with foreign key enforcement and transactional migration runner
- **UI Dashboard**: Built-in responsive Single Page Application served from `/dashboard` and `/app`
- **Data Layers**:
  - `SourceRecord`: Verbatim JSON snapshot of original source records (e.g. Apache Kafka engineering history).
  - `Domain Normalized Entities`: Issues, Comments, Commits, Code Changes, Issue Components, Fix Versions.
  - `EventCandidates`: Extracted candidates evaluated against taxonomy and conservative guardrails.
  - `TemporalEvents`: Accepted, deduplicated historical product events (19,319 verified events).
  - `EventEvidence`: Verifiable link between temporal events and exact verbatim source text with SHA-256 integrity hash.
  - `Feature & FeatureAlias`: Canonical feature identities with dynamic aliases, KIPs, and rename tracking.
  - `FeatureRecordLink`: Traceable provenance links connecting features to issues, commits, and events.
  - `Knowledge Graph Engine`: Multi-hop BFS graph traversal engine with depth bounding and temporal slicing.
  - `Historical QA Engine`: Natural language question answering grounded in chronological events and evidence.

---

## 2. Local Deployment

### Prerequisites
- Python 3.10+
- SQLite 3.35+
- Git

### Setup Steps
```bash
# Clone repository
git clone https://github.com/thabishgafar10/H4H003.git
cd Temporal-PK

# Create and activate virtual environment
python -m venv venv
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r backend/requirements.txt

# Execute database migrations (001, 002, 003)
python backend/migrations/runner.py

# Seed canonical feature identities and record links
python backend/scripts/seed_features.py

# Start application server
uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port 8000 --reload
```

Access the system at:
- **Interactive UI Dashboard**: `http://localhost:8000/dashboard`
- **API Swagger Documentation**: `http://localhost:8000/docs`
- **System Healthcheck**: `http://localhost:8000/health`

---

## 3. Docker Deployment

### Using Docker Compose
```bash
# Build and run the container
docker compose up -d --build

# Inspect logs
docker compose logs -f

# Verify running container
docker compose ps
```

### Manual Docker Build
```bash
# Build container image
docker build -t temporal-product-knowledge:latest .

# Run container with persistent storage
docker run -d \
  -p 8000:8000 \
  -v $(pwd)/backend/data:/app/backend/data \
  --name temporal-pk \
  temporal-product-knowledge:latest
```

---

## 4. Cloud Platform Deployment

### Railway / Render / Fly.io
1. Connect GitHub repository `https://github.com/thabishgafar10/H4H003.git`.
2. Configure Build Command:
   ```bash
   pip install -r backend/requirements.txt
   ```
3. Configure Start Command:
   ```bash
   python backend/migrations/runner.py && python backend/scripts/seed_features.py && uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port $PORT
   ```
4. Configure Environment Variables:
   - `APP_ENV`: `production`
   - `SOURCE_DB_PATH`: `sample_data/raw/kafka.sqlite3`
   - `APP_DB_PATH`: `backend/data/temporal_knowledge.db`
   - `GEMINI_API_KEY`: *(Optional)* API key for LLM-assisted question answering and extraction.

---

## 5. Verification & Smoke Testing

### Automated Test Suite Execution
```bash
# Run the complete test suite (113 passing tests)
python -m pytest backend/tests -v
```

### Smoke Test Endpoints
```bash
# 1. System Healthcheck
curl http://localhost:8000/health
# Expected: {"status": "healthy"}

# 2. Canonical Features List
curl http://localhost:8000/api/features

# 3. Layered Entity Resolution
curl -X POST http://localhost:8000/api/features/resolve \
  -H "Content-Type: application/json" \
  -d '{"query": "KIP-500 Raft mode"}'

# 4. Knowledge Graph Subgraph
curl "http://localhost:8000/api/graph/features/1?depth=2&max_nodes=50"

# 5. Graph Topology Statistics
curl http://localhost:8000/api/graph/stats

# 6. Historical Question Answering
curl -X POST http://localhost:8000/api/qa/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Why and when was ZooKeeper replaced by KRaft Mode in Apache Kafka?"}'

# 7. Curated Sample Questions Catalog
curl http://localhost:8000/api/qa/sample-questions
```

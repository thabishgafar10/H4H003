# Deployment & Operational Guide
## Temporal Product Knowledge System

The **Temporal Product Knowledge System** extracts, reconstructs, and queries the evolution of software products by connecting historical engineering records (issues, comments, commits, code changes) with verifiable evidence provenance.

---

## 1. Architecture Overview

- **Backend**: FastAPI (Python 3.12)
- **Database**: SQLite with foreign key enforcement and transactional migration runner
- **UI Dashboard**: Built-in responsive Single Page Application served from `/dashboard`
- **Data Layers**:
  - `SourceRecord`: Verbatim JSON snapshot of original source records (e.g. Apache Kafka engineering history).
  - `Domain Normalized Entities`: Issues, Comments, Commits, Code Changes, Issue Components, Fix Versions.
  - `EventCandidates`: Extracted candidates evaluated against taxonomy and conservative guardrails.
  - `TemporalEvents`: Accepted, deduplicated historical product events.
  - `EventEvidence`: Verifiable link between temporal events and exact verbatim source text with SHA-256 integrity hash.

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
cd H4H003

# Create and activate virtual environment
python -m venv venv
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r backend/requirements.txt

# Execute database migrations
python backend/migrations/runner.py

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
   python backend/migrations/runner.py && uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port $PORT
   ```
4. Configure Environment Variables:
   - `APP_ENV`: `production`
   - `SOURCE_DB_PATH`: `sample_data/raw/kafka.sqlite3`
   - `APP_DB_PATH`: `backend/data/temporal_knowledge.db`
   - `LLM_API_KEY`: *(Optional)* API key for hybrid LLM-assisted candidate extraction.
   - `LLM_MODEL`: *(Optional, default: `gpt-4o-mini`)*

---

## 5. Verification & Smoke Testing

Run the included automated verification script:
```bash
python -m pytest backend/tests/test_work03_extraction.py -v
```

### Smoke Test Endpoints
```bash
# 1. Healthcheck
curl http://localhost:8000/health
# Expected: {"status": "healthy"}

# 2. Taxonomy Definition
curl http://localhost:8000/api/events/taxonomy

# 3. Extraction Statistics
curl "http://localhost:8000/api/extraction/statistics?dataset_name=KafkaSQLite"

# 4. Chronological Issue Timeline
curl http://localhost:8000/api/events/issues/KAFKA-1/timeline
```

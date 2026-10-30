from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.api.routes import router
from app.api.ingestion_routes import router as ingestion_router
from app.api.extraction_routes import router as extraction_router
from app.api.event_routes import router as event_router
from app.db.database import init_db

app = FastAPI(
    title="Temporal Product Knowledge System",
    description="API for extracting and querying temporal product evolution, decisions, and development events.",
    version="0.2.0"
)

static_dir = Path(__file__).resolve().parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.on_event("startup")
def startup_event():
    init_db()

app.include_router(router, prefix="/api", tags=["core"])
app.include_router(ingestion_router, prefix="/api/ingestion", tags=["ingestion"])
app.include_router(extraction_router, prefix="/api/extraction", tags=["extraction"])
app.include_router(event_router, prefix="/api/events", tags=["events"])

@app.get("/")
def root():
    return {"message": "Welcome to the Temporal Product Knowledge System"}

@app.get("/dashboard", response_class=FileResponse)
def get_dashboard():
    return FileResponse(static_dir / "index.html")

@app.get("/app", response_class=FileResponse)
def get_app():
    return FileResponse(static_dir / "index.html")

@app.get("/health")
def health():
    return {"status": "healthy"}

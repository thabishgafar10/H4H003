from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.api.routes import router
from app.api.ingestion_routes import router as ingestion_router
from app.api.extraction_routes import router as extraction_router
from app.api.event_routes import router as event_router
from app.api.feature_routes import router as feature_router
from app.api.graph_routes import router as graph_router
from app.api.qa_routes import router as qa_router
from app.db.database import init_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(
    title="Temporal Product Knowledge System",
    description="API for extracting and querying temporal product evolution, decisions, and development events.",
    version="0.2.0",
    lifespan=lifespan,
)

static_dir = Path(__file__).resolve().parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(router, prefix="/api", tags=["core"])
app.include_router(ingestion_router, prefix="/api/ingestion", tags=["ingestion"])
app.include_router(extraction_router, prefix="/api/extraction", tags=["extraction"])
app.include_router(event_router, prefix="/api/events", tags=["events"])
app.include_router(feature_router, prefix="/api/features", tags=["features"])
app.include_router(graph_router)
app.include_router(qa_router)

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

from fastapi import FastAPI
from app.api.routes import router
from app.api.ingestion_routes import router as ingestion_router
from app.db.database import init_db

app = FastAPI(
    title="Temporal Product Knowledge System",
    description="API for extracting and querying temporal product evolution, decisions, and development events.",
    version="0.1.0"
)

@app.on_event("startup")
def startup_event():
    init_db()

app.include_router(router, prefix="/api", tags=["core"])
app.include_router(ingestion_router, prefix="/api/ingestion", tags=["ingestion"])

@app.get("/")
def root():
    return {"message": "Welcome to the Temporal Product Knowledge System"}

@app.get("/health")
def health():
    return {"status": "healthy"}

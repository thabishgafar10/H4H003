from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(
    title="Temporal Product Knowledge System",
    description="API for extracting and querying temporal product evolution, decisions, and development events.",
    version="0.1.0"
)

app.include_router(router)

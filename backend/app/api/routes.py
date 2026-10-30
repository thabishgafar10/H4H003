from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Welcome to the Temporal Product Knowledge System"}

@router.get("/health")
async def health():
    return {"status": "healthy"}

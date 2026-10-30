import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
import os
from pathlib import Path
import sqlite3

@pytest.mark.asyncio
async def test_root():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Temporal Product Knowledge System"}

@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_dataset_exists():
    project_root = Path(__file__).resolve().parent.parent.parent
    db_path = project_root / "sample_data" / "raw" / "kafka.sqlite3"
    assert db_path.exists()

def test_database_inspection_read_only():
    project_root = Path(__file__).resolve().parent.parent.parent
    db_path = project_root / "sample_data" / "raw" / "kafka.sqlite3"

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    conn.close()

    assert "issue" in tables
    assert "change_set" in tables
    assert "code_change" in tables

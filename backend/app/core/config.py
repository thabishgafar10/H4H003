import os
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Temporal Product Knowledge System"
    VERSION: str = "0.1.0"

    # Paths
    # config.py lives at: <repo_root>/backend/app/core/config.py
    # BASE_DIR should point to <repo_root>
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent
    SOURCE_DB_PATH: Path = BASE_DIR / "sample_data" / "raw" / "kafka.sqlite3"
    APP_DB_PATH: Path = BASE_DIR / "backend" / "data" / "temporal_knowledge.db"

    # Database URIs
    @property
    def APP_DATABASE_URL(self) -> str:
        # Ensure data directory exists
        self.APP_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{self.APP_DB_PATH}"

    class Config:
        env_file = ".env"

settings = Settings()

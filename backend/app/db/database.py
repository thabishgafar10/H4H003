from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.core.config import settings
from typing import Generator

engine = create_engine(
    settings.APP_DATABASE_URL,
    connect_args={"check_same_thread": False} # Needed for SQLite
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db(target_engine=None):
    from app.db.base import Base
    # Import all models to ensure they are registered with Base.metadata
    from app.models.source_record import SourceRecord
    from app.models.issue import Issue
    from app.models.issue_comment import IssueComment
    from app.models.commit import Commit
    from app.models.code_change import CodeChange
    from app.models.issue_relationships import IssueComponent, IssueFixVersion, IssueLink, CommitIssueLink
    from app.models.ingestion_batch import IngestionBatch
    from app.models.unresolved_relationship import UnresolvedRelationship
    from app.models.extraction_run import ExtractionRun
    from app.models.event_candidate import EventCandidate
    from app.models.temporal_event import TemporalEvent
    from app.models.event_evidence import EventEvidence

    eng = target_engine if target_engine is not None else engine
    Base.metadata.create_all(bind=eng)

    if target_engine is None:
        try:
            from migrations.runner import run_all_migrations
        except ModuleNotFoundError:
            from backend.migrations.runner import run_all_migrations
        run_all_migrations(settings.APP_DB_PATH)

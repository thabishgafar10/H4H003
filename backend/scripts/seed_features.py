"""
Seed features and initial links for physical database.
"""
import sys
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app.db.database import SessionLocal
from app.services.feature_service import FeatureService

def main():
    db = SessionLocal()
    try:
        svc = FeatureService(db, dataset_name="KafkaSQLite")
        count = svc.seed_kafka_core_features()
        print(f"Seeded {count} canonical Kafka core features.")
        links = svc.link_features_to_events_and_issues(limit_events=5000)
        print(f"Linked {links} feature-event-issue records.")
    finally:
        db.close()

if __name__ == "__main__":
    main()


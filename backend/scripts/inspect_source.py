import sqlite3
from pathlib import Path

db_path = Path(__file__).resolve().parent.parent.parent / "sample_data" / "raw" / "kafka.sqlite3"
conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [t[0] for t in cursor.fetchall()]
print("Tables:", tables)
for t in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"  {t}: {cursor.fetchone()[0]}")

# Sample columns
print("\nColumn info:")
for t in tables:
    cursor.execute(f"PRAGMA table_info({t})")
    cols = [row[1] for row in cursor.fetchall()]
    print(f"  {t}: {cols}")

conn.close()


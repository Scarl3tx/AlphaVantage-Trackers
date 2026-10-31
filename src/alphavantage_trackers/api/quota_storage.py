import sqlite3
from pathlib import Path
from contextlib import closing

class QuotaStorage:

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_at REAL NOT NULL
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_requested_at
                ON api_requests(requested_at)
            """)

            conn.commit()

    def connect(self):
        return sqlite3.connect(self.db_path, timeout=30)
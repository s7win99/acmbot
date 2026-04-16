"""SQLite storage for administrator operation logs."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "acm_bot.db"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class AdminLogStore:
    """Persistence API for administrator operation logs."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _connection(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self) -> None:
        """Create admin log table when it does not exist."""
        with self._connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operator_qq_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    detail TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at_id
                    ON admin_logs (created_at, id)
                """
            )
            conn.commit()

    def add_log(
        self, operator_qq_id: str, action: str, detail: str | None = None
    ) -> None:
        """Record one administrator operation."""
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO admin_logs
                    (operator_qq_id, action, detail, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (str(operator_qq_id), action, detail, _now_text()),
            )
            conn.commit()

    def get_recent_logs(self, limit: int = 10) -> list[dict]:
        """Return recent admin logs, newest first."""
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT operator_qq_id, action, detail, created_at
                FROM admin_logs
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

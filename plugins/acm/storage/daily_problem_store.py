"""SQLite storage for daily problem feature."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "acm_bot.db"


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


class DailyProblemStore:
    """Persistence API for daily problem groups and problem queue."""

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
        with self._connection() as conn:
            # Check if daily_problems table exists and has old schema
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='daily_problems'"
            )
            row = cursor.fetchone()
            if row and "group_id" in (row[0] or ""):
                conn.execute("DROP TABLE daily_problems")

            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS daily_problem_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS daily_problems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    publish_time TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_daily_problem_groups_enabled
                    ON daily_problem_groups (enabled);
                CREATE INDEX IF NOT EXISTS idx_daily_problems_status
                    ON daily_problems (status);
                CREATE INDEX IF NOT EXISTS idx_daily_problems_publish_time
                    ON daily_problems (publish_time);
                """
            )
            conn.commit()

    def enable_group(self, group_id: str) -> dict:
        now = _now_text()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO daily_problem_groups (group_id, enabled, created_at, updated_at)
                VALUES (?, 1, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    enabled = 1,
                    updated_at = excluded.updated_at
                """,
                (str(group_id), now, now),
            )
            conn.commit()
            return self.get_group(str(group_id)) or {}

    def disable_group(self, group_id: str) -> dict | None:
        now = _now_text()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE daily_problem_groups
                SET enabled = 0, updated_at = ?
                WHERE group_id = ?
                """,
                (now, str(group_id)),
            )
            conn.commit()
        return self.get_group(str(group_id))

    def get_group(self, group_id: str) -> dict | None:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT id, group_id, enabled, created_at, updated_at
                FROM daily_problem_groups
                WHERE group_id = ?
                """,
                (str(group_id),),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_enabled_groups(self) -> list[dict]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT id, group_id, enabled, created_at, updated_at
                FROM daily_problem_groups
                WHERE enabled = 1
                ORDER BY id ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def add_problem(
        self,
        title: str,
        link: str,
        publish_time: str,
        created_by: str,
    ) -> dict:
        now = _now_text()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO daily_problems
                    (title, link, publish_time, status, created_by, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?)
                """,
                (title, link, publish_time, str(created_by), now, now),
            )
            conn.commit()
            return self.get_problem(cursor.lastrowid) or {}

    def get_problem(self, problem_id: int) -> dict | None:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT id, title, link, publish_time, status, created_by, created_at, updated_at
                FROM daily_problems
                WHERE id = ?
                """,
                (problem_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_pending_problems(self) -> list[dict]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT id, title, link, publish_time, status, created_by, created_at, updated_at
                FROM daily_problems
                WHERE status = 'pending'
                ORDER BY publish_time ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def get_problems_to_publish(self, now: datetime) -> list[dict]:
        now_text = now.isoformat(timespec="seconds")
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT id, title, link, publish_time, status, created_by, created_at, updated_at
                FROM daily_problems
                WHERE status = 'pending' AND publish_time <= ?
                ORDER BY publish_time ASC
                """,
                (now_text,),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_problem(self, problem_id: int, **kwargs) -> dict | None:
        allowed_fields = {"title", "link", "publish_time"}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}
        if not updates:
            return self.get_problem(problem_id)

        now = _now_text()
        set_clause = ", ".join(f"{field} = ?" for field in updates)
        values = list(updates.values()) + [now, problem_id]

        with self._connection() as conn:
            conn.execute(
                f"""
                UPDATE daily_problems
                SET {set_clause}, updated_at = ?
                WHERE id = ?
                """,
                values,
            )
            conn.commit()
        return self.get_problem(problem_id)

    def delete_problem(self, problem_id: int) -> bool:
        with self._connection() as conn:
            cursor = conn.execute(
                "DELETE FROM daily_problems WHERE id = ?",
                (problem_id,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_published(self, problem_id: int) -> None:
        now = _now_text()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE daily_problems
                SET status = 'published', updated_at = ?
                WHERE id = ?
                """,
                (now, problem_id),
            )
            conn.commit()

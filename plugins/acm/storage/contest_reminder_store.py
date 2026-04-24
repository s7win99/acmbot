"""SQLite storage for contest reminder settings and deduplication."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "acm_bot.db"


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


class ContestReminderStore:
    """Persistence API for contest reminder groups and records."""

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
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS contest_reminder_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    daily_hour INTEGER NOT NULL DEFAULT 9,
                    daily_minute INTEGER NOT NULL DEFAULT 0,
                    platforms TEXT NOT NULL DEFAULT 'cf,atc',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS contest_reminder_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    contest_id TEXT NOT NULL,
                    remind_type TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    UNIQUE(group_id, platform, contest_id, remind_type)
                );

                CREATE INDEX IF NOT EXISTS idx_contest_reminder_groups_enabled
                    ON contest_reminder_groups (enabled);
                CREATE INDEX IF NOT EXISTS idx_contest_reminder_records_group_id
                    ON contest_reminder_records (group_id);
                CREATE INDEX IF NOT EXISTS idx_contest_reminder_records_sent_at
                    ON contest_reminder_records (sent_at);
                """
            )
            conn.commit()

    def enable_group(self, group_id: str) -> dict:
        now = _now_text()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO contest_reminder_groups
                    (
                        group_id,
                        enabled,
                        daily_hour,
                        daily_minute,
                        platforms,
                        created_at,
                        updated_at
                    )
                VALUES (?, 1, 9, 0, 'cf,atc', ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    enabled = 1,
                    daily_hour = 9,
                    daily_minute = 0,
                    platforms = 'cf,atc',
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
                UPDATE contest_reminder_groups
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
                SELECT
                    id,
                    group_id,
                    enabled,
                    daily_hour,
                    daily_minute,
                    platforms,
                    created_at,
                    updated_at
                FROM contest_reminder_groups
                WHERE group_id = ?
                """,
                (str(group_id),),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_enabled_groups(self) -> list[dict]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    group_id,
                    enabled,
                    daily_hour,
                    daily_minute,
                    platforms,
                    created_at,
                    updated_at
                FROM contest_reminder_groups
                WHERE enabled = 1
                ORDER BY id ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def has_record(
        self, group_id: str, platform: str, contest_id: str, remind_type: str
    ) -> bool:
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM contest_reminder_records
                WHERE group_id = ? AND platform = ? AND contest_id = ? AND remind_type = ?
                LIMIT 1
                """,
                (str(group_id), platform, contest_id, remind_type),
            ).fetchone()
            return row is not None

    def add_record(
        self, group_id: str, platform: str, contest_id: str, remind_type: str
    ) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO contest_reminder_records
                    (group_id, platform, contest_id, remind_type, sent_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(group_id), platform, contest_id, remind_type, _now_text()),
            )
            conn.commit()

    def add_records(self, records: list[tuple[str, str, str, str]]) -> None:
        if not records:
            return
        now = _now_text()
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO contest_reminder_records
                    (group_id, platform, contest_id, remind_type, sent_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(group_id, platform, contest_id, remind_type, now) for group_id, platform, contest_id, remind_type in records],
            )
            conn.commit()

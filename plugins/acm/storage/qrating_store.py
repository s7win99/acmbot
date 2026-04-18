"""SQLite storage for the qrating system."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


INITIAL_QRATING = 1200
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "acm_bot.db"


class QratingStoreError(Exception):
    """Base class for qrating storage errors."""


class QratingValidationError(QratingStoreError):
    """Raised when qrating input cannot be applied."""


class QratingUserNotFoundError(QratingValidationError):
    """Raised when a qrating user does not exist."""


class QratingDuplicateUpdateError(QratingValidationError):
    """Raised when an update contains the same nickname more than once."""


class QratingNoRollbackEventError(QratingValidationError):
    """Raised when there is no qrating event to rollback."""


class QratingAmbiguousNicknameError(QratingValidationError):
    """Raised when a nickname matches more than one active qrating user."""


@dataclass(frozen=True)
class QratingManualChangeInput:
    """One manual qrating delta supplied by an admin."""

    identifier: str
    delta: int


@dataclass(frozen=True)
class QratingRankEntryInput:
    """One contest rank entry supplied by an admin."""

    rank: int
    identifier: str


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return dict(row)


class QratingStore:
    """Persistence API for qrating users, events, and changes."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self) -> None:
        """Create qrating tables when they do not exist."""
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS qrating_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qq_id TEXT UNIQUE NOT NULL,
                    nickname TEXT NOT NULL,
                    qrating INTEGER NOT NULL DEFAULT 1200,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS qrating_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_name TEXT NOT NULL,
                    operator_qq_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    rolled_back INTEGER NOT NULL DEFAULT 0,
                    rolled_back_at TEXT,
                    source TEXT NOT NULL DEFAULT 'manual',
                    algorithm TEXT,
                    k_factor INTEGER
                );

                CREATE TABLE IF NOT EXISTS qrating_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    qq_id TEXT NOT NULL,
                    old_qrating INTEGER NOT NULL,
                    delta INTEGER NOT NULL,
                    new_qrating INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (event_id) REFERENCES qrating_events(id),
                    FOREIGN KEY (qq_id) REFERENCES qrating_users(qq_id)
                );

                CREATE INDEX IF NOT EXISTS idx_qrating_changes_qq_id_id
                    ON qrating_changes (qq_id, id);
                CREATE INDEX IF NOT EXISTS idx_qrating_events_rollback
                    ON qrating_events (rolled_back, id);
                """
            )
            self._migrate_qrating_events(conn)
            conn.commit()

    def _migrate_qrating_events(self, conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(qrating_events)").fetchall()
        }
        if "source" not in columns:
            conn.execute(
                "ALTER TABLE qrating_events "
                "ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'"
            )
        if "algorithm" not in columns:
            conn.execute("ALTER TABLE qrating_events ADD COLUMN algorithm TEXT")
        if "k_factor" not in columns:
            conn.execute("ALTER TABLE qrating_events ADD COLUMN k_factor INTEGER")

    def add_user(self, qq_id: str, nickname: str) -> tuple[bool, dict]:
        """Add a qrating user, returning (created, user)."""
        qq_id = str(qq_id).strip()
        nickname = nickname.strip()
        if not qq_id or not nickname:
            raise QratingValidationError("QQ号和昵称不能为空。")

        now = _now_text()
        with self._connection() as conn:
            existing = self._get_user(conn, qq_id)
            if existing is not None:
                return False, existing

            conn.execute(
                """
                INSERT INTO qrating_users
                    (qq_id, nickname, qrating, created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (qq_id, nickname, INITIAL_QRATING, now, now),
            )
            user = self._get_user(conn, qq_id)
            if user is None:
                raise QratingStoreError("failed to load inserted qrating user")
            conn.commit()
            return True, user

    def get_profile(self, qq_id: str) -> dict | None:
        """Return a user profile with the latest non-rolled-back change."""
        with self._connection() as conn:
            user = self._get_user(conn, str(qq_id).strip())
            if user is None:
                return None

            latest = conn.execute(
                """
                SELECT c.delta, e.event_name
                FROM qrating_changes c
                JOIN qrating_events e ON e.id = c.event_id
                WHERE c.qq_id = ? AND e.rolled_back = 0
                ORDER BY c.id DESC
                LIMIT 1
                """,
                (user["qq_id"],),
            ).fetchone()

            profile = dict(user)
            if latest is None:
                profile["recent_delta"] = None
                profile["recent_event_name"] = None
            else:
                profile["recent_delta"] = int(latest["delta"])
                profile["recent_event_name"] = latest["event_name"]
            return profile

    def get_leaderboard(self, limit: int = 50) -> list[dict]:
        """Return active users ordered by qrating."""
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT qq_id, nickname, qrating, created_at
                FROM qrating_users
                WHERE is_active = 1
                ORDER BY qrating DESC, created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_leaderboard_with_latest_diff(self, limit: int = 50) -> dict | None:
        """Return active users with deltas from the latest non-rolled-back event."""
        with self._connection() as conn:
            event = conn.execute(
                """
                SELECT id, event_name, source, algorithm, k_factor
                FROM qrating_events
                WHERE rolled_back = 0
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if event is None:
                return None

            rows = conn.execute(
                """
                SELECT
                    u.qq_id,
                    u.nickname,
                    u.qrating,
                    u.created_at,
                    c.delta
                FROM qrating_users u
                LEFT JOIN qrating_changes c
                    ON c.qq_id = u.qq_id AND c.event_id = ?
                WHERE u.is_active = 1
                ORDER BY u.qrating DESC, u.created_at ASC, u.id ASC
                LIMIT ?
                """,
                (event["id"], limit),
            ).fetchall()

            return {
                "event_name": event["event_name"],
                "source": event["source"],
                "algorithm": event["algorithm"],
                "k_factor": event["k_factor"],
                "users": [dict(row) for row in rows],
            }

    def find_active_users_by_nickname(self, nickname: str) -> list[dict]:
        """Return active qrating users whose nickname exactly matches."""
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT id, qq_id, nickname, qrating, created_at, updated_at, is_active
                FROM qrating_users
                WHERE nickname = ? AND is_active = 1
                ORDER BY id ASC
                """,
                (str(nickname).strip(),),
            ).fetchall()
            return [dict(row) for row in rows]

    def resolve_rank_entries(
        self, entries: Iterable[QratingRankEntryInput]
    ) -> list[dict]:
        """Resolve contest rank entries to qrating users."""
        resolved: list[dict] = []
        seen_qq_ids: set[str] = set()
        with self._connection() as conn:
            for entry in entries:
                identifier = str(entry.identifier).strip()
                user = self._get_active_user_by_identifier(conn, identifier)
                if user is None:
                    raise QratingUserNotFoundError(
                        f"更新失败：用户 {identifier} 不存在，请先使用 /qrating add 添加。"
                    )
                if user["qq_id"] in seen_qq_ids:
                    raise QratingDuplicateUpdateError(
                        f"更新失败：用户 {user['nickname']} 重复出现。"
                    )
                seen_qq_ids.add(user["qq_id"])
                resolved.append(
                    {
                        "qq_id": user["qq_id"],
                        "nickname": user["nickname"],
                        "rank": int(entry.rank),
                        "old_qrating": int(user["qrating"]),
                    }
                )
        return sorted(resolved, key=lambda item: (item["rank"], item["nickname"]))

    def record_rank_update(
        self,
        event_name: str,
        operator_qq_id: str,
        calculated_changes: Iterable[dict],
        k_factor: int,
        algorithm: str = "pairwise_elo",
    ) -> dict:
        """Persist a rank-calculated qrating update in one transaction."""
        return self._record_changes(
            event_name=event_name,
            operator_qq_id=operator_qq_id,
            changes=calculated_changes,
            source="rank_calc",
            algorithm=algorithm,
            k_factor=k_factor,
        )

    def adjust_qrating(
        self,
        event_name: str,
        operator_qq_id: str,
        changes: Iterable[QratingManualChangeInput],
    ) -> dict:
        """Apply a manual qrating adjustment in one transaction."""
        event_name = event_name.strip()
        if not event_name:
            raise QratingValidationError("比赛名称不能为空。")

        normalized_changes = [
            QratingManualChangeInput(str(change.identifier).strip(), int(change.delta))
            for change in changes
        ]
        if not normalized_changes:
            raise QratingValidationError("没有提供任何更新行。")

        seen_qq_ids: set[str] = set()
        resolved_changes: list[dict] = []
        for change in normalized_changes:
            if not change.identifier:
                raise QratingValidationError("用户标识不能为空。")

        with self._connection() as conn:
            for change in normalized_changes:
                user = self._get_active_user_by_identifier(conn, change.identifier)
                if user is None:
                    raise QratingUserNotFoundError(
                        f"更新失败：用户 {change.identifier} 不存在，请先使用 /qrating add 添加。"
                    )
                if user["qq_id"] in seen_qq_ids:
                    raise QratingDuplicateUpdateError(
                        f"更新失败：用户 {user['nickname']} 重复出现。"
                    )
                seen_qq_ids.add(user["qq_id"])

                old_qrating = int(user["qrating"])
                new_qrating = max(0, old_qrating + change.delta)
                resolved_changes.append(
                    {
                        "qq_id": user["qq_id"],
                        "nickname": user["nickname"],
                        "old_qrating": old_qrating,
                        "delta": new_qrating - old_qrating,
                        "new_qrating": new_qrating,
                    }
                )

        return self._record_changes(
            event_name=event_name,
            operator_qq_id=operator_qq_id,
            changes=resolved_changes,
            source="manual_adjust",
            algorithm="manual",
            k_factor=None,
        )

    def rollback_latest(self) -> dict:
        """Rollback the latest non-rolled-back qrating event in one transaction."""
        now = _now_text()
        with self._connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                event = conn.execute(
                    """
                    SELECT id, event_name, source, algorithm, k_factor
                    FROM qrating_events
                    WHERE rolled_back = 0
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
                if event is None:
                    raise QratingNoRollbackEventError("暂无可回滚的 qrating 修改。")

                changes = conn.execute(
                    """
                    SELECT
                        c.qq_id,
                        u.nickname,
                        c.old_qrating,
                        c.new_qrating
                    FROM qrating_changes c
                    JOIN qrating_users u ON u.qq_id = c.qq_id
                    WHERE c.event_id = ?
                    ORDER BY c.id ASC
                    """,
                    (event["id"],),
                ).fetchall()

                result_changes: list[dict] = []
                for change in changes:
                    conn.execute(
                        """
                        UPDATE qrating_users
                        SET qrating = ?, updated_at = ?
                        WHERE qq_id = ?
                        """,
                        (change["old_qrating"], now, change["qq_id"]),
                    )
                    result_changes.append(
                        {
                            "qq_id": change["qq_id"],
                            "nickname": change["nickname"],
                            "from_qrating": int(change["new_qrating"]),
                            "to_qrating": int(change["old_qrating"]),
                        }
                    )

                conn.execute(
                    """
                    UPDATE qrating_events
                    SET rolled_back = 1, rolled_back_at = ?
                    WHERE id = ?
                    """,
                    (now, event["id"]),
                )
                conn.commit()
                return {
                    "event_name": event["event_name"],
                    "source": event["source"],
                    "algorithm": event["algorithm"],
                    "k_factor": event["k_factor"],
                    "changes": result_changes,
                }
            except Exception:
                conn.rollback()
                raise

    def _record_changes(
        self,
        event_name: str,
        operator_qq_id: str,
        changes: Iterable[dict],
        source: str,
        algorithm: str | None,
        k_factor: int | None,
    ) -> dict:
        event_name = event_name.strip()
        if not event_name:
            raise QratingValidationError("比赛名称不能为空。")

        normalized_changes = list(changes)
        if not normalized_changes:
            raise QratingValidationError("没有提供任何更新行。")

        now = _now_text()
        with self._connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    """
                    INSERT INTO qrating_events
                        (
                            event_name,
                            operator_qq_id,
                            created_at,
                            rolled_back,
                            source,
                            algorithm,
                            k_factor
                        )
                    VALUES (?, ?, ?, 0, ?, ?, ?)
                    """,
                    (event_name, str(operator_qq_id), now, source, algorithm, k_factor),
                )
                event_id = int(cursor.lastrowid)

                result_changes: list[dict] = []
                for change in normalized_changes:
                    conn.execute(
                        """
                        INSERT INTO qrating_changes
                            (event_id, qq_id, old_qrating, delta, new_qrating, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            change["qq_id"],
                            int(change["old_qrating"]),
                            int(change["delta"]),
                            int(change["new_qrating"]),
                            now,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE qrating_users
                        SET qrating = ?, updated_at = ?
                        WHERE qq_id = ?
                        """,
                        (int(change["new_qrating"]), now, change["qq_id"]),
                    )
                    result_changes.append(dict(change))

                conn.commit()
                return {
                    "event_name": event_name,
                    "source": source,
                    "algorithm": algorithm,
                    "k_factor": k_factor,
                    "changes": result_changes,
                }
            except Exception:
                conn.rollback()
                raise

    def _get_user(self, conn: sqlite3.Connection, qq_id: str) -> dict | None:
        row = conn.execute(
            """
            SELECT id, qq_id, nickname, qrating, created_at, updated_at, is_active
            FROM qrating_users
            WHERE qq_id = ?
            """,
            (qq_id,),
        ).fetchone()
        return _row_to_dict(row)

    def _get_active_user_by_nickname(
        self, conn: sqlite3.Connection, nickname: str
    ) -> dict | None:
        rows = conn.execute(
            """
            SELECT id, qq_id, nickname, qrating, created_at, updated_at, is_active
            FROM qrating_users
            WHERE nickname = ? AND is_active = 1
            ORDER BY id ASC
            LIMIT 2
            """,
            (nickname,),
        ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            raise QratingAmbiguousNicknameError(
                f"更新失败：昵称 {nickname} 对应多个用户，请先保证 qrating 系统内昵称唯一。"
            )
        return dict(rows[0])

    def _get_active_user_by_identifier(
        self, conn: sqlite3.Connection, identifier: str
    ) -> dict | None:
        rows = conn.execute(
            """
            SELECT id, qq_id, nickname, qrating, created_at, updated_at, is_active
            FROM qrating_users
            WHERE is_active = 1 AND (qq_id = ? OR nickname = ?)
            ORDER BY id ASC
            LIMIT 3
            """,
            (identifier, identifier),
        ).fetchall()

        users_by_qq_id = {row["qq_id"]: dict(row) for row in rows}
        if not users_by_qq_id:
            return None
        if len(users_by_qq_id) > 1:
            raise QratingAmbiguousNicknameError(
                f"更新失败：用户标识 {identifier} 匹配多个用户，请使用 QQ号。"
            )
        return next(iter(users_by_qq_id.values()))

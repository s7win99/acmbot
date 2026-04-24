"""SQLite storage for the qrating system."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..utils.qrating_algorithm import (
    DISPLAY_RULE_VERSION,
    calc_display_qrating,
)


INITIAL_INTERNAL_QRATING = 1200
INITIAL_DISPLAY_QRATING = 1000
INITIAL_RATED_CONTEST_COUNT = 0
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "acm_bot.db"


class QratingStoreError(Exception):
    """Base class for qrating storage errors."""


class QratingValidationError(QratingStoreError):
    """Raised when qrating input cannot be applied."""


class QratingUserNotFoundError(QratingValidationError):
    """Raised when a qrating user does not exist."""


class QratingDuplicateUpdateError(QratingValidationError):
    """Raised when an update contains the same user more than once."""


class QratingNoRollbackEventError(QratingValidationError):
    """Raised when there is no qrating event to rollback."""


class QratingAmbiguousNicknameError(QratingValidationError):
    """Raised when an identifier matches more than one active qrating user."""


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
        """Create qrating tables using the current double-score schema."""
        with self._connection() as conn:
            self._drop_legacy_qrating_tables_if_needed(conn)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS qrating_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qq_id TEXT NOT NULL UNIQUE,
                    nickname TEXT NOT NULL UNIQUE,
                    internal_qrating INTEGER NOT NULL,
                    display_qrating INTEGER NOT NULL,
                    rated_contest_count INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS qrating_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    operator_qq_id TEXT NOT NULL,
                    algorithm TEXT,
                    k_factor INTEGER,
                    display_rule_version TEXT NOT NULL DEFAULT 'v1',
                    participant_count INTEGER NOT NULL DEFAULT 0,
                    rolled_back INTEGER NOT NULL DEFAULT 0,
                    rolled_back_at TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS qrating_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    qq_id TEXT NOT NULL,
                    nickname_snapshot TEXT NOT NULL,

                    contest_rank INTEGER,

                    old_internal_qrating INTEGER NOT NULL,
                    new_internal_qrating INTEGER NOT NULL,
                    internal_delta INTEGER NOT NULL,

                    old_display_qrating INTEGER NOT NULL,
                    new_display_qrating INTEGER NOT NULL,
                    display_delta INTEGER NOT NULL,

                    old_rated_contest_count INTEGER NOT NULL,
                    new_rated_contest_count INTEGER NOT NULL,

                    created_at TEXT NOT NULL,

                    FOREIGN KEY (event_id) REFERENCES qrating_events(id),
                    FOREIGN KEY (qq_id) REFERENCES qrating_users(qq_id)
                );

                CREATE INDEX IF NOT EXISTS idx_qrating_users_is_active
                    ON qrating_users (is_active);
                CREATE INDEX IF NOT EXISTS idx_qrating_users_display_qrating
                    ON qrating_users (display_qrating);

                CREATE INDEX IF NOT EXISTS idx_qrating_events_created_at
                    ON qrating_events (created_at);
                CREATE INDEX IF NOT EXISTS idx_qrating_events_rolled_back
                    ON qrating_events (rolled_back);
                CREATE INDEX IF NOT EXISTS idx_qrating_events_source
                    ON qrating_events (source);

                CREATE INDEX IF NOT EXISTS idx_qrating_changes_event_id
                    ON qrating_changes (event_id);
                CREATE INDEX IF NOT EXISTS idx_qrating_changes_qq_id
                    ON qrating_changes (qq_id);
                CREATE INDEX IF NOT EXISTS idx_qrating_changes_created_at
                    ON qrating_changes (created_at);
                """
            )
            conn.commit()

    def _drop_legacy_qrating_tables_if_needed(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'qrating_users'
            """
        ).fetchone()
        if row is None:
            return

        columns = {
            item["name"]
            for item in conn.execute("PRAGMA table_info(qrating_users)").fetchall()
        }
        if {"internal_qrating", "display_qrating", "rated_contest_count"} <= columns:
            return

        conn.executescript(
            """
            DROP TABLE IF EXISTS qrating_changes;
            DROP TABLE IF EXISTS qrating_events;
            DROP TABLE IF EXISTS qrating_users;
            """
        )

    def add_user(self, qq_id: str, nickname: str) -> tuple[bool, dict]:
        """Add a qrating user, returning (created, user)."""
        qq_id = str(qq_id).strip()
        nickname = nickname.strip()
        if not qq_id or not nickname:
            raise QratingValidationError("QQ号和昵称不能为空。")

        now = _now_text()
        with self._connection() as conn:
            existing = self._get_user_by_qq_id(conn, qq_id)
            if existing is not None:
                return False, existing
            existing_nickname = self._get_user_by_nickname(conn, nickname)
            if existing_nickname is not None:
                raise QratingValidationError(
                    f"添加失败：昵称 {nickname} 已存在于 qrating 系统中。"
                )

            conn.execute(
                """
                INSERT INTO qrating_users
                    (
                        qq_id,
                        nickname,
                        internal_qrating,
                        display_qrating,
                        rated_contest_count,
                        is_active,
                        created_at,
                        updated_at
                    )
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    qq_id,
                    nickname,
                    INITIAL_INTERNAL_QRATING,
                    INITIAL_DISPLAY_QRATING,
                    INITIAL_RATED_CONTEST_COUNT,
                    now,
                    now,
                ),
            )
            user = self._get_user_by_qq_id(conn, qq_id)
            if user is None:
                raise QratingStoreError("failed to load inserted qrating user")
            conn.commit()
            return True, user

    def get_profile(self, qq_id: str) -> dict | None:
        """Return a user profile with the latest non-rolled-back display change."""
        with self._connection() as conn:
            user = self._get_user_by_qq_id(conn, str(qq_id).strip())
            if user is None:
                return None

            latest = conn.execute(
                """
                SELECT c.display_delta, e.event_name
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
                profile["recent_delta"] = int(latest["display_delta"])
                profile["recent_event_name"] = latest["event_name"]
            return profile

    def get_leaderboard(self, limit: int = 50) -> list[dict]:
        """Return active users ordered by public display qrating."""
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT qq_id, nickname, display_qrating, rated_contest_count, created_at
                FROM qrating_users
                WHERE is_active = 1
                ORDER BY display_qrating DESC, created_at ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_leaderboard_with_latest_diff(self, limit: int = 50) -> dict | None:
        """Return active users with display deltas from the latest live event."""
        with self._connection() as conn:
            event = conn.execute(
                """
                SELECT
                    id,
                    event_name,
                    source,
                    algorithm,
                    k_factor,
                    display_rule_version,
                    participant_count
                FROM qrating_events
                WHERE rolled_back = 0
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if event is None:
                return None

            totals = conn.execute(
                """
                SELECT
                    COALESCE(SUM(internal_delta), 0) AS total_internal_delta,
                    COALESCE(SUM(display_delta), 0) AS total_display_delta
                FROM qrating_changes
                WHERE event_id = ?
                """,
                (event["id"],),
            ).fetchone()

            rows = conn.execute(
                """
                SELECT
                    u.qq_id,
                    u.nickname,
                    u.display_qrating,
                    u.rated_contest_count,
                    u.created_at,
                    c.display_delta
                FROM qrating_users u
                LEFT JOIN qrating_changes c
                    ON c.qq_id = u.qq_id AND c.event_id = ?
                WHERE u.is_active = 1
                ORDER BY u.display_qrating DESC, u.created_at ASC, u.id ASC
                LIMIT ?
                """,
                (event["id"], limit),
            ).fetchall()

            result = dict(event)
            result["total_internal_delta"] = int(totals["total_internal_delta"])
            result["total_display_delta"] = int(totals["total_display_delta"])
            result["users"] = [dict(row) for row in rows]
            return result

    def find_active_users_by_nickname(self, nickname: str) -> list[dict]:
        """Return active qrating users whose nickname exactly matches."""
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    qq_id,
                    nickname,
                    internal_qrating,
                    display_qrating,
                    rated_contest_count,
                    created_at,
                    updated_at,
                    is_active
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
        """Resolve contest rank entries to active qrating users for Elo calculation."""
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
                        "old_internal_qrating": int(user["internal_qrating"]),
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
        changes = list(calculated_changes)
        if len(changes) < 2:
            raise QratingValidationError("更新失败：至少需要 2 名参赛者。")

        def build_change(conn: sqlite3.Connection, raw_change: dict) -> dict:
            user = self._get_active_user_by_qq_id(conn, str(raw_change["qq_id"]))
            if user is None:
                raise QratingUserNotFoundError(
                    f"更新失败：用户 {raw_change['qq_id']} 不存在。"
                )
            old_internal = int(user["internal_qrating"])
            if old_internal != int(raw_change["old_internal_qrating"]):
                raise QratingValidationError("更新失败：qrating 数据已变化，请重新执行命令。")

            new_internal = int(raw_change["new_internal_qrating"])
            old_display = int(user["display_qrating"])
            old_count = int(user["rated_contest_count"])
            new_count = old_count + 1
            new_display = calc_display_qrating(new_internal, new_count)
            return {
                "qq_id": user["qq_id"],
                "nickname": user["nickname"],
                "nickname_snapshot": user["nickname"],
                "contest_rank": int(raw_change["rank"]),
                "old_internal_qrating": old_internal,
                "new_internal_qrating": new_internal,
                "internal_delta": new_internal - old_internal,
                "old_display_qrating": old_display,
                "new_display_qrating": new_display,
                "display_delta": new_display - old_display,
                "old_rated_contest_count": old_count,
                "new_rated_contest_count": new_count,
            }

        return self._record_changes(
            event_name=event_name,
            operator_qq_id=operator_qq_id,
            raw_changes=changes,
            source="rank_calc",
            algorithm=algorithm,
            k_factor=k_factor,
            build_change=build_change,
        )

    def adjust_qrating(
        self,
        event_name: str,
        operator_qq_id: str,
        changes: Iterable[QratingManualChangeInput],
    ) -> dict:
        """Apply manual internal qrating adjustments in one transaction."""
        normalized_changes = [
            QratingManualChangeInput(str(change.identifier).strip(), int(change.delta))
            for change in changes
        ]
        if not normalized_changes:
            raise QratingValidationError("没有提供任何更新行。")

        seen_identifiers = set()
        for change in normalized_changes:
            if not change.identifier:
                raise QratingValidationError("用户标识不能为空。")
            if change.identifier in seen_identifiers:
                raise QratingDuplicateUpdateError(
                    f"更新失败：用户标识 {change.identifier} 重复出现。"
                )
            seen_identifiers.add(change.identifier)

        seen_qq_ids: set[str] = set()

        def build_change(
            conn: sqlite3.Connection, raw_change: QratingManualChangeInput
        ) -> dict:
            user = self._get_active_user_by_identifier(conn, raw_change.identifier)
            if user is None:
                raise QratingUserNotFoundError(
                    f"更新失败：用户 {raw_change.identifier} 不存在，请先使用 /qrating add 添加。"
                )
            if user["qq_id"] in seen_qq_ids:
                raise QratingDuplicateUpdateError(
                    f"更新失败：用户 {user['nickname']} 重复出现。"
                )
            seen_qq_ids.add(user["qq_id"])

            old_internal = int(user["internal_qrating"])
            new_internal = old_internal + int(raw_change.delta)
            old_display = int(user["display_qrating"])
            old_count = int(user["rated_contest_count"])
            new_count = old_count
            new_display = calc_display_qrating(new_internal, new_count)
            return {
                "qq_id": user["qq_id"],
                "nickname": user["nickname"],
                "nickname_snapshot": user["nickname"],
                "contest_rank": None,
                "old_internal_qrating": old_internal,
                "new_internal_qrating": new_internal,
                "internal_delta": new_internal - old_internal,
                "old_display_qrating": old_display,
                "new_display_qrating": new_display,
                "display_delta": new_display - old_display,
                "old_rated_contest_count": old_count,
                "new_rated_contest_count": new_count,
            }

        return self._record_changes(
            event_name=event_name,
            operator_qq_id=operator_qq_id,
            raw_changes=normalized_changes,
            source="manual_adjust",
            algorithm="manual",
            k_factor=None,
            build_change=build_change,
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
                        qq_id,
                        nickname_snapshot,
                        old_internal_qrating,
                        new_internal_qrating,
                        old_display_qrating,
                        new_display_qrating,
                        old_rated_contest_count,
                        new_rated_contest_count
                    FROM qrating_changes
                    WHERE event_id = ?
                    ORDER BY id ASC
                    """,
                    (event["id"],),
                ).fetchall()

                result_changes: list[dict] = []
                for change in changes:
                    conn.execute(
                        """
                        UPDATE qrating_users
                        SET
                            internal_qrating = ?,
                            display_qrating = ?,
                            rated_contest_count = ?,
                            updated_at = ?
                        WHERE qq_id = ?
                        """,
                        (
                            int(change["old_internal_qrating"]),
                            int(change["old_display_qrating"]),
                            int(change["old_rated_contest_count"]),
                            now,
                            change["qq_id"],
                        ),
                    )
                    result_changes.append(
                        {
                            "qq_id": change["qq_id"],
                            "nickname": change["nickname_snapshot"],
                            "from_internal_qrating": int(
                                change["new_internal_qrating"]
                            ),
                            "to_internal_qrating": int(change["old_internal_qrating"]),
                            "from_display_qrating": int(change["new_display_qrating"]),
                            "to_display_qrating": int(change["old_display_qrating"]),
                            "from_rated_contest_count": int(
                                change["new_rated_contest_count"]
                            ),
                            "to_rated_contest_count": int(
                                change["old_rated_contest_count"]
                            ),
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
        raw_changes: Iterable,
        source: str,
        algorithm: str | None,
        k_factor: int | None,
        build_change,
    ) -> dict:
        event_name = event_name.strip()
        if not event_name:
            raise QratingValidationError("比赛名称不能为空。")

        raw_changes = list(raw_changes)
        if not raw_changes:
            raise QratingValidationError("没有提供任何更新行。")

        now = _now_text()
        with self._connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                final_changes = [build_change(conn, change) for change in raw_changes]

                cursor = conn.execute(
                    """
                    INSERT INTO qrating_events
                        (
                            event_name,
                            source,
                            operator_qq_id,
                            algorithm,
                            k_factor,
                            display_rule_version,
                            participant_count,
                            rolled_back,
                            created_at
                        )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (
                        event_name,
                        source,
                        str(operator_qq_id),
                        algorithm,
                        k_factor,
                        DISPLAY_RULE_VERSION,
                        len(final_changes),
                        now,
                    ),
                )
                event_id = int(cursor.lastrowid)

                for change in final_changes:
                    conn.execute(
                        """
                        INSERT INTO qrating_changes
                            (
                                event_id,
                                qq_id,
                                nickname_snapshot,
                                contest_rank,
                                old_internal_qrating,
                                new_internal_qrating,
                                internal_delta,
                                old_display_qrating,
                                new_display_qrating,
                                display_delta,
                                old_rated_contest_count,
                                new_rated_contest_count,
                                created_at
                            )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            change["qq_id"],
                            change["nickname_snapshot"],
                            change["contest_rank"],
                            int(change["old_internal_qrating"]),
                            int(change["new_internal_qrating"]),
                            int(change["internal_delta"]),
                            int(change["old_display_qrating"]),
                            int(change["new_display_qrating"]),
                            int(change["display_delta"]),
                            int(change["old_rated_contest_count"]),
                            int(change["new_rated_contest_count"]),
                            now,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE qrating_users
                        SET
                            internal_qrating = ?,
                            display_qrating = ?,
                            rated_contest_count = ?,
                            updated_at = ?
                        WHERE qq_id = ?
                        """,
                        (
                            int(change["new_internal_qrating"]),
                            int(change["new_display_qrating"]),
                            int(change["new_rated_contest_count"]),
                            now,
                            change["qq_id"],
                        ),
                    )

                conn.commit()
                return {
                    "event_name": event_name,
                    "source": source,
                    "algorithm": algorithm,
                    "k_factor": k_factor,
                    "display_rule_version": DISPLAY_RULE_VERSION,
                    "participant_count": len(final_changes),
                    "changes": final_changes,
                }
            except Exception:
                conn.rollback()
                raise

    def _get_user_by_qq_id(self, conn: sqlite3.Connection, qq_id: str) -> dict | None:
        row = conn.execute(
            """
            SELECT
                id,
                qq_id,
                nickname,
                internal_qrating,
                display_qrating,
                rated_contest_count,
                created_at,
                updated_at,
                is_active
            FROM qrating_users
            WHERE qq_id = ?
            """,
            (qq_id,),
        ).fetchone()
        return _row_to_dict(row)

    def _get_user_by_nickname(
        self, conn: sqlite3.Connection, nickname: str
    ) -> dict | None:
        row = conn.execute(
            """
            SELECT
                id,
                qq_id,
                nickname,
                internal_qrating,
                display_qrating,
                rated_contest_count,
                created_at,
                updated_at,
                is_active
            FROM qrating_users
            WHERE nickname = ?
            """,
            (nickname,),
        ).fetchone()
        return _row_to_dict(row)

    def _get_active_user_by_qq_id(
        self, conn: sqlite3.Connection, qq_id: str
    ) -> dict | None:
        row = conn.execute(
            """
            SELECT
                id,
                qq_id,
                nickname,
                internal_qrating,
                display_qrating,
                rated_contest_count,
                created_at,
                updated_at,
                is_active
            FROM qrating_users
            WHERE qq_id = ? AND is_active = 1
            """,
            (qq_id,),
        ).fetchone()
        return _row_to_dict(row)

    def _get_active_user_by_identifier(
        self, conn: sqlite3.Connection, identifier: str
    ) -> dict | None:
        rows = conn.execute(
            """
            SELECT
                id,
                qq_id,
                nickname,
                internal_qrating,
                display_qrating,
                rated_contest_count,
                created_at,
                updated_at,
                is_active
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

"""ACM Bot command handlers."""

import logging
import sqlite3
import time
from datetime import datetime

from .platforms.codeforces import (
    CodeforcesApiError,
    CodeforcesDataError,
    CodeforcesRequestError,
    UserNotFoundError,
    fetch_user_info,
    get_upcoming_contests,
)
from .storage.qrating_store import (
    QratingAmbiguousNicknameError,
    QratingDuplicateUpdateError,
    QratingManualChangeInput,
    QratingNoRollbackEventError,
    QratingRankEntryInput,
    QratingStore,
    QratingStoreError,
    QratingUserNotFoundError,
    QratingValidationError,
)
from .storage.admin_log_store import AdminLogStore
from .utils.qrating_algorithm import (
    DEFAULT_PAIRWISE_ELO_K,
    calculate_pairwise_elo_deltas,
)
from .utils.config import is_admin


logger = logging.getLogger(__name__)
qrating_store = QratingStore()
admin_log_store = AdminLogStore()
ADMIN_LOG_LIMIT = 10
ADMIN_LOG_DETAIL_MAX_LENGTH = 120

USER_HELP_TEXT = """ACM Bot 帮助菜单

基础命令：
/ping        测试机器人是否在线
/help        查看帮助菜单
/about       查看机器人项目信息
/cf 用户名    查询 Codeforces 用户基础信息
/contest     查询近期 Codeforces 比赛
/qrating     查询队内自己的 qrating
/qrating rank 查看 qrating 排行榜
"""
# 后续规划：
# /bind cf 用户名  绑定 Codeforces 账号
# """
# 说明：
# 当前版本支持基础命令、Codeforces 用户查询、近期比赛查询和训练队内部 qrating。

ADMIN_HELP_TEXT = (
    USER_HELP_TEXT
    + """

管理员命令：
/qrating add QQ号 昵称
/qrating update 比赛名称
名次 昵称
名次 昵称
/qrating adjust 比赛名称
昵称 +25
昵称 -10
/qrating rank diff
/qrating rollback
/admin log"""
)

ABOUT_TEXT = """ACM Bot
一个面向 ACM/ICPC 训练群的 QQ 机器人。
当前版本：v0.3.0
当前阶段：基础命令、Codeforces 用户信息查询与近期比赛查询。"""

CF_USAGE_TEXT = """用法：/cf 用户名
示例：/cf tourist"""

QRATING_HELP_TEXT = """qrating 命令帮助：

普通用户：
/qrating              查询自己的 qrating
/qrating rank         查看 qrating 排行榜

管理员：
/qrating add QQ号 昵称
/qrating update 比赛名称
名次 昵称
名次 昵称
/qrating adjust 比赛名称
昵称+25
昵称 -10
/qrating rank diff
/qrating rollback"""

QRATING_UPDATE_USAGE_TEXT = """用法：
/qrating update 比赛名称
名次 昵称或QQ号
名次 昵称或QQ号

示例：
/qrating update 周赛第4场
1 张三
2 李四
3 王五"""

QRATING_ADJUST_USAGE_TEXT = """用法：
/qrating adjust 比赛名称
昵称 +25
昵称 -10"""

QRATING_OLD_UPDATE_FORMAT_TEXT = """/qrating update 现在用于按比赛排名自动计算 qrating。
手动调整请使用：
/qrating adjust 比赛名称
昵称 +25
昵称 -10"""


def handle_ping() -> str:
    """Return the ping response."""
    return "Yes,I'm ok."


def handle_help(user_id: str | None = None) -> str:
    """Return the help menu."""
    if is_admin(user_id):
        return ADMIN_HELP_TEXT
    return USER_HELP_TEXT


def handle_about() -> str:
    """Return project information."""
    return ABOUT_TEXT


def _format_value(value, default: str = "未评级") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _format_cf_user(user: dict) -> str:
    avatar = user.get("titlePhoto") or user.get("avatar")

    return "\n".join(
        [
            "Codeforces 用户信息",
            "",
            f"用户名：{_format_value(user.get('handle'))}",
            f"Rank：{_format_value(user.get('rank'))}",
            f"Rating：{_format_value(user.get('rating'))}",
            f"Max Rank：{_format_value(user.get('maxRank'))}",
            f"Max Rating：{_format_value(user.get('maxRating'))}",
            # f"贡献值：{_format_value(user.get('contribution'))}",
            # f"好友数：{_format_value(user.get('friendOfCount'))}",
            # f"组织：{_format_value(user.get('organization'), default='未填写')}",
            # f"头像：{_format_value(avatar, default='未填写')}",
        ]
    )


async def handle_cf_user(handle: str | None) -> str:
    """Query and format Codeforces user information."""
    if not handle:
        return CF_USAGE_TEXT

    try:
        user = await fetch_user_info(handle)
    except UserNotFoundError:
        return "未找到该 Codeforces 用户，请检查用户名是否正确。"
    except CodeforcesRequestError:
        return "Codeforces 查询失败，请稍后重试。"
    except CodeforcesApiError:
        return "Codeforces 返回异常，请稍后重试。"

    return _format_cf_user(user)


def format_datetime(timestamp: int) -> str:
    """Format a Unix timestamp as local time."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")


def format_duration(seconds: int) -> str:
    """Format seconds as hours and minutes."""
    total_minutes = max(0, seconds // 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours} 小时 {minutes} 分钟"


def format_countdown(start_timestamp: int) -> str:
    """Format the remaining time before a contest starts."""
    remaining_seconds = start_timestamp - int(time.time())
    if remaining_seconds <= 0:
        return "即将开始"

    total_minutes = remaining_seconds // 60
    days = total_minutes // (24 * 60)
    hours = (total_minutes % (24 * 60)) // 60
    minutes = total_minutes % 60
    return f"{days} 天 {hours} 小时 {minutes} 分钟"


def _format_contests(contests: list[dict]) -> str:
    if not contests:
        return "暂无即将开始的 Codeforces 比赛。"

    lines = ["近期 Codeforces 比赛："]
    for index, contest in enumerate(contests, start=1):
        lines.extend(
            [
                "",
                f"{index}. {contest.get('name') or '未命名比赛'}",
                f"开始时间：{format_datetime(contest['startTimeSeconds'])}",
                f"时长：{format_duration(contest['durationSeconds'])}",
                f"距离开始：{format_countdown(contest['startTimeSeconds'])}",
            ]
        )
    return "\n".join(lines)


async def handle_contest() -> str:
    """Query and format upcoming Codeforces contests."""
    print("[ACM Bot] handling /contest")
    try:
        contests = await get_upcoming_contests(limit=5)
    except CodeforcesRequestError:
        return "Codeforces 比赛查询失败，请稍后重试。"
    except CodeforcesDataError:
        return "Codeforces 返回数据格式异常，请稍后重试。"
    except CodeforcesApiError:
        return "Codeforces 返回异常，请稍后重试。"

    return _format_contests(contests)


def _format_qrating_delta(delta: int | None) -> str:
    if delta is None:
        return "暂无"
    if delta > 0:
        return f"+{delta}"
    return str(delta)


def _format_event_type(source: str | None) -> str:
    if source == "rank_calc":
        return "按排名自动计算"
    if source in {"manual_adjust", "manual"}:
        return "手动调整"
    return "未知类型"


def _require_admin(user_id: str | None) -> str | None:
    if not is_admin(user_id):
        return "你没有权限执行该命令。"
    return None


def _record_admin_log(
    operator_qq_id: str | None, action: str, detail: str | None = None
) -> None:
    """Best-effort admin log write. Main commands must not fail if this fails."""
    try:
        admin_log_store.add_log(str(operator_qq_id), action, detail)
    except Exception:
        logger.exception("failed to write admin operation log")


def _truncate_detail(detail: str | None) -> str:
    if not detail:
        return ""
    if len(detail) <= ADMIN_LOG_DETAIL_MAX_LENGTH:
        return detail
    return detail[: ADMIN_LOG_DETAIL_MAX_LENGTH - 3] + "..."


def _format_admin_log_time(created_at: str) -> str:
    if not created_at:
        return ""
    return created_at.replace("T", " ")[:16]


def handle_admin_log(user_id: str | None) -> str:
    """Format recent administrator operation logs."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    try:
        logs = admin_log_store.get_recent_logs(limit=ADMIN_LOG_LIMIT)
    except sqlite3.Error:
        logger.exception("failed to query admin operation logs")
        return "管理员操作日志查询失败，请稍后重试。"

    if not logs:
        return "暂无管理员操作日志。"

    lines = ["管理员操作日志："]
    for index, log in enumerate(logs, start=1):
        lines.extend(
            [
                "",
                f"{index}. {_format_admin_log_time(str(log['created_at']))}",
                f"操作者：{log['operator_qq_id']}",
                f"操作：{log['action']}",
                f"说明：{_truncate_detail(log.get('detail'))}",
            ]
        )
    return "\n".join(lines)


def handle_qrating_profile(user_id: str | None) -> str:
    """Format the sender's qrating profile."""
    if not user_id:
        return "无法识别你的 QQ 号，请稍后重试。"

    try:
        profile = qrating_store.get_profile(user_id)
    except sqlite3.Error:
        logger.exception("failed to query qrating profile")
        return "qrating 数据库查询失败，请稍后重试。"

    if profile is None:
        return "你还没有加入 qrating 系统，请联系管理员添加。"

    recent_delta = _format_qrating_delta(profile.get("recent_delta"))
    recent_event = profile.get("recent_event_name") or "暂无"
    return "\n".join(
        [
            "你的 qrating：",
            "",
            f"昵称：{profile['nickname']}",
            f"QQ：{profile['qq_id']}",
            f"当前 qrating：{profile['qrating']}",
            f"最近变动：{recent_delta}",
            f"最近比赛：{recent_event}",
        ]
    )


def handle_qrating_rank() -> str:
    """Format the qrating leaderboard."""
    try:
        users = qrating_store.get_leaderboard(limit=50)
    except sqlite3.Error:
        logger.exception("failed to query qrating leaderboard")
        return "qrating 数据库查询失败，请稍后重试。"

    if not users:
        return "qrating 系统中暂无用户。"

    lines = ["qrating 排行榜：", ""]
    for index, user in enumerate(users, start=1):
        lines.append(f"{index}. {user['nickname']}  {user['qrating']}")
    return "\n".join(lines)


def handle_qrating_rank_diff(user_id: str | None) -> str:
    """Format the qrating leaderboard with latest event deltas."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    try:
        result = qrating_store.get_leaderboard_with_latest_diff(limit=50)
    except sqlite3.Error:
        logger.exception("failed to query qrating leaderboard diff")
        return "qrating 数据库查询失败，请稍后重试。"

    if result is None:
        return "暂无可展示的 qrating 变化记录。"

    users = result["users"]
    if not users:
        return "qrating 系统中暂无用户。"

    _record_admin_log(
        user_id,
        "qrating_rank_diff",
        f"查看最近一次变化榜：{result['event_name']}",
    )

    lines = [
        "qrating 排行榜：",
    ]
    if result.get("source") == "rank_calc":
        k_factor = result.get("k_factor") or DEFAULT_PAIRWISE_ELO_K
        lines.extend(
            [
                f"最近比赛：{result['event_name']}",
                f"算法：Pairwise Elo，K={k_factor}",
                "括号内为本次变化",
                "",
            ]
        )
    elif result.get("source") in {"manual_adjust", "manual"}:
        lines.extend(
            [
                f"最近修改：手动调整 - {result['event_name']}",
                "括号内为本次变化",
                "",
            ]
        )
    else:
        lines.extend(
            [
                f"最近修改：{result['event_name']}",
                "括号内为本次变化",
                "",
            ]
        )

    for index, user in enumerate(users, start=1):
        delta = user.get("delta")
        delta_text = _format_qrating_delta(delta) if delta is not None else "-"
        lines.append(f"{index}. {user['nickname']}  {user['qrating']}  ({delta_text})")
    return "\n".join(lines)


def handle_qrating_add(first_line: str, user_id: str | None) -> str:
    """Handle /qrating add."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    parts = first_line.split(maxsplit=3)
    if len(parts) < 4 or not parts[2].strip() or not parts[3].strip():
        return "用法：\n/qrating add QQ号 昵称"

    qq_id = parts[2].strip()
    nickname = parts[3].strip()

    try:
        created, user = qrating_store.add_user(qq_id, nickname)
    except QratingValidationError as exc:
        return str(exc)
    except sqlite3.Error:
        logger.exception("failed to add qrating user")
        return "qrating 数据库操作失败，请稍后重试。"

    if created:
        _record_admin_log(
            user_id,
            "qrating_add",
            (
                f"添加用户：{user['nickname']}，QQ：{user['qq_id']}，"
                f"初始 qrating：{user['qrating']}"
            ),
        )
        return "\n".join(
            [
                "添加成功：",
                f"用户：{user['nickname']}",
                f"QQ：{user['qq_id']}",
                f"初始 qrating：{user['qrating']}",
            ]
        )

    return "\n".join(
        [
            "该用户已存在于 qrating 系统中。",
            f"用户：{user['nickname']}",
            f"当前 qrating：{user['qrating']}",
        ]
    )


def _is_int_text(text: str) -> bool:
    try:
        int(text)
    except ValueError:
        return False
    return True


def _parse_qrating_update_lines(
    update_lines: list[str],
) -> list[QratingRankEntryInput] | str:
    entries: list[QratingRankEntryInput] = []

    for line in update_lines:
        if not line.strip():
            return QRATING_UPDATE_USAGE_TEXT

        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            return QRATING_UPDATE_USAGE_TEXT

        rank_text, identifier = parts[0].strip(), parts[1].strip()
        if not rank_text.isdigit():
            if _is_int_text(identifier):
                return QRATING_OLD_UPDATE_FORMAT_TEXT
            return "更新失败：名次必须是正整数。\n\n" + QRATING_UPDATE_USAGE_TEXT

        try:
            rank = int(rank_text)
        except ValueError:
            return "更新失败：名次必须是正整数。\n\n" + QRATING_UPDATE_USAGE_TEXT

        if rank <= 0:
            return "更新失败：名次必须是正整数。\n\n" + QRATING_UPDATE_USAGE_TEXT
        if not identifier:
            return QRATING_UPDATE_USAGE_TEXT

        entries.append(QratingRankEntryInput(rank=rank, identifier=identifier))

    if len(entries) < 2:
        return "更新失败：至少需要 2 名参赛者。"
    return entries


def _parse_qrating_adjust_lines(
    adjust_lines: list[str],
) -> list[QratingManualChangeInput] | str:
    changes: list[QratingManualChangeInput] = []

    for line in adjust_lines:
        if not line.strip():
            return QRATING_ADJUST_USAGE_TEXT

        parts = line.split()
        if len(parts) < 2:
            return QRATING_ADJUST_USAGE_TEXT

        identifier = " ".join(parts[:-1]).strip()
        delta_text = parts[-1].strip()
        try:
            delta = int(delta_text)
        except ValueError:
            return "更新失败：变化量必须是整数，例如 +25 或 -10。"

        if not identifier:
            return QRATING_ADJUST_USAGE_TEXT

        changes.append(QratingManualChangeInput(identifier=identifier, delta=delta))

    return changes


def handle_qrating_update(lines: list[str], user_id: str | None) -> str:
    """Handle /qrating update."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    first_line = lines[0]
    parts = first_line.split(maxsplit=2)
    event_name = parts[2].strip() if len(parts) >= 3 else ""
    if not event_name or len(lines) <= 1:
        return QRATING_UPDATE_USAGE_TEXT

    parsed_entries = _parse_qrating_update_lines(lines[1:])
    if isinstance(parsed_entries, str):
        return parsed_entries

    try:
        participants = qrating_store.resolve_rank_entries(parsed_entries)
        calculated_changes = calculate_pairwise_elo_deltas(
            participants,
            k=DEFAULT_PAIRWISE_ELO_K,
        )
        result = qrating_store.record_rank_update(
            event_name=event_name,
            operator_qq_id=str(user_id),
            calculated_changes=calculated_changes,
            k_factor=DEFAULT_PAIRWISE_ELO_K,
        )
    except (
        QratingAmbiguousNicknameError,
        QratingUserNotFoundError,
        QratingDuplicateUpdateError,
        QratingValidationError,
    ) as exc:
        return str(exc)
    except (QratingStoreError, sqlite3.Error):
        logger.exception("failed to update qrating")
        return "qrating 更新失败，请稍后重试。"
    except Exception:
        logger.exception("failed to calculate qrating")
        return "qrating 算法计算失败，请检查输入后重试。"

    total_delta = sum(int(change["delta"]) for change in result["changes"])
    _record_admin_log(
        user_id,
        "qrating_update",
        (
            f"{result['event_name']}，参赛人数 {len(result['changes'])}，"
            f"算法 pairwise_elo，K={DEFAULT_PAIRWISE_ELO_K}，"
            f"总变化 {_format_qrating_delta(total_delta)}"
        ),
    )
    reply_lines = [
        f"qrating 更新完成：{result['event_name']}",
        f"算法：Pairwise Elo，K={DEFAULT_PAIRWISE_ELO_K}",
        "",
    ]
    for change in result["changes"]:
        delta_text = _format_qrating_delta(change["delta"])
        reply_lines.append(
            f"{change['nickname']}：{change['old_qrating']} -> "
            f"{change['new_qrating']}（{delta_text}）"
        )
    reply_lines.extend(
        [
            "",
            f"本场 qrating 总变化：{_format_qrating_delta(total_delta)}",
            "",
            "如录入有误，可使用 /qrating rollback 回滚本次修改。",
        ]
    )
    return "\n".join(reply_lines)


def handle_qrating_adjust(lines: list[str], user_id: str | None) -> str:
    """Handle /qrating adjust."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    first_line = lines[0]
    parts = first_line.split(maxsplit=2)
    event_name = parts[2].strip() if len(parts) >= 3 else ""
    if not event_name or len(lines) <= 1:
        return QRATING_ADJUST_USAGE_TEXT

    parsed_changes = _parse_qrating_adjust_lines(lines[1:])
    if isinstance(parsed_changes, str):
        return parsed_changes

    try:
        result = qrating_store.adjust_qrating(
            event_name=event_name,
            operator_qq_id=str(user_id),
            changes=parsed_changes,
        )
    except (
        QratingAmbiguousNicknameError,
        QratingUserNotFoundError,
        QratingDuplicateUpdateError,
        QratingValidationError,
    ) as exc:
        return str(exc)
    except (QratingStoreError, sqlite3.Error):
        logger.exception("failed to adjust qrating")
        return "qrating 手动调整失败，请稍后重试。"

    total_delta = sum(int(change["delta"]) for change in result["changes"])
    _record_admin_log(
        user_id,
        "qrating_adjust",
        (
            f"手动修正，调整人数 {len(result['changes'])}，"
            f"总变化 {_format_qrating_delta(total_delta)}"
        ),
    )
    reply_lines = [f"qrating 手动调整完成：{result['event_name']}", ""]
    for change in result["changes"]:
        delta_text = _format_qrating_delta(change["delta"])
        reply_lines.append(
            f"{change['nickname']}：{change['old_qrating']} -> "
            f"{change['new_qrating']}（{delta_text}）"
        )
    reply_lines.extend(["", f"本次 qrating 总变化：{_format_qrating_delta(total_delta)}"])
    return "\n".join(reply_lines)


def handle_qrating_rollback(user_id: str | None) -> str:
    """Handle /qrating rollback."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    try:
        result = qrating_store.rollback_latest()
    except QratingNoRollbackEventError:
        return "暂无可回滚的 qrating 修改。"
    except (QratingStoreError, sqlite3.Error):
        logger.exception("failed to rollback qrating")
        return "qrating 回滚失败，请稍后重试。"

    _record_admin_log(
        user_id,
        "qrating_rollback",
        (
            f"回滚事件：{result['event_name']}，类型：{result.get('source')}，"
            f"影响人数 {len(result['changes'])}"
        ),
    )
    reply_lines = [
        f"已回滚最近一次 qrating 修改：{result['event_name']}",
        f"类型：{_format_event_type(result.get('source'))}",
        "",
    ]
    for change in result["changes"]:
        reply_lines.append(
            f"{change['nickname']}：{change['from_qrating']} -> "
            f"{change['to_qrating']}"
        )
    return "\n".join(reply_lines)


def handle_qrating_command(message: str, user_id: str | None) -> str:
    """Dispatch /qrating subcommands."""
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return QRATING_HELP_TEXT

    first_line = lines[0]
    parts = first_line.split(maxsplit=2)
    if len(parts) == 1:
        return handle_qrating_profile(user_id)

    subcommand = parts[1].strip().lower()
    if subcommand == "rank" and len(parts) == 2:
        return handle_qrating_rank()
    if subcommand == "rank" and len(parts) == 3 and parts[2].strip().lower() == "diff":
        return handle_qrating_rank_diff(user_id)
    if subcommand == "add":
        return handle_qrating_add(first_line, user_id)
    if subcommand == "update":
        return handle_qrating_update(lines, user_id)
    if subcommand == "adjust":
        return handle_qrating_adjust(lines, user_id)
    if subcommand == "rollback" and len(parts) == 2:
        return handle_qrating_rollback(user_id)

    return QRATING_HELP_TEXT


async def dispatch_command(message: str, user_id: str | None = None) -> str | None:
    """Dispatch a raw text message to a command handler."""
    command = message.strip()

    if command == "/ping":
        return handle_ping()
    if command == "/help":
        return handle_help(user_id)
    if command == "/about":
        return handle_about()
    if command == "/cf":
        return await handle_cf_user(None)
    if command.startswith("/cf "):
        handle = command.removeprefix("/cf ").strip()
        return await handle_cf_user(handle)
    if command == "/contest":
        return await handle_contest()
    if command == "/admin log":
        return handle_admin_log(user_id)
    if command == "/qrating" or command.startswith("/qrating "):
        return handle_qrating_command(command, user_id)

    return None

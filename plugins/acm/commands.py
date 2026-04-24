"""ACM Bot command handlers."""

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .platforms.codeforces import (
    CodeforcesApiError,
    CodeforcesRequestError,
    UserNotFoundError,
    fetch_user_info,
)
from .services.contest_service import (
    ContestServiceError,
    format_contest_list,
    get_recent_contests,
)
from .storage.contest_reminder_store import ContestReminderStore
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
from .utils.vjudge_import import (
    VJudgeImportError,
    VJudgeStandingRow,
    parse_vjudge_xlsx,
)


logger = logging.getLogger(__name__)
qrating_store = QratingStore()
admin_log_store = AdminLogStore()
contest_reminder_store = ContestReminderStore()
ADMIN_LOG_LIMIT = 10
ADMIN_LOG_DETAIL_MAX_LENGTH = 120


@dataclass(frozen=True)
class QratingImportAttachment:
    """Downloaded attachment state for /qrating import."""

    status: str
    path: Path | None = None
    detail: str | None = None


@dataclass(frozen=True)
class QratingAddInput:
    """One qrating user supplied by an admin."""

    qq_id: str
    nickname: str
    line_no: int


@dataclass(frozen=True)
class CommandContext:
    """Context for one incoming command."""

    user_id: str | None
    is_group: bool
    group_id: str | None = None


USER_HELP_TEXT = """ACM Bot 帮助菜单

基础命令：
/ping        测试机器人是否在线
/help        查看帮助菜单
/about       查看机器人项目信息
/cf 用户名    查询 Codeforces 用户基础信息
/contest     查询近期比赛
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
/qrating add
QQ号 昵称
QQ号 昵称
/qrating update 比赛名称
名次 昵称或QQ号
名次 昵称或QQ号
/qrating import 比赛名称
/qrating adjust 比赛名称
昵称 +25
昵称 -10
/qrating rank diff
/qrating rollback
/contest remind on
/contest remind off
/contest remind status
/admin log"""
)

ABOUT_TEXT = """ACM Bot
一个面向 ACM/ICPC 训练群的 QQ 机器人。
当前版本：v0.3.0
当前阶段：基础命令、Codeforces 用户信息查询、比赛查询与 qrating。"""

CF_USAGE_TEXT = """用法：/cf 用户名
示例：/cf tourist"""

QRATING_HELP_TEXT = """qrating 命令帮助：

普通用户：
/qrating              查询自己的 qrating
/qrating rank         查看 qrating 排行榜

管理员：
/qrating add QQ号 昵称
/qrating add
QQ号 昵称
QQ号 昵称
/qrating update 比赛名称
名次 昵称或QQ号
名次 昵称或QQ号
/qrating import 比赛名称
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

QRATING_ADD_USAGE_TEXT = """用法：
/qrating add QQ号 昵称

或批量添加：
/qrating add
QQ号 昵称
QQ号 昵称"""

QRATING_IMPORT_USAGE_TEXT = """用法：
/qrating import 比赛名称

请先发送 xlsx 文件，并回复该文件消息执行导入。"""

CONTEST_HELP_TEXT = """用法：
/contest

管理员可在群聊中配置提醒：
/contest remind on
/contest remind off
/contest remind status"""


def handle_ping() -> str:
    """Return the ping response."""
    return "别ping了，我还活着~"


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
        return "Codeforces 查询失败，请稍后重试。"

    return _format_cf_user(user)


def _require_group_context(context: CommandContext) -> str | None:
    if not context.is_group or not context.group_id:
        return "比赛提醒功能仅支持在群聊中配置。"
    return None


async def handle_contest() -> str:
    """Query and format merged upcoming contests."""
    try:
        contests = await get_recent_contests(limit=5)
    except ContestServiceError:
        return "比赛查询失败，请稍后重试。"

    return format_contest_list(contests, title="近期比赛：")


def handle_contest_remind_on(context: CommandContext) -> str:
    """Enable contest reminders for the current group."""
    group_error = _require_group_context(context)
    if group_error:
        return group_error

    permission_error = _require_admin(context.user_id)
    if permission_error:
        return permission_error

    contest_reminder_store.enable_group(str(context.group_id))
    _record_admin_log(
        context.user_id,
        "contest_remind_on",
        f"group_id={context.group_id}; daily=09:00; reminders=60,5; platforms=cf,atc",
    )
    return "\n".join(
        [
            "已开启本群比赛提醒。",
            # "每日速览时间：09:00",
            # "赛前提醒：60 分钟、5 分钟",
            # "平台：Codeforces, AtCoder",
        ]
    )


def handle_contest_remind_off(context: CommandContext) -> str:
    """Disable contest reminders for the current group."""
    group_error = _require_group_context(context)
    if group_error:
        return group_error

    permission_error = _require_admin(context.user_id)
    if permission_error:
        return permission_error

    contest_reminder_store.disable_group(str(context.group_id))
    _record_admin_log(
        context.user_id,
        "contest_remind_off",
        f"group_id={context.group_id}",
    )
    return "已关闭本群比赛提醒。"


def handle_contest_remind_status(context: CommandContext) -> str:
    """Show contest reminder status for the current group."""
    group_error = _require_group_context(context)
    if group_error:
        return group_error

    permission_error = _require_admin(context.user_id)
    if permission_error:
        return permission_error

    config = contest_reminder_store.get_group(str(context.group_id))
    _record_admin_log(
        context.user_id,
        "contest_remind_status",
        f"group_id={context.group_id}; enabled={int(bool(config and config.get('enabled')))}",
    )
    if not config or not int(config.get("enabled", 0)):
        return "\n".join(
            [
                "比赛提醒状态：",
                "当前群：未开启",
            ]
        )

    return "\n".join(
        [
            "比赛提醒状态：",
            "当前群：已开启",
            # "每日速览时间：09:00",
            # "赛前提醒：60 分钟、5 分钟",
            # "平台：Codeforces, AtCoder",
        ]
    )


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
            f"当前显示 qrating：{profile['display_qrating']}",
            f"已参加场次：{profile['rated_contest_count']}",
            f"最近比赛：{recent_event}",
            f"最近变化：{recent_delta}",
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
        lines.append(f"{index}. {user['nickname']}  {user['display_qrating']}")
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
        (
            f"event_name={result['event_name']}; "
            f"participant_count={result.get('participant_count')}; "
            f"source={result.get('source')}; "
            f"algorithm={result.get('algorithm')}; "
            f"k_factor={result.get('k_factor')}; "
            f"total_internal_delta="
            f"{_format_qrating_delta(result.get('total_internal_delta'))}; "
            f"total_display_delta="
            f"{_format_qrating_delta(result.get('total_display_delta'))}"
        ),
    )

    lines = [
        "qrating 变化榜：",
        f"事件：{result['event_name']}",
    ]
    algorithm = result.get("algorithm") or "未知"
    k_factor = result.get("k_factor")
    algorithm_line = f"算法：{algorithm}"
    if k_factor is not None:
        algorithm_line += f"，K={k_factor}"
    lines.extend([algorithm_line, "括号内为本次显示 qrating 变化", ""])

    for index, user in enumerate(users, start=1):
        delta = user.get("display_delta")
        delta_text = _format_qrating_delta(delta) if delta is not None else "-"
        lines.append(
            f"{index}. {user['nickname']}  {user['display_qrating']}  ({delta_text})"
        )
    return "\n".join(lines)


def _parse_qrating_add_entries(lines: list[str]) -> list[QratingAddInput] | str:
    if not lines:
        return QRATING_ADD_USAGE_TEXT

    first_line = lines[0]
    first_parts = first_line.split(maxsplit=3)
    body_lines = lines[1:]
    entries: list[QratingAddInput] = []

    if len(first_parts) >= 4:
        if body_lines:
            return QRATING_ADD_USAGE_TEXT
        return [
            QratingAddInput(
                qq_id=first_parts[2].strip(),
                nickname=first_parts[3].strip(),
                line_no=1,
            )
        ]

    if len(first_parts) != 2 or not body_lines:
        return QRATING_ADD_USAGE_TEXT

    seen_qq_ids: dict[str, int] = {}
    for line_no, line in enumerate(body_lines, start=2):
        if not line.strip():
            return QRATING_ADD_USAGE_TEXT

        parts = line.split(maxsplit=1)
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            return "\n".join(
                [
                    f"添加失败：第 {line_no} 行格式错误。",
                    "",
                    QRATING_ADD_USAGE_TEXT,
                ]
            )

        qq_id = parts[0].strip()
        nickname = parts[1].strip()
        previous_line_no = seen_qq_ids.get(qq_id)
        if previous_line_no is not None:
            return "\n".join(
                [
                    "添加失败：QQ号重复",
                    "",
                    f"第 {previous_line_no} 行和第 {line_no} 行都使用了 QQ号：{qq_id}",
                ]
            )
        seen_qq_ids[qq_id] = line_no
        entries.append(QratingAddInput(qq_id=qq_id, nickname=nickname, line_no=line_no))

    return entries


def _format_qrating_add_reply(
    created_users: list[dict], existing_users: list[dict]
) -> str:
    if len(created_users) == 1 and not existing_users:
        user = created_users[0]
        return "\n".join(
            [
                "添加成功：",
                f"用户：{user['nickname']}",
                f"QQ：{user['qq_id']}",
                f"内部 qrating：{user['internal_qrating']}",
                f"显示 qrating：{user['display_qrating']}",
                f"已参加场次：{user['rated_contest_count']}",
            ]
        )

    if len(existing_users) == 1 and not created_users:
        user = existing_users[0]
        return "\n".join(
            [
                "该用户已存在于 qrating 系统中。",
                f"用户：{user['nickname']}",
                f"当前显示 qrating：{user['display_qrating']}",
                f"已参加场次：{user['rated_contest_count']}",
            ]
        )

    lines = [
        "qrating 用户批量添加完成：",
        "",
        f"新增：{len(created_users)}",
        f"已存在：{len(existing_users)}",
    ]
    if created_users:
        lines.extend(["", "新增用户："])
        for user in created_users:
            lines.append(
                f"{user['qq_id']} {user['nickname']}  "
                f"内部 {user['internal_qrating']} / 显示 {user['display_qrating']} / "
                f"场次 {user['rated_contest_count']}"
            )
    if existing_users:
        lines.extend(["", "已存在用户："])
        for user in existing_users:
            lines.append(
                f"{user['qq_id']} {user['nickname']}  "
                f"显示 {user['display_qrating']} / 场次 {user['rated_contest_count']}"
            )
    return "\n".join(lines)


def handle_qrating_add(lines: list[str], user_id: str | None) -> str:
    """Handle /qrating add."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    parsed_entries = _parse_qrating_add_entries(lines)
    if isinstance(parsed_entries, str):
        return parsed_entries

    created_users: list[dict] = []
    existing_users: list[dict] = []
    try:
        for entry in parsed_entries:
            created, user = qrating_store.add_user(entry.qq_id, entry.nickname)
            if created:
                created_users.append(user)
            else:
                existing_users.append(user)
    except QratingValidationError as exc:
        return str(exc)
    except sqlite3.Error:
        logger.exception("failed to add qrating user")
        return "qrating 数据库操作失败，请稍后重试。"

    if created_users:
        _record_admin_log(
            user_id,
            "qrating_add",
            (
                f"action=qrating_add; participant_count={len(created_users)}; "
                f"source=admin; algorithm=manual; "
                f"initial_internal={created_users[0]['internal_qrating']}; "
                f"initial_display={created_users[0]['display_qrating']}"
            ),
        )

    return _format_qrating_add_reply(created_users, existing_users)


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


def is_qrating_import_command(message: str) -> bool:
    """Return whether a raw message is a /qrating import command."""
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return False
    parts = lines[0].split(maxsplit=2)
    return len(parts) >= 2 and parts[0] == "/qrating" and parts[1].lower() == "import"


def precheck_qrating_import_command(message: str, user_id: str | None) -> str | None:
    """Validate permissions and event name before plugin downloads an attachment."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    event_name = _parse_qrating_import_event_name(message)
    if not event_name:
        return QRATING_IMPORT_USAGE_TEXT
    return None


def _parse_qrating_import_event_name(message: str) -> str:
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if not lines:
        return ""
    parts = lines[0].split(maxsplit=2)
    if len(parts) < 3:
        return ""
    return parts[2].strip()


def _format_qrating_import_reply_file_hint(event_name: str) -> str:
    import_command = f"/qrating import {event_name or '比赛名称'}"
    return "\n".join(
        [
            "请先发送 xlsx 文件，并回复该文件消息执行：",
            import_command,
        ]
    )


def _format_qrating_import_match_error(row: VJudgeStandingRow, reason: str) -> str:
    return "\n".join(
        [
            "导入失败：存在未匹配用户",
            "",
            f"第 {row.row_no} 行：",
            f"Rank = {row.rank}",
            f"Team = {row.team_raw}",
            f"解析昵称 = {row.candidate_nickname}",
            reason,
            "",
            "请先添加或修正 qrating 用户昵称后再重试。",
        ]
    )


def _match_vjudge_rows_to_qrating_users(rows: list[VJudgeStandingRow]) -> list[dict]:
    matched_rows: list[dict] = []
    for row in rows:
        users = qrating_store.find_active_users_by_nickname(row.candidate_nickname)
        if len(users) != 1:
            reason = "在 qrating 用户表中未找到唯一匹配"
            if len(users) > 1:
                reason = "在 qrating 用户表中匹配到多个用户，未找到唯一匹配"
            raise QratingValidationError(
                _format_qrating_import_match_error(row, reason)
            )

        user = users[0]
        matched_rows.append(
            {
                "row_no": row.row_no,
                "rank": row.rank,
                "team_raw": row.team_raw,
                "candidate_nickname": row.candidate_nickname,
                "matched_nickname": user["nickname"],
                "qq_id": user["qq_id"],
            }
        )
    return matched_rows


def _format_qrating_import_success(event_name: str, matched_rows: list[dict]) -> str:
    sorted_rows = sorted(matched_rows, key=lambda item: (item["rank"], item["row_no"]))
    command_lines = [f"/qrating update {event_name}"]
    command_lines.extend(
        f"{row['rank']} {row['matched_nickname']}" for row in sorted_rows
    )
    command_text = "\n".join(command_lines)

    return "\n".join(
        [
            f"VJudge 榜单解析成功：{event_name}",
            "",
            f"识别到参赛者：{len(matched_rows)}",
            f"成功匹配：{len(matched_rows)}",
            "已生成 /qrating update 命令",
            "",
            command_text,
        ]
    )


def handle_qrating_import(
    message: str,
    user_id: str | None,
    attachment: QratingImportAttachment | None = None,
) -> str:
    """Handle /qrating import preview generation."""
    permission_error = _require_admin(user_id)
    if permission_error:
        return permission_error

    event_name = _parse_qrating_import_event_name(message)
    if not event_name:
        return QRATING_IMPORT_USAGE_TEXT

    attachment = attachment or QratingImportAttachment(status="not_reply")
    if attachment.status == "not_reply":
        return _format_qrating_import_reply_file_hint(event_name)
    if attachment.status == "no_file":
        return "未检测到 xlsx 文件，请回复一个 .xlsx 文件消息后再执行导入。"
    if attachment.status == "not_xlsx":
        return "仅支持导入 .xlsx 文件。"
    if attachment.status == "download_failed":
        if attachment.detail:
            return f"文件下载失败：{attachment.detail}"
        return "文件下载失败，请稍后重试或重新发送文件。"
    if attachment.status != "ok" or attachment.path is None:
        return "文件下载失败，请稍后重试或重新发送文件。"

    try:
        parsed_rows = parse_vjudge_xlsx(attachment.path)
        matched_rows = _match_vjudge_rows_to_qrating_users(parsed_rows)
    except VJudgeImportError as exc:
        return str(exc)
    except QratingValidationError as exc:
        return str(exc)
    except sqlite3.Error:
        logger.exception("failed to query qrating users for import")
        return "qrating 数据库查询失败，请稍后重试。"
    except Exception:
        logger.exception("failed to import VJudge xlsx")
        return "Excel 读取失败，请确认文件是有效的 .xlsx 文件。"
    finally:
        try:
            attachment.path.unlink(missing_ok=True)
        except OSError:
            logger.warning("failed to remove temporary import file: %s", attachment.path)

    _record_admin_log(
        user_id,
        "qrating_import_preview",
        (
            f"{event_name}，识别 {len(parsed_rows)} 人，"
            f"成功匹配 {len(matched_rows)} 人，生成 update 命令"
        ),
    )
    return _format_qrating_import_success(event_name, matched_rows)


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

    total_internal_delta = sum(
        int(change["internal_delta"]) for change in result["changes"]
    )
    total_display_delta = sum(
        int(change["display_delta"]) for change in result["changes"]
    )
    _record_admin_log(
        user_id,
        "qrating_update",
        (
            f"event_name={result['event_name']}; "
            f"participant_count={result['participant_count']}; "
            f"source={result['source']}; algorithm={result['algorithm']}; "
            f"k_factor={result['k_factor']}; "
            f"total_internal_delta={_format_qrating_delta(total_internal_delta)}; "
            f"total_display_delta={_format_qrating_delta(total_display_delta)}"
        ),
    )
    reply_lines = [
        f"qrating 更新完成：{result['event_name']}",
        f"算法：Pairwise Elo，K={DEFAULT_PAIRWISE_ELO_K}",
        "以下为显示 qrating 变化：",
        "",
    ]
    for change in result["changes"]:
        delta_text = _format_qrating_delta(change["display_delta"])
        reply_lines.append(
            f"{change['nickname']}：{change['old_display_qrating']} -> "
            f"{change['new_display_qrating']}（{delta_text}，"
            f"场次 {change['old_rated_contest_count']} -> "
            f"{change['new_rated_contest_count']}）"
        )
    reply_lines.extend(
        [
            "",
            f"本场显示 qrating 总变化：{_format_qrating_delta(total_display_delta)}",
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

    total_internal_delta = sum(
        int(change["internal_delta"]) for change in result["changes"]
    )
    total_display_delta = sum(
        int(change["display_delta"]) for change in result["changes"]
    )
    _record_admin_log(
        user_id,
        "qrating_adjust",
        (
            f"event_name={result['event_name']}; "
            f"participant_count={result['participant_count']}; "
            f"source={result['source']}; algorithm={result['algorithm']}; "
            f"k_factor={result['k_factor']}; "
            f"total_internal_delta={_format_qrating_delta(total_internal_delta)}; "
            f"total_display_delta={_format_qrating_delta(total_display_delta)}"
        ),
    )
    reply_lines = [
        f"qrating 手动调整完成：{result['event_name']}",
        "以下为显示 qrating 变化：",
        "",
    ]
    for change in result["changes"]:
        delta_text = _format_qrating_delta(change["display_delta"])
        reply_lines.append(
            f"{change['nickname']}：{change['old_display_qrating']} -> "
            f"{change['new_display_qrating']}（{delta_text}）"
        )
    reply_lines.extend(
        ["", f"本次显示 qrating 总变化：{_format_qrating_delta(total_display_delta)}"]
    )
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
            f"event_name={result['event_name']}; source={result.get('source')}; "
            f"algorithm={result.get('algorithm')}; k_factor={result.get('k_factor')}; "
            f"participant_count={len(result['changes'])}"
        ),
    )
    reply_lines = [
        f"已回滚最近一次 qrating 修改：{result['event_name']}",
        f"类型：{_format_event_type(result.get('source'))}",
        "",
    ]
    for change in result["changes"]:
        reply_lines.append(
            f"{change['nickname']}：显示 {change['from_display_qrating']} -> "
            f"{change['to_display_qrating']}，场次 "
            f"{change['from_rated_contest_count']} -> "
            f"{change['to_rated_contest_count']}"
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
        return handle_qrating_add(lines, user_id)
    if subcommand == "import":
        return handle_qrating_import(message, user_id)
    if subcommand == "update":
        return handle_qrating_update(lines, user_id)
    if subcommand == "adjust":
        return handle_qrating_adjust(lines, user_id)
    if subcommand == "rollback" and len(parts) == 2:
        return handle_qrating_rollback(user_id)

    return QRATING_HELP_TEXT


async def handle_contest_command(message: str, context: CommandContext) -> str:
    """Dispatch /contest subcommands."""
    command = message.strip()
    if command == "/contest":
        return await handle_contest()

    lines = [line.strip() for line in command.splitlines() if line.strip()]
    if not lines:
        return CONTEST_HELP_TEXT

    parts = lines[0].split()
    if len(parts) == 3 and parts[0] == "/contest" and parts[1].lower() == "remind":
        action = parts[2].lower()
        if action == "on":
            return handle_contest_remind_on(context)
        if action == "off":
            return handle_contest_remind_off(context)
        if action == "status":
            return handle_contest_remind_status(context)

    return CONTEST_HELP_TEXT


async def dispatch_command(
    message: str,
    user_id: str | None = None,
    *,
    is_group: bool = False,
    group_id: str | None = None,
) -> str | None:
    """Dispatch a raw text message to a command handler."""
    command = message.strip()
    context = CommandContext(user_id=user_id, is_group=is_group, group_id=group_id)

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
    if command == "/contest" or command.startswith("/contest "):
        return await handle_contest_command(command, context)
    if command == "/admin log":
        return handle_admin_log(user_id)
    if command == "/qrating" or command.startswith("/qrating "):
        return handle_qrating_command(command, user_id)

    return None

"""Contest aggregation and formatting helpers."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from ..platforms.atcoder import (
    AtCoderDataError,
    AtCoderRequestError,
    get_upcoming_contests as get_atcoder_upcoming_contests,
)
from ..platforms.codeforces import (
    CodeforcesApiError,
    CodeforcesDataError,
    CodeforcesRequestError,
    get_upcoming_contests as get_codeforces_upcoming_contests,
)


logger = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class ContestServiceError(Exception):
    """Raised when no contest source can provide usable data."""


def get_now() -> datetime:
    """Return the current timezone-aware Beijing time."""
    return datetime.now(SHANGHAI_TZ)


def get_platform_label(platform: str) -> str:
    if platform == "cf":
        return "CF"
    if platform == "atc":
        return "ATC"
    return platform.upper()


def format_contest_start_time(start_time: datetime) -> str:
    return start_time.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")


def format_duration_minutes(duration_minutes: int) -> str:
    total = max(0, int(duration_minutes))
    hours = total // 60
    minutes = total % 60
    if hours and minutes:
        return f"{hours}h{minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def format_time_until(start_time: datetime, now: datetime | None = None) -> str:
    current = now or get_now()
    delta_seconds = int((start_time - current).total_seconds())
    if delta_seconds <= 0:
        return "即将开始"

    total_minutes = delta_seconds // 60
    days = total_minutes // (24 * 60)
    hours = (total_minutes % (24 * 60)) // 60
    minutes = total_minutes % 60

    if days > 0:
        if hours > 0:
            return f"{days}天{hours}小时"
        return f"{days}天"
    if hours > 0:
        if minutes > 0:
            return f"{hours}小时{minutes}分钟"
        return f"{hours}小时"
    return f"{minutes}分钟"


async def get_all_upcoming_contests() -> list[dict]:
    """Fetch and merge upcoming contests from all supported platforms."""
    results = await asyncio.gather(
        get_codeforces_upcoming_contests(limit=None),
        get_atcoder_upcoming_contests(limit=None),
        return_exceptions=True,
    )

    contests: list[dict] = []
    success_count = 0

    cf_result, atc_result = results
    if isinstance(cf_result, Exception):
        logger.warning(
            "failed to fetch Codeforces contests",
            exc_info=(type(cf_result), cf_result, cf_result.__traceback__),
        )
    else:
        success_count += 1
        contests.extend(cf_result)

    if isinstance(atc_result, Exception):
        logger.warning(
            "failed to fetch AtCoder contests",
            exc_info=(type(atc_result), atc_result, atc_result.__traceback__),
        )
    else:
        success_count += 1
        contests.extend(atc_result)

    if success_count == 0:
        raise ContestServiceError("all contest sources failed")

    now = get_now()
    contests = [contest for contest in contests if contest["start_time"] > now]
    contests.sort(key=lambda item: item["start_time"])
    return contests


async def get_recent_contests(limit: int = 5) -> list[dict]:
    """Return the nearest upcoming contests from all sources."""
    contests = await get_all_upcoming_contests()
    return contests[:limit]


def get_contests_in_window(
    contests: list[dict],
    *,
    upper_minutes: int,
    lower_minutes_exclusive: int,
    now: datetime | None = None,
) -> list[dict]:
    """Return contests whose start time falls inside a reminder window."""
    current = now or get_now()
    matched: list[dict] = []
    for contest in contests:
        minutes_until = (contest["start_time"] - current).total_seconds() / 60
        if lower_minutes_exclusive < minutes_until <= upper_minutes:
            matched.append(contest)
    matched.sort(key=lambda item: item["start_time"])
    return matched


def _format_contest_block(
    contest: dict,
    *,
    index: int,
    include_countdown: bool,
    now: datetime | None = None,
) -> list[str]:
    lines = [
        f"{index}. [{get_platform_label(contest['platform'])}] {contest['title']}",
        f"开始：{format_contest_start_time(contest['start_time'])}",
        f"时长：{format_duration_minutes(contest['duration_minutes'])}",
    ]
    if include_countdown:
        lines.append(f"距离开始：{format_time_until(contest['start_time'], now=now)}")
    lines.append(f"链接：{contest['url']}")
    return lines


def format_contest_list(
    contests: list[dict],
    *,
    title: str = "近期比赛：",
    include_countdown: bool = True,
    now: datetime | None = None,
) -> str:
    """Format a contest list for commands or reminders."""
    if not contests:
        return "暂无即将开始的比赛。"

    lines = [title]
    for index, contest in enumerate(contests, start=1):
        lines.extend(["", *_format_contest_block(
            contest,
            index=index,
            include_countdown=include_countdown,
            now=now,
        )])
    return "\n".join(lines)


def format_contest_reminder(title: str, contests: list[dict]) -> str:
    """Format reminder text without countdown lines."""
    return format_contest_list(
        contests,
        title=title,
        include_countdown=False,
    )


__all__ = [
    "AtCoderDataError",
    "AtCoderRequestError",
    "CodeforcesApiError",
    "CodeforcesDataError",
    "CodeforcesRequestError",
    "ContestServiceError",
    "SHANGHAI_TZ",
    "format_contest_list",
    "format_contest_reminder",
    "format_duration_minutes",
    "format_time_until",
    "get_all_upcoming_contests",
    "get_contests_in_window",
    "get_now",
    "get_recent_contests",
]

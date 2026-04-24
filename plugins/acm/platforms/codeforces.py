"""Codeforces API client."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx


CODEFORCES_USER_INFO_URL = "https://codeforces.com/api/user.info"
CODEFORCES_CONTEST_LIST_URL = "https://codeforces.com/api/contest.list"
REQUEST_TIMEOUT_SECONDS = 10.0
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class CodeforcesError(Exception):
    """Base class for Codeforces query errors."""


class CodeforcesRequestError(CodeforcesError):
    """Raised when the HTTP request fails."""


class CodeforcesApiError(CodeforcesError):
    """Raised when Codeforces returns a non-OK API status."""


class CodeforcesDataError(CodeforcesError):
    """Raised when Codeforces returns unexpected data."""


class UserNotFoundError(CodeforcesApiError):
    """Raised when a Codeforces handle does not exist."""


async def _get_codeforces_payload(url: str, params: dict | None = None) -> dict:
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError:
        raise CodeforcesRequestError("Codeforces returned bad HTTP status") from None
    except (httpx.TimeoutException, httpx.RequestError):
        raise CodeforcesRequestError("Codeforces request failed") from None
    except ValueError:
        raise CodeforcesRequestError("Codeforces returned invalid JSON") from None


async def fetch_user_info(handle: str) -> dict:
    """Fetch one Codeforces user's basic information."""
    handle = handle.strip()
    if not handle:
        raise UserNotFoundError("empty handle")

    payload = await _get_codeforces_payload(
        CODEFORCES_USER_INFO_URL,
        params={"handles": handle},
    )

    if payload.get("status") != "OK":
        comment = str(payload.get("comment", ""))
        if "not found" in comment.lower():
            raise UserNotFoundError(comment)
        raise CodeforcesApiError(comment or "Codeforces returned non-OK status")

    result = payload.get("result")
    if not isinstance(result, list) or not result:
        raise UserNotFoundError("empty user result")

    user = result[0]
    if not isinstance(user, dict):
        raise CodeforcesApiError("invalid user result")

    return user


def _build_contest_url(contest_id: str) -> str:
    return f"https://codeforces.com/contest/{contest_id}"


async def get_upcoming_contests(limit: int | None = None) -> list[dict]:
    """Fetch upcoming Codeforces contests as unified contest objects."""
    payload = await _get_codeforces_payload(CODEFORCES_CONTEST_LIST_URL)

    if payload.get("status") != "OK":
        raise CodeforcesApiError("Codeforces returned non-OK status")

    result = payload.get("result")
    if not isinstance(result, list):
        raise CodeforcesDataError("contest result is not a list")

    contests: list[dict] = []
    for contest in result:
        if not isinstance(contest, dict):
            continue
        if contest.get("phase") != "BEFORE":
            continue

        contest_id = contest.get("id")
        title = contest.get("name")
        start_time_seconds = contest.get("startTimeSeconds")
        duration_seconds = contest.get("durationSeconds")

        if contest_id is None or not title:
            continue
        if not isinstance(start_time_seconds, int) or not isinstance(
            duration_seconds, int
        ):
            continue

        start_time = datetime.fromtimestamp(
            start_time_seconds, tz=timezone.utc
        ).astimezone(SHANGHAI_TZ)

        contests.append(
            {
                "platform": "cf",
                "contest_id": str(contest_id),
                "title": str(title),
                "start_time": start_time,
                "duration_minutes": max(0, duration_seconds // 60),
                "url": _build_contest_url(str(contest_id)),
            }
        )

    contests.sort(key=lambda item: item["start_time"])
    if limit is not None:
        return contests[:limit]
    return contests

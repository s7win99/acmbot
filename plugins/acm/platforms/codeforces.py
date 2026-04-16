"""Codeforces API client."""

from __future__ import annotations

import httpx


CODEFORCES_USER_INFO_URL = "https://codeforces.com/api/user.info"
CODEFORCES_CONTEST_LIST_URL = "https://codeforces.com/api/contest.list"
REQUEST_TIMEOUT_SECONDS = 10.0


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
            return response.json()
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


async def get_upcoming_contests(limit: int = 5) -> list[dict]:
    """Fetch upcoming Codeforces contests."""
    print("[Codeforces] fetching upcoming contests")
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

        start_time = contest.get("startTimeSeconds")
        duration = contest.get("durationSeconds")
        if not isinstance(start_time, int) or not isinstance(duration, int):
            continue

        contests.append(
            {
                "id": contest.get("id"),
                "name": contest.get("name") or "未命名比赛",
                "phase": contest.get("phase"),
                "durationSeconds": duration,
                "startTimeSeconds": start_time,
            }
        )

    contests.sort(key=lambda item: item["startTimeSeconds"])
    return contests[:limit]

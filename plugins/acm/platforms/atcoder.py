"""AtCoder upcoming contest fetcher."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)
ATCODER_CONTESTS_URL = "https://atcoder.jp/contests/"
REQUEST_TIMEOUT_SECONDS = 10.0
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class AtCoderError(Exception):
    """Base class for AtCoder contest query errors."""


class AtCoderRequestError(AtCoderError):
    """Raised when the AtCoder request fails."""


class AtCoderDataError(AtCoderError):
    """Raised when the AtCoder page structure is unexpected."""


async def _get_atcoder_page() -> str:
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(ATCODER_CONTESTS_URL)
            response.raise_for_status()
            return response.text
    except httpx.HTTPStatusError:
        raise AtCoderRequestError("AtCoder returned bad HTTP status") from None
    except (httpx.TimeoutException, httpx.RequestError):
        raise AtCoderRequestError("AtCoder request failed") from None


def _parse_duration_minutes(duration_text: str) -> int:
    value = duration_text.strip()
    match = re.fullmatch(r"(\d+):(\d{2})", value)
    if not match:
        raise AtCoderDataError(f"invalid AtCoder duration: {duration_text}")
    hours, minutes = map(int, match.groups())
    return hours * 60 + minutes


def _find_upcoming_rows(soup: BeautifulSoup):
    container = soup.find(id="contest-table-upcoming")
    if container is not None:
        body = container.find("tbody")
        if body is not None:
            rows = body.find_all("tr")
            if rows:
                return rows

    for heading in soup.find_all(["h3", "h4"]):
        title = heading.get_text(" ", strip=True).lower()
        if "upcoming contests" not in title:
            continue
        table = heading.find_next("table")
        if table is None:
            continue
        body = table.find("tbody")
        if body is None:
            continue
        rows = body.find_all("tr")
        if rows:
            return rows

    raise AtCoderDataError("failed to find AtCoder Upcoming Contests table")


async def get_upcoming_contests(limit: int | None = None) -> list[dict]:
    """Fetch upcoming AtCoder contests as unified contest objects."""
    html = await _get_atcoder_page()
    soup = BeautifulSoup(html, "html.parser")
    rows = _find_upcoming_rows(soup)

    contests: list[dict] = []
    now = datetime.now(SHANGHAI_TZ)

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        time_link = cells[0].find("a")
        contest_link = cells[1].find("a", href=True)
        duration_text = cells[2].get_text(" ", strip=True)

        if time_link is None or contest_link is None:
            continue

        start_text = time_link.get_text(" ", strip=True)
        title = contest_link.get_text(" ", strip=True)
        href = str(contest_link.get("href") or "").strip()
        if not start_text or not title or not href:
            continue

        try:
            start_time = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S%z")
            start_time = start_time.astimezone(SHANGHAI_TZ)
            duration_minutes = _parse_duration_minutes(duration_text)
        except ValueError:
            logger.warning("failed to parse AtCoder start time: %s", start_text)
            continue
        except AtCoderDataError:
            logger.warning("failed to parse AtCoder duration: %s", duration_text)
            continue

        if start_time <= now:
            continue

        contest_slug = href.rstrip("/").split("/")[-1]
        contests.append(
            {
                "platform": "atc",
                "contest_id": contest_slug,
                "title": title,
                "start_time": start_time,
                "duration_minutes": duration_minutes,
                "url": f"https://atcoder.jp{href}",
            }
        )

    contests.sort(key=lambda item: item["start_time"])
    if limit is not None:
        return contests[:limit]
    return contests

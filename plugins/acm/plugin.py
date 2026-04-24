"""ACM Bot NcatBot plugin."""

import logging
import re
import time
from pathlib import Path

from ncatbot.core import GroupMessageEvent, PrivateMessageEvent
from ncatbot.core.event.message_segment import File, Reply
from ncatbot.plugin_system import NcatBotPlugin
from ncatbot.plugin_system.event import NcatBotEvent
from ncatbot.utils import (
    OFFICIAL_GROUP_MESSAGE_EVENT,
    OFFICIAL_PRIVATE_MESSAGE_EVENT,
)

from .commands import (
    QratingImportAttachment,
    dispatch_command,
    handle_qrating_import,
    is_qrating_import_command,
    precheck_qrating_import_command,
)
from .services.contest_service import (
    ContestServiceError,
    format_contest_list,
    format_contest_reminder,
    get_all_upcoming_contests,
    get_contests_in_window,
    get_now,
)
from .storage.contest_reminder_store import ContestReminderStore


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TMP_IMPORT_DIR = PROJECT_ROOT / "data" / "tmp_imports"
TMP_IMPORT_MAX_AGE_SECONDS = 24 * 60 * 60
contest_reminder_store = ContestReminderStore()


class Plugin(NcatBotPlugin):
    """Minimal ACM command plugin."""

    name = "acm"
    version = "0.3.0"
    author = "ACM Bot"
    description = "ACM Bot basic commands and Codeforces query plugin"
    dependencies = {}

    async def on_load(self) -> None:
        """Register message handlers when the plugin is loaded."""
        self.register_handler(OFFICIAL_GROUP_MESSAGE_EVENT, self.handle_group_message)
        self.register_handler(
            OFFICIAL_PRIVATE_MESSAGE_EVENT, self.handle_private_message
        )
        self.add_scheduled_task(
            self._run_contest_reminder_checks,
            name="acm_contest_reminder_checks",
            interval="60s",
        )
        print("[ACM Bot] plugin loaded")

    async def handle_group_message(self, event: NcatBotEvent) -> None:
        """Handle group messages."""
        message_event: GroupMessageEvent = event.data
        await self._reply_if_command(message_event, source="group")

    async def handle_private_message(self, event: NcatBotEvent) -> None:
        """Handle private messages."""
        message_event: PrivateMessageEvent = event.data
        await self._reply_if_command(message_event, source="private")

    async def _reply_if_command(
        self, message_event: GroupMessageEvent | PrivateMessageEvent, source: str
    ) -> None:
        message = self._get_message_text(message_event)
        user_id = str(message_event.user_id) if message_event.user_id else None
        if is_qrating_import_command(message):
            precheck_reply = precheck_qrating_import_command(message, user_id)
            if precheck_reply is not None:
                reply_text = precheck_reply
            else:
                attachment = await self._prepare_qrating_import_attachment(message_event)
                reply_text = handle_qrating_import(
                    message, user_id=user_id, attachment=attachment
                )
        else:
            reply_text = await dispatch_command(
                message,
                user_id=user_id,
                is_group=isinstance(message_event, GroupMessageEvent),
                group_id=(
                    str(message_event.group_id)
                    if isinstance(message_event, GroupMessageEvent)
                    and getattr(message_event, "group_id", None)
                    else None
                ),
            )

        if reply_text is None:
            return

        print(f"[ACM Bot] {source} command: {message}")

        if isinstance(message_event, GroupMessageEvent):
            await message_event.reply(reply_text, at=False, space=False)
        else:
            await message_event.reply(reply_text)

    def _get_message_text(
        self, message_event: GroupMessageEvent | PrivateMessageEvent
    ) -> str:
        text = ""
        try:
            text = message_event.message.concatenate_text()
        except Exception:
            logger.debug("failed to concatenate message text", exc_info=True)
        return (text or message_event.raw_message or "").strip()

    def _get_reply_message_id(
        self, message_event: GroupMessageEvent | PrivateMessageEvent
    ) -> str | None:
        reply_segments = message_event.message.filter(Reply)
        if reply_segments:
            return str(reply_segments[0].id)

        raw_message = message_event.raw_message or ""
        match = re.search(r"\[CQ:reply,[^\]]*id=([^,\]]+)", raw_message)
        if match:
            return match.group(1).strip()

        try:
            for segment in message_event.message.to_list():
                if segment.get("type") == "reply":
                    reply_id = segment.get("data", {}).get("id")
                    if reply_id:
                        return str(reply_id)
        except Exception:
            logger.debug("failed to inspect reply segment dict", exc_info=True)
        return None

    async def _prepare_qrating_import_attachment(
        self, message_event: GroupMessageEvent | PrivateMessageEvent
    ) -> QratingImportAttachment:
        """Find and download the replied xlsx file for /qrating import."""
        reply_message_id = self._get_reply_message_id(message_event)
        if not reply_message_id:
            return QratingImportAttachment(status="not_reply")

        try:
            replied_message = await self.api.get_msg(reply_message_id)
        except Exception:
            logger.exception("failed to fetch replied message for qrating import")
            return QratingImportAttachment(
                status="download_failed",
                detail="无法获取被回复消息，请稍后重试或重新发送文件。",
            )

        file_segments = replied_message.message.filter(File)
        if not file_segments:
            return QratingImportAttachment(status="no_file")

        xlsx_file = None
        for file_segment in file_segments:
            if self._get_attachment_name(file_segment).lower().endswith(".xlsx"):
                xlsx_file = file_segment
                break
        if xlsx_file is None:
            return QratingImportAttachment(status="not_xlsx")

        try:
            TMP_IMPORT_DIR.mkdir(parents=True, exist_ok=True)
            self._cleanup_old_import_files()
            await self._ensure_download_url(xlsx_file, message_event, replied_message)
            safe_name = self._safe_tmp_file_name(
                self._get_attachment_name(xlsx_file), message_event.message_id
            )
            downloaded_path = await xlsx_file.download_to(str(TMP_IMPORT_DIR), safe_name)
        except Exception:
            logger.exception("failed to download qrating import xlsx")
            return QratingImportAttachment(
                status="download_failed",
                detail="请稍后重试或重新发送文件。",
            )

        return QratingImportAttachment(status="ok", path=Path(downloaded_path))

    async def _run_contest_reminder_checks(self) -> None:
        """Run daily overview and pre-contest reminders for enabled groups."""
        enabled_groups = contest_reminder_store.get_enabled_groups()
        if not enabled_groups:
            return

        try:
            contests = await get_all_upcoming_contests()
        except ContestServiceError:
            logger.warning("contest reminder skipped because all contest sources failed")
            return
        except Exception:
            logger.exception("contest reminder task failed to fetch contests")
            return

        now = get_now()
        recent_contests = contests[:5]

        for group in enabled_groups:
            group_id = str(group["group_id"])
            try:
                await self._maybe_send_daily_digest(group_id, group, recent_contests, now)
                await self._maybe_send_window_reminder(
                    group_id,
                    contests,
                    now,
                    upper_minutes=60,
                    lower_minutes_exclusive=50,
                    remind_type="before_60",
                    title="比赛提醒（1 小时后开始）",
                )
                await self._maybe_send_window_reminder(
                    group_id,
                    contests,
                    now,
                    upper_minutes=5,
                    lower_minutes_exclusive=0,
                    remind_type="before_5",
                    title="比赛提醒（5 分钟后开始）",
                )
            except Exception:
                logger.exception("contest reminder task failed for group %s", group_id)

    async def _maybe_send_daily_digest(
        self,
        group_id: str,
        group_config: dict,
        recent_contests: list[dict],
        now,
    ) -> None:
        if not recent_contests:
            return

        daily_hour = int(group_config.get("daily_hour", 9))
        daily_minute = int(group_config.get("daily_minute", 0))
        if not (
            now.hour == daily_hour
            and daily_minute <= now.minute < daily_minute + 5
        ):
            return

        daily_key = f"daily_{now.date().isoformat()}"
        if contest_reminder_store.has_record(group_id, "all", daily_key, daily_key):
            return

        message = format_contest_list(recent_contests, title="每日比赛速览：", now=now)
        if await self._send_group_text(group_id, message):
            contest_reminder_store.add_record(group_id, "all", daily_key, daily_key)

    async def _maybe_send_window_reminder(
        self,
        group_id: str,
        contests: list[dict],
        now,
        *,
        upper_minutes: int,
        lower_minutes_exclusive: int,
        remind_type: str,
        title: str,
    ) -> None:
        window_contests = get_contests_in_window(
            contests,
            upper_minutes=upper_minutes,
            lower_minutes_exclusive=lower_minutes_exclusive,
            now=now,
        )
        if not window_contests:
            return

        unsent_contests: list[dict] = []
        pending_records: list[tuple[str, str, str, str]] = []
        for contest in window_contests:
            if contest_reminder_store.has_record(
                group_id,
                contest["platform"],
                contest["contest_id"],
                remind_type,
            ):
                continue
            unsent_contests.append(contest)
            pending_records.append(
                (
                    group_id,
                    contest["platform"],
                    contest["contest_id"],
                    remind_type,
                )
            )

        if not unsent_contests:
            return

        message = format_contest_reminder(title, unsent_contests)
        if await self._send_group_text(group_id, message):
            contest_reminder_store.add_records(pending_records)

    async def _send_group_text(self, group_id: str, message: str) -> bool:
        try:
            await self.api.send_group_plain_text(group_id, message)
            return True
        except Exception:
            logger.exception("failed to send contest reminder to group %s", group_id)
            return False

    async def _ensure_download_url(
        self,
        file_segment: File,
        message_event: GroupMessageEvent | PrivateMessageEvent,
        replied_message,
    ) -> None:
        """Fill a file segment URL when NapCat only provides a file_id."""
        if getattr(file_segment, "url", None):
            return

        file_id = getattr(file_segment, "file_id", None) or getattr(
            file_segment, "file", None
        )
        if not file_id:
            return

        if isinstance(message_event, GroupMessageEvent):
            group_id = (
                getattr(replied_message, "group_id", None) or message_event.group_id
            )
            url = await self.api.get_group_file_url(group_id, file_id)
        else:
            url = await self.api.get_private_file_url(file_id)

        if url:
            file_segment.url = url

    def _get_attachment_name(self, file_segment: File) -> str:
        name = getattr(file_segment, "file_name", None) or file_segment.get_file_name()
        return str(name or "")

    def _safe_tmp_file_name(self, original_name: str, message_id: str) -> str:
        suffix = Path(original_name).suffix or ".xlsx"
        stem = Path(original_name).stem or "vjudge"
        safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._") or "vjudge"
        return f"{int(time.time())}_{message_id}_{safe_stem}{suffix}"

    def _cleanup_old_import_files(self) -> None:
        now = time.time()
        for path in TMP_IMPORT_DIR.glob("*"):
            try:
                if (
                    path.is_file()
                    and now - path.stat().st_mtime > TMP_IMPORT_MAX_AGE_SECONDS
                ):
                    path.unlink()
            except OSError:
                logger.warning("failed to remove old import temp file: %s", path)

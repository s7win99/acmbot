"""ACM Bot NcatBot plugin."""

from ncatbot.core import GroupMessageEvent, PrivateMessageEvent
from ncatbot.plugin_system import NcatBotPlugin
from ncatbot.plugin_system.event import NcatBotEvent
from ncatbot.utils import (
    OFFICIAL_GROUP_MESSAGE_EVENT,
    OFFICIAL_PRIVATE_MESSAGE_EVENT,
)

from .commands import dispatch_command


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
        message = (message_event.raw_message or "").strip()
        user_id = str(message_event.user_id) if message_event.user_id else None
        reply_text = await dispatch_command(message, user_id=user_id)

        if reply_text is None:
            return

        print(f"[ACM Bot] {source} command: {message}")

        if isinstance(message_event, GroupMessageEvent):
            await message_event.reply(reply_text, at=False, space=False)
        else:
            await message_event.reply(reply_text)

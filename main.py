"""ACM Bot entry point."""

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
os.environ.setdefault("NCATBOT_CONFIG_PATH", str(PROJECT_ROOT / "config.yaml"))

from ncatbot.core import BotClient  # noqa: E402


def main() -> None:
    """Start NcatBot and load configured plugins."""
    print("[ACM Bot] starting")
    bot = BotClient()
    bot.run()


if __name__ == "__main__":
    main()

import os
import subprocess
import requests
import logging

logger = logging.getLogger(__name__)


def _tmux_fmt(fmt: str, target: str | None = None) -> str:
    cmd = ["tmux", "display-message", "-p"]
    if target:
        cmd += ["-t", target]
    cmd.append(fmt)
    return subprocess.check_output(cmd, text=True).strip()


def is_tmux_focused() -> bool:
    """
    True if this process is in tmux AND its pane is the active pane in the active window.
    False if not in tmux, or pane/window not active, or tmux not reachable.
    """
    pane = os.environ.get("TMUX_PANE")
    if not pane:
        return False  # not running inside tmux

    try:
        # Both are documented format vars.
        pane_active = _tmux_fmt("#{pane_active}", target=pane) == "1"
        window_active = _tmux_fmt("#{window_active}", target=pane) == "1"
        return pane_active and window_active
    except Exception:
        return False


ZULIP_ADDR = "https://chat.dm.informatik.tu-darmstadt.de/api/v1/messages"
ZULIP_EMAIL = "bespoke-bot@chat.dm.informatik.tu-darmstadt.de"
ZULIP_API_KEY = "Rg2M2jvVLuFJ5tRqOIuVBHQ9tprfQPrn"

TO_USER = "matthias.jasny@cs.tu-darmstadt.de"
TO_CHNL = "project/Oktopus"
TO_TOPIC = "agent_alerts"


class ZulipBot:
    def __init__(self, email, api_key):
        self.email = email
        self.api_key = api_key
        self.url = ZULIP_ADDR

    def send_to_user(self, to, msg):
        r = requests.post(
            self.url,
            auth=(self.email, self.api_key),
            data={"type": "private", "to": to, "content": msg},
        )
        assert r.json()["result"] == "success"

    def send_to_stream(self, to, topic, msg):
        r = requests.post(
            self.url,
            auth=(self.email, self.api_key),
            data={"type": "stream", "to": to, "topic": topic, "content": msg},
        )
        assert r.json()["result"] == "success"


def send_notification(msg, check_tmux=False):
    if check_tmux and is_tmux_focused():
        logger.info("No notification, tmux pane is focused")
        return
    bot = ZulipBot(ZULIP_EMAIL, ZULIP_API_KEY)
    # bot.send_to_user(TO_USER, msg=msg)
    bot.send_to_stream(TO_CHNL, topic=TO_TOPIC, msg=msg)

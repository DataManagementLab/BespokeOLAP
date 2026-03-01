import os
import signal
import time
import logging

logger = logging.getLogger(__name__)


class ProcTreeTimeoutKiller:
    def __init__(self, root_pid: int, timeout: int):
        self.root_pid = root_pid
        self.timeout = timeout
        self.start = time.monotonic()
        self.killed = False

    def expired(self) -> bool:
        if self.timeout <= 0:
            return False
        return (time.monotonic() - self.start) >= self.timeout

    def enforce(self) -> None:
        """
        Kill the rightmost descendant exactly once when timeout expires.
        """
        if self.killed or not self.expired():
            return

        victim = self._rightmost_descendant(self.root_pid)
        # avoid killing root proc with victim != self.root_pid
        logger.warning(f"Timeout, killing {victim}")
        self._kill(victim)

        self.killed = True

    def _children(self, pid: int) -> list[int]:
        path = f"/proc/{pid}/task/{pid}/children"
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = f.read().strip()
        except OSError:
            return []

        if not data:
            return []

        out: list[int] = []
        for part in data.split():
            try:
                out.append(int(part))
            except ValueError:
                pass
        return out

    def _rightmost_descendant(self, pid: int) -> int:
        cur = pid
        while True:
            kids = self._children(cur)
            if not kids:
                return cur
            cur = kids[-1]  # "most right"

    def _kill(self, pid: int) -> None:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

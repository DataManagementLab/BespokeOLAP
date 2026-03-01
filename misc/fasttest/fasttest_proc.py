import logging
import os
import subprocess
import shlex
from pathlib import Path
import select
from misc.fasttest.proc_utils import ProcTreeTimeoutKiller


logger = logging.getLogger(__name__)


class FasttestProc:
    def __init__(
        self,
        command: str,
        *,
        echo_output: bool = False,
        cwd: Path,
    ) -> None:
        self._command = command
        self._echo_output = echo_output
        self._cwd = cwd
        self._proc: subprocess.Popen[bytes] | None = None
        self._p2c_w: int | None = None
        self._c2p_file = None
        self._c2p_r: int | None = None
        self._stdout_fd: int | None = None
        self._stderr_fd: int | None = None
        self._stdin = None

    def _start(self) -> None:
        if self._proc is not None:
            return
        p2c_r, p2c_w = os.pipe()
        c2p_r, c2p_w = os.pipe()

        if isinstance(self._command, str):
            cmd = self._command.strip()
            cmd = cmd if cmd else "./db"
            argv = shlex.split(cmd)
            if not argv:
                argv = ["./db"]
        else:
            argv = [str(self._command)]
        self._proc = subprocess.Popen(
            argv,
            pass_fds=(p2c_r, c2p_w),
            env={
                **os.environ,
                "P2C_FD": str(p2c_r),
                "C2P_FD": str(c2p_w),
            },
            cwd=self._cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
        )

        os.close(p2c_r)
        os.close(c2p_w)

        self._p2c_w = p2c_w
        self._c2p_r = c2p_r
        os.set_blocking(c2p_r, False)
        self._c2p_file = os.fdopen(c2p_r, "rb", buffering=0)
        self._stdin = self._proc.stdin
        if self._proc.stdout is not None:
            self._stdout_fd = self._proc.stdout.fileno()
            os.set_blocking(self._stdout_fd, False)
        if self._proc.stderr is not None:
            self._stderr_fd = self._proc.stderr.fileno()
            os.set_blocking(self._stderr_fd, False)

    def run(self, timeout: int = 0) -> tuple[str, str, str]:
        self._start()
        if self._p2c_w is None or self._c2p_file is None or self._c2p_r is None:
            raise RuntimeError("runner not initialized")
        os.write(self._p2c_w, b"run\n")

        out_buf = bytearray()
        err_buf = bytearray()
        resp_buf = bytearray()

        killer = (
            ProcTreeTimeoutKiller(self._proc.pid, timeout)
            if timeout > 0 and self._proc is not None
            else None
        )

        while True:
            fds = [self._c2p_r]
            if self._stdout_fd is not None:
                fds.append(self._stdout_fd)
            if self._stderr_fd is not None:
                fds.append(self._stderr_fd)

            select_timeout = 1.0 if killer is not None else None
            rlist, _, _ = select.select(fds, [], [], select_timeout)

            if killer is not None:
                killer.enforce()

            for fd in rlist:
                if fd == self._c2p_r:
                    chunk = os.read(fd, 4096)
                    if not chunk:
                        rc = self._proc.wait() if self._proc is not None else None
                        if rc is not None:
                            err_buf.extend(
                                f"process exited with code {rc}\n".encode("utf-8")
                            )
                        while self._stdout_fd is not None:
                            more = os.read(self._stdout_fd, 4096)
                            if not more:
                                break
                            out_buf.extend(more)
                        while self._stderr_fd is not None:
                            more = os.read(self._stderr_fd, 4096)
                            if not more:
                                break
                            err_buf.extend(more)
                        response = resp_buf.decode("utf-8", errors="replace")
                        out = out_buf.decode("utf-8", errors="replace")
                        err = err_buf.decode("utf-8", errors="replace")
                        if killer is not None and killer.killed:
                            response = f"{response}\nTerminated after {timeout} seconds due to timeout."
                        return response, out, err
                    resp_buf.extend(chunk)
                elif fd == self._stdout_fd:
                    chunk = os.read(fd, 4096)
                    if chunk:
                        out_buf.extend(chunk)
                        if self._echo_output:
                            os.write(1, chunk)
                elif fd == self._stderr_fd:
                    chunk = os.read(fd, 4096)
                    if chunk:
                        err_buf.extend(chunk)
                        if self._echo_output:
                            os.write(2, chunk)
            if b"\n" in resp_buf:
                line, _, rest = resp_buf.partition(b"\n")
                response = line.decode("utf-8", errors="replace")
                out = out_buf.decode("utf-8", errors="replace")
                err = err_buf.decode("utf-8", errors="replace")
                if killer is not None and killer.killed:
                    response = f"{response}\nTerminated after {timeout} seconds due to timeout."
                return response, out, err

    def send(self, line: str) -> None:
        self._start()
        if self._stdin is None:
            raise RuntimeError("stdin not available")
        self._stdin.write((line + "\n").encode("utf-8"))
        self._stdin.flush()

    def close_stdin(self) -> None:
        if self._stdin is not None:
            self._stdin.close()
            self._stdin = None

    def terminate(self) -> None:
        if self._proc is None:
            return
        if self._stdin is not None:
            try:
                self._stdin.write(b"stop\n")
                self._stdin.flush()
            except Exception:
                pass
        if self._p2c_w is not None:
            try:
                os.close(self._p2c_w)
            except OSError:
                pass
            self._p2c_w = None
        if self._c2p_file is not None:
            try:
                self._c2p_file.close()
            except Exception:
                pass
            self._c2p_file = None
        if self._c2p_r is not None:
            self._c2p_r = None
        if self._stdin is not None:
            try:
                self._stdin.close()
            except Exception:
                pass
            self._stdin = None
        self._proc.wait()
        if self._proc.returncode not in (0, None):
            raise RuntimeError(f"process exited with code {self._proc.returncode}")
        self._proc = None

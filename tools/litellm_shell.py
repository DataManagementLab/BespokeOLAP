import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Optional

from agents.run_context import RunContextWrapper
from agents.tool import FunctionTool
from pydantic import BaseModel, Field

from llm_cache import utils
from llm_cache.git_snapshotter import GitSnapshotter
from tools.sandbox import SandboxConfig, sandbox_shell_async
from utils.wandb_stats_logging import WandbRunHook

logger = logging.getLogger(__name__)


class LitellmShellTool:
    def __init__(
        self,
        cwd: Path,
        cache_dir: Path,
        git_snapshotter: Optional[GitSnapshotter] = None,
        wandb_metrics_hook: WandbRunHook | None = None,
    ) -> None:
        self.cwd = cwd
        self.cache_dir = cache_dir
        self.git_snapshotter = git_snapshotter
        self.wandb_metrics_hook = wandb_metrics_hook
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_dir.chmod(0o777)

    def _cache_path_for(self, hash: str) -> Path:
        return self.cache_dir / f"{hash}.pkl"

    async def __call__(self, command: str, timeout_ms: int | None) -> str:
        if "sudo" in command:
            raise RuntimeError("sudo rejected")
        logger.debug(f"Running shell command: {command}")

        payload = {
            "snapshotter_hash": self.git_snapshotter.current_hash
            if self.git_snapshotter
            else None,
            "command": command,
            "timeout_ms": timeout_ms,
        }
        hash = utils.sha256(utils.stable_json(payload))
        path = self._cache_path_for(hash)

        if path.exists():
            cached = utils.load_pickle(path, str)
            if cached is not None:
                return cached

        cfg = SandboxConfig(
            writable_roots=[str(self.cwd), "/tmp"],
            cwd=str(self.cwd),
            nproc=None,
        )
        proc = await sandbox_shell_async(
            command,
            cfg=cfg,
            env=os.environ.copy(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        timed_out = False
        try:
            timeout = (timeout_ms or 0) / 1000 or None
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            stdout_bytes, stderr_bytes = await proc.communicate()
            timed_out = True

        stdout = stdout_bytes.decode("utf-8", errors="ignore")
        stderr = stderr_bytes.decode("utf-8", errors="ignore")
        exit_code = getattr(proc, "returncode", None)

        output = (
            f"$ {command}\n"
            f"stdout: {stdout[:200]}\n"
            f"stderr: {stderr[:200]}\n"
            f"exit_code: {exit_code}\n"
            f"status: {'timeout' if timed_out else 'exit'}"
        )

        utils.dump_pickle(path, output)

        if self.wandb_metrics_hook is not None:
            self.wandb_metrics_hook.log_metrics_callback(
                {
                    "type": "shell_command",
                    "shell/num_commands": 1,
                    "shell/commands": [command[:20]],
                },
                log_and_increment=True,
            )

        return output


class LitellmShellArgs(BaseModel):
    command: str = Field(..., description="Shell command to execute")
    timeout_ms: int | None = Field(
        None, description="Timeout in milliseconds (optional)"
    )


def make_litellm_shell_tool(
    cwd: Path,
    cache_dir: Path,
    git_snapshotter: Optional[GitSnapshotter] = None,
    wandb_metrics_hook: WandbRunHook | None = None,
) -> FunctionTool:
    impl = LitellmShellTool(
        cwd=cwd,
        cache_dir=cache_dir,
        git_snapshotter=git_snapshotter,
        wandb_metrics_hook=wandb_metrics_hook,
    )

    async def on_invoke(ctx: RunContextWrapper[Any], args_json: str) -> str:
        args = LitellmShellArgs.model_validate_json(args_json)
        return await impl(command=args.command, timeout_ms=args.timeout_ms)

    return FunctionTool(
        name="shell",
        description="Runs a shell command locally",
        params_json_schema=LitellmShellArgs.model_json_schema(),
        on_invoke_tool=on_invoke,
    )

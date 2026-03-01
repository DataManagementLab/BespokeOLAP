import asyncio
import logging
import os
from collections.abc import Sequence
from pathlib import Path

from agents import (
    ShellCallOutcome,
    ShellCommandOutput,
    ShellCommandRequest,
    ShellResult,
    custom_span,
)

from llm_cache import utils
from llm_cache.git_snapshotter import GitSnapshotter
from tools.sandbox import SandboxConfig, sandbox_shell_async
from utils.wandb_stats_logging import WandbRunHook

logger = logging.getLogger(__name__)


class ShellCacheType:
    def __init__(self, outputs: list[ShellCommandOutput]):
        self.outputs = outputs


class ShellExecutor:
    """Executes shell commands with optional approval."""

    def __init__(
        self,
        cwd: Path,
        snapshotter: GitSnapshotter,
        cache_dir: Path,
        wandb_metrics_hook: WandbRunHook | None,
    ) -> None:
        self.cwd = cwd
        self.snapshotter = snapshotter
        self.cache_dir = cache_dir
        self.wandb_metrics_hook = wandb_metrics_hook

        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_dir.chmod(0o777)

    def _cache_path_for(self, hash: str) -> Path:
        return self.cache_dir / f"{hash}.pkl"

    async def _get_outputs(
        self, request: ShellCommandRequest
    ) -> list[ShellCommandOutput]:
        action = request.data.action
        await self.require_approval(action.commands)

        outputs: list[ShellCommandOutput] = []
        for command in action.commands:
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
                timeout = (action.timeout_ms or 0) / 1000 or None
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout
                )
            except asyncio.TimeoutError:
                proc.kill()
                stdout_bytes, stderr_bytes = await proc.communicate()
                timed_out = True

            stdout = stdout_bytes.decode("utf-8", errors="ignore")
            stderr = stderr_bytes.decode("utf-8", errors="ignore")
            outputs.append(
                ShellCommandOutput(
                    command=command,
                    stdout=stdout,
                    stderr=stderr,
                    outcome=ShellCallOutcome(
                        type="timeout" if timed_out else "exit",
                        exit_code=getattr(proc, "returncode", None),
                    ),
                )
            )

            if timed_out:
                break

        return outputs

    async def __call__(self, request: ShellCommandRequest) -> ShellResult:
        payload = {
            "snapshotter_hash": self.snapshotter.current_hash,
            "commands": request.data.action.commands,
            # These are different per user!
            # "cwd": str(self.cwd),
            # "env": os.environ.copy(),
        }
        hash = utils.sha256(utils.stable_json(payload))
        path = self._cache_path_for(hash)

        abbr = request.data.action.commands[0][:30] + (
            "..." if len(request.data.action.commands[0]) > 30 else ""
        )

        shorted_cmds = "\n".join([c[:100] for c in request.data.action.commands])
        with custom_span(f'shell command ("{abbr}")', {"commands": shorted_cmds}):
            if path.exists():
                cached = utils.load_pickle(path, ShellCacheType)
                outputs = cached.outputs  # type: ignore
                logger.debug(
                    f"Read shell output for ({abbr}) from cache: {os.path.basename(path)}"
                )
            else:
                outputs = await self._get_outputs(request)
                utils.dump_pickle(path, ShellCacheType(outputs=outputs))

        # check if output is <500kb, if not return: "output too large to display"
        total_size = sum(len(out.stdout) + len(out.stderr) for out in outputs)
        max_len = 500 * 1024
        if total_size >= max_len:
            tmp_outputs = []
            for out in outputs:
                tmp_outputs.append(
                    ShellCommandOutput(
                        command=out.command,
                        stdout="output too large to display (>500kb)",
                        stderr="output too large to display (>500kb)",
                        outcome=out.outcome,
                    )
                )
            outputs = tmp_outputs

        output_str = "\n".join(
            f"$ {out.command}\nstdout: {out.stdout[:200]}\nstderr: {out.stderr[:200]}"
            for out in outputs
        )

        if self.wandb_metrics_hook is not None:
            log_cmd_list = [c[:20] for c in request.data.action.commands]
            self.wandb_metrics_hook.log_metrics_callback(
                {
                    "type": "shell_command",
                    "shell/num_commands": len(request.data.action.commands),
                    "shell/commands": log_cmd_list,
                },
                log_and_increment=True,
            )

        with custom_span(f'shell command result ("{abbr}")', {"outputs": output_str}):
            return ShellResult(
                output=outputs,
                provider_data={"working_directory": str(self.cwd)},
            )

    async def require_approval(self, commands: Sequence[str]) -> None:
        for entry in commands:
            lines = entry.splitlines()
            max_lines = 20
            if len(lines) > max_lines:
                # show only first 20 lines
                tmp_str = (
                    "\n".join(lines[:max_lines]) + f"\n... (total {len(lines)} lines)"
                )
                logger.debug(f"Running: \n {tmp_str}")
            else:
                logger.debug(f"Running: \n {entry}")
            if "sudo" in entry:
                raise RuntimeError("sudo rejected")

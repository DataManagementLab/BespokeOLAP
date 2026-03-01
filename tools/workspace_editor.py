import logging
from pathlib import Path

from agents import apply_diff, custom_span
from agents.editor import ApplyPatchOperation, ApplyPatchResult

from llm_cache.logger import PLAIN
from utils.wandb_stats_logging import WandbRunHook

logger = logging.getLogger(__name__)


def print_colored_diff(diff: str, is_create: bool = False) -> None:
    RED = "\033[31m"
    GREEN = "\033[32m"
    CYAN = "\033[36m"
    RESET = "\033[0m"

    lines = diff.splitlines()

    if is_create:
        max_lines = 20
        cutoff = len(lines) > max_lines
        lines = lines[:max_lines]  # show only first 20 lines for create
        if cutoff:
            lines.append("...")

    for line in lines:
        if line.startswith("+") and not line.startswith("+++"):
            logger.log(PLAIN, f"{GREEN}{line}{RESET}")
        elif line.startswith("-") and not line.startswith("---"):
            logger.log(PLAIN, f"{RED}{line}{RESET}")
        elif line.startswith("@@"):
            logger.log(PLAIN, f"{CYAN}{line}{RESET}")
        else:
            logger.log(PLAIN, line)


class WorkspaceEditor:
    def __init__(self, root: Path, wandb_metrics_hook: WandbRunHook | None) -> None:
        self._root = root.resolve()
        self._wandb_metrics_hook = wandb_metrics_hook

    def create_file(self, operation: ApplyPatchOperation) -> ApplyPatchResult:
        with custom_span(
            f"create file ({operation.path})",
            {
                "path": operation.path,
                "diff": operation.diff[:1000] if operation.diff else None,
            },
        ):
            relative = self._relative_path(operation.path)
            target = self._resolve(operation.path, ensure_parent=True)
            logger.info(f"Creating: {target}")
            diff = operation.diff or ""
            content = apply_diff("", diff, mode="create")
            print_colored_diff(diff, is_create=True)
            target.write_text(content, encoding="utf-8")

            if self._wandb_metrics_hook is not None:
                added, deleted = count_diff_operations(diff)
                assert deleted == 0, "Create operation should not have deleted lines"
                self._wandb_metrics_hook.log_apply_patch_stats(
                    "create", added_lines=added, deleted_lines=deleted
                )
            return ApplyPatchResult(output=f"Created {relative}")

    def update_file(self, operation: ApplyPatchOperation) -> ApplyPatchResult:
        with custom_span(
            f"update file ({operation.path})",
            {
                "file": operation.path,
                "diff": operation.diff[:1000] if operation.diff else None,
            },
        ):
            relative = self._relative_path(operation.path)
            target = self._resolve(operation.path)
            logger.info(f"Updating: {target}")
            original = target.read_text(encoding="utf-8")
            diff = operation.diff or ""
            print_colored_diff(diff)
            patched = apply_diff(original, diff)
            target.write_text(patched, encoding="utf-8")
            if self._wandb_metrics_hook is not None:
                added, deleted = count_diff_operations(diff)
                self._wandb_metrics_hook.log_apply_patch_stats(
                    "update", added_lines=added, deleted_lines=deleted
                )
            return ApplyPatchResult(output=f"Updated {relative}")

    def delete_file(self, operation: ApplyPatchOperation) -> ApplyPatchResult:
        with custom_span(f"delete file ({operation.path})", {"file": operation.path}):
            relative = self._relative_path(operation.path)
            target = self._resolve(operation.path)
            logger.info(f"Deleting: {target}")
            original = target.read_text(encoding="utf-8")
            target.unlink(missing_ok=True)
            if self._wandb_metrics_hook is not None:
                self._wandb_metrics_hook.log_apply_patch_stats(
                    "delete", added_lines=0, deleted_lines=len(original.splitlines())
                )
            return ApplyPatchResult(output=f"Deleted {relative}")

    def _relative_path(self, value: str) -> str:
        resolved = self._resolve(value)
        return resolved.relative_to(self._root).as_posix()

    def _resolve(self, relative: str, ensure_parent: bool = False) -> Path:
        candidate = Path(relative)
        target = candidate if candidate.is_absolute() else (self._root / candidate)
        target = target.resolve()
        # Only allow files directly in the root directory (no subdirectories)
        if target.parent != self._root:
            raise RuntimeError(
                f"Operation outside allowed root dir (no subdirs): {relative}"
            )
        try:
            target.relative_to(self._root)
        except ValueError:
            raise RuntimeError(f"Operation outside workspace: {relative}") from None
        if ensure_parent:
            target.parent.mkdir(parents=True, exist_ok=True)
        return target


def count_diff_operations(diff: str) -> tuple[int, int]:
    added = sum(
        1
        for line in diff.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    )
    deleted = sum(
        1
        for line in diff.splitlines()
        if line.startswith("-") and not line.startswith("---")
    )
    return added, deleted

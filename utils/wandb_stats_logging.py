import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional

from agents import RunContextWrapper, RunHooks, TContext, Tool

import wandb
from llm_cache.git_snapshotter import GitSnapshotter
from utils.cloc_utils import calculate_loc
from utils.token_usage import get_tokens_context_and_dollar_info

logger = logging.getLogger(__name__)


class WandbRunHook(RunHooks):
    """Hooks for tracking agent execution metrics to wandb"""

    logged_turn = -1
    apply_patch_added_ctr = 0
    apply_patch_deleted_ctr = 0

    def __init__(
        self,
        model,
        git_snapshotter: GitSnapshotter,
        prompt_idx: int = 0,
        disable: bool = False,
        cloc_cache_dir: Path | None = None,
    ):
        self.model = model
        self.git_snapshotter = git_snapshotter
        self.prompt_idx = prompt_idx  # will be externally set by conversation loop
        self.disable = disable
        self.current_prompt: Optional[str] = (
            None  # will be externally set by conversation loop
        )
        self.current_prompt_descriptor: Optional[str] = (
            None  # set externally by conversation loop
        )

        self.current_turn_tools = {}
        self.last_turn = 0  # Track last known turn for validation callback

        self.total_stats = defaultdict(int)

        self.total_type_counts = defaultdict(
            int
        )  # llm-call, handoff, (+ specific tool calls)

        # per tool stats
        self.apply_patch_stats = defaultdict(int)

        self.cloc_cache_dir = cloc_cache_dir
        if self.cloc_cache_dir is not None:
            self.cloc_cache_dir.mkdir(parents=True, exist_ok=True)

    # Callback for validation tool to report metrics
    def log_metrics_callback(
        self, metrics: dict, log_and_increment: bool = False
    ) -> None:
        """Callback for validation tool to report query-specific metrics to wandb"""
        if self.disable:
            return
        # Use the last known turn from hooks
        turn = self.last_turn

        assert self.logged_turn + 1 == turn, (
            f"Logged turn {self.logged_turn} is not one behind current turn {turn}"
        )
        self.logged_turn = turn

        metrics["turn"] = turn

        # Track total counts per type
        self.total_type_counts[metrics["type"]] += 1

        # assemble full action list
        action_names = [
            "llm_call",
            "apply_patch_tool",
            "handoff",
            "shell_command",
            "validate",
            "compaction",
        ]
        for a in self.total_type_counts.keys():
            if a not in action_names:
                action_names.append(a)

        # log total counts
        for action in action_names:
            action_str = action.replace("_", "")  # strip _ from type for action_str
            metrics[f"tool/{action_str}_count"] = self.total_type_counts[action]

        metrics["current_hash"] = self.git_snapshotter.current_hash
        assert self.git_snapshotter.current_hash is not None, (
            "Current hash should not be None"
        )
        metrics["current_loc"] = calculate_loc(
            self.cloc_cache_dir,
            self.git_snapshotter.current_hash,
            self.git_snapshotter.working_dir,
        )

        wandb.log(metrics, step=turn, commit=False)

        # increment turn
        assert log_and_increment, "log_and_increment must be True to increment turn"
        if log_and_increment:
            self.last_turn += 1

    def log_apply_patch_stats(
        self, operation_type: str, added_lines: int, deleted_lines: int
    ) -> None:
        """Log apply patch operation stats"""
        if self.disable:
            return
        self.apply_patch_stats[operation_type] += 1

        wandb.log(
            {
                f"apply_patch/{operation_type}_count": self.apply_patch_stats[
                    operation_type
                ]
            },
            step=self.last_turn,
            commit=False,  # avoid incrementing step / push too early
        )

        self.apply_patch_added_ctr += added_lines
        self.apply_patch_deleted_ctr += deleted_lines

    async def on_agent_start(self, ctx, agent):
        """Called when an agent starts processing"""
        if self.disable:
            return
        logger.debug(f"[HOOK] Agent {agent.name} started (turn {self.last_turn})")

    async def on_llm_end(self, ctx, agent, output):
        """Called after each LLM call completes - log metrics here for accurate per-turn tracking"""
        if self.disable:
            return

        # Get usage from context
        assert hasattr(ctx, "usage"), "Context missing usage attribute"
        usage = ctx.usage

        # retrieve num tokens
        token_stats = get_tokens_context_and_dollar_info(
            usage, self.model, last_entry_only=True, log=False
        )

        assert token_stats["num_llm_request"] == 1, (
            "Expected single LLM request for last entry"
        )
        logger.info(
            f"[HOOK] LLM ended: Turn {self.last_turn} - Input tokens: {token_stats['input_tokens']}, Output tokens: {token_stats['output_tokens']}, Cost: ${token_stats['cost']:0.6f}, Context window usage: {token_stats['context_window_usage'] * 100:.1f}%"
        )

        # Build wandb metrics
        wandb_metrics = {
            "type": "llm_call",
            "prompt_idx": self.prompt_idx,
            "agent_name": agent.name,
            "cost_usd": token_stats["cost"],
            "input_tokens": token_stats["input_tokens"],
            "cached_tokens": token_stats["cached_tokens"],
            "output_tokens": token_stats["output_tokens"],
            "reasoning_tokens": token_stats["reasoning_tokens"],
            "context_window_usage": token_stats["context_window_usage"],
            "current_prompt": self.current_prompt,
            "current_prompt_descriptor": self.current_prompt_descriptor,
        }

        # reset current prompt
        self.current_prompt = None
        self.current_prompt_descriptor = None

        self.total_stats["input_tokens"] += token_stats["input_tokens"]
        self.total_stats["cached_tokens"] += token_stats["cached_tokens"]
        self.total_stats["output_tokens"] += token_stats["output_tokens"]
        self.total_stats["reasoning_tokens"] += token_stats["reasoning_tokens"]
        self.total_stats["cost_usd"] += token_stats["cost"]

        # total info to wandb
        wandb_metrics.update(
            {
                "total/input_tokens": self.total_stats["input_tokens"],
                "total/cached_tokens": self.total_stats["cached_tokens"],
                "total/output_tokens": self.total_stats["output_tokens"],
                "total/reasoning_tokens": self.total_stats["reasoning_tokens"],
                "total/cost_usd": self.total_stats["cost_usd"],
            }
        )

        # Log to wandb
        self.log_metrics_callback(wandb_metrics, log_and_increment=True)

    async def on_agent_end(self, ctx, agent, output):
        """Called when an agent finishes processing"""
        if self.disable:
            return
        logger.debug(f"[HOOK] Agent {agent.name} ended (turn {self.last_turn})")

    async def on_tool_start(
        self,
        context: RunContextWrapper[TContext],
        agent,
        tool: Tool,
    ):
        """Called when a tool starts executing"""
        if self.disable:
            return
        tool_name = tool.name if hasattr(tool, "name") else str(tool)
        logger.debug(
            f"[HOOK] Agent {agent.name} starting tool: {tool_name} (turn {self.last_turn})"
        )

        if tool_name == "apply_patch":
            self.apply_patch_added_ctr = 0
            self.apply_patch_deleted_ctr = 0

    async def on_tool_end(
        self,
        context: RunContextWrapper[TContext],
        agent,
        tool: Tool,
        result: str,
    ):
        """Called when a tool finishes - track tool usage"""
        if self.disable:
            return

        # stats logging happens inside tools with callback

        tool_name = tool.name if hasattr(tool, "name") else str(tool)
        self.current_turn_tools[tool_name] = (
            self.current_turn_tools.get(tool_name, 0) + 1
        )

        if tool_name == "apply_patch":
            self.log_metrics_callback(
                {
                    "type": "apply_patch_tool",
                    "apply_patch/added_loc_count": self.apply_patch_added_ctr,
                    "apply_patch/deleted_loc_count": self.apply_patch_deleted_ctr,
                },
                log_and_increment=True,
            )

    async def on_handoff(self, ctx, from_agent, to_agent):
        """Called when control is handed off between agents"""
        if self.disable:
            return

        raise Exception(
            "handoff loging not implemented yet! Log to wandb and co please"
        )

        logger.info(
            f"[HOOK] Handoff from {from_agent.name} to {to_agent.name} (turn {self.last_turn})"
        )
        wandb.log(
            {
                "handoff/from": from_agent.name,
                "handoff/to": to_agent.name,
                "type": "handoff",
            },
            step=self.last_turn,
            commit=False,
        )
